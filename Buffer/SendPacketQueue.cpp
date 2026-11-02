#include "SendPacketQueue.h"

#include "../Diagnostics/EngineAssert.h"

#include "../Memory/EngineMemoryPoolHelper.h"

#include <utility> // for std::move

SendPacketQueue::SendPacketQueue()
{
}

bool SendPacketQueue::Initialize(TlsMemoryPool* sendQueueMemoryPool, uint32_t maxCount)
{
	if (!sendQueueMemoryPool)
	{
		return false;
	}

	if (maxCount == 0)
	{
		ENGINE_VIOLATION("send queue max count is zero");
		return false;
	}

	Finalize();

	m_sendQueueMemoryPool = sendQueueMemoryPool;
	m_maxCount = maxCount;

	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;

	return true;
}

SendPacketQueue::~SendPacketQueue()
{
	Finalize();
}

void SendPacketQueue::Finalize()
{
	// Return queued packets and entries before clearing the entry-pool reference.
	Reset();

	m_sendQueueMemoryPool = nullptr;
	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;
	m_maxCount = 0;
}

bool SendPacketQueue::Enqueue(PacketHandle&& packetHandle)
{
	if (!m_sendQueueMemoryPool)
	{
		return false;
	}

	if (!packetHandle ||
		packetHandle.Size() == 0 ||
		packetHandle.Size() > PACKET_SIZE_LIMIT)
	{
		return false;
	}

	SendPacketEntry* entry = MEMORY_POOL::CreateSendPacketEntry(*m_sendQueueMemoryPool);

	if (!entry)
	{
		return false;
	}

	entry->packetSize = static_cast<uint32_t>(packetHandle.Size());

	// Transfer ownership before publishing the entry to the queue.
	entry->packetHandle = std::move(packetHandle);
	entry->packetData = reinterpret_cast<const char*>(entry->packetHandle.View().data());

	if (!PushBack(entry))
	{
		// 큐에 들어가지 않았으므로 호출자에게 소유권을 복구한다.

		packetHandle = std::move(entry->packetHandle);

		entry->Reset();

		MEMORY_POOL::ReleaseSendPacketEntry(*m_sendQueueMemoryPool, entry);

		return false;
	}

	// 여기서부터는 소비자가 엔트리를 꺼내서 반환했을 수도 있다.
	// entry 에 다시 접근하면 안된다.
	return true;
}

bool SendPacketQueue::EnqueueShared(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext)
{
	if (!m_sendQueueMemoryPool)
		return false;

	if (!packetData || packetSize == 0 || packetSize > PACKET_SIZE_LIMIT || !releaseFunc)
		return false;

	SendPacketEntry* entry = MEMORY_POOL::CreateSendPacketEntry(*m_sendQueueMemoryPool);
	if (!entry)
		return false;

	entry->packetData = static_cast<const char*>(packetData);
	entry->packetSize = packetSize;
	entry->releaseFunc = releaseFunc;
	entry->releaseContext = releaseContext;

	if (!PushBack(entry))
	{
		MEMORY_POOL::ReleaseSendPacketEntry(*m_sendQueueMemoryPool, entry);
		return false;
	}

	return true;
}

bool SendPacketQueue::PushBack(SendPacketEntry* entry)
{
	entry->next = nullptr;

	::AcquireSRWLockExclusive(&m_srwLock);

	if (m_count >= m_maxCount)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);
		return false;
	}

	if (m_tail)
		m_tail->next = entry;
	else
		m_head = entry;

	m_tail = entry;
	++m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	return true;
}

bool SendPacketQueue::Dequeue(SendPacketEntry*& outEntry)
{
	outEntry = nullptr;

	::AcquireSRWLockExclusive(&m_srwLock);

	SendPacketEntry* entry = m_head;
	if (!entry)
	{
		::ReleaseSRWLockExclusive(&m_srwLock);
		return false;
	}

	m_head = entry->next;
	if (!m_head)
		m_tail = nullptr;

	--m_count;

	::ReleaseSRWLockExclusive(&m_srwLock);

	// 꺼낸 엔트리는 더 이상 리스트의 일부가 아니다. 링크를 남겨 두면
	// 반납 루프가 이미 남의 것이 된 뒤를 따라간다.
	entry->next = nullptr;
	outEntry = entry;

	return true;
}

void SendPacketQueue::ReleaseEntry(SendPacketEntry* entry)
{
	if (!entry)
		return;

	if (entry->packetHandle)
	{
		entry->packetHandle.Reset();
	}
	else if (entry->packetData)
	{
		if (entry->releaseFunc)
		{
			entry->releaseFunc(entry->packetData, entry->releaseContext);
		}
		else
		{
			// No valid enqueue path produces an ownerless packet.
			// Do not guess which pool owns the memory.
			ENGINE_VIOLATION("send queue entry %p has packet data without an owner",
				static_cast<const void*>(entry));
		}
	}

	entry->Reset();

	if (m_sendQueueMemoryPool)
	{
		MEMORY_POOL::ReleaseSendPacketEntry(*m_sendQueueMemoryPool, entry);
	}
	else
	{
		ENGINE_VIOLATION("send queue entry %p cannot be returned : the entry pool is gone",
			static_cast<const void*>(entry));
	}
}

void SendPacketQueue::Reset()
{
	::AcquireSRWLockExclusive(&m_srwLock);

	// 리스트 전체를 한 번에 떼어낸다.
	SendPacketEntry* chain = m_head;
	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;

	::ReleaseSRWLockExclusive(&m_srwLock);

	// 반납은 락을 놓고 한다.
	//
	// 패킷 반납 경로는 로그를 남길 수 있어서, 락을 쥔 채로 돌면
	// "큐 락 -> 로거 락" 순서가 생긴다. Enqueue 가 엔트리 획득을 락 밖으로
	// 뺀 것과 같은 이유다. 이미 떼어낸 리스트이므로 남이 볼 수 없다.
	//
	// 예전에는 용량 전체를 훑었다. depth 4096 인 세션을 정리할 때마다
	// 4096회를 돌았고, disconnect storm 에서는 세션 수만큼 곱해졌다.
	// 지금은 담긴 개수만큼만 돈다.
	while (chain)
	{
		SendPacketEntry* next = chain->next;
		ReleaseEntry(chain);
		chain = next;
	}
}

bool SendPacketQueue::IsEmpty() const
{
	// m_count 는 exclusive 락 아래에서만 변경되므로 shared 락으로 충분하다.
	::AcquireSRWLockShared(&m_srwLock);
	const bool empty = (m_count == 0);
	::ReleaseSRWLockShared(&m_srwLock);

	return empty;
}

uint32_t SendPacketQueue::GetCount() const
{
	::AcquireSRWLockShared(&m_srwLock);
	const uint32_t count = m_count;
	::ReleaseSRWLockShared(&m_srwLock);

	return count;
}
