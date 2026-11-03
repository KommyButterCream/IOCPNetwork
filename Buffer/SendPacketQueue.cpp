#include "SendPacketQueue.h"

#include "PreDefine.h"

#include "../Diagnostics/EngineAssert.h"
#include "../Memory/TlsMemoryPool.h"
#include "../../Core/Sync/SRWLockGuard.h"

#include <memory> // for std::construct_at, std::destroy_at
#include <utility> // for std::move

// 연결 정보와 노드 수명은 큐 내부에서만 관리합니다.
struct SendPacketQueue::Node
{
	Node* next = nullptr;
	SendPacket packet;
};

SendPacketQueue::~SendPacketQueue()
{
	Finalize();
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

void SendPacketQueue::Finalize()
{
	// 노드 풀 참조를 지우기 전에 대기 패킷과 노드를 모두 반환합니다.
	Reset();

	m_sendQueueMemoryPool = nullptr;
	m_head = nullptr;
	m_tail = nullptr;
	m_count = 0;
	m_maxCount = 0;
}

bool SendPacketQueue::Enqueue(PacketHandle&& packetHandle)
{
	return EnqueueHandle(std::move(packetHandle));
}

bool SendPacketQueue::EnqueueShared(SharedPacketHandle packetHandle)
{
	return EnqueueHandle(std::move(packetHandle));
}

SendPacket SendPacketQueue::Dequeue()
{
	Node* node = nullptr;
	{
		Core::Sync::SRWWriteLockGuard lock(m_srwLock);

		node = m_head;

		if (!node)
		{
			return {};
		}

		m_head = node->next;

		if (!m_head)
		{
			m_tail = nullptr;
		}

		--m_count;
	}

	// 분리한 노드는 이 함수만 소유합니다. 풀 반환은 큐 락 밖에서 수행합니다.
	SendPacket packet = std::move(node->packet);

	ReleaseNode(node);

	return packet;
}

void SendPacketQueue::Reset()
{
	Node* chain = nullptr;
	{
		Core::Sync::SRWWriteLockGuard lock(m_srwLock);

		chain = m_head;
		m_head = nullptr;
		m_tail = nullptr;
		m_count = 0;
	}

	// 목록 전체를 분리한 뒤 락 밖에서 패킷과 노드를 반환합니다.
	// 풀 반환과 로그 기록이 큐의 임계 구역을 늘리지 않도록 합니다.
	while (chain)
	{
		Node* next = chain->next;
		ReleaseNode(chain);
		chain = next;
	}
}

bool SendPacketQueue::IsEmpty() const
{
	Core::Sync::SRWReadLockGuard lock(m_srwLock);

	return m_count == 0;
}

uint32_t SendPacketQueue::GetCount() const
{
	Core::Sync::SRWReadLockGuard lock(m_srwLock);

	return m_count;
}

std::size_t SendPacketQueue::GetNodeSize() noexcept
{
	return sizeof(Node);
}

template<typename Handle>
bool SendPacketQueue::EnqueueHandle(Handle&& packetHandle)
{
	if (!m_sendQueueMemoryPool || !packetHandle)
	{
		return false;
	}

	const auto bytes = packetHandle.View();
	if (bytes.empty() || bytes.size() > PACKET_SIZE_LIMIT)
	{
		return false;
	}

	// 풀 할당은 큐 락을 잡기 전에 수행합니다.
	Node* node = CreateNode();
	if (!node)
	{
		return false;
	}

	{
		Core::Sync::SRWWriteLockGuard lock(m_srwLock);

		if (m_count < m_maxCount)
		{
			// 등록이 확정된 뒤에만 소유권을 이동합니다.
			node->packet = SendPacket(std::move(packetHandle));

			if (m_tail)
			{
				m_tail->next = node;
			}
			else
			{
				m_head = node;
			}

			m_tail = node;
			++m_count;

			// 반환 시 guard가 락을 해제합니다. 이후 노드에는 접근하지 않습니다.
			return true;
		}
	}

	// 큐가 가득 차면 핸들을 유지하고 빈 노드만 락 밖에서 반환합니다.
	ReleaseNode(node);

	return false;
}

SendPacketQueue::Node* SendPacketQueue::CreateNode()
{
	if (!m_sendQueueMemoryPool)
	{
		return nullptr;
	}

	void* memory = m_sendQueueMemoryPool->Acquire(sizeof(Node));

	if (!memory)
	{
		return nullptr;
	}

	LOGT("send node acquire %p", memory);

	// 풀에서 받은 저장 공간에 새 노드 객체를 생성합니다.
	// next와 packet은 기본 멤버 초기화에 따라 빈 상태가 됩니다.
	return std::construct_at(static_cast<Node*>(memory));
}

void SendPacketQueue::ReleaseNode(Node* node)
{
	if (!node)
	{
		return;
	}

	if (!m_sendQueueMemoryPool)
	{
		ENGINE_VIOLATION(
			"send queue node %p cannot be returned : "
			"the node pool is gone",
			static_cast<const void*>(node));

		// 풀 수명 순서가 잘못된 경로이므로 패킷 소유권만 정리합니다.
		node->packet.Reset();
		return;
	}

	LOGT("send node release %p", static_cast<void*>(node));

	// 패킷을 소멸시킨 뒤 노드 저장 공간을 반환합니다.
	std::destroy_at(node);
	m_sendQueueMemoryPool->Release(static_cast<const void*>(node));
}
