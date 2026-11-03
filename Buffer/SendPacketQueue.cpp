#include "SendPacketQueue.h"

#include "../Protocol/PacketHeader.h"
#include "../../Core/Sync/SRWLockGuard.h"

#include <new> // for std::nothrow
#include <utility> // for std::move

SendPacketQueue::~SendPacketQueue()
{
	Finalize();
}

bool SendPacketQueue::Initialize(uint32_t maxCount)
{
	if (maxCount == 0)
	{
		return false;
	}

	// 새 배열 확보에 실패하면 기존 큐는 그대로 유지합니다.
	auto packets = std::unique_ptr<SendPacket[]>(new (std::nothrow) SendPacket[maxCount]);
	if (!packets)
	{
		return false;
	}

	Finalize();

	m_packets = std::move(packets);
	m_maxCount = maxCount;

	return true;
}

void SendPacketQueue::Finalize()
{
	// 생산자와 소비자가 중지된 상태에서 호출합니다.
	m_packets.reset();
	m_head = m_tail = m_count = m_maxCount = 0;
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
	Core::Sync::SRWWriteLockGuard lock(m_srwLock);

	if (m_count == 0)
	{
		return {};
	}

	SendPacket packet = std::move(m_packets[m_head]);

	if (++m_head == m_maxCount)
	{
		m_head = 0;
	}

	--m_count;

	return packet;
}

void SendPacketQueue::Reset()
{
	// 세션 정리 중 큐를 비웁니다. 배열은 세션 재사용을 위해 유지합니다.
	// 목록 분리가 불가능하므로 패킷 반환까지 락으로 보호합니다.
	Core::Sync::SRWWriteLockGuard lock(m_srwLock);
	while (m_count != 0)
	{
		m_packets[m_head].Reset();

		if (++m_head == m_maxCount)
		{
			m_head = 0;
		}
		--m_count;
	}

	m_head = m_tail = 0;
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

template<typename Handle>
bool SendPacketQueue::EnqueueHandle(Handle&& packetHandle)
{
	if (!packetHandle)
	{
		return false;
	}

	const auto bytes = packetHandle.View();

	if (bytes.empty() || bytes.size() > PACKET_SIZE_LIMIT)
	{
		return false;
	}

	Core::Sync::SRWWriteLockGuard lock(m_srwLock);

	if (m_count >= m_maxCount)
	{
		return false;
	}

	// 빈 슬롯을 확인한 뒤에만 소유권을 이동합니다.
	m_packets[m_tail].SetPacket(std::move(packetHandle));

	if (++m_tail == m_maxCount)
	{
		m_tail = 0;
	}

	++m_count;

	return true;
}
