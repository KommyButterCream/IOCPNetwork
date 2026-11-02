#include "RecvPacketBuffer.h"

#include "PreDefine.h"
#include "PacketHandle.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;

#include "../Memory/TlsMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include <memory.h>
#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free
#include <cstring> // for std::memcpy
#include <utility> // for std::move

RecvPacketBuffer::RecvPacketBuffer()
{
}

bool RecvPacketBuffer::Initialize(TlsMemoryPool* packetMemoryPool, uint32_t ringSize, uint32_t maxPacketSize)
{
	if (packetMemoryPool == nullptr)
	{
		return false;
	}

	// 예전에는 static_assert 였다. 크기가 런타임 값이 되었으므로 여기서 막는다.
	if (ringSize == 0 || (ringSize & (ringSize - 1)) != 0)
	{
		ENGINE_VIOLATION("recv ring size %u is not a power of two", ringSize);
		return false;
	}

	if (maxPacketSize < sizeof(PACKET_HEADER) || maxPacketSize > PACKET_SIZE_LIMIT)
	{
		ENGINE_VIOLATION("max recv packet size %u is outside [%zu, %u]",
			maxPacketSize, sizeof(PACKET_HEADER), PACKET_SIZE_LIMIT);
		return false;
	}

	// PrepareWrite 가 조각을 앞으로 당긴 뒤에도 최대 패킷 하나가 통째로
	// 들어가야 한다. 링이 최대 패킷의 2배보다 작으면 그 보장이 깨진다.
	if (ringSize < maxPacketSize * 2)
	{
		ENGINE_VIOLATION("recv ring %u cannot hold a fragment plus a %u byte packet", ringSize, maxPacketSize);
		return false;
	}

	Finalize();

	m_packetMemoryPool = packetMemoryPool;
	m_capacity = ringSize;
	m_capacityMask = ringSize - 1;
	m_maxPacketSize = maxPacketSize;

	m_buffer = static_cast<char*>(_aligned_malloc(m_capacity, 64));
	if (!m_buffer)
	{
		Finalize();
		return false;
	}

	Reset();
	return true;
}

RecvPacketBuffer::~RecvPacketBuffer()
{
	Finalize();
}

void RecvPacketBuffer::Finalize()
{
	m_packetMemoryPool = nullptr;

	m_writePos = 0;
	m_readPos = 0;
	m_storedSize = 0;

	m_capacity = 0;
	m_capacityMask = 0;
	m_maxPacketSize = 0;

	if (m_buffer)
	{
		_aligned_free(m_buffer);
		m_buffer = nullptr;
	}
}

void RecvPacketBuffer::Reset()
{
	m_writePos = 0;
	m_readPos = 0;
	m_storedSize = 0;
}

char* RecvPacketBuffer::GetWriteablePtr()
{
	if (!m_buffer)
	{
		return nullptr;
	}

	return m_buffer + m_writePos;
}

uint32_t RecvPacketBuffer::GetWriteableSize() const
{
	if (!m_buffer)
		return 0;

	if (IsFull())
		return 0;

	if (m_writePos >= m_readPos)
	{
		const uint32_t spaceToEnd = m_capacity - m_writePos;
		const uint32_t totalSpace = m_capacity - m_storedSize;

		return spaceToEnd < totalSpace ? spaceToEnd : totalSpace;
	}
	else
		return m_readPos - m_writePos;
}

bool RecvPacketBuffer::CommitWrite(const uint32_t bytesReceived)
{
	if (!m_buffer || bytesReceived == 0)
		return false;

	const uint32_t writeableSize = GetWriteableSize();
	if (bytesReceived > writeableSize)
	{
		// 버퍼 overflow. GetWriteableSize 가 준 크기보다 많이 받았다는 뜻이므로
		// WSARecv 에 넘긴 버퍼 길이와 커밋 크기가 어긋난 것이다.
		ENGINE_VIOLATION("recv ring overflow : committing %u bytes but only %u writeable (stored %u)",
			bytesReceived, writeableSize, m_storedSize);
		return false;
	}

	m_writePos = (m_writePos + bytesReceived) & m_capacityMask;
	m_storedSize += bytesReceived;

	return true;
}

bool RecvPacketBuffer::PeekHeader(PACKET_HEADER& header)
{
	if (!m_buffer || IsEmpty())
		return false;

	if (m_storedSize < sizeof(PACKET_HEADER))
		return false;

	uint32_t remainFirst = m_capacity - m_readPos;
	if (remainFirst >= sizeof(PACKET_HEADER)) [[likely]]
	{
		memcpy(&header, m_buffer + m_readPos, sizeof(PACKET_HEADER));
	}
	else [[unlikely]]
	{
		// 헤더 split
		memcpy(&header, m_buffer + m_readPos, remainFirst);
		memcpy(reinterpret_cast<char*>(&header) + remainFirst, m_buffer, sizeof(PACKET_HEADER) - remainFirst);
	}

	return true;
}

void RecvPacketBuffer::PrepareWrite()
{
	if (!m_buffer)
		return;

	// 다 비었으면 원점으로 되돌린다. 제어 메시지처럼 받는 족족 소비되는
	// 트래픽은 거의 항상 여기로 떨어져서, 링이 사실상 앞부분만 쓴다.
	if (m_storedSize == 0)
	{
		m_readPos = 0;
		m_writePos = 0;
		return;
	}

	// 조각이 뒤로 감겨 있으면 앞으로 당길 수 없다. 이때 쓰기 공간은 readPos
	// 앞의 틈이라 어차피 이미 연속이다.
	//
	// readPos == writePos 인데 비어 있지 않은 "가득 참" 도 여기서 걸러진다.
	// 그 경우를 Compact 로 넘기면 링 끝을 넘어 읽는다.
	if (m_readPos + m_storedSize > m_capacity)
		return;

	// 끝까지 최대 패킷이 들어갈 만큼 남았으면 건드리지 않는다.
	if (m_capacity - m_writePos >= m_maxPacketSize)
		return;

	Compact();
}

void RecvPacketBuffer::Compact()
{
	// 여기 오는 조각은 연속이고 최대 패킷보다 작다. 완성된 패킷은 이미 다
	// 꺼내갔고 남은 건 헤더가 주장한 크기에 못 미치는 꼬리뿐이기 때문이다.
	// 그래서 복사량이 최대 패킷을 넘지 않고, 링 끝에 닿았을 때만 일어난다.
	memmove(m_buffer, m_buffer + m_readPos, m_storedSize);

	m_readPos = 0;
	m_writePos = m_storedSize;
}

PacketReadResult RecvPacketBuffer::ReadPacket(PacketHandle& packetHandle, uint16_t& packetId)
{
	packetHandle.Reset();
	packetId = ToInteger(PACKET_ID::NONE);

	PACKET_HEADER header = {};

	if (!PeekHeader(header))
	{
		return PacketReadResult::NeedMoreData;
	}

	if (header.packetSize < sizeof(PACKET_HEADER) || header.packetSize > m_maxPacketSize)
	{
		return PacketReadResult::Invalid;
	}

	if (m_storedSize < header.packetSize)
	{
		return PacketReadResult::NeedMoreData;
	}

	if (!m_packetMemoryPool)
	{
		return PacketReadResult::OutOfMemory;
	}

	auto packet = MEMORY_POOL::CreatePacketHandle(*m_packetMemoryPool, header.packetSize);

	if (!packet)
	{
		LOGE("failed to acquire %u bytes for an incoming packet, "
			"dropping the session",
			header.packetSize);

		return PacketReadResult::OutOfMemory;
	}

	auto* destination = packet.MutableView().data();
	const uint32_t remainFirst = m_capacity - m_readPos;

	if (header.packetSize <= remainFirst) [[likely]]
	{
		std::memcpy(destination, m_buffer + m_readPos, header.packetSize);
	}
	else [[unlikely]]
	{
		std::memcpy(destination, m_buffer + m_readPos, remainFirst);

		std::memcpy(destination + remainFirst, m_buffer, header.packetSize - remainFirst);
	}

	m_readPos = (m_readPos + header.packetSize) & m_capacityMask;
	m_storedSize -= header.packetSize;

	packetId = header.packetId;
	packetHandle = std::move(packet);

	return PacketReadResult::Ok;
}