#include "RecvPacketBuffer.h"

#include "PreDefine.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;

#include "../Memory/EngineMemoryPool.h"
#include "../Memory/EngineMemoryPoolHelper.h"

#include <memory.h>
#include <assert.h> // for assert
#include <malloc.h> // for _aligned_malloc, _aligned_free

RecvPacketBuffer::RecvPacketBuffer()
{
}

bool RecvPacketBuffer::Initialize(EngineMemoryPool* packetMemoryPool, uint32_t ringSize, uint32_t maxPacketSize)
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

PacketReadResult RecvPacketBuffer::ReadPacket(char*& outBuffer, uint32_t& outSize, uint16_t& outPacketId)
{
	outSize = 0;
	outPacketId = 0;
	outBuffer = nullptr;

	PACKET_HEADER header = {};

	if (!PeekHeader(header))
		return PacketReadResult::NeedMoreData;

	// 피어가 규칙 밖의 크기를 적어 보냈다. 이걸 NeedMoreData 로 뭉개면 그
	// 세션은 영원히 이 헤더에 막힌 채 살아남으므로 위반으로 올려 보낸다.
	if (header.packetSize < sizeof(PACKET_HEADER) || header.packetSize > m_maxPacketSize)
		return PacketReadResult::Invalid;

	if (m_storedSize < header.packetSize)
		return PacketReadResult::NeedMoreData; // 아직 패킷 전체 수신 안됨

	if (!m_packetMemoryPool)
		return PacketReadResult::OutOfMemory;

	// 패킷 메모리 버퍼 풀로 부터
	// RecvPacketBuffer 의 m_buffer 에 저장된 패킷 데이터를 카피한다.

	char* packetMemory = reinterpret_cast<char*>(MEMORY_POOL::CreatePacket(*m_packetMemoryPool, header.packetSize));

	//char* packetMemory = reinterpret_cast<char*>(m_packetMemoryPool->Acquire(header.packetSize));
	if (!packetMemory)
	{
		// 패킷 풀에서 메모리를 못 얻었다. 구체적 원인은 EngineMemoryPool 이 남긴다.
		//
		// 위반이 아니라 오류로 남긴다.
		//
		// 예전에는 ENGINE_VIOLATION 이었다. 그때는 여기 오는 유일한 길이
		// "OS 가 메모리를 거부했다" 여서 예외적 상황이 맞았다. 이제는 풀에
		// 커밋 상한이 있고, 상한에 걸려 확장을 거부하는 것은 설계된 동작이다 —
		// 소비자가 생산자보다 느릴 때 프로세스가 죽는 대신 패킷을 버리게
		// 하려고 넣은 장치다.
		//
		// 그걸 위반으로 세면, 백프레셔가 제대로 동작하는 실행이 곧 "위반이
		// 쏟아진 실행" 이 되어 계수기의 의미가 사라진다. 실측: 상한을 낮게
		// 걸고 폭주시키자 위반 53건이 전부 이 경로였다.
		//
		// 호출부가 OutOfMemory 를 보고 세션을 정리하므로 정보는 이미 전달된다.
		LOGE("failed to acquire %u bytes for an incoming packet, dropping the session",
			header.packetSize);
		return PacketReadResult::OutOfMemory;
	}

	uint32_t remainFirst = m_capacity - m_readPos;
	if (header.packetSize <= remainFirst) [[likely]]
	{
		memcpy(packetMemory, m_buffer + m_readPos, header.packetSize);
	}
	else [[unlikely]]
	{
		// payload split
		memcpy(packetMemory, m_buffer + m_readPos, remainFirst);
		memcpy(packetMemory + remainFirst, m_buffer, header.packetSize - remainFirst);
	}

	outBuffer = packetMemory;
	outSize = header.packetSize;
	outPacketId = header.packetId;

	m_readPos = (m_readPos + header.packetSize) & m_capacityMask;
	m_storedSize -= header.packetSize;

	return PacketReadResult::Ok;
}

