#pragma once

#include <stdint.h>

#include "PreDefine.h"

#include "../Protocol/PacketHeader.h"

#include "../Memory/EngineMemoryPoolFwd.h"

// 링에서 패킷 하나를 꺼낸 결과.
//
//   Ok           : outBuffer 에 패킷이 담겼다. 호출부가 풀에 반납할 책임을 진다
//   NeedMoreData : 아직 다 안 왔다. 다음 수신을 기다리면 된다
//   Invalid      : 헤더가 규칙을 어겼다. 피어 잘못이므로 세션을 끊어야 한다
//   OutOfMemory  : 패킷 풀이 고갈됐다. 우리 잘못이지만 역시 세션을 끊는다
//
// 예전에는 넷을 모두 false 로 뭉뚱그려서, 크기가 깨진 헤더가 "덜 왔다" 와
// 구분되지 않았다. 그래서 packetSize 를 크게 적어 보내면 그 세션은 영원히
// 파싱이 막힌 채로 살아남았다.
enum class PacketReadResult
{
	Ok,
	NeedMoreData,
	Invalid,
	OutOfMemory,
};

class RecvPacketBuffer
{
public:
	RecvPacketBuffer();
	~RecvPacketBuffer();

	RecvPacketBuffer(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer& operator=(const RecvPacketBuffer&) = delete;
	RecvPacketBuffer(RecvPacketBuffer&&) = delete;
	RecvPacketBuffer& operator=(RecvPacketBuffer&&) = delete;

private:
	EngineMemoryPool* m_packetMemoryPool = nullptr;

	char* m_buffer = nullptr;
	uint32_t m_writePos = 0;
	uint32_t m_readPos = 0;
	uint32_t m_storedSize = 0;

	// 예전에는 컴파일 타임 상수였다. 역할마다 적정값이 달라서 런타임으로
	// 옮겼다. 마스크는 매번 빼지 않으려고 미리 만들어 둔다.
	uint32_t m_capacity = 0;
	uint32_t m_capacityMask = 0;
	uint32_t m_maxPacketSize = 0;

public:
	// ringSize 는 2의 거듭제곱이어야 하고 maxPacketSize 의 2배 이상이어야
	// 한다. 어기면 false 를 반환한다. (예전에는 static_assert 였다)
	bool Initialize(EngineMemoryPool* packetMemoryPool, uint32_t ringSize, uint32_t maxPacketSize);
	void Finalize();
	void Reset();

	// WSARecv용
	char* GetWriteablePtr();
	uint32_t   GetWriteableSize() const;

	// 수신 완료 후 처리
	bool CommitWrite(const uint32_t bytesReceived);

	// 패킷 추출
	PacketReadResult ReadPacket(char*& outBuffer, uint32_t& outSize, uint16_t& outPacketId);

	// WSARecv 를 걸기 직전에 부른다. 링 끝에 붙어 있으면 앞으로 당겨서
	// 한 번에 최대 패킷을 받을 수 있는 연속 공간을 만든다.
	void PrepareWrite();

	// 상태 확인
	uint32_t GetStoredSize() const { return m_storedSize; }
	uint32_t GetCapacity() const { return m_capacity; }
	uint32_t GetMaxPacketSize() const { return m_maxPacketSize; }
	bool IsFull() const { return m_storedSize == m_capacity; }
	bool IsEmpty() const { return m_storedSize == 0; }

private:
	bool PeekHeader(PACKET_HEADER& header);

	// 남아 있는 조각을 오프셋 0 으로 모은다. 조각은 최대 패킷보다 작다.
	void Compact();
};

