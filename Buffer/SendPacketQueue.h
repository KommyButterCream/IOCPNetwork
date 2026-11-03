#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"
#include "SendPacketEntry.h"

class TlsMemoryPool;

// 엔진 내부 FIFO 송신 큐입니다. 엔트리는 패킷 소유권 또는 반환 콜백을 보관합니다.
// 여러 생산자가 동시에 등록할 수 있으며, ClientSession이 소비를 직렬화합니다.
class SendPacketQueue
{
public:
	SendPacketQueue();
	~SendPacketQueue();

	// maxCount 는 이 큐에 동시에 담을 수 있는 패킷 수의 상한이다.
	// 2의 거듭제곱이 아니어도 된다.
	// Packet ownership stays with each handle or shared release callback.
	bool Initialize(TlsMemoryPool* sendQueueMemoryPool, uint32_t maxCount);
	void Finalize();

	[[nodiscard]]
	bool Enqueue(PacketHandle&& packetHandle);
	bool EnqueueShared(const void* packetData, uint32_t packetSize, SendPacketReleaseFunc releaseFunc, void* releaseContext);
	bool Dequeue(SendPacketEntry*& outEntry);

	// Dequeue 로 꺼낸 엔트리를 패킷과 함께 반납한다.
	//
	// 예전에는 꺼낸 쪽이 "패킷 반납" 과 "서술자 반납" 을 각각 불러야 했고,
	// 그 두 줄이 ClientSession 안에 네 군데(OnSendCompleted, PostCurrentSend,
	// HandleSocketError, ResetSession/Finalize) 복제되어 있었다. 한쪽만 빠진
	// 사본이 곧 누수였다. 큐가 준 것은 큐가 회수한다.
	void ReleaseEntry(SendPacketEntry* entry);

	void Reset();

	// 락을 획득해서 확인한다. 락을 보유하지 않은 외부 경로에서 사용한다.
	bool IsEmpty() const;

	// 지금 담겨 있는 개수 / 설정된 상한. 진단과 부품 벤치용이다.
	uint32_t GetCount() const;
	uint32_t GetMaxCount() const
	{ 
		return m_maxCount;
	}

private:
	// 성공하면 엔트리 소유권이 큐로 넘어간다. 상한에 걸리면 false 다.
	bool PushBack(SendPacketEntry* entry);

private:
	mutable SRWLOCK m_srwLock = SRWLOCK_INIT;

	SendPacketEntry* m_head = nullptr;
	SendPacketEntry* m_tail = nullptr;
	uint32_t m_count = 0;

	// 예전에는 컴파일 타임 상수였다. 역할마다 적정 깊이가 달라 런타임으로 옮겼다.
	uint32_t m_maxCount = 0;

	TlsMemoryPool* m_sendQueueMemoryPool = nullptr;
};
