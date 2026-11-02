#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "PreDefine.h"
#include "SendPacketEntry.h"

class TlsMemoryPool;

// 세션 하나의 송신 대기열.
//
// 구조
//   엔트리를 TlsMemoryPool 에서 받아 next 로 엮은 단방향 리스트다.
//   head 에서 꺼내고 tail 에 붙이므로 FIFO 이고, 깊이는 개수 상한으로만
//   제한한다.
//
//   예전에는 고정 크기 링 버퍼였다. 포인터 배열을 기동 시점에 전부 잡고
//   (깊이 x 8바이트, 4096 이면 세션당 32KB) 위치를 마스크로 계산했으므로
//   깊이가 2의 거듭제곱이어야 했다. 엔트리도 전용 고정 풀에서 나왔다.
//   그 제약과 배열이 함께 사라졌다 — 자세한 사정은 SendPacketEntry.h 주석.
//
// 소유권
//   Enqueue 가 성공하면 패킷 메모리의 소유권이 큐로 넘어온다. Dequeue 로
//   꺼낸 쪽은 엔트리 하나를 잠시 빌린 것이고, 다 쓰면 ReleaseEntry 로
//   되돌려야 한다. 그러면 패킷과 엔트리가 함께 반납된다.
//   (반납 경로를 여기 하나로 모은 이유는 ReleaseEntry 주석 참고)
//
// 동시성
//   생산자는 여럿, 소비자는 하나다(ClientSession 의 m_sending 토큰이
//   보장한다). 그래도 head/tail 조작은 SRWLOCK 으로 묶는다 — 실측에서
//   큐 연산 왕복이 40ns 이고 패킷 하나의 비용은 마이크로초 단위라,
//   락프리로 바꿔 얻을 수 있는 몫이 1% 미만이다.
//
// 이 타입은 DLL 밖으로 내보낸다.
//
// 부품 벤치(tools/sendbench)가 큐를 직접 만들어 두들겨야 하기 때문이다.
// 엔진 전체 처리량으로는 "큐가 병목인지" 를 판단할 수 없다 —
// SessionJobQueue 가 같은 이유로 이미 익스포트되어 있다.
//
// ClientSession::GetSendPacketQueue() 는 이미 익스포트되어 SendPacketQueue*
// 를 돌려주고 있었다. 밖에서 포인터는 받을 수 있는데 그 위의 멤버는 부를 수
// 없는 상태였고, 이 선언이 그 불일치도 함께 없앤다.
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API SendPacketQueue
{
public:
	SendPacketQueue();
	~SendPacketQueue();

	// maxCount 는 이 큐에 동시에 담을 수 있는 패킷 수의 상한이다.
	// 2의 거듭제곱이 아니어도 된다.
	bool Initialize(TlsMemoryPool* sendQueueMemoryPool, TlsMemoryPool* packetMemoryPool, uint32_t maxCount);
	void Finalize();

	bool Enqueue(void** packetData, uint32_t packetSize);
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
	uint32_t GetMaxCount() const { return m_maxCount; }

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
	TlsMemoryPool* m_packetMemoryPool = nullptr;
};
