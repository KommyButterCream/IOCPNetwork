#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>
#include "ISessionEvent.h"
#include "SessionDefs.h"
#include <memory> // for std::unique_ptr

#include "../Buffer/SessionBufferConfig.h"


class ISession;
class BaseSession;
class ClientSession;
class TlsMemoryPool;

class ClientSessionPool
{
public:
	struct Callbacks
	{
		ISessionEvent& eventHandler;
		CloseSocketFunc closeSocket;
		SessionDisconnectNotifyFunc notifyDisconnect;
		void* disconnectContext;
	};

private:
	struct SessionNode;

	std::unique_ptr<SessionNode[]> m_nodes;
	std::unique_ptr<ClientSession[]> m_sessions;
	SessionNode* m_freeList = nullptr;		// 프리 리스트 헤드
	uint32_t m_capacity = 0;
	mutable SRWLOCK m_lock = SRWLOCK_INIT;
	const Callbacks m_callbacks;

	ClientSessionPool(std::unique_ptr<ClientSession[]> sessions, uint32_t capacity, const Callbacks& callbacks);

public:
	[[nodiscard]] static std::unique_ptr<ClientSessionPool> Create(uint32_t capacity,
		TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig,
		const Callbacks& callbacks);
	~ClientSessionPool();

	// 세션 획득
	//
	// 이 풀은 ClientSession 만 담는다. 기반 타입으로 돌려주면 호출부가
	// 곧바로 구체 타입으로 되돌려야 하고, 그 자리에 dynamic_cast 가 붙었다.
	ClientSession* Acquire();

	// 세션 반환
	//
	// 기다리지 않는다. 아직 완료되지 않은 I/O 가 있으면 반납을 예약만
	// 하고 즉시 돌아가며, 그 마지막 완료가 CompleteRelease 를 수행한다.
	// 그래서 완료 핸들러 안에서 불러도 안전하다.
	void Release(ClientSession* clientSession);

	// 세션 수량 반환
	uint32_t GetSessionCount() const;

	// 지금 임대되어 있는 세션 수.
	// 노드 상태를 훑어서 센다. 별도 카운터를 두면 예외 경로에서 어긋날 수
	// 있고, 이 함수는 접속/인증 때만 불리므로 O(capacity) 로 충분하다.
	uint32_t GetInUseCount() const;

	// 같은 원격 주소에서 온 접속 수. 접속 폭주 방어용.
	uint32_t CountSessionsFromAddress(const char* ipAddress) const;

	// 세션 아이디 기반 세션 객체 반환
	ClientSession* GetSession(const uint32_t sessionId);   // 범위 밖이면 nullptr

	// 세션 연결 해제
	void RequestAllRecvSendIOCancel();
	IOWaitResult WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();
	uint32_t SendHeartbeatRequests();
	// stalledPeerTimeout_ms 는 "우리 송신이 밀려 있는" 세션에만 적용하는 더 긴
	// 타임아웃이다. 왜 두 개를 두는지는 HeartbeatConfig 주석 참고.
	uint32_t DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms,
		uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms);

	// 지금 이 풀의 세션들이 들고 있는 미완료 I/O 총합.
	// 하네스가 시나리오 경계에서 회계를 검사한다. 정상 종료 후에는 0 이어야 한다.
	// 가장 밀린 세션의 잡 큐 최고 수위. 상한을 정하려면 먼저 재야 한다.
	uint32_t GetPeakJobQueueDepth() const;

	// 이 풀의 세션들에 백프레셔가 걸린 총 횟수.
	//
	// 최고 수위와 달리 합이다. "얼마나 깊었나" 가 아니라 "몇 번 개입했나"
	// 라서, 어느 세션에서 났는지는 중요하지 않다. 부하를 걸었는데 이 값이
	// 0 이면 백프레셔가 한 번도 동작하지 않은 것이다.
	uint32_t GetTotalRecvPauseCount() const;

	uint32_t GetOutstandingIOCount() const;

private:
	struct SessionReferenceReleaser
	{
		void operator()(ClientSession* session) const noexcept;
	};

	using SessionReference = std::unique_ptr<ClientSession, SessionReferenceReleaser>;

	[[nodiscard]] SessionReference TryAcquireSessionReference(uint32_t slotIndex) const;


	// 반납의 뒷부분. 소켓을 닫고 상태를 되돌린 뒤 프리 리스트에 올린다.
	// Release 가 그 자리에서 부르거나, 마지막 DecrementIO 가 부른다.
	void CompleteRelease(ClientSession* clientSession);

	// BaseSession 이 마지막 DecrementIO 에서 부르는 진입점.
	static void OnReleaseReady(void* context, BaseSession* session);

	// 접속을 알린 적 있는 세션에 한해 종료를 한 번만 알린다.
	// 반납 경로 전부가 이 함수를 지나야 한다.
	void NotifyServiceDisconnect(ClientSession* clientSession);
};
