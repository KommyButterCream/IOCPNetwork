#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h> // for uint32_t, uint64_t
#include "ClientSession.h"
#include <memory> // for std::unique_ptr

#include "../Buffer/SessionBufferConfig.h"

class TlsMemoryPool;

class ClientSessionPool
{
public:
	// 풀이 소켓을 닫고 서비스에 종료를 통지하는 진입점이다.
	struct Callbacks
	{
		CloseSocketFunc closeSocket;
		SessionDisconnectNotifyFunc notifyDisconnect;
		void* disconnectContext;
	};

	// 모든 슬롯 초기화가 성공한 풀만 반환한다.
	[[nodiscard]] static std::unique_ptr<ClientSessionPool> Create(uint32_t capacity,
		TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig,
		const Callbacks& callbacks);
	~ClientSessionPool();

	// 대여 중인 세션은 마지막 참조가 반환된 후 다시 대여한다.
	ClientSession* Acquire();

	// 풀 용량과 사용 중인 슬롯을 조회한다.
	uint32_t GetSessionCount() const;
	uint32_t GetInUseCount() const;
	uint32_t CountSessionsFromAddress(const char* ipAddress) const;

	// 종료 시 발행을 중단하고 완료를 기다린 뒤 남은 연결을 정리한다.
	void RequestAllRecvSendIOCancel();
	IOWaitResult WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();

	// 순회 중인 세션의 수명을 보호하며 하트비트와 만료 검사를 수행한다.
	uint32_t SendHeartbeatRequests();
	uint32_t DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms,
		uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms);

	// 전체 세션의 큐 최고 수위, 수신 정지 횟수, 미완료 I/O를 집계한다.
	uint32_t GetPeakJobQueueDepth() const;
	uint32_t GetTotalRecvPauseCount() const;
	uint32_t GetOutstandingIOCount() const;

private:
	struct SessionNode;

	ClientSessionPool(std::unique_ptr<ClientSession[]> sessions, uint32_t capacity, const Callbacks& callbacks);

	[[nodiscard]] ClientSession::Reference TryAcquireSessionReference(uint32_t slotIndex) const;

	// 반환 권한을 선점하고 마지막 참조가 사라진 뒤 프리 리스트에 올린다.
	void Release(ClientSession* clientSession);
	bool TryBeginRelease(uint32_t sessionId);
	void CleanupDisconnectedSession(ClientSession& session);
	void CompleteRelease(ClientSession* clientSession);

	// 세션 소유자로서 종료 요청과 지연 반환을 처리한다.
	static void OnDisconnectRequest(void* context, ClientSession* session);
	static void OnReleaseReady(void* context, ClientSession* session);

	// 노드 상태와 프리 리스트는 풀의 락으로 함께 변경한다.
	std::unique_ptr<SessionNode[]> m_nodes;
	std::unique_ptr<ClientSession[]> m_sessions;
	SessionNode* m_freeList = nullptr;
	uint32_t m_capacity = 0;
	mutable SRWLOCK m_lock = SRWLOCK_INIT;
	const Callbacks m_callbacks;
};
