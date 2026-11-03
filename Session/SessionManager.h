#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "ISessionEvent.h"

#include "../Buffer/SessionBufferConfig.h"

class ISession;
class AcceptSession;
class ClientSession;
class ClientSessionPool;
class AcceptSessionPool;
class TlsMemoryPool;

class SessionManager : public ISessionEvent
{
public:
	SessionManager();
	~SessionManager();

private:
	SessionManager(const SessionManager& rhs) = delete;
	SessionManager& operator = (const SessionManager& rhs) = delete;
	SessionManager(SessionManager&& rhs) noexcept = delete;
	SessionManager& operator = (SessionManager&& rhs) noexcept = delete;

private:
	AcceptSessionPool* m_acceptSessionPool = nullptr;
	ClientSessionPool* m_clientSessionPool = nullptr;

	uint32_t m_acceptSessionCount = 0;
	uint32_t m_clientSessionCount = 0;

public:
	bool Initialize(const uint32_t acceptSessionCount, const uint32_t clientSessionCount, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, CloseSocketFunc closeSocketFunc, const SessionBufferConfig& bufferConfig);
	void Finalize();

	// Accept Session //
public:
	AcceptSession* GetAcceptSession(const uint32_t sessionId);
	uint32_t GetAcceptSessionCount() const noexcept;

	void RequestAllAcceptIOCancel();
	bool WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms);

	// Client Session //
public:
	bool IsClientSessionFull() const;
	uint32_t GetClientSessionInUseCount() const;
	uint32_t CountClientSessionsFromAddress(const char* ipAddress) const;

	ClientSession* GetClientSession(const uint32_t sessionId);
	uint32_t GetClientSessionCount() const noexcept;

	// Accept / Client 양쪽 세션이 들고 있는 미완료 I/O 총합.
	// 정상 종료 후에는 0 이어야 한다.
public:
	uint32_t GetOutstandingIOCount() const;

	// 가장 밀린 세션의 잡 큐 최고 수위.
	uint32_t GetPeakJobQueueDepth() const;

	// 백프레셔가 걸린 총 횟수 (전 세션 합).
	uint32_t GetTotalRecvPauseCount() const;

	ClientSession* AcquireClientSession();
	void ReleaseClientSession(ISession* session);

	// 세션 종료를 서비스에 알릴 진입점을 클라이언트 세션 풀에 건다.
	// 통지를 부르는 자리를 풀 하나로 모으기 위한 배선이다.
	void SetSessionDisconnectNotifyFunc(SessionDisconnectNotifyFunc notifyFunc, void* context);

	void RequestAllRecvSendIOCancel();
	bool WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();
	uint32_t SendHeartbeatRequests();

	uint32_t DisconnectZombieSessions(uint64_t heartbeatTimeout_ms,
		uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms);

	void OnDisconnectRequest(ISession* session) override;
};
