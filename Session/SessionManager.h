#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#include "ISessionEvent.h"
#include "SessionDefs.h"

#include "../Buffer/SessionBufferConfig.h"
#include <memory> // for std::unique_ptr

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
	std::unique_ptr<ClientSessionPool> m_clientSessionPool;

	std::unique_ptr<AcceptSessionPool> m_acceptSessionPool;

public:
	bool Initialize(const uint32_t acceptSessionCount, 
		const uint32_t clientSessionCount, 
		TlsMemoryPool* jobMemoryPool, 
		TlsMemoryPool* packetMemoryPool,
		CloseSocketFunc closeSocketFunc, 
		const SessionBufferConfig& bufferConfig,
		SessionDisconnectNotifyFunc disconnectNotifyFunc = nullptr, 
		void* disconnectNotifyContext = nullptr);

	void Finalize();

	// Accept Session //
public:
	AcceptSession* GetAcceptSession(const uint32_t sessionId);
	uint32_t GetAcceptSessionCount() const noexcept;

	void CancelAllAcceptIORequests();
	IOWaitResult WaitForAllAcceptIOComplete(const uint32_t timeout_ms);

	// Client Session //
public:
	uint32_t GetClientSessionInUseCount() const;
	uint32_t CountClientSessionsFromAddress(const char* ipAddress) const;

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
	void ReleaseClientSession(ClientSession* session);

	void RequestAllRecvSendIOCancel();
	IOWaitResult WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms);
	void DisconnectAllSessions();
	uint32_t SendHeartbeatRequests();

	uint32_t DisconnectZombieSessions(uint64_t heartbeatTimeout_ms,
		uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms);


private:
	void OnDisconnectRequest(ISession* session) override;
};
