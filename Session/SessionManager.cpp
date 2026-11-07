#include "SessionManager.h"

#include "AcceptSession.h"

#include "../../Core/Util/Logger.h"
#include <utility> // for std::move

using namespace Core::Util;
#include "AcceptSessionPool.h"
#include "ClientSessionPool.h"

SessionManager::SessionManager() = default;

SessionManager::~SessionManager() = default;

bool SessionManager::Initialize(
	const uint32_t acceptSessionCount, 
	const uint32_t clientSessionCount, 
	TlsMemoryPool* jobMemoryPool,
	TlsMemoryPool* packetMemoryPool,
	CloseSocketFunc closeSocketFunc, 
	const SessionBufferConfig& bufferConfig,
	SessionDisconnectNotifyFunc disconnectNotifyFunc,
	void* disconnectNotifyContext)
{
	// 실행 중인 풀을 교체하면 기존 세션을 사용하는 워커의 참조가 무효화된다.
	if (m_acceptSessionPool || m_clientSessionPool)
		return false;

	auto acceptPool = AcceptSessionPool::Create(acceptSessionCount, closeSocketFunc);
	if (!acceptPool)
		return false;

	auto clientPool = ClientSessionPool::Create(clientSessionCount, jobMemoryPool, packetMemoryPool, bufferConfig,
		{ .closeSocket = closeSocketFunc,
		  .notifyDisconnect = disconnectNotifyFunc, .disconnectContext = disconnectNotifyContext });
	if (!clientPool)
		return false;

	m_acceptSessionPool = std::move(acceptPool);
	m_clientSessionPool = std::move(clientPool);

	return true;
}

void SessionManager::Finalize()
{
	m_acceptSessionPool.reset();
	m_clientSessionPool.reset();
}

AcceptSession* SessionManager::GetAcceptSession(const uint32_t sessionId)
{
	return m_acceptSessionPool ? m_acceptSessionPool->GetSession(sessionId) : nullptr;
}

uint32_t SessionManager::GetAcceptSessionCount() const noexcept
{
	return m_acceptSessionPool ? m_acceptSessionPool->GetSessionCount() : 0;
}

void SessionManager::CancelAllAcceptIORequests()
{
	if (!m_acceptSessionPool)
		return;

	m_acceptSessionPool->CancelAllAcceptIORequests();
}

IOWaitResult SessionManager::WaitForAllAcceptIOComplete(const uint32_t timeout_ms)
{
	// 풀이 생성되기 전에 종료하면 기다릴 Accept I/O가 없다.
	if (!m_acceptSessionPool)
		return { IOWaitStatus::Completed };

	return m_acceptSessionPool->WaitForAllAcceptIOComplete(timeout_ms);
}

uint32_t SessionManager::GetClientSessionInUseCount() const
{
	if (!m_clientSessionPool)
		return 0;

	return m_clientSessionPool->GetInUseCount();
}

bool SessionManager::TryAdmitClientSession(ClientSession& session, uint32_t capacity)
{
	return m_clientSessionPool && m_clientSessionPool->TryAdmit(session, capacity);
}

uint32_t SessionManager::GetClientSessionCount() const noexcept
{
	return m_clientSessionPool ? m_clientSessionPool->GetSessionCount() : 0;
}

uint32_t SessionManager::GetPeakJobQueueDepth() const
{
	return m_clientSessionPool ? m_clientSessionPool->GetPeakJobQueueDepth() : 0;
}

uint32_t SessionManager::GetTotalRecvPauseCount() const
{
	return m_clientSessionPool ? m_clientSessionPool->GetTotalRecvPauseCount() : 0;
}

uint32_t SessionManager::GetOutstandingIOCount() const
{
	uint32_t total = 0;

	if (m_acceptSessionPool)
		total += m_acceptSessionPool->GetAcceptIOCount();

	if (m_clientSessionPool)
		total += m_clientSessionPool->GetOutstandingIOCount();

	return total;
}

ClientSession* SessionManager::AcquireClientSession(uint32_t remoteAddress, uint32_t addressLimit)
{
	if (!m_clientSessionPool)
		return nullptr;

	// 이전에는 실패 시 100회 busy retry 를 돌렸다. Acquire 가 nullptr 이면
	// 프리 리스트가 비었다는 뜻이고, 양보 없는 루프로는 채워질 수 없으므로
	// CPU 만 태우는 코드였다. 게다가 정확히 100번째에 성공하면
	// retryCount == maxRetryCount 가 참이 되어 성공했는데도 단정에 걸렸다.
	ClientSession* session = m_clientSessionPool->Acquire(remoteAddress, addressLimit);

	if (!session)
	{
		// 호출부(HandleAccept)가 연결을 거절하는 정상 경로다.
		LOGW("connection rejected by session capacity or per-address limit (pool capacity %u)",
			m_clientSessionPool->GetSessionCount());
	}

	return session;
}

void SessionManager::RequestAllRecvSendIOCancel()
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->RequestAllRecvSendIOCancel();
}

IOWaitResult SessionManager::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	// 풀이 생성되기 전에 종료하면 기다릴 송수신 I/O가 없다.
	if (!m_clientSessionPool)
		return { IOWaitStatus::Completed };

	return m_clientSessionPool->WaitForAllRecvSendIOCancelComplete(timeout_ms);
}

void SessionManager::DisconnectAllSessions()
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->DisconnectAllSessions();
}

uint32_t SessionManager::SendHeartbeatRequests()
{
	if (!m_clientSessionPool)
	{
		return 0;
	}

	return m_clientSessionPool->SendHeartbeatRequests();
}

uint32_t SessionManager::DisconnectZombieSessions(uint64_t heartbeatTimeout_ms,
	uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms)
{
	if (!m_clientSessionPool || heartbeatTimeout_ms == 0)
	{
		return 0;
	}

	return m_clientSessionPool->DisconnectZombieSessions(::GetTickCount64(), heartbeatTimeout_ms,
		stalledPeerTimeout_ms, releaseBudget_ms);
}
