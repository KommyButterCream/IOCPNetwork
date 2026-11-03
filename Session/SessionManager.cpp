#include "SessionManager.h"

#include "AcceptSession.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;
#include "AcceptSessionPool.h"
#include "ClientSession.h"
#include "ClientSessionPool.h"

SessionManager::SessionManager()
{
}

SessionManager::~SessionManager()
{
	Finalize();
}

bool SessionManager::Initialize(const uint32_t acceptSessionCount, const uint32_t clientSessionCount, TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, CloseSocketFunc closeSocketFunc, const SessionBufferConfig& bufferConfig)
{
	m_acceptSessionCount = acceptSessionCount;
	m_clientSessionCount = clientSessionCount;

	m_acceptSessionPool = new AcceptSessionPool(acceptSessionCount);
	if (!m_acceptSessionPool || !m_acceptSessionPool->IsReady())
		return false;

	m_acceptSessionPool->SetSocketCloseFunc(closeSocketFunc);

	m_clientSessionPool = new ClientSessionPool(clientSessionCount, jobMemoryPool, packetMemoryPool, bufferConfig);
	if (!m_clientSessionPool || !m_clientSessionPool->IsReady())
		return false;

	m_clientSessionPool->SetSocketCloseFunc(closeSocketFunc);
	m_clientSessionPool->SetEventHandler(this);

	return true;
}

void SessionManager::Finalize()
{
	m_acceptSessionCount = 0;
	m_clientSessionCount = 0;

	if (m_acceptSessionPool)
	{
		delete m_acceptSessionPool;
		m_acceptSessionPool = nullptr;
	}

	if (m_clientSessionPool)
	{
		delete m_clientSessionPool;
		m_clientSessionPool = nullptr;
	}
}

AcceptSession* SessionManager::GetAcceptSession(const uint32_t sessionId)
{
	if (!m_acceptSessionPool)
		return nullptr;

	if (sessionId >= m_acceptSessionPool->GetSessionCount())
		return nullptr;

	return m_acceptSessionPool->GetSession(sessionId);
}

uint32_t SessionManager::GetAcceptSessionCount() const noexcept
{
	return m_acceptSessionCount;
}

void SessionManager::RequestAllAcceptIOCancel()
{
	if (!m_acceptSessionPool)
		return;

	m_acceptSessionPool->RequestAllAcceptIOCancel();
}

bool SessionManager::WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_acceptSessionPool)
		return false;

	return m_acceptSessionPool->WaitForAllAcceptIOCancelComplete(timeout_ms);
}

bool SessionManager::IsClientSessionFull() const
{
	// 이 파일의 다른 함수들과 달리 널 검사가 빠져 있었다.
	// 풀이 없으면 세션을 내줄 수 없으므로 "가득 찼다" 가 안전한 답이다.
	// 그러면 호출부(HandleAccept)가 임대를 시도하지 않고 연결을 거절한다.
	if (!m_clientSessionPool)
		return true;

	return m_clientSessionPool->IsSessionFull();
}

uint32_t SessionManager::GetClientSessionInUseCount() const
{
	if (!m_clientSessionPool)
		return 0;

	return m_clientSessionPool->GetInUseCount();
}

uint32_t SessionManager::CountClientSessionsFromAddress(const char* ipAddress) const
{
	if (!m_clientSessionPool)
		return 0;

	return m_clientSessionPool->CountSessionsFromAddress(ipAddress);
}

ClientSession* SessionManager::GetClientSession(const uint32_t sessionId)
{
	if (!m_clientSessionPool)
		return nullptr;

	if (sessionId >= m_clientSessionPool->GetSessionCount())
		return nullptr;

	return m_clientSessionPool->GetSession(sessionId);
}

uint32_t SessionManager::GetClientSessionCount() const noexcept
{
	return m_clientSessionCount;
}

void SessionManager::SetSessionDisconnectNotifyFunc(SessionDisconnectNotifyFunc notifyFunc, void* context)
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->SetDisconnectNotifyFunc(notifyFunc, context);
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
		total += m_acceptSessionPool->GetOutstandingIOCount();

	if (m_clientSessionPool)
		total += m_clientSessionPool->GetOutstandingIOCount();

	return total;
}

ClientSession* SessionManager::AcquireClientSession()
{
	if (!m_clientSessionPool)
		return nullptr;

	// 이전에는 실패 시 100회 busy retry 를 돌렸다. Acquire 가 nullptr 이면
	// 프리 리스트가 비었다는 뜻이고, 양보 없는 루프로는 채워질 수 없으므로
	// CPU 만 태우는 코드였다. 게다가 정확히 100번째에 성공하면
	// retryCount == maxRetryCount 가 참이 되어 성공했는데도 단정에 걸렸다.
	ClientSession* session = m_clientSessionPool->Acquire();

	if (!session)
	{
		// 호출부(HandleAccept)가 연결을 거절하는 정상 경로다.
		LOGW("client session pool is exhausted (capacity %u), the connection will be rejected",
			m_clientSessionPool->GetSessionCount());
	}

	return session;
}

void SessionManager::ReleaseClientSession(ISession* session)
{
	if (!m_clientSessionPool)
		return;

	ENGINE_CHECK_RETVOID(session != nullptr, "ReleaseClientSession called with a null session");

	const uint32_t sessionId = session->GetSessionID();
	ENGINE_CHECK_RETVOID(sessionId < m_clientSessionPool->GetSessionCount(),
		"ReleaseClientSession called with session id %u but capacity is %u",
		sessionId, m_clientSessionPool->GetSessionCount());

	// 기반 타입이 들어오는 유일한 지점이다. ISessionEvent::OnDisconnectRequest
	// 가 ISession* 로 고정돼 있어서 여기까지는 기반 타입으로 온다.
	// 클라 세션 풀에는 ClientSession 만 들어가므로 RTTI 조회는 필요 없다.
	// 경계에서 한 번만 좁히고, 그 아래는 전부 구체 타입으로 다닌다.
	m_clientSessionPool->Release(static_cast<ClientSession*>(session));
}

void SessionManager::RequestAllRecvSendIOCancel()
{
	if (!m_clientSessionPool)
		return;

	m_clientSessionPool->RequestAllRecvSendIOCancel();
}

bool SessionManager::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_clientSessionPool)
		return false;

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

void SessionManager::OnDisconnectRequest(ISession* session)
{
	if (!m_clientSessionPool)
		return;

	ReleaseClientSession(session);
}
