#include "ClientSessionPool.h"

#include "ClientSession.h"
#include "SessionJobQueue.h"

#include "../Diagnostics/EngineAssert.h"
#include <memory>  // for std::make_unique, std::unique_ptr
#include <utility> // for std::move
#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

struct ClientSessionPool::SessionNode
{
	enum class State { Free, InUse, Releasing };

	// 상태 전이와 프리 리스트 변경은 풀의 쓰기 락 아래에서 수행한다.
	ClientSession* session = nullptr;
	SessionNode* nextNode = nullptr;

	State state = State::Free;
};

std::unique_ptr<ClientSessionPool> ClientSessionPool::Create(uint32_t capacity,
	TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig,
	const Callbacks& callbacks)
{
	auto sessions = std::make_unique<ClientSession[]>(capacity);
	for (uint32_t i = 0; i < capacity; ++i)
	{
		if (!sessions[i].Initialize(SESSION_ROLE::SERVER, i))
		{
			ENGINE_VIOLATION("failed to initialize client session %u of %u", i, capacity);
			return nullptr;
		}

		if (!sessions[i].InitializeMemoryPool(jobMemoryPool, packetMemoryPool, bufferConfig))
		{
			ENGINE_VIOLATION("failed to bind memory pools to client session %u of %u", i, capacity);
			return nullptr;
		}
	}

	return std::unique_ptr<ClientSessionPool>(new ClientSessionPool(std::move(sessions), capacity, callbacks));
}

ClientSessionPool::~ClientSessionPool() = default;

ClientSession* ClientSessionPool::Acquire()
{
	Core::Sync::SRWWriteLockGuard lockguard(m_lock);

	if (!m_freeList)
	{
		return nullptr;
	}

	SessionNode* node = m_freeList;

	// 프리 리스트에 올라와 있던 노드는 반드시 FREE 상태여야 한다.
	// 여기서 어긋나면 프리 리스트가 오염된 것이므로 임대하지 않고 실패시킨다.
	if (node->state != SessionNode::State::Free)
	{
		LOGE("free list corrupted : node is not FREE (session %u)", node->session ? node->session->GetSessionID() : UINT32_MAX);
		return nullptr;
	}

	m_freeList = node->nextNode;
	node->nextNode = nullptr;
	node->state = SessionNode::State::InUse;
	return node->session;
}

void ClientSessionPool::Release(ClientSession* clientSession)
{
	ENGINE_CHECK_RETVOID(clientSession != nullptr, "Release called with a null session");

	const uint32_t sessionId = clientSession->GetSessionID();

	if (!m_nodes || sessionId >= m_capacity)
	{
		LOGE("invalid session id %u (capacity %u)", sessionId, m_capacity);
		return;
	}

	if (!TryBeginRelease(sessionId))
	{
		LOGI("session %u release skipped (already releasing or not in use)", sessionId);
		return;
	}

	if (clientSession->IsTransportConnected())
	{
		if (!clientSession->CancelPendingIO())
		{
			ENGINE_VIOLATION("session %u CancelPendingIO failed during release", sessionId);
		}
	}

	FinishReleaseWhenReady(*clientSession);
}

uint32_t ClientSessionPool::GetSessionCount() const
{
	return m_capacity;
}

uint32_t ClientSessionPool::GetInUseCount() const
{
	if (!m_nodes)
		return 0;

	Core::Sync::SRWReadLockGuard lock(m_lock);
	uint32_t count = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (m_nodes[i].state != SessionNode::State::Free)
		{
			++count;
		}
	}

	return count;
}

uint32_t ClientSessionPool::CountSessionsFromAddress(const char* ipAddress) const
{
	if (!m_nodes || !m_sessions || !ipAddress || ipAddress[0] == '\0')
		return 0;

	uint32_t count = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto session = TryAcquireSessionReference(i);
		if (session && session->HasRemoteAddress(ipAddress))
		{
			++count;
		}
	}

	return count;
}

void ClientSessionPool::RequestAllRecvSendIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto session = TryAcquireSessionReference(i);

		if (!session)
			continue;
		session->GetJobQueue().RequestStop();

		if (session->IsTransportConnected())
		{
			if (!session->CancelPendingIO())
			{
				ENGINE_VIOLATION("session %u CancelPendingIO failed during bulk cancel", session->GetSessionID());
			}
		}
	}
}

IOWaitResult ClientSessionPool::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	// 모든 세션이 이번 대기 주기의 같은 마감 시각을 사용한다.
	const ULONGLONG deadline = ::GetTickCount64() + timeout_ms;
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const ULONGLONG now = ::GetTickCount64();
		const DWORD remaining = now < deadline ? static_cast<DWORD>(deadline - now) : 0;
		const auto result = m_sessions[i].WaitForIOCancelComplete(remaining);
		if (result.status != IOWaitStatus::Completed)
			return result;
	}

	return { IOWaitStatus::Completed };
}

void ClientSessionPool::DisconnectAllSessions()
{
	// 워커 종료 후에는 연결 상태와 관계없이 아직 대여 중인 세션을 회수한다.
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (!TryBeginRelease(i))
			continue;

		auto& session = m_sessions[i];
		session.NoteDisconnectReason(DisconnectReason::LocalShutdown);
		FinishReleaseWhenReady(session);
	}
}

uint32_t ClientSessionPool::SendHeartbeatRequests()
{
	if (!m_sessions)
	{
		return 0;
	}

	uint32_t sentCount = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto session = TryAcquireSessionReference(i);

		if (!session || session->GetSessionRole() != SESSION_ROLE::SERVER)
		{
			continue;
		}

		if (!session->IsEstablished() || session->IsSocketInvalid())
		{
			continue;
		}

		if (session->SendSystemHeartbeatRequest())
		{
			++sentCount;
		}
	}

	return sentCount;
}

uint32_t ClientSessionPool::DisconnectZombieSessions(uint64_t nowTick, uint64_t heartbeatTimeout_ms,
	uint64_t stalledPeerTimeout_ms, uint64_t releaseBudget_ms)
{
	if (!m_sessions || heartbeatTimeout_ms == 0)
	{
		return 0;
	}

	// 유예를 주지 않기로 했다면(0) 본 타임아웃을 그대로 쓴다.
	if (stalledPeerTimeout_ms < heartbeatTimeout_ms)
	{
		stalledPeerTimeout_ms = heartbeatTimeout_ms;
	}

	const uint64_t passBeginTick = ::GetTickCount64();
	uint32_t disconnectedCount = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto session = TryAcquireSessionReference(i);

		if (!session || !session->IsTransportConnected())
		{
			continue;
		}

		if (session->GetSessionRole() != SESSION_ROLE::SERVER)
		{
			continue;
		}

		if (session->IsSocketInvalid())
		{
			continue;
		}

		if (!session->IsHeartbeatTimedOut(nowTick, heartbeatTimeout_ms))
		{
			continue;
		}

		// 본 타임아웃은 넘겼다. 이제 이 침묵이 죽은 것인지 느린 것인지 가른다.
		//
		// 우리 송신이 밀려 있다는 것은 상대의 TCP 수신 윈도가 닫혀 있다는
		// 뜻이고, 윈도를 닫아 두려면 상대의 커널이 살아서 ACK 를 하고 있어야
		// 한다. 즉 "느리지만 살아 있다" 는 증거다. 죽은 상대는 윈도를 닫아
		// 주지 못한다 — 재전송이 쌓이다가 소켓 오류로 끝나고, 그건 이 주기가
		// 아니라 HandleSocketError 가 처리한다.
		//
		// 실측 : 클라 잡 큐를 recvPauseJobDepth(256)까지 채워 25초 멈춰 두면
		// 정상 동작 중인 세션이 정확히 15초에 끊겼다. (tools/bpzombie)
		// 백프레셔는 "유실도 끊김도 없이 속도만 맞춘다" 가 목적인데, 느린
		// 소비자를 살리려는 장치가 느린 소비자를 죽이고 있었다.
		//
		// 무한정 봐주지는 않는다. 이유는 HeartbeatConfig 주석 참고.
		//
		// HasPendingSend 는 락을 잡으므로 본 타임아웃을 넘긴 세션에서만
		// 부른다. 정상 세션은 위 한 줄에서 걸러진다.
		const bool stalledPeer = session->HasPendingSend();

		if (stalledPeer && !session->IsHeartbeatTimedOut(nowTick, stalledPeerTimeout_ms))
		{
			continue;
		}

		// 타임아웃 세션 하나는 시간 예산과 관계없이 반환을 요청한다.
		// 이후 순회 시간이 예산에 도달하면 남은 세션은 다음 주기에 확인한다.
		if (disconnectedCount > 0 && ::GetTickCount64() - passBeginTick >= releaseBudget_ms)
		{
			LOGW("zombie release budget of %llu ms is used up after %u sessions. the rest are left for the next cycle",
				releaseBudget_ms, disconnectedCount);
			break;
		}

		// 두 사유를 구분해서 남긴다. 앞쪽은 상대가 사라진 것이고, 뒤쪽은
		// 상대가 살아는 있으나 유예 시간 내내 읽지 않은 것이다. 운영에서
		// 해야 할 일이 다르다.
		if (stalledPeer)
		{
			LOGW("session %u was not reading for %llu ms while sends were still queued, disconnecting",
				session->GetSessionID(), stalledPeerTimeout_ms);
			session->NoteDisconnectReason(DisconnectReason::StalledPeer);
		}
		else
		{
			LOGW("session %u heartbeat timeout, disconnecting", session->GetSessionID());
			session->NoteDisconnectReason(DisconnectReason::HeartbeatTimeout);
		}
		session->MarkHeartbeatTimeout();
		Release(session.get());
		++disconnectedCount;
	}

	return disconnectedCount;
}

uint32_t ClientSessionPool::GetPeakJobQueueDepth() const
{
	if (!m_sessions)
		return 0;

	// 합이 아니라 최댓값이다.
	//
	// 잡 큐는 세션마다 따로이고 소비자도 세션 단위로 붙으므로, 위험한 것은
	// 총합이 아니라 "가장 밀린 세션 하나" 다. 합으로 보면 세션 천 개가
	// 하나씩 들고 있는 정상 상태와 한 세션이 천 개를 쌓아둔 상태가
	// 같은 숫자로 보인다.
	uint32_t peak = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const uint32_t sessionPeak = m_sessions[i].GetJobQueuePeakDepth();

		if (sessionPeak > peak)
			peak = sessionPeak;
	}

	return peak;
}

uint32_t ClientSessionPool::GetTotalRecvPauseCount() const
{
	if (!m_sessions)
		return 0;

	uint32_t total = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		total += m_sessions[i].GetRecvPauseCount();
	}

	return total;
}

uint32_t ClientSessionPool::GetOutstandingIOCount() const
{
	if (!m_sessions)
		return 0;

	uint32_t total = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const LONG outstanding = m_sessions[i].GetOutstandingIOCount();

		// 음수는 짝이 안 맞는다는 뜻이라 이미 위반으로 잡힌다.
		// 여기서 더해 상쇄시키면 합계가 0 으로 보여 오히려 숨는다.
		if (outstanding > 0)
			total += static_cast<uint32_t>(outstanding);
	}

	return total;
}

ClientSessionPool::ClientSessionPool(std::unique_ptr<ClientSession[]> sessions, uint32_t capacity, const Callbacks& callbacks)
	: m_nodes(std::make_unique<SessionNode[]>(capacity)), m_sessions(std::move(sessions)), m_capacity(capacity),
	m_callbacks(callbacks)
{
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		m_sessions[i].SetOwnerCallbacks({this, &ClientSessionPool::OnDisconnectRequest, &ClientSessionPool::OnReleaseReady});
		m_nodes[i].session = &m_sessions[i];
		m_nodes[i].nextNode = m_freeList;
		m_freeList = &m_nodes[i];
	}
}

void ClientSessionPool::SessionReferenceReleaser::operator()(ClientSession* session) const noexcept
{
	session->ReleaseLifetimeReference();
}

ClientSessionPool::SessionReference ClientSessionPool::TryAcquireSessionReference(uint32_t slotIndex) const
{
	Core::Sync::SRWReadLockGuard lock(m_lock);

	if (!m_nodes || slotIndex >= m_capacity)
	{
		return {};
	}

	const SessionNode& node = m_nodes[slotIndex];
	if (node.state != SessionNode::State::InUse)
	{
		return {};
	}

	// 읽기 락을 보유한 동안에는 Release가 이 슬롯을 RELEASING으로 바꿀 수 없습니다.
	node.session->AddLifetimeReference();
	return SessionReference{ node.session };
}

bool ClientSessionPool::TryBeginRelease(uint32_t sessionId)
{
	// 참조를 빌리는 순회자와 같은 락에서 반환 권한을 한 번만 확보한다.
	Core::Sync::SRWWriteLockGuard lock(m_lock);
	auto& node = m_nodes[sessionId];
	if (node.state != SessionNode::State::InUse)
		return false;

	node.state = SessionNode::State::Releasing;
	return true;
}

void ClientSessionPool::FinishReleaseWhenReady(ClientSession& session)
{
	// 참조가 남으면 마지막 참조를 반환하는 쪽이 OnReleaseReady에서 마무리한다.
	if (session.RequestRelease())
		CompleteRelease(&session);
}

void ClientSessionPool::CleanupDisconnectedSession(ClientSession& session)
{
	const SOCKET socket = session.DetachSocket();
	if (socket != INVALID_SOCKET)
	{
		if (m_callbacks.closeSocket)
			m_callbacks.closeSocket(socket);
		else if (::closesocket(socket) == SOCKET_ERROR)
			LOGE("failed to close client socket (error %d)", ::WSAGetLastError());
	}

	// 서비스가 원격 주소와 컨텍스트를 조회할 수 있도록 초기화 전에 알린다.
	NotifyServiceDisconnect(&session);
	session.OnDisconnect();

	session.ResetSession();
}

void ClientSessionPool::CompleteRelease(ClientSession* clientSession)
{
	const uint32_t sessionId = clientSession->GetSessionID();

	if (!m_nodes || sessionId >= m_capacity)
	{
		LOGE("invalid session id %u (capacity %u)", sessionId, m_capacity);
		return;
	}

	SessionNode* node = &m_nodes[sessionId];

	CleanupDisconnectedSession(*clientSession);

	{
		Core::Sync::SRWWriteLockGuard lockguard(m_lock);
		node->nextNode = m_freeList;
		m_freeList = node;

		node->state = SessionNode::State::Free;
	}
}

void ClientSessionPool::OnDisconnectRequest(void* context, ClientSession* session)
{
	static_cast<ClientSessionPool*>(context)->Release(session);
}

void ClientSessionPool::OnReleaseReady(void* context, ClientSession* session)
{
	ClientSessionPool* pool = static_cast<ClientSessionPool*>(context);

	pool->CompleteRelease(session);
}

void ClientSessionPool::NotifyServiceDisconnect(ClientSession* clientSession)
{
	if (!m_callbacks.notifyDisconnect)
		return;

	// 접속을 알린 적 없는 세션은 종료도 알리지 않는다. 수락 직후 거절된
	// 접속이나 접속 시퀀스가 중간에 실패한 세션이 여기 해당한다.
	// 래치를 소비하는 스레드는 하나뿐이라 두 번 알리지도 않는다.
	if (!clientSession->ConsumeServiceConnectNotified())
		return;

	m_callbacks.notifyDisconnect(m_callbacks.disconnectContext, clientSession);
}
