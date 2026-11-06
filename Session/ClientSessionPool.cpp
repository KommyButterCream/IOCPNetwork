#include "ClientSessionPool.h"

#include "ClientSession.h"

#include "../Diagnostics/EngineAssert.h"
#include <memory>  // for std::make_unique, std::unique_ptr
#include <utility> // for std::move
#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

namespace
{
	enum SESSION_POOL_STATE : LONG
	{
		SESSION_POOL_FREE = 0,
		SESSION_POOL_IN_USE = 1,
		SESSION_POOL_RELEASING = 2
	};
}

struct ClientSessionPool::SessionNode
{
	// 세션 객체는 풀이 소유하고, 노드는 해당 슬롯을 가리킨다.
	ClientSession* session = nullptr;
	SessionNode* nextNode = nullptr;

	volatile LONG poolState = SESSION_POOL_FREE;
};

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
	if (::ReadAcquire(&node.poolState) != SESSION_POOL_IN_USE)
	{
		return {};
	}

	// 읽기 락을 보유한 동안에는 Release가 이 슬롯을 RELEASING으로 바꿀 수 없습니다.
	node.session->AddLifetimeReference();
	return SessionReference{ node.session };
}

std::unique_ptr<ClientSessionPool> ClientSessionPool::Create(uint32_t capacity,
	TlsMemoryPool* jobMemoryPool, TlsMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig)
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

	return std::unique_ptr<ClientSessionPool>(new ClientSessionPool(std::move(sessions), capacity));
}

ClientSessionPool::ClientSessionPool(std::unique_ptr<ClientSession[]> sessions, uint32_t capacity)
	: m_nodes(std::make_unique<SessionNode[]>(capacity)), m_sessions(std::move(sessions)), m_capacity(capacity)
{
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		m_sessions[i].SetReleaseReadyFunc(&ClientSessionPool::OnReleaseReady, this);
		m_nodes[i].session = &m_sessions[i];
		m_nodes[i].nextNode = m_freeList;
		m_freeList = &m_nodes[i];
	}
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
	m_freeList = node->nextNode;
	node->nextNode = nullptr;

	// 프리 리스트에 올라와 있던 노드는 반드시 FREE 상태여야 한다.
	// 여기서 어긋나면 프리 리스트가 오염된 것이므로 임대하지 않고 실패시킨다.
	if (::InterlockedCompareExchange(&node->poolState, SESSION_POOL_IN_USE, SESSION_POOL_FREE) != SESSION_POOL_FREE)
	{
		LOGE("free list corrupted : node is not FREE (session %u)", node->session ? node->session->GetSessionID() : UINT32_MAX);
		return nullptr;
	}

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

	SessionNode* node = &m_nodes[sessionId];

	{
		// 순회자가 사용 중인 슬롯을 확인하고 참조를 확보하는 구간과
		// 같은 락을 사용하여, 두 동작 사이에 반환이 시작되는 것을 막습니다.
		Core::Sync::SRWWriteLockGuard lock(m_lock);

		if (::InterlockedCompareExchange(&node->poolState, SESSION_POOL_STATE::SESSION_POOL_RELEASING, SESSION_POOL_IN_USE) != SESSION_POOL_IN_USE)
		{
			// 동시 disconnect 경로가 실제로 겹쳤다는 신호다. 드물게 발생하고
			// 정상 처리되지만, 프리 리스트 가드가 동작했다는 유일한 증거이므로 남긴다.
			LOGI("session %u release skipped (already releasing or not in use)", sessionId);
			return;
		}
	}

	if (clientSession->IsTransportConnected())
	{
		if (!clientSession->CancelPendingIO())
		{
			ENGINE_VIOLATION("session %u CancelPendingIO failed during release", sessionId);
		}
	}

	// 여기서 기다리지 않는다.
	//
	// 예전에는 WaitForIOCancelComplete(10초) 였다. 이 함수는 완료 핸들러
	// 안에서도 불린다(HandleRecv 안의 시스템 패킷 처리가 송신에 실패하면
	// HandleSocketError -> NotifyDisconnect -> 여기로 온다). 그때 그 핸들러는
	// 자기 몫의 IO 카운트를 아직 들고 있으므로, 대기는 자기 자신이 내려놓기를
	// 기다리는 꼴이 된다. 내려놓을 스레드가 바로 대기 중인 자신이니 풀리지
	// 않고 10초를 꽉 채운다.
	//
	// 실측(bench 1회): "IO cancel timed out" 204회, 그 직전에 찍힌
	// "CancelIoEx found nothing to cancel but N IO operations are still
	// counted" 142회. 취소할 I/O 는 없는데 카운트만 남아 있는 상태 —
	// 남아 있던 그 카운트가 대기 중인 스레드 자신의 몫이었다.
	//
	// 순서를 바꿔서는 풀 수 없다. HandleRecv 는 세션 사용을 끝낼 때까지
	// 카운트를 들고 있어야 하고(그게 "카운트 0 = 아무도 안 만짐" 의 근거다),
	// 반납은 카운트 0 을 봐야 진행할 수 있다. 두 요구가 정면으로 부딪힌다.
	// 그래서 막지 않는다 — 예약해 두고 마지막 완료가 마무리한다.
	if (!clientSession->RequestRelease())
	{
		return;
	}

	CompleteRelease(clientSession);
}

void ClientSessionPool::NotifyServiceDisconnect(ClientSession* clientSession)
{
	if (!m_disconnectNotifyFunc)
		return;

	// 접속을 알린 적 없는 세션은 종료도 알리지 않는다. 수락 직후 거절된
	// 접속이나 접속 시퀀스가 중간에 실패한 세션이 여기 해당한다.
	// 래치를 소비하는 스레드는 하나뿐이라 두 번 알리지도 않는다.
	if (!clientSession->ConsumeServiceConnectNotified())
		return;

	m_disconnectNotifyFunc(m_disconnectNotifyContext, clientSession);
}

void ClientSessionPool::OnReleaseReady(void* context, BaseSession* session)
{
	ClientSessionPool* pool = static_cast<ClientSessionPool*>(context);

	// 이 풀에 담긴 세션은 전부 ClientSession 이다.
	pool->CompleteRelease(static_cast<ClientSession*>(session));
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

	if (m_closeSocketFunc)
	{
		m_closeSocketFunc(clientSession->DetachSocket());
	}

	// 서비스에 먼저 알린다.
	//
	// OnDisconnect 보다 앞이어야 한다. 그쪽이 원격 주소를 지우므로,
	// 뒤에 알리면 서비스는 "누가 끊겼는지" 를 아이디로만 알게 된다.
	// 프리 리스트에 올리기 전이어야 하는 것은 더 분명하다 — 올린 뒤에는
	// 이 세션이 다른 접속에 임대된 상태일 수 있다.
	NotifyServiceDisconnect(clientSession);

	if (!clientSession->OnDisconnect())
	{
		ENGINE_VIOLATION("session %u OnDisconnect reported failure during release", sessionId);
	}

	clientSession->ResetSession();

	{
		Core::Sync::SRWWriteLockGuard lockguard(m_lock);
		node->nextNode = m_freeList;
		m_freeList = node;

		// 프리 리스트에 올린 다음 락 안에서 FREE 로 전이한다.
		// 순서를 바꾸면 Acquire 가 아직 프리 리스트에 없는 노드를 FREE 로 보거나,
		// 반대로 프리 리스트에 있는 노드를 RELEASING 으로 보게 된다.
		::InterlockedExchange(&node->poolState, SESSION_POOL_FREE);
	}
}

uint32_t ClientSessionPool::GetSessionCount() const
{
	return m_capacity;
}

uint32_t ClientSessionPool::GetInUseCount() const
{
	if (!m_nodes)
		return 0;

	uint32_t count = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		// poolState 는 Interlocked 로만 전이하므로 원자적으로 읽으면 된다.
		// 세는 도중에 값이 바뀔 수 있어 결과는 근사치다. 접속 수용 판단은
		// 어차피 다음 순간에 또 달라지므로 근사치로 충분하다.
		if (::ReadAcquire(&m_nodes[i].poolState) != SESSION_POOL_FREE)
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

ClientSession* ClientSessionPool::GetSession(const uint32_t sessionId)
{
	// 이 함수만 검사가 없었다. 같은 클래스의 Release 도, AcceptSessionPool 의
	// 같은 함수도 범위를 본다. 여기만 빠져 있으면 잘못된 id 하나가 배열 밖을
	// 읽고 그 쓰레기 포인터가 세션으로 유통된다.
	if (!m_nodes)
		return nullptr;

	if (sessionId >= m_capacity)
	{
		LOGE("GetSession called with session id %u but capacity is %u", sessionId, m_capacity);
		return nullptr;
	}

	return m_nodes[sessionId].session;
}

void ClientSessionPool::RequestAllRecvSendIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		auto session = TryAcquireSessionReference(i);

		if (session && session->IsTransportConnected())
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
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (m_closeSocketFunc)
			{
				m_closeSocketFunc(session->DetachSocket());
			}

			// 종료 중에도 알린다.
			//
			// 서버가 내려갈 때 살아 있던 세션은 반납 경로를 지나지 않아
			// 통지를 못 받았다. 그러면 "OnClientConnect 를 받은 세션은
			// 반드시 OnClientDisconnect 를 받는다" 가 성립하지 않고,
			// 서비스는 종료 시점에만 조용히 자원을 흘린다.
			//
			// 이 호출은 StopServer 를 부른 스레드(보통 앱 스레드)에서
			// 실행된다. 운영 중의 통지가 워커 스레드에서 오는 것과 다르다.
			session->NoteDisconnectReason(DisconnectReason::LocalShutdown);
			NotifyServiceDisconnect(session);

			if (!session->OnDisconnect())
			{
				ENGINE_VIOLATION("session %u OnDisconnect reported failure", session->GetSessionID());
			}

			session->ResetSession();
		}
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

void ClientSessionPool::SetDisconnectNotifyFunc(SessionDisconnectNotifyFunc notifyFunc, void* context)
{
	m_disconnectNotifyFunc = notifyFunc;
	m_disconnectNotifyContext = context;
}

void ClientSessionPool::SetSocketCloseFunc(CloseSocketFunc closeSocketFunc)
{
	m_closeSocketFunc = closeSocketFunc;
}

void ClientSessionPool::SetEventHandler(ISessionEvent* handler)
{
	m_eventHandler = handler;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		m_sessions[i].SetEventHandler(m_eventHandler);
	}
}
