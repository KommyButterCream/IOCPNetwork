#include "ClientSessionPool.h"

#include "ClientSession.h"

#include "../Diagnostics/EngineAssert.h"
#include "SessionNode.h"

#include <string.h> // for strncmp

#include "../../Core/Sync/SRWLockGuard.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ClientSessionPool::ClientSessionPool(uint32_t capacity, EngineMemoryPool* sendQueueMemoryPool, EngineMemoryPool* jobMemoryPool, EngineMemoryPool* packetMemoryPool, const SessionBufferConfig& bufferConfig)
{
	m_capacity = capacity;

	m_sessions = new ClientSession[m_capacity];
	if (!m_sessions)
		return;

	m_nodes = new SessionNode[m_capacity];
	if (!m_nodes)
		return;

	// 초기 프리 리스트 구성
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (!m_sessions[i].Initialize(SESSION_ROLE::SERVER, i))
		{
			ENGINE_VIOLATION("failed to initialize client session %u of %u", i, m_capacity);
			return;
		}

		if (!m_sessions[i].InitializeMemoryPool(sendQueueMemoryPool, jobMemoryPool, packetMemoryPool, bufferConfig))
		{
			ENGINE_VIOLATION("failed to bind memory pools to client session %u of %u", i, m_capacity);
			return;
		}

		// 마지막 완료가 반납을 마무리할 수 있도록 진입점을 걸어 둔다.
		m_sessions[i].SetReleaseReadyFunc(&ClientSessionPool::OnReleaseReady, this);

		m_nodes[i].session = &m_sessions[i];
		m_nodes[i].nextNode = m_freeList;
		m_freeList = &m_nodes[i];
	}

	m_ready = true;
}

ClientSessionPool::~ClientSessionPool()
{
	if (m_sessions)
	{
		delete[] m_sessions;
		m_sessions = nullptr;
	}

	if (m_nodes)
	{
		delete[] m_nodes;
		m_nodes = nullptr;
	}
}

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

	// 반납은 세션당 정확히 한 번만 수행되어야 한다.
	// 같은 세션에 대해 아래 경로들이 동시에 들어올 수 있다.
	//   - WSASend 실패 -> HandleSocketError -> NotifyDisconnect
	//   - RECV 0바이트(정상 종료) -> HandleSessionDisconnected
	//   - HeartbeatThread 의 좀비 세션 정리
	// 이 전이를 이긴 스레드만 아래 정리를 수행하고 나머지는 즉시 돌아간다.
	// 가드가 없으면 같은 노드가 프리 리스트에 두 번 들어가
	// node->nextNode == node 인 자기참조 루프가 만들어지고,
	// 그 이후 Acquire 는 사용 중인 같은 세션을 반복해서 배포한다.
	if (::InterlockedCompareExchange(&node->poolState, SESSION_POOL_RELEASING, SESSION_POOL_IN_USE) != SESSION_POOL_IN_USE)
	{
		// 동시 disconnect 경로가 실제로 겹쳤다는 신호다. 드물게 발생하고
		// 정상 처리되지만, 프리 리스트 가드가 동작했다는 유일한 증거이므로 남긴다.
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

bool ClientSessionPool::IsSessionFull() const
{
	Core::Sync::SRWReadLockGuard lockguard(m_lock);
	return (m_freeList == nullptr);
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
		if (::ReadAcquire(&m_nodes[i].poolState) == SESSION_POOL_FREE)
		{
			continue;
		}

		// 임대 중인 세션의 주소만 본다. 반납된 세션의 주소는 지워진다.
		if (::strncmp(m_sessions[i].GetClientIPAddress(), ipAddress, INET_ADDRSTRLEN) == 0)
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
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (!session->CancelPendingIO())
			{
				ENGINE_VIOLATION("session %u CancelPendingIO failed during bulk cancel", session->GetSessionID());
			}
		}
	}
}

bool ClientSessionPool::WaitForAllRecvSendIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_sessions)
		return false;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		ClientSession* session = &m_sessions[i];

		if (session != nullptr && session->IsTransportConnected())
		{
			if (!session->WaitForIOCancelComplete(timeout_ms))
			{
				ENGINE_VIOLATION("session %u IO cancel did not complete within %u ms", session->GetSessionID(), timeout_ms);
				return false;
			}
		}
	}

	return true;
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
		ClientSession* session = &m_sessions[i];
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
		ClientSession* session = &m_sessions[i];
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

		// 여기서부터가 막힐 수 있는 구간이다.
		
		// Release 안에는 최대 10초짜리 WaitForIOCancelComplete 가 있고,
		// 이 루프는 좀비 수만큼 그 대기를 직렬로 쌓는다. 부르는 쪽은
		// HeartbeatThread 한 스레드이므로 그 시간 동안 하트비트 발송과
		// accept 슬롯 보충이 함께 멈춘다.
		
		// 하트비트 발송이 멈추면 멀쩡한 세션도 응답할 기회를 잃고 좀비로
		// 판정된다. 실측한 연쇄다 — 정리 한 건이 2초일 때 좀비 6개면
		// 발송 간격이 17초로 벌어지고(주기는 5초), 정상 응답하던 세션이
		// 타임아웃 판정으로 끊겼다.
	
		// 그래서 한 주기에 쓸 시간에 상한을 둔다. 남은 좀비는 다음 주기가
		// 가져간다. 한 건은 예산과 무관하게 처리하므로 예산이 아무리
		// 작아도 정리가 영원히 밀리지는 않는다.
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
		Release(session);
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
