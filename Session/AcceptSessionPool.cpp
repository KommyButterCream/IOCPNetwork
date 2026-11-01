#include "AcceptSessionPool.h"

#include "AcceptSession.h"

#include "../Diagnostics/EngineAssert.h"

using namespace Core::Util;

#include <WinSock2.h>

AcceptSessionPool::AcceptSessionPool(uint32_t capacity)
{
	m_capacity = capacity;

	// Accept 세션 풀 생성
	m_sessions = new AcceptSession[m_capacity];
	if (!m_sessions)
		return;

	// Accept 세션 초기화
	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		if (!m_sessions[i].Initialize(SESSION_ROLE::ACCEPT, i))
		{
			ENGINE_VIOLATION("failed to initialize accept session %u of %u", i, m_capacity);
			return;
		}
	}

	m_ready = true;
}

AcceptSessionPool::~AcceptSessionPool()
{
	// 여기서 취소를 다시 요청하거나 완료를 기다리지 않는다.
	//
	// 이 풀은 SessionManager 가, SessionManager 는 IOCPServer::StopServer 가
	// 지운다. 그래서 이 소멸자에 도달한 시점은 StopServer 가
	// RequestAllAcceptIOCancel / WaitForAllAcceptIOCancelComplete 를 이미
	// 끝내고 리슨 소켓까지 닫은 뒤이며, IOCPCore::Stop 도 이미 불린 상태다.
	//
	// 즉 완료 통지를 큐에서 꺼내 줄 스레드가 남아 있지 않다. 그런데도
	// 예전 소멸자는 같은 취소·대기를 한 번 더 수행했다.
	//
	// 취소 요청은 소켓이 이미 전부 떼어져 있어 아무 일도 하지 않는다.
	// 대기는 더 나쁘다. 이 시점에 이벤트를 세워 줄 DecrementIO 는 올 수
	// 없으므로, 상태가 ACCEPT_READY 로 정리되지 않은 세션마다 10초를 꽉
	// 채우고 실패한다. 측정해 보면 남은 I/O 가 0 인 세션을 10초 기다린다
	// ("io count still 0"). 기다릴 대상이 없는데 기다리는 것이다.
	//
	// 취소·대기의 소유권은 StopServer 에 있다. 소멸자는 자원 회수만 한다.
	// 다만 진단과 누수 방지는 남긴다 (아래 참고).
	CloseLeftoverSockets();

	if (m_sessions)
	{
		delete[] m_sessions;
		m_sessions = nullptr;
	}
}

// 종료 절차가 제대로 돌았다면 이 시점에 살아 있는 소켓은 없어야 한다.
// 남아 있다면 StopServer 를 건너뛴 경로가 있다는 뜻이므로 크게 남긴다.
//
// 진단만 하고 끝내면 소켓이 새기 때문에 닫는 것까지 여기서 한다.
// (~AcceptSession -> BaseSession::Finalize 는 살아 있는 소켓과 남은 IO 개수를
//  보고하기는 하지만 닫아 주지는 않는다)
void AcceptSessionPool::CloseLeftoverSockets()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		AcceptSession* session = &m_sessions[i];

		if (session->IsSocketInvalid())
			continue;

		ENGINE_VIOLATION("accept session %u still holds socket %d when the pool is destroyed. the shutdown sequence did not run for it",
			session->GetSessionID(), static_cast<int>(session->GetClientSocket()));

		if (m_closeSocketFunc)
		{
			m_closeSocketFunc(session->DetachSocket());
		}
	}
}

AcceptSession* AcceptSessionPool::GetSession(const uint32_t sessionId)
{
	if (sessionId >= INVALID_SESSION_ID)
		return nullptr;

	if (sessionId >= m_capacity)
		return nullptr;

	return &m_sessions[sessionId];
}

uint32_t AcceptSessionPool::GetSessionCount() const
{
	return m_capacity;
}

uint32_t AcceptSessionPool::GetOutstandingIOCount() const
{
	if (!m_sessions)
		return 0;

	uint32_t total = 0;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		const LONG outstanding = m_sessions[i].GetOutstandingIOCount();

		// 음수는 짝이 맞지 않는다는 뜻이고 이미 위반으로 잡힌다.
		// 여기서 더해 상쇄시키면 합계가 0 으로 보여 오히려 숨는다.
		if (outstanding > 0)
			total += static_cast<uint32_t>(outstanding);
	}

	return total;
}

void AcceptSessionPool::RequestAllAcceptIOCancel()
{
	if (!m_sessions)
		return;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		AcceptSession* session = &m_sessions[i];

		if (session != nullptr && !session->IsSocketInvalid())
		{
			// Accept Session 은 CancelIOEx 를 호출해도 IO 통지가 오지 않는다.
			// 여기에서 호출해주는 이유는 m_cancelIo 와 m_ioCancelCompleteEvent 를 설정해주기 위함.
			if (!session->CancelPendingIO())
			{
				ENGINE_VIOLATION("accept session %u CancelPendingIO failed", session->GetSessionID());
			}

			// Accept Session 은 소켓을 강제로 닫거나, 서버 소켓이 닫히는 경우에만 IO Abort 를 수신한다.
			if (m_closeSocketFunc)
			{
				m_closeSocketFunc(session->DetachSocket());
			}
		}
	}
}

bool AcceptSessionPool::WaitForAllAcceptIOCancelComplete(const uint32_t timeout_ms)
{
	if (!m_sessions)
		return false;

	// [2] Accept 세션의 IO 취소 완료 대기
	// 모든 I/O 가 취소되기를 기다린다.
	// 세션 내부의 IO Count 를 체크해서
	// 0 개가 되면 이벤트가 Set 된다.
	//
	// 한 세션이 시간을 넘겨도 즉시 돌아가지 않는다. 예전에는 여기서
	// return false 를 해서 뒤쪽 세션은 대기도, ResetSession 도 받지 못했다.
	// 그렇게 남은 세션은 ACCEPT_READY 가 아닌 상태로 남아 소멸자의 두 번째
	// 대기에 걸렸고, 그 대기는 이미 만족될 수 없는 상태였다.
	bool allCompleted = true;

	for (uint32_t i = 0; i < m_capacity; ++i)
	{
		AcceptSession* session = &m_sessions[i];

		if (session == nullptr)
		{
			continue;
		}

		if (session->GetAcceptSessionState() == AcceptSessionState::ACCEPT_READY)
		{
			continue;
		}

		if (!session->WaitForIOCancelComplete(timeout_ms))
		{
			ENGINE_VIOLATION("accept session %u IO cancel did not complete within %u ms", session->GetSessionID(), timeout_ms);
			allCompleted = false;
		}

		// 대기가 실패했더라도 리셋한다. 어차피 이 세션은 곧 풀과 함께
		// 사라지고, 리셋을 건너뛰면 소멸자가 다시 기다리게 된다.
		session->ResetSession();
	}

	return allCompleted;
}

void AcceptSessionPool::SetSocketCloseFunc(CloseSocketFunc closeSocketFunc)
{
	m_closeSocketFunc = closeSocketFunc;
}
