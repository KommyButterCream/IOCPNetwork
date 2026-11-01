#include "HeartbeatThread.h"

#include <Windows.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"
#include "../Session/SessionManager.h"

using namespace Core::Util;

HeartbeatThread::HeartbeatThread(SessionManager* sessionManager, const HeartbeatConfig& config,
	AcceptRepostFunc acceptRepostFunc, void* acceptRepostContext)
	: ThreadBase(L"HeartbeatThread")
{
	m_sessionManager = sessionManager;
	m_checkInterval_ms = config.checkInterval_ms;
	m_heartbeatTimeout_ms = config.timeout_ms;
	m_stalledPeerTimeout_ms = config.EffectiveStalledTimeout();
	m_acceptRepostFunc = acceptRepostFunc;
	m_acceptRepostContext = acceptRepostContext;
}

void HeartbeatThread::SetCheckInterval(uint64_t checkInterval_ms)
{
	m_checkInterval_ms = checkInterval_ms;
}

void HeartbeatThread::SetHeartbeatTimeout(uint64_t heartbeatTimeout_ms)
{
	m_heartbeatTimeout_ms = heartbeatTimeout_ms;
}

void HeartbeatThread::Run()
{
	// 앞 주기의 일이 오래 걸렸으면 그만큼 덜 기다린다. 고정 간격으로
	// 기다리면 하트비트 발송 간격이 "간격 + 일한 시간" 으로 벌어진다.
	// 좀비 정리가 12초를 먹었을 때 발송 간격이 17초로 늘어나면서 정상
	// 응답하던 세션까지 타임아웃(15초) 판정을 받는 것이 실측됐다.
	uint64_t nextCycleTick = ::GetTickCount64() + m_checkInterval_ms;

	// 앞 주기가 목표 시각을 넘겼어도 최소한은 쉰다. 그러지 않으면 점검이
	// 계속 늦는 상황에서 이 스레드가 쉬지 않고 돈다.
	constexpr DWORD MinCycleWait_ms = 50;

	while (!IsStopRequested())
	{
		const uint64_t beforeWaitTick = ::GetTickCount64();

		uint64_t remaining_ms = (nextCycleTick > beforeWaitTick) ? (nextCycleTick - beforeWaitTick) : 0;
		if (remaining_ms > static_cast<uint64_t>(MAXDWORD))
			remaining_ms = MAXDWORD;

		DWORD wait_ms = static_cast<DWORD>(remaining_ms);
		if (wait_ms < MinCycleWait_ms)
			wait_ms = MinCycleWait_ms;

		const DWORD waitResult = ::WaitForSingleObject(GetStopEvent(), wait_ms);
		if (waitResult == WAIT_OBJECT_0)
		{
			break;
		}

		if (waitResult != WAIT_TIMEOUT)
		{
			ENGINE_VIOLATION("heartbeat wait returned %lu (error %lu), stopping the thread", waitResult, ::GetLastError());
			break;
		}

		nextCycleTick = ::GetTickCount64() + m_checkInterval_ms;

		if (!m_sessionManager)
		{
			continue;
		}

		// 아래 세 가지는 순서가 의미를 가진다. 막힐 수 있는 일을 뒤로 보낸다.

		// [1] 하트비트 발송. 이게 늦으면 멀쩡한 세션이 좀비로 오판된다.
		//
		// 이전 코드는 heartbeatRequestCount 대신 m_heartbeatTimeout_ms 를 찍고 있었다.
		// (uint64_t 를 %u 로 넘겨서 가변인자 UB 이기도 했다)
		const uint32_t heartbeatRequestCount = m_sessionManager->SendHeartbeatRequests();
		if (heartbeatRequestCount > 0)
		{
			LOGI("sent %u heartbeat requests", heartbeatRequestCount);
		}

		// [2] 비어 버린 accept 슬롯 다시 post. 슬롯이 없으면 새 접속을 못 받는다.
		// (왜 이 스레드가 하는지는 AcceptRepostFunc 선언부 주석 참고)
		//
		// 좀비 정리보다 앞에 둔다. 정리는 막힐 수 있고, 그 뒤에 있으면
		// 정리가 오래 걸리는 동안 슬롯 회복이 통째로 멈춘다.
		if (m_acceptRepostFunc)
		{
			m_acceptRepostFunc(m_acceptRepostContext);
		}

		// [3] 좀비 정리. 이 주기에서 유일하게 오래 막힐 수 있는 일이라
		// 마지막에 두고, 쓸 수 있는 시간에 상한을 준다.
		// 상한은 점검 주기에서 따온다 — 주기가 바뀌면 같이 바뀌어야 한다.
		const uint64_t zombieReleaseBudget_ms = m_checkInterval_ms / 4;

		const uint32_t disconnectedCount = m_sessionManager->DisconnectZombieSessions(
			m_heartbeatTimeout_ms, m_stalledPeerTimeout_ms, zombieReleaseBudget_ms);
		if (disconnectedCount > 0)
		{
			LOGW("disconnected %u zombie sessions (timeout %llu ms)", disconnectedCount, m_heartbeatTimeout_ms);
		}
	}
}
