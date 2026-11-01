#include "ClientWatchdogThread.h"

#include <Windows.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"
#include "../Session/ClientSession.h"

using namespace Core::Util;

namespace
{
	// 점검 주기는 타임아웃에서 따온다. 너무 촘촘하면 깨어나는 비용만 내고,
	// 너무 성기면 실제 감지가 타임아웃보다 한참 늦는다. 1/4 이면 최악의
	// 경우에도 타임아웃 + 25% 안에 감지된다.
	constexpr uint64_t CheckDivisor = 4;

	// 그래도 이보다 촘촘하거나 성기게는 돌지 않는다.
	constexpr DWORD MinCheckInterval_ms = 250;
	constexpr DWORD MaxCheckInterval_ms = 5'000;
}

ClientWatchdogThread::ClientWatchdogThread(ClientSession* session, const ClientLivenessConfig& config)
	: ThreadBase(L"ClientWatchdogThread")
{
	m_session = session;
	m_config = config;
}

void ClientWatchdogThread::Arm(uint32_t serverHeartbeatInterval_ms)
{
	const uint64_t timeout_ms = m_config.ResolveIdleTimeout(serverHeartbeatInterval_ms);

	::InterlockedExchange64(&m_idleTimeout_ms, static_cast<LONGLONG>(timeout_ms));

	if (timeout_ms == 0)
	{
		LOGI("client liveness watchdog stays disarmed (the idle timeout is turned off)");
		return;
	}

	LOGI("client liveness watchdog armed : idle timeout %llu ms "
		"(server heartbeat interval %u ms x %u missed)",
		timeout_ms, serverHeartbeatInterval_ms, m_config.missedHeartbeatLimit);
}

void ClientWatchdogThread::Disarm()
{
	::InterlockedExchange64(&m_idleTimeout_ms, 0);
}

bool ClientWatchdogThread::HasFiredTimeout() const
{
	return ::ReadAcquire(&m_fired) != 0;
}

void ClientWatchdogThread::Run()
{
	while (!IsStopRequested())
	{
		const uint64_t timeout_ms =
			static_cast<uint64_t>(::ReadAcquire64(&m_idleTimeout_ms));

		// 무장 전에는 짧게 돌면서 무장을 기다린다. 무장은 인증 응답이
		// 도착한 뒤에 일어나므로 여기서는 기다리는 것 말고 할 일이 없다.
		DWORD wait_ms = MinCheckInterval_ms;

		if (timeout_ms != 0)
		{
			const uint64_t derived = timeout_ms / CheckDivisor;

			wait_ms = (derived < MinCheckInterval_ms) ? MinCheckInterval_ms
				: (derived > MaxCheckInterval_ms) ? MaxCheckInterval_ms
				: static_cast<DWORD>(derived);
		}

		const DWORD waitResult = ::WaitForSingleObject(GetStopEvent(), wait_ms);
		if (waitResult == WAIT_OBJECT_0)
		{
			break;
		}

		if (waitResult != WAIT_TIMEOUT)
		{
			ENGINE_VIOLATION("client watchdog wait returned %lu (error %lu), stopping the thread",
				waitResult, ::GetLastError());
			break;
		}

		// 대기 중에 무장이 풀렸을 수 있다. 다시 읽는다.
		const uint64_t armed_ms =
			static_cast<uint64_t>(::ReadAcquire64(&m_idleTimeout_ms));

		if (armed_ms == 0 || m_session == nullptr)
		{
			continue;
		}

		// 세션이 이미 정리 중이면 우리가 할 일이 없다. 여기서 한 번 더
		// 끊자고 하면 종료 절차만 중복된다 (막히지는 않는다 —
		// IOCPClient::m_disconnecting 이 거른다).
		if (!m_session->IsEstablished())
		{
			continue;
		}

		const uint64_t nowTick = ::GetTickCount64();

		// 우리가 스스로 읽기를 멈춘 침묵은 상대 탓이 아니다.
		//
		// 수신 백프레셔가 걸리면 다음 WSARecv 를 걸지 않으므로 아무것도
		// 도착하지 않는다. 그런데 데이터는 사라진 게 아니라 우리 커널
		// 버퍼에 쌓여 있다 — 상대는 멀쩡히 보내고 있고 우리가 안 받는
		// 것뿐이다. 이걸 "서버가 죽었다" 로 읽으면 느린 소비자가 자기
		// 연결을 끊는다.
		//
		// 실측 : 이 예외 없이 잡 큐를 256까지 채워 25초 멈춰 두면 클라가
		// 15,282 ms 에 스스로 연결을 끊었다. (tools/bpzombie)
		// 서버의 좀비 정리가 같은 함정에 빠져 있던 것과 같은 모양이다.
		//
		// 멈춰 있는 동안의 시각을 기준선으로 남긴다. 재개한 직후에 낡은
		// lastRecvTick 으로 판정하면 그 자리에서 바로 타임아웃이 되기
		// 때문이다. 재개하면 온전한 한 주기를 다시 준다.
		if (m_session->IsReceivePaused())
		{
			m_resumeBaselineTick = nowTick;
			continue;
		}

		const uint64_t lastRecvTick = m_session->GetLastRecvTick();

		// 둘 중 나중 것이 "마지막으로 들을 수 있었던 시각" 이다.
		const uint64_t lastListeningTick =
			(lastRecvTick > m_resumeBaselineTick) ? lastRecvTick : m_resumeBaselineTick;

		if (lastListeningTick == 0 || nowTick < lastListeningTick)
		{
			continue;
		}

		const uint64_t idle_ms = nowTick - lastListeningTick;
		if (idle_ms < armed_ms)
		{
			continue;
		}

		// 한 번만 쏜다. 여기서 무장을 푸는 것이 그 역할도 겸한다.
		::InterlockedExchange64(&m_idleTimeout_ms, 0);
		::InterlockedExchange(&m_fired, 1);

		LOGW("session %u heard nothing from the server for %llu ms (limit %llu ms). "
			"the connection is dead, dropping it",
			m_session->GetSessionID(), idle_ms, armed_ms);

		m_session->NoteDisconnectReason(DisconnectReason::IdleTimeout);

		// 이 호출 이후로 세션을 만지면 안 된다. 종료 절차가 다른 스레드에서
		// 이어질 수 있고, 마지막 DecrementIO 가 세션을 정리한다.
		m_session->NotifyDisconnect();
	}
}
