#include "BackgroundTasks.h"
#include "IOCPServer.h"
#include "../Session/SessionManager.h"
#include "../Session/ClientSession.h"
#include "../../Core/Util/Logger.h"

using namespace Core::Util;

ServerHeartbeatTask::ServerHeartbeatTask(SessionManager& sessions, const HeartbeatConfig& config)
    : m_sessions(sessions), m_config(config) {}

void ServerHeartbeatTask::Tick(uint64_t)
{
    const auto sent = m_sessions.SendHeartbeatRequests();
    if (sent) LOGI("sent %u heartbeat requests", sent);
    const auto disconnected = m_sessions.DisconnectZombieSessions(m_config.timeout_ms,
        m_config.EffectiveStalledTimeout(), m_config.checkInterval_ms / 4);
    if (disconnected) LOGW("disconnected %u timed-out sessions", disconnected);
}

void AcceptRecoveryTask::Tick(uint64_t)
{
    m_server.RefillAcceptSessions();
}

ClientWatchdogTask::ClientWatchdogTask(ClientSession& session, const ClientLivenessConfig& config)
    : m_session(session), m_config(config) {}

void ClientWatchdogTask::Arm(uint32_t serverHeartbeatIntervalMs)
{
    if (m_stopped.load()) return;
    const auto timeout = m_config.ResolveIdleTimeout(serverHeartbeatIntervalMs);
    m_idleTimeoutMs.store(timeout);
    // 종료와 겹쳐 값이 기록되어도 m_stopped가 우선하며 타이머를 재예약하지 않는다.
    LOGI("client watchdog armed with idle timeout %llu ms", timeout);
}

void ClientWatchdogTask::RequestStop() noexcept
{
    m_stopped.store(true);
}

void ClientWatchdogTask::Tick(uint64_t nowTick)
{
    const auto timeout = m_idleTimeoutMs.load();
    if (m_stopped.load() || m_fired.load() || !timeout || !m_session.IsEstablished()) return;
    // 백프레셔로 직접 수신을 멈춘 시간은 서버의 무응답 시간에서 제외한다.
    if (m_session.IsReceivePaused())
    {
        m_resumeBaselineTick = nowTick;
        return;
    }
    const auto lastReceived = m_session.GetLastRecvTick();
    const auto baseline = lastReceived > m_resumeBaselineTick ? lastReceived : m_resumeBaselineTick;
    if (!baseline || nowTick < baseline || nowTick - baseline < timeout) return;
    if (m_stopped.load()) return;
    m_fired.store(true);
    LOGW("session %u idle for %llu ms (limit %llu ms)", m_session.GetSessionID(), nowTick - baseline, timeout);
    m_session.RequestDisconnect(DisconnectReason::IdleTimeout);
}
