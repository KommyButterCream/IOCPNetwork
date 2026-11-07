#pragma once
#include "BackgroundTaskGroup.h"
#include "HeartbeatConfig.h"
#include "ClientLivenessConfig.h"
#include <atomic> // for std::atomic

class SessionManager;
class ClientSession;
class IOCPServer;

// 하트비트 전송 후 시간 초과 세션을 정리한다. 같은 작업의 순서는 유지한다.
class ServerHeartbeatTask final : public IPeriodicTask
{
public:
    ServerHeartbeatTask(SessionManager& sessions, const HeartbeatConfig& config);
    void Tick(uint64_t nowTick) override;
private:
    SessionManager& m_sessions;
    const HeartbeatConfig m_config;
};

// 하트비트 주기와 무관하게 비어 있는 Accept 요청을 복구한다.
class AcceptRecoveryTask final : public IPeriodicTask
{
public:
    explicit AcceptRecoveryTask(IOCPServer& server) : m_server(server) {}
    void Tick(uint64_t nowTick) override;
private:
    IOCPServer& m_server;
};

// 인증 이후 서버 무응답을 점검한다. 종료 후 Arm은 새 감시를 시작하지 않는다.
class ClientWatchdogTask final : public IPeriodicTask
{
public:
    static constexpr uint32_t CheckPeriodMs = 250;
    ClientWatchdogTask(ClientSession& session, const ClientLivenessConfig& config);
    void Arm(uint32_t serverHeartbeatIntervalMs);
    void RequestStop() noexcept override;
    void Tick(uint64_t nowTick) override;
    [[nodiscard]] bool HasFiredTimeout() const noexcept { return m_fired.load(); }
private:
    ClientSession& m_session;
    const ClientLivenessConfig m_config;
    std::atomic<uint64_t> m_idleTimeoutMs{0};
    std::atomic<bool> m_stopped{false};
    std::atomic<bool> m_fired{false};
    // 동일 작업의 Tick은 직렬 실행되므로 이 기준 시각은 원자 변수일 필요가 없다.
    uint64_t m_resumeBaselineTick = 0;
};
