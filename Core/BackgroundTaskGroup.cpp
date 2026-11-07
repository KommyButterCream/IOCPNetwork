#include "BackgroundTaskGroup.h"
#include "EngineExecutionScope.h"
#include "../../Core/Concurrency/ThreadTimer.h"
#include "../../Core/Util/Logger.h"
#include <utility> // for std::move
#include <exception> // for std::exception

using namespace Core::Util;
using Core::Concurrency::ThreadTimer;

struct BackgroundTaskGroup::Entry
{
    std::unique_ptr<IPeriodicTask> task;
    uint32_t periodMs;
    // 멤버 파괴 시에도 타이머가 작업보다 먼저 정리된다.
    ThreadTimer timer;
    Entry(std::unique_ptr<IPeriodicTask> value, uint32_t period) : task(std::move(value)), periodMs(period) {}
};

BackgroundTaskGroup::BackgroundTaskGroup() = default;
BackgroundTaskGroup::~BackgroundTaskGroup() { StopAll(); }

bool BackgroundTaskGroup::Add(std::unique_ptr<IPeriodicTask> task, uint32_t periodMs)
{
    if (ThreadTimer::IsCallbackThread() || !task || periodMs == 0) return false;
    std::lock_guard lock(m_controlMutex);
    if (m_started || m_stopped) return false;
    m_entries.push_back(std::make_unique<Entry>(std::move(task), periodMs));
    return true;
}

bool BackgroundTaskGroup::StartAll()
{
    if (ThreadTimer::IsCallbackThread()) return false;
    std::lock_guard lock(m_controlMutex);
    if (m_started || m_stopped) return false;
    try
    {
        for (const auto& entry : m_entries)
        {
            auto* target = entry.get();
            if (!target->timer.Start([this, target]
                {
                    if (!m_enabled.load()) return;
                    EngineExecutionScope execution;
                    try { target->task->Tick(::GetTickCount64()); }
                    catch (const std::exception& error)
                    {
                        LOGE("background task failed and will stop: %s", error.what());
                        throw;
                    }
                    catch (...)
                    {
                        LOGE("background task failed and will stop with an unknown exception");
                        throw;
                    }
                }, target->periodMs, target->periodMs))
            {
                LOGE("failed to create a background timer (error %lu)", ::GetLastError());
                StopLocked();
                return false;
            }
        }
    }
    catch (...)
    {
        StopLocked();
        throw;
    }
    m_started = true;
    m_enabled.store(true);
    return true;
}

bool BackgroundTaskGroup::StopAll()
{
    if (ThreadTimer::IsCallbackThread()) return false;
    std::lock_guard lock(m_controlMutex);
    StopLocked();
    return true;
}

void BackgroundTaskGroup::StopLocked()
{
    if (m_stopped) return;
    m_enabled.store(false);
    // 한 작업을 기다리는 동안 다른 작업이 계속 예약되지 않도록 먼저 전부 취소한다.
    for (const auto& entry : m_entries) entry->timer.RequestStop();
    for (const auto& entry : m_entries) entry->task->RequestStop();
    for (const auto& entry : m_entries) entry->timer.Stop();
    m_stopped = true;
}

bool BackgroundTaskGroup::HasFailed() const
{
    // 콜백이 제어 락을 기다리면 StopAll의 완료 대기와 교착할 수 있다.
    if (ThreadTimer::IsCallbackThread()) return false;
    std::lock_guard lock(m_controlMutex);
    for (const auto& entry : m_entries)
        if (entry->timer.HasFailed()) return true;
    return false;
}
