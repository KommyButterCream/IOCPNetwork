#pragma once
#include <atomic> // for std::atomic
#include <cstdint> // for uint32_t, uint64_t
#include <memory> // for std::unique_ptr
#include <mutex> // for std::mutex
#include <vector> // for std::vector

// 주기 작업 한 번의 실행만 구현한다. 대기 루프와 타이머 수명은 그룹이 관리한다.
class IPeriodicTask
{
public:
    virtual ~IPeriodicTask() = default;
    virtual void Tick(uint64_t nowTick) = 0;
    // Tick 및 외부 활성화 요청과 겹칠 수 있다. 짧고 예외 없이 종료 상태만 설정한다.
    virtual void RequestStop() noexcept {}
};

class BackgroundTaskGroup final
{
public:
    // 제어와 파괴는 타이머 콜백 밖에서 수행한다. 파괴는 다른 제어 호출과 겹치지 않게 한다.
    BackgroundTaskGroup();
    ~BackgroundTaskGroup();
    BackgroundTaskGroup(const BackgroundTaskGroup&) = delete;
    BackgroundTaskGroup& operator=(const BackgroundTaskGroup&) = delete;

    // 시작 전 작업과 주기를 등록한다. 작업 객체 주소는 그룹이 파괴될 때까지 유지한다.
    [[nodiscard]] bool Add(std::unique_ptr<IPeriodicTask> task, uint32_t periodMs);
    // 모든 타이머가 준비된 후 실행을 허용한다. 부분 실패 시 전부 정리한다.
    [[nodiscard]] bool StartAll();
    // 전체 예약 취소 후 전체 콜백을 기다린다. 정지한 그룹은 재시작하지 않고 새로 구성한다.
    bool StopAll();
    // 외부 제어 스레드에서 조회한다. 콜백에서는 종료 대기와의 교착을 피하려고 false를 반환한다.
    [[nodiscard]] bool HasFailed() const;

private:
    struct Entry;
    void StopLocked();
    mutable std::mutex m_controlMutex;
    std::vector<std::unique_ptr<Entry>> m_entries;
    std::atomic<bool> m_enabled{false};
    bool m_started = false;
    bool m_stopped = false;
};
