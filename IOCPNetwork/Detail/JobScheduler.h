#pragma once

#include "Job.h"
#include "SessionFinalization.h"
#include "SessionReference.h"

#include <chrono>             // for std::chrono::milliseconds
#include <condition_variable> // for std::condition_variable
#include <cstddef>            // for std::size_t
#include <functional>         // for std::function
#include <memory>             // for std::shared_ptr, std::unique_ptr
#include <mutex>              // for std::mutex
#include <thread>             // for std::thread
#include <variant>            // for std::variant

namespace IOCPNetwork::Detail
{
    class PacketPool;

    // 일반 Job 실행 참조 또는 세션 최종 마무리 책임을 보관한다.
    // 준비 큐와 워커 사이에서 복사 없이 소유권을 이동한다.
    using ReadyItem = std::variant<SessionReference, SessionFinalization>;

    // 엔진이 제공하는 연결·종료 통지와 패킷 처리 경로다.
    // 생성 시 확정하고 워커 실행 중에는 변경하지 않는다.
    struct JobCallbacks final
    {
        // 서비스 연결 준비가 확정된 뒤 호출한다.
        std::function<void(ClientSession&)> connected;

        // 수신 완료 I/O가 아니라 Packet Job의 엔진 디스패치 경로다.
        std::function<void(ClientSession&, Packet)> received;

        // 연결 통지를 확정했던 세션의 최종 마무리에서 호출한다.
        std::function<void(ClientSession&)> disconnected;
    };

    // 준비된 세션을 여러 워커에 배분하고 세션별 Job을 순차 처리한다.
    // 준비 큐와 워커별 배치 저장소를 생성 시 확보한다.
    class JobScheduler final
    {
    public:
        // 저장소와 작업 처리 경로를 준비한다.
        // 잘못된 설정, 필수 패킷 처리 함수 누락, 메모리 부족이면 nullptr를 반환한다.
        [[nodiscard]] static std::unique_ptr<JobScheduler> Create(
            std::size_t maxSessions,
            std::size_t workerCount,
            std::size_t maxJobsPerBatch,
            std::chrono::milliseconds jobBatchTime,
            JobCallbacks callbacks,
            std::shared_ptr<PacketPool> packetPool) noexcept;

        // 생산자와 세션 정리를 끝낸 상태에서 남은 워커를 정지하고 합류한다.
        ~JobScheduler();

        JobScheduler(const JobScheduler&) = delete;
        JobScheduler& operator=(const JobScheduler&) = delete;

        JobScheduler(JobScheduler&&) = delete;
        JobScheduler& operator=(JobScheduler&&) = delete;

        // 모든 워커를 시작한다. 이미 실행 중이거나 시작에 실패하면 false를 반환한다.
        // 성공 반환 전에는 작업을 등록하지 않으며, 시작·정지 호출은 직렬화한다.
        [[nodiscard]] bool Start() noexcept;

        // 모든 작업 생산과 세션 최종 마무리가 끝난 뒤 워커를 정지한다.
        // 워커 밖에서 호출하며, 시작·정지·파괴 호출은 외부에서 직렬화한다.
        void Stop() noexcept;

        // 유효한 준비 항목을 인수하고 대기 워커 하나를 깨운다.
        // 실행 중인 스케줄러에 중복 예약되지 않은 항목만 전달해야 한다.
        void Enqueue(ReadyItem&& item) noexcept;

        // 세션 풀이 넘긴 최종 마무리 책임을 담당 스케줄러의 준비 큐에 등록한다.
        // context는 해당 JobScheduler의 주소여야 한다.
        static void EnqueueFinalization(void* context, SessionFinalization finalization) noexcept;

    private:
#if defined(IOCPNETWORK_TESTING)
        friend struct SchedulerTestAccess;
#endif
        // 세션의 연결 상태와 별개인 스케줄러 실행 상태다.
        enum class State
        {
            Stopped,
            Running,
            Stopping
        };

        // 워커 하나의 배치 저장소와 스레드를 소유한다.
        struct WorkerStorage final
        {
            std::unique_ptr<Job[]> jobs;

            std::thread thread;
        };

        JobScheduler(
            std::unique_ptr<ReadyItem[]> readyItems,
            std::unique_ptr<WorkerStorage[]> workers,
            std::size_t readyCapacity,
            std::size_t workerCount,
            std::size_t maxJobsPerBatch,
            std::chrono::milliseconds jobBatchTime,
            JobCallbacks callbacks,
            std::shared_ptr<PacketPool> packetPool) noexcept;

        // 자신의 배치 저장소로 준비 항목을 반복 처리한다.
        // 정지 신호와 빈 큐를 확인하면 반환한다.
        void WorkerLoop(std::size_t workerIndex) noexcept;

        // 준비 항목이 생기거나 정지를 요청할 때까지 기다린다.
        // 정지 상태에서 큐가 비었으면 빈 SessionReference 대안을 반환한다.
        [[nodiscard]] ReadyItem WaitDequeue() noexcept;

        // 예약용 참조를 인수하여 세션의 Job 배치를 한 차례 처리한다.
        // 남은 작업이 있으면 같은 참조로 재예약하고, 없으면 반환한다.
        void ProcessSession(SessionReference reference, WorkerStorage& worker) noexcept;

        // 접수된 Job 하나를 실행하거나 취소하고 공통 정리를 수행한다.
        // 호출 워커는 별도의 세션 참조를 유지해야 한다.
        void ExecuteJob(ClientSession& session, Job& job) noexcept;

        // 필요한 종료 통지를 수행하고 최종 마무리 책임을 완료한다.
        // 통지에서 예외가 발생해도 세션을 풀에 반환한다.
        void ProcessFinalization(SessionFinalization finalization) noexcept;

        // 일반 실행 참조와 최종 마무리 항목을 보관하는 준비 큐다.
        std::unique_ptr<ReadyItem[]> m_readyItems;

        // 각 워커는 자신의 배치 배열만 사용한다.
        std::unique_ptr<WorkerStorage[]> m_workers;

        const std::size_t m_readyCapacity;
        const std::size_t m_workerCount;

        // 한 차례에 인출할 개수와 Job 사이에서 확인할 시간 예산이다.
        const std::size_t m_maxJobsPerBatch;
        const std::chrono::milliseconds m_jobBatchTime;

        // 연결·종료 통지와 패킷 처리 경로를 소유한다.
        // 여러 세션의 워커가 동시에 호출할 수 있다.
        const JobCallbacks m_callbacks;

        // 모든 Job 워커가 캐시로 사용할 원래 풀을 유지한다.
        const std::shared_ptr<PacketPool> m_packetPool;

        // 준비 큐와 실행 상태를 같은 락으로 보호한다.
        std::mutex m_mutex;

        // 준비 항목 등록 또는 워커 정지를 알린다.
        std::condition_variable m_condition;

        State m_state = State::Stopped;

        // 준비 큐의 다음 인출 위치, 다음 등록 위치, 보관 중인 항목 수다.
        std::size_t m_readyHead = 0;
        std::size_t m_readyTail = 0;
        std::size_t m_readyCount = 0;
    };
}
