#include "JobScheduler.h"

#include "../ClientSession.h"
#include "SessionJobQueue.h"
#include "ReceiveProcessor.h"

#include <chrono>       // for std::chrono::steady_clock, std::chrono::duration_cast, std::chrono::milliseconds
#include <exception>    // for std::terminate
#include <limits>       // for std::numeric_limits
#include <mutex>        // for std::lock_guard, std::unique_lock
#include <new>          // for std::bad_alloc
#include <span>         // for std::span
#include <system_error> // for std::system_error
#include <thread>       // for std::thread, std::this_thread::get_id
#include <utility>      // for std::move, std::in_place_type
#include <variant>      // for std::get_if, std::get, std::holds_alternative

namespace IOCPNetwork::Detail
{
#if defined(IOCPNETWORK_TESTING)
    namespace TestHooks
    {
        // 로컬 테스트 실행 파일에서만 정의하며 제품 DLL에는 포함하지 않는다.
        void BeforeWorkerStart(std::size_t index);
    }
#endif
    namespace
    {
        // 접수된 Job 하나의 완료 정리를 범위 종료 시 수행한다.
        // 작업 데이터 -> 접수 예산 -> 세션 참조 순서를 보장한다.
        class JobCompletionScope final
        {
        public:
            JobCompletionScope(SessionJobQueue& queue, Job& job) noexcept
                : m_queue(queue)
                , m_job(job)
                , m_budget(GetJobBudget(job))
            {
            }

            ~JobCompletionScope() noexcept
            {
                m_job.ClearData();

                m_queue.CompleteJob(m_budget);

                m_job.Reset();
            }

            JobCompletionScope(const JobCompletionScope&) = delete;
            JobCompletionScope& operator=(const JobCompletionScope&) = delete;

            JobCompletionScope(JobCompletionScope&&) = delete;
            JobCompletionScope& operator=(JobCompletionScope&&) = delete;

        private:
            SessionJobQueue& m_queue;

            Job& m_job;

            const JobBudget m_budget;
        };
    }

    std::unique_ptr<JobScheduler> JobScheduler::Create(
        std::size_t maxSessions,
        std::size_t workerCount,
        std::size_t maxJobsPerBatch,
        std::chrono::milliseconds jobBatchTime,
        JobCallbacks callbacks) noexcept
    {
        if (maxSessions == 0 ||
            workerCount == 0 ||
            maxJobsPerBatch == 0 ||
            jobBatchTime.count() <= 0 ||
            !callbacks.received)
        {
            return {};
        }

        constexpr auto maxSize = (std::numeric_limits<std::size_t>::max)();

        // 각 배열의 원소 수를 바이트 크기로 변환할 수 있는지 확인한다.
        if (maxSessions > maxSize / sizeof(ReadyItem) ||
            workerCount > maxSize / sizeof(WorkerStorage) ||
            maxJobsPerBatch > maxSize / sizeof(Job))
        {
            return {};
        }

        const std::size_t batchBytes = maxJobsPerBatch * sizeof(Job);

        // 워커 전체의 배치 저장소 크기도 표현 가능한 범위여야 한다.
        if (workerCount > maxSize / batchBytes)
        {
            return {};
        }

        try
        {
            auto readyItems = std::make_unique<ReadyItem[]>(maxSessions);

            auto workers = std::make_unique<WorkerStorage[]>(workerCount);

            for (std::size_t index = 0; index < workerCount; ++index)
            {
                workers[index].jobs = std::make_unique<Job[]>(maxJobsPerBatch);
            }

            return std::unique_ptr<JobScheduler>{
                new JobScheduler(
                    std::move(readyItems),
                    std::move(workers),
                    maxSessions,
                    workerCount,
                    maxJobsPerBatch,
                    jobBatchTime,
                    std::move(callbacks))
            };
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    JobScheduler::JobScheduler(
        std::unique_ptr<ReadyItem[]> readyItems,
        std::unique_ptr<WorkerStorage[]> workers,
        std::size_t readyCapacity,
        std::size_t workerCount,
        std::size_t maxJobsPerBatch,
        std::chrono::milliseconds jobBatchTime,
        JobCallbacks callbacks) noexcept
        : m_readyItems(std::move(readyItems))
        , m_workers(std::move(workers))
        , m_readyCapacity(readyCapacity)
        , m_workerCount(workerCount)
        , m_maxJobsPerBatch(maxJobsPerBatch)
        , m_jobBatchTime(jobBatchTime)
        , m_callbacks(std::move(callbacks))
    {
    }

    JobScheduler::~JobScheduler()
    {
        Stop();
    }

    bool JobScheduler::Start() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_state != State::Stopped)
            {
                return false;
            }

            // 재시작은 이전 실행의 준비 항목이 모두 정리된 상태에서만 가능하다.
            if (m_readyCount != 0)
            {
                std::terminate();
            }

            // 실행 스레드가 남아 있는 슬롯에 새 스레드를 대입하지 않는다.
            for (std::size_t index = 0; index < m_workerCount; ++index)
            {
                if (m_workers[index].thread.joinable())
                {
                    std::terminate();
                }
            }

            // 생성된 워커가 빈 큐에서 종료하지 않고 대기하도록 먼저 전환한다.
            m_state = State::Running;
        }

        try
        {
            for (std::size_t index = 0; index < m_workerCount; ++index)
            {
#if defined(IOCPNETWORK_TESTING)
                TestHooks::BeforeWorkerStart(index);
#endif
                m_workers[index].thread = std::thread(
                    [this, index]
                    {
                        WorkerLoop(index);
                    });
            }

            return true;
        }
        catch (const std::system_error&)
        {
            // 스레드 생성에 실패하면 앞서 시작한 워커도 모두 정리한다.
            Stop();

            return false;
        }
        catch (const std::bad_alloc&)
        {
            // 스레드 실행 정보 등의 메모리 확보 실패도 같은 경로로 복구한다.
            Stop();

            return false;
        }
    }

    void JobScheduler::Stop() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_state == State::Stopped)
            {
                return;
            }

            // 시작·정지 호출은 외부에서 직렬화해야 한다.
            if (m_state != State::Running)
            {
                std::terminate();
            }

            // 준비 항목을 버리는 방식으로 스케줄러를 정지하지 않는다.
            if (m_readyCount != 0)
            {
                std::terminate();
            }

            m_state = State::Stopping;
        }

        // 준비 항목을 기다리는 모든 워커가 종료 조건을 다시 확인하게 한다.
        m_condition.notify_all();

        // 자기 자신을 join하는 잘못된 호출을 먼저 확인한다.
        const auto callerId = std::this_thread::get_id();

        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            const auto& thread = m_workers[index].thread;

            if (thread.joinable() && thread.get_id() == callerId)
            {
                std::terminate();
            }
        }

        // 워커가 종료 과정에서 큐 락을 사용할 수 있으므로 락 밖에서 기다린다.
        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            auto& thread = m_workers[index].thread;

            if (thread.joinable())
            {
                thread.join();
            }
        }

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            m_readyHead = 0;

            m_readyTail = 0;

            m_state = State::Stopped;
        }
    }

    void JobScheduler::Enqueue(ReadyItem&& item) noexcept
    {
        // 빈 참조나 빈 최종 마무리 항목은 실제 실행 대상이 아니다.
        if (const auto* reference = std::get_if<SessionReference>(&item))
        {
            if (!*reference)
            {
                std::terminate();
            }
        }
        else
        {
            if (!std::get<SessionFinalization>(item))
            {
                std::terminate();
            }
        }

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 생산자와 최종 마무리를 모두 끝낸 뒤에만 스케줄러를 정지해야 한다.
            if (m_state != State::Running)
            {
                std::terminate();
            }

            // 세션당 예약 하나와 최대 세션 수 용량을 지키면 포화되지 않는다.
            if (m_readyCount == m_readyCapacity)
            {
                std::terminate();
            }

            // 비활성 슬롯은 빈 SessionReference 대안으로 통일한다.
            const auto* empty = std::get_if<SessionReference>(&m_readyItems[m_readyTail]);

            if (empty == nullptr || static_cast<bool>(*empty))
            {
                std::terminate();
            }

            m_readyItems[m_readyTail] = std::move(item);

            ++m_readyTail;

            if (m_readyTail == m_readyCapacity)
            {
                m_readyTail = 0;
            }

            ++m_readyCount;
        }

        // 깨어난 워커가 곧바로 큐 락을 얻을 수 있도록 락 밖에서 통지한다.
        m_condition.notify_one();
    }

    void JobScheduler::EnqueueFinalization(void* context, SessionFinalization finalization) noexcept
    {
        if (context == nullptr || !finalization)
        {
            std::terminate();
        }

        auto& scheduler = *static_cast<JobScheduler*>(context);

        ReadyItem item{
            std::in_place_type<SessionFinalization>,
            std::move(finalization)
        };

        scheduler.Enqueue(std::move(item));

        // 인계 이후 최종 마무리가 이미 끝났을 수 있으므로 세션에 접근하지 않는다.
    }

    void JobScheduler::WorkerLoop(std::size_t workerIndex) noexcept
    {
        if (workerIndex >= m_workerCount)
        {
            std::terminate();
        }

        WorkerStorage& worker = m_workers[workerIndex];

        for (;;)
        {
            ReadyItem item = WaitDequeue();

            if (auto* reference = std::get_if<SessionReference>(&item))
            {
                // WaitDequeue는 정지 상태에서 큐가 비면 빈 참조를 반환한다.

                if (!*reference)
                {
                    return;
                }

                ProcessSession(std::move(*reference), worker);

                continue;
            }

            auto& finalization = std::get<SessionFinalization>(item);

            if (!finalization)
            {
                std::terminate();
            }

            ProcessFinalization(std::move(finalization));
        }
    }

    ReadyItem JobScheduler::WaitDequeue() noexcept
    {
        std::unique_lock<std::mutex> lock{ m_mutex };

        m_condition.wait(lock, [this]
            {
                // 처리할 항목이 있거나, 더 이상 실행 상태가 아니면 대기를 끝낸다.
                return m_readyCount != 0 || m_state != State::Running;
            });

        // 정지 요청으로 깨어났고 더 처리할 항목도 없으면 워커를 종료시킨다.
        if (m_readyCount == 0)
        {
            return ReadyItem{};
        }

        ReadyItem item = std::move(m_readyItems[m_readyHead]);

        // 이동된 원본 대안을 제거하고 빈 슬롯 표현으로 통일한다.
        m_readyItems[m_readyHead].emplace<SessionReference>();

        ++m_readyHead;

        if (m_readyHead == m_readyCapacity)
        {
            m_readyHead = 0;
        }

        --m_readyCount;

        return item;
    }

    void JobScheduler::ProcessSession(SessionReference reference, WorkerStorage& worker) noexcept
    {
        ClientSession* const session = reference.Get();

        if (session == nullptr ||
            session->m_scheduler != this ||
            !session->m_jobQueue)
        {
            std::terminate();
        }

        SessionJobQueue& queue = *session->m_jobQueue;

        std::span<Job> batch{ worker.jobs.get(), m_maxJobsPerBatch };

        const std::size_t count = queue.DequeueBatch(batch);

        const auto startedAt = std::chrono::steady_clock::now();

        std::size_t completedCount = 0;

        while (completedCount < count)
        {
            // 함수가 반환되면 해당 Job의 데이터·예산·참조 정리가 끝난 상태다.
            ExecuteJob(*session, batch[completedCount]);

            ++completedCount;

            // 최소 한 작업을 처리한 뒤 시간 예산을 확인한다.
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startedAt);

            if (elapsed >= m_jobBatchTime)
            {
                break;
            }
        }

        if (completedCount < count)
        {
            auto remaining = batch.subspan(completedCount, count - completedCount);

            bool cancelRemaining = false;

            {
                std::lock_guard<std::mutex> lock{ session->m_mutex };

                if (session->m_state == ClientSession::State::Closing)
                {
                    cancelRemaining = true;
                }
                else
                {
                    // 종료 상태 확인과 복원을 같은 세션 락 범위에서 처리한다.
                    queue.RestoreBatch(remaining);
                }
            }

            if (cancelRemaining)
            {
                // 데이터·캡처 정리는 세션 락 밖에서 수행한다.
                // ExecuteJob은 Closing의 일반 작업을 취소하고 연결 통지는 유지한다.
                for (Job& job : remaining)
                {
                    ExecuteJob(*session, job);
                }
            }
        }

        ReceiveProcessor* receiveProcessor = nullptr;

        {
            std::lock_guard<std::mutex> lock{ session->m_mutex };

            receiveProcessor = session->m_receiveProcessor;
        }

        // 현재 워커의 reference가 재개 처리 중에도 세션을 보호한다.
        if (receiveProcessor != nullptr)
        {
            receiveProcessor->ResumeReceive(*session);
        }

        // FinishBatch 이후에는 현재 워커가 세션의 직렬 후처리를 계속하지 않는다.
        if (queue.FinishBatch())
        {
            ReadyItem item{
                std::in_place_type<SessionReference>,
                std::move(reference)
            };

            Enqueue(std::move(item));
        }

        // 재예약하지 않았으면 지역 reference 소멸 시 사용 참조를 반환한다.
    }

    void JobScheduler::ExecuteJob(ClientSession& session, Job& job) noexcept
    {
        if (job.GetSession() != &session || !session.m_jobQueue)
        {
            std::terminate();
        }

        JobCompletionScope completion{ *session.m_jobQueue, job };

        // 취소 대상도 completion의 소멸 경로에서 자원과 예산을 반환한다.
        if (!session.TryBeginJob(job))
        {
            return;
        }

        try
        {
            JobData& data = job.GetData();

            if (std::holds_alternative<ConnectedNotification>(data))
            {
                if (m_callbacks.connected)
                {
                    m_callbacks.connected(session);
                }

                return;
            }

            if (auto* packet = std::get_if<Packet>(&data))
            {
                m_callbacks.received(session, std::move(*packet));

                return;
            }

            if (auto* task = std::get_if<std::unique_ptr<SessionTask>>(&data))
            {
                (**task)(session);

                return;
            }

            // 유효한 접수 Job은 위 세 종류 중 하나여야 한다.
            std::terminate();
        }
        catch (...)
        {
            // 서비스 예외가 워커 실행 루프 밖으로 전파되지 않게 한다.
            // 현재 Job을 정리한 뒤 남은 일반 작업은 Closing 정책으로 취소한다.
            session.BeginClose();
        }
    }

    void JobScheduler::ProcessFinalization(SessionFinalization finalization) noexcept
    {
        ClientSession* const session = finalization.Get();

        if (session == nullptr)
        {
            std::terminate();
        }

        bool notifyDisconnected = false;

        {
            std::lock_guard<std::mutex> lock{ session->m_mutex };

            if (session->m_scheduler != this ||
                session->m_state != ClientSession::State::Finalizing)
            {
                std::terminate();
            }

            notifyDisconnected = session->m_connectedNotificationQueued;
        }

        try
        {
            // 서비스 코드는 세션 락을 놓은 상태에서 호출한다.
            if (notifyDisconnected && m_callbacks.disconnected)
            {
                m_callbacks.disconnected(*session);
            }
        }
        catch (...)
        {
            // 종료 통지를 재실행하지 않으며, 예외가 나도 풀 반환을 계속한다.
            // 예외 진단 기록은 이후 엔진의 진단 경로와 연결한다.
        }

        finalization.Finish();

        // 풀 반환 이후에는 session에 접근하지 않는다.
    }
}
