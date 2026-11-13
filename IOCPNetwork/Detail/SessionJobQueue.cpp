#include "SessionJobQueue.h"

#include "../ClientSession.h"

#include <algorithm> // for std::min
#include <cassert> // for assert
#include <exception> // for std::terminate
#include <limits> // for std::numeric_limits
#include <mutex> // for std::lock_guard
#include <new> // for std::bad_alloc
#include <utility> // for std::move
#include <variant> // for std::get_if, std::holds_alternative

namespace IOCPNetwork::Detail
{
    JobBudget GetJobBudget(const Job& job) noexcept
    {
        if (job.GetSession() == nullptr)
        {
            std::terminate();
        }

        const JobData& data = job.GetData();

        if (std::holds_alternative<ConnectedNotification>(data))
        {
            return {};
        }

        if (const auto* packet = std::get_if<Packet>(&data))
        {
            if (!*packet)
            {
                std::terminate();
            }

            return { 1, GetPacketWireBytes(*packet).size() };
        }

        if (const auto* task = std::get_if<std::unique_ptr<SessionTask>>(&data))
        {
            // 접수된 Task는 저장소와 실행할 함수를 모두 가지고 있어야 한다.
            if (!*task || !static_cast<bool>(**task))
            {
                std::terminate();
            }

            return { 1, 0 };
        }

        // 빈 Job이나 데이터 정리를 마친 Job에서는 예산을 읽을 수 없다.
        std::terminate();
    }

    std::unique_ptr<SessionJobQueue> SessionJobQueue::Create(
        std::size_t maxPendingJobs, std::size_t maxPendingPacketBytes) noexcept
    {
        if (maxPendingJobs == 0 || maxPendingPacketBytes == 0)
        {
            return {};
        }

        constexpr auto maxSize = (std::numeric_limits<std::size_t>::max)();

        // 연결 통지용 여유분을 더할 때 정수 오버플로가 나지 않게 한다.
        if (maxPendingJobs == maxSize)
        {
            return {};
        }

        const std::size_t capacity = maxPendingJobs + 1;

        // 배열 전체의 바이트 크기가 표현 가능한 범위를 넘지 않게 한다.
        if (capacity > maxSize / sizeof(Job))
        {
            return {};
        }

        try
        {
            auto jobs = std::make_unique<Job[]>(capacity);

            return std::unique_ptr<SessionJobQueue>{
                new SessionJobQueue(std::move(jobs), capacity, maxPendingJobs, maxPendingPacketBytes) };
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    SessionJobQueue::SessionJobQueue(
        std::unique_ptr<Job[]> jobs,
        std::size_t capacity,
        std::size_t maxPendingJobs,
        std::size_t maxPendingPacketBytes) noexcept
        : m_jobs(std::move(jobs))
        , m_capacity(capacity)
        , m_maxPendingJobs(maxPendingJobs)
        , m_maxPendingPacketBytes(maxPendingPacketBytes)
    {
    }

    JobEnqueueResult SessionJobQueue::Enqueue(Job&& job) noexcept
    {
        return EnqueueImpl(std::move(job), nullptr);
    }

    JobEnqueueResult SessionJobQueue::Enqueue(Job&& preparedTaskJob, SessionTask& sourceTask) noexcept
    {
        return EnqueueImpl(std::move(preparedTaskJob), &sourceTask);
    }

    std::size_t SessionJobQueue::DequeueBatch(std::span<Job> destination) noexcept
    {
        if (destination.empty())
        {
            return 0;
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        const std::size_t count = (std::min)(destination.size(), m_queuedCount);

        if (count == 0)
        {
            return 0;
        }

        // 등록된 작업은 준비 큐 대기 또는 워커 실행 예약에 포함되어야 한다.
        assert(m_scheduled);

        // 기존 작업을 덮어쓰거나 락 안에서 자원을 반환하지 않도록 검사한다.
        // 전체 목적지를 먼저 확인한 뒤 이동을 시작한다.
        for (std::size_t index = 0; index < count; ++index)
        {
            if (destination[index].GetSession() != nullptr ||
                !std::holds_alternative<std::monostate>(
                    destination[index].GetData()))
            {
                std::terminate();
            }
        }

        for (std::size_t index = 0; index < count; ++index)
        {
            assert(m_jobs[m_head].GetSession() != nullptr);

            destination[index] = std::move(m_jobs[m_head]);

            ++m_head;

            if (m_head == m_capacity)
            {
                m_head = 0;
            }
        }

        m_queuedCount -= count;

        return count;
    }

    void SessionJobQueue::RestoreBatch(std::span<Job> remainingJobs) noexcept
    {
        if (remainingJobs.empty())
        {
            return;
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        // 배치를 인출한 워커가 실행 예약을 유지한 상태에서 복원해야 한다.
        if (!m_scheduled)
        {
            std::terminate();
        }

        assert(m_queuedCount <= m_capacity);

        // 인출한 작업도 예산을 유지하므로 정상 흐름에서는 복원 공간이 보장된다.
        if (remainingJobs.size() > m_capacity - m_queuedCount)
        {
            std::terminate();
        }

        // 빈 슬롯이나 데이터 정리를 끝낸 Job을 복원하지 않도록 확인한다.
        for (const Job& job : remainingJobs)
        {
            if (job.GetSession() == nullptr ||
                std::holds_alternative<std::monostate>(job.GetData()))
            {
                std::terminate();
            }
        }

        // 뒤쪽 Job부터 앞에 넣어 배치의 원래 FIFO 순서를 유지한다.
        for (std::size_t count = remainingJobs.size(); count != 0; --count)
        {
            if (m_head == 0)
            {
                m_head = m_capacity;
            }

            --m_head;

            // 복원 위치는 비어 있어야 하며 기존 작업을 덮어쓰지 않는다.
            assert(m_jobs[m_head].GetSession() == nullptr);
            assert(std::holds_alternative<std::monostate>(m_jobs[m_head].GetData()));

            m_jobs[m_head] = std::move(remainingJobs[count - 1]);
        }

        m_queuedCount += remainingJobs.size();
    }

    void SessionJobQueue::CompleteJob(JobBudget budget) noexcept
    {
        // 한 번에 Job 하나의 예산만 반환한다.
        if (budget.jobCount > 1 ||
            (budget.jobCount == 0 && budget.packetBytes != 0))
        {
            std::terminate();
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        if (!m_scheduled)
        {
            std::terminate();
        }

        // 잘못된 예산 반환으로 부호 없는 카운터가 언더플로우되지 않게 한다.
        if (budget.jobCount > m_pendingJobCount ||
            budget.packetBytes > m_pendingPacketBytes)
        {
            std::terminate();
        }

        m_pendingJobCount -= budget.jobCount;

        m_pendingPacketBytes -= budget.packetBytes;
    }

    bool SessionJobQueue::FinishBatch() noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        if (!m_scheduled)
        {
            std::terminate();
        }

        // 남은 작업은 다른 실행 차례에 처리한다.
        // 준비 큐로 다시 전달하는 동안에도 예약 상태를 유지한다.
        if (m_queuedCount != 0)
        {
            // 호출자는 기존 워커 참조를 준비 큐로 다시 이동한다.
            return true;
        }

        // 워커가 인출한 일반 작업을 완료하거나 복원하지 않은 상태면 오류다.
        if (m_pendingJobCount != 0 || m_pendingPacketBytes != 0)
        {
            std::terminate();
        }

        m_scheduled = false;

        // 완전히 빈 큐는 인덱스를 초기화해 다음 작업이나 세션 재대여에 사용한다.
        m_head = 0;

        m_tail = 0;

        // 예약 해제를 알린다. 워커 참조는 호출자가 락 밖에서 반환한다.
        return false;
    }

    bool SessionJobQueue::IsIdle() const noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        return m_queuedCount == 0 &&
            m_pendingJobCount == 0 &&
            m_pendingPacketBytes == 0 &&
            !m_scheduled;
    }

    JobEnqueueResult SessionJobQueue::EnqueueImpl(Job&& job, SessionTask* sourceTask) noexcept
    {
        ClientSession* const session = job.GetSession();

        // Packet 작업이면 저장된 Packet의 주소를 얻고, 다른 종류이면 nullptr를 얻는다.
        const auto* packet = std::get_if<Packet>(&job.GetData());

        auto* taskStorage = std::get_if<std::unique_ptr<SessionTask>>(&job.GetData());

        // 세션의 연결 상태가 아니라 현재 Job이 연결 통지인지 확인한다.
        const bool isConnectNotify = std::holds_alternative<ConnectedNotification>(job.GetData());

        // 모든 작업은 유효한 세션 사용 참조를 소유해야 한다.
        if (session == nullptr)
        {
            std::terminate();
        }

        if (sourceTask != nullptr)
        {
            // Task 경로는 할당된 저장소와 실행할 원본 함수를 받아야 한다.
            if (taskStorage == nullptr || !*taskStorage || !*sourceTask)
            {
                std::terminate();
            }

            // 기존 함수를 교체하지 않도록 목적지 저장소는 비어 있어야 한다.
            if (static_cast<bool>(**taskStorage))
            {
                std::terminate();
            }
        }
        else
        {
            // 원본 Task가 없는 경로는 Packet 또는 연결 통지만 받는다.
            if (packet == nullptr && !isConnectNotify)
            {
                std::terminate();
            }
        }

        std::size_t packetBytes = 0;

        if (packet != nullptr)
        {
            if (!*packet)
            {
                std::terminate();
            }

            packetBytes = GetPacketWireBytes(*packet).size();
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        // 연결 통지도 배열 슬롯 하나를 사용한다.
        if (m_queuedCount == m_capacity)
        {
            return {};
        }

        if (!isConnectNotify)
        {
            // Packet과 Task 모두 인출 배치와 실행 중인 작업까지 개수 한도에 포함된다.
            if (m_pendingJobCount == m_maxPendingJobs)
            {
                return {};
            }

            assert(m_pendingPacketBytes <= m_maxPendingPacketBytes);

            // 덧셈 오버플로우 없이 남은 바이트 예산과 비교한다.
            if (packetBytes > m_maxPendingPacketBytes - m_pendingPacketBytes)
            {
                return {};
            }
        }

        SessionReference readySession;

        if (!m_scheduled)
        {
            // Job의 참조와 별도로 준비 큐·워커가 사용할 참조를 확보한다.
            readySession = session->AcquireReference();
        }

        // 비어 있는 슬롯에만 이동하여 락 안에서 기존 작업을 파괴하지 않는다.
        assert(m_jobs[m_tail].GetSession() == nullptr);
        assert(std::holds_alternative<std::monostate>(m_jobs[m_tail].GetData()));

        if (sourceTask != nullptr)
        {
            // 모든 접수 검사를 통과한 뒤에만 원본 함수의 소유권을 인수한다.
            (*taskStorage)->swap(*sourceTask);
        }

        m_jobs[m_tail] = std::move(job);

        ++m_tail;

        if (m_tail == m_capacity)
        {
            m_tail = 0;
        }

        ++m_queuedCount;

        if (!isConnectNotify)
        {
            ++m_pendingJobCount;

            m_pendingPacketBytes += packetBytes;
        }

        m_scheduled = true;

        return { JobEnqueueStatus::Queued, std::move(readySession) };
    }
}
