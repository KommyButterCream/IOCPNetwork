#pragma once

#include "Job.h"

#include <cstddef> // for std::size_t
#include <memory>  // for std::unique_ptr
#include <mutex>   // for std::mutex
#include <span>    // for std::span

namespace IOCPNetwork::Detail
{
    // 큐의 접수 결과다. 연결 상태에 따른 거절은 세션에서 판단한다.
    enum class JobEnqueueStatus
    {
        Queued,
        QueueFull
    };

    // 최초 실행 예약일 때만 준비 큐에 전달할 참조를 소유한다.
    struct JobEnqueueResult final
    {
        JobEnqueueStatus status = JobEnqueueStatus::QueueFull;

        SessionReference readySession;
    };

    // 작업 실행 전에 기록하고 완료 시 반환할 접수 예산이다.
    struct JobBudget final
    {
        std::size_t jobCount = 0;

        std::size_t packetBytes = 0;
    };

    // 접수된 Job의 데이터를 이동하거나 정리하기 전에 예산을 읽는다.
    [[nodiscard]] JobBudget GetJobBudget(const Job& job) noexcept;

    // 세션의 Job을 원형 배열에 FIFO로 보관한다.
    // 미완료 작업 예산과 실행 예약 상태를 같은 락으로 보호한다.
    class SessionJobQueue final
    {
    public:
        // 일반 작업 한도에 연결 통지용 여유분 하나를 더해 확보한다.
        // 잘못된 용량이나 메모리 부족이면 nullptr을 반환한다.
        [[nodiscard]] static std::unique_ptr<SessionJobQueue> Create(
            std::size_t maxPendingJobs, std::size_t maxPendingPacketBytes) noexcept;

        // 모든 작업과 실행 예약을 정리한 뒤 파괴해야 한다.
        ~SessionJobQueue() = default;

        SessionJobQueue(const SessionJobQueue&) = delete;
        SessionJobQueue& operator=(const SessionJobQueue&) = delete;

        SessionJobQueue(SessionJobQueue&&) = delete;
        SessionJobQueue& operator=(SessionJobQueue&&) = delete;

        // Packet 또는 연결 통지를 FIFO에 등록한다.
        // 포화 시 Job을 유지하고, 최초 예약 시 준비 항목용 참조를 반환한다.
        [[nodiscard]] JobEnqueueResult Enqueue(Job&& job) noexcept;

        // 빈 Task 저장소가 들어 있는 준비 Job을 접수한다.
        // 성공할 때만 원본 Task를 인수하며, 포화 시 두 인자를 유지한다.
        [[nodiscard]] JobEnqueueResult Enqueue(Job&& preparedTaskJob, SessionTask& sourceTask) noexcept;

        // FIFO 앞부분을 비어 있는 배치 슬롯으로 이동하고 인출 수를 반환한다.
        // 미완료 작업 예산과 실행 예약은 유지하며, 예약을 맡은 워커만 호출한다.
        [[nodiscard]] std::size_t DequeueBatch(std::span<Job> destination) noexcept;

        // 미실행 배치를 기존 순서대로 큐 앞에 복원한다.
        // 접수 예산과 실행 예약은 유지하며, 배치를 인출한 워커만 호출한다.
        void RestoreBatch(std::span<Job> remainingJobs) noexcept;

        // 데이터 정리가 끝난 작업의 접수 예산을 한 번 반환한다.
        // Job의 세션 참조는 이 호출이 끝날 때까지 유지해야 한다.
        void CompleteJob(JobBudget budget) noexcept;

        // 배치 정리와 세션 후처리가 끝난 뒤 예약을 마무리한다.
        // true이면 기존 워커 참조로 재예약하고, false이면 예약을 해제한다.
        [[nodiscard]] bool FinishBatch() noexcept;

        // 대기 작업·미완료 예산·실행 예약이 모두 없는지 확인한다.
        // 결과는 조회 시점의 상태이며 새 접수를 차단하지 않는다.
        [[nodiscard]] bool IsIdle() const noexcept;

    private:
        SessionJobQueue(
            std::unique_ptr<Job[]> jobs,
            std::size_t capacity,
            std::size_t maxPendingJobs,
            std::size_t maxPendingPacketBytes) noexcept;

        // 등록과 예약을 공통 처리한다.
        // sourceTask가 있으면 접수 확정 후 빈 Task 저장소와 교환한다.
        [[nodiscard]] JobEnqueueResult EnqueueImpl(Job&& job, SessionTask* sourceTask) noexcept;

        // 시작 시 확보하고 세션 재대여 시에도 재사용하는 Job 슬롯이다.
        std::unique_ptr<Job[]> m_jobs;

        // 실제 배열 크기는 일반 작업 한도보다 하나 크다.
        const std::size_t m_capacity;

        // 큐·인출 배치·실행 중인 일반 작업에 적용할 한도다.
        const std::size_t m_maxPendingJobs;
        const std::size_t m_maxPendingPacketBytes;

        // 배열 위치, 미완료 예산, 실행 예약 상태를 보호한다.
        mutable std::mutex m_mutex;

        // 다음 인출 위치와 다음 등록 위치다.
        std::size_t m_head = 0;
        std::size_t m_tail = 0;

        // 현재 배열에 보관 중인 Job 수이며 연결 통지를 포함한다.
        std::size_t m_queuedCount = 0;

        // 아직 완료하지 않은 일반 Job 수와 패킷 전체 프레임 바이트다.
        std::size_t m_pendingJobCount = 0;
        std::size_t m_pendingPacketBytes = 0;

        // 준비 큐 등록부터 워커의 직렬 후처리까지 실행 예약을 유지한다.
        bool m_scheduled = false;
    };
}
