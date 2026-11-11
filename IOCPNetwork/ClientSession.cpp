#include "ClientSession.h"

#include "Detail/SessionReference.h"
#include "Detail/ClientSessionPool.h"
#include "Detail/SessionFinalization.h"
#include "Detail/SessionJobQueue.h"
#include "Detail/Job.h"
#include "Detail/JobScheduler.h"

#include <exception> // for std::terminate
#include <limits>    // for std::numeric_limits
#include <utility> // for std::move, std::in_place_type
#include <variant> // for std::holds_alternative

namespace IOCPNetwork
{
    ClientSession::ClientSession() noexcept = default;

    ClientSession::~ClientSession() = default;

    ConnectionId ClientSession::GetConnectionId() const noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        return m_connectionId;
    }

    void ClientSession::BeginClose() noexcept
    {
        Detail::SessionReference connectionReference;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            switch (m_state)
            {
            case State::Free:
            case State::Closing:
            case State::Finalizing:
            {
                return;
            }

            default:
            {
                break;
            }
            }

            // 종료 전 연결에는 연결 유지용 참조가 반드시 있어야 한다.
            if (!m_connectionReference)
            {
                std::terminate();
            }

            m_state = State::Closing;

            // 참조 수를 줄이지 않고 반환 책임만 지역 변수로 옮긴다.
            connectionReference = std::move(m_connectionReference);
        }

        // 소켓 닫기와 I/O 취소는 이후 이 위치에 연결한다.
        // 접수된 Job은 워커가 Closing 정책에 따라 실행하거나 취소한다.

        // 마지막 참조 반환 경로가 세션 락을 사용할 수 있으므로 락 밖에서 반환한다.
        connectionReference.Reset();
    }

    bool ClientSession::InitializeJobQueue(
        std::size_t maxPendingJobs,
        std::size_t maxPendingPacketBytes,
        Detail::JobScheduler& scheduler) noexcept
    {
        // 공개되거나 이미 초기화된 세션의 실행 경로를 교체하지 않는다.
        if (m_pool != nullptr ||
            m_state != State::Free ||
            m_jobQueue ||
            m_scheduler != nullptr)
        {
            std::terminate();
        }

        auto jobQueue = Detail::SessionJobQueue::Create(maxPendingJobs, maxPendingPacketBytes);

        if (!jobQueue)
        {
            return false;
        }

        m_jobQueue = std::move(jobQueue);

        m_scheduler = &scheduler;

        return true;
    }

    ClientSession::EnqueueJobResult ClientSession::EnqueueJob(Detail::Job&& job, SessionTask* sourceTask) noexcept
    {
        // 다른 세션의 Job이나 빈 참조를 이 세션에 등록하지 않는다.
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        Detail::JobEnqueueResult result;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (!m_jobQueue || m_scheduler == nullptr)
            {
                std::terminate();
            }

            switch (m_state)
            {
            case State::Preparing:
            {
                // 서비스 준비를 확정하는 연결 통지를 먼저 등록한다.
                if (!isConnectNotify)
                {
                    return EnqueueJobResult::NotReady;
                }

                break;
            }

            case State::Ready:
            {
                // 같은 연결의 준비 완료 통지를 두 번 등록하는 것은 내부 오류다.
                if (isConnectNotify)
                {
                    std::terminate();
                }

                break;
            }

            default:
            {
                return EnqueueJobResult::Closed;
            }
            }

            if (sourceTask != nullptr)
            {
                result = m_jobQueue->Enqueue(std::move(job), *sourceTask);
            }
            else
            {
                result = m_jobQueue->Enqueue(std::move(job));
            }

            if (result.status == Detail::JobEnqueueStatus::QueueFull)
            {
                // Preparing에서는 일반 Job을 받지 않아 통지용 공간이 있어야 한다.
                if (isConnectNotify)
                {
                    std::terminate();
                }

                return EnqueueJobResult::QueueFull;
            }

            if (isConnectNotify)
            {
                // 연결 통지 접수 이력은 최종 종료 통지까지 유지한다.
                m_connectedNotificationQueued = true;

                // 통지가 FIFO에 들어간 뒤에만 일반 작업 접수를 허용한다.
                m_state = State::Ready;
            }
        }

        if (result.readySession)
        {
            Detail::ReadyItem item{ std::in_place_type<Detail::SessionReference>, std::move(result.readySession) };

            // 세션 락을 놓은 뒤 실행 책임을 스케줄러에 전달한다.
            m_scheduler->Enqueue(std::move(item));
        }

        return EnqueueJobResult::Queued;
    }

    bool ClientSession::TryBeginJob(const Detail::Job& job) noexcept
    {
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        std::lock_guard<std::mutex> lock{ m_mutex };

        switch (m_state)
        {
        case State::Ready:
        case State::Draining:
        {
            // 이미 접수된 작업은 정상 종료의 처리 범위에 포함된다.
            return true;
        }

        case State::Closing:
        {
            // 확정된 연결 통지는 유지하고 일반 Packet·Task는 취소한다.
            return isConnectNotify;
        }

        default:
        {
            // 접수된 Job이 보호하는 세션은 이 상태에 있을 수 없다.
            std::terminate();
        }
        }
    }

    Detail::SessionReference ClientSession::AcquireReference() noexcept
    {
        std::size_t count = m_referenceCount.load(std::memory_order_relaxed);

        for (;;)
        {
            // 기존 연결의 참조를 0에서 되살리거나 카운터가 넘치게 할 수는 없다.
            if (count == 0 || count == (std::numeric_limits<std::size_t>::max)())
            {
                std::terminate();
            }

            // 값이 같아도 실패할 수 있으므로 성공할 때까지 다시 시도한다.
            if (m_referenceCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
            {
                return Detail::SessionReference{ this };
            }
        }
    }

    void ClientSession::ReleaseReference() noexcept
    {
        const std::size_t previous = m_referenceCount.fetch_sub(1, std::memory_order_acq_rel);

        // 소유하지 않은 참조를 반환한 내부 오류이다.
        if (previous == 0)
        {
            std::terminate();
        }

        if (previous != 1)
        {
            return;
        }

        Detail::SessionFinalization finalization;

        Detail::ClientSessionPool* pool = nullptr;

        // 마지막 사용 참조를 반환한 경로만 최종 마무리 상태로 전환한다.
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_state != State::Closing ||
                m_pool == nullptr ||
                m_connectionReference)
            {
                // 종료 상태·소속 풀·연결 유지용 참조가 마지막 반환 조건과 일치해야 한다.
                std::terminate();
            }

            m_state = State::Finalizing;

            pool = m_pool;

            // 참조를 다시 늘리지 않고 최종 마무리 책임을 만든다.
            finalization = Detail::SessionFinalization{
                *pool,
                *this,
                m_connectionId
            };
        }

        // 최종 항목의 전달은 세션 락을 해제한 뒤 수행한다.
        pool->DispatchFinalization(std::move(finalization));

        // 인계 후 같은 슬롯이 재대여될 수 있으므로 멤버에 다시 접근하지 않는다.
    }
}
