#include "ClientSession.h"

#include "Detail/SessionReference.h"
#include "Detail/ClientSessionPool.h"
#include "Detail/SessionFinalization.h"

#include <exception> // for std::terminate
#include <limits>    // for std::numeric_limits
#include <utility>   // for std::move

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

        // 이후 소켓·I/O·Job 종료 처리는 이 위치에서 연결한다.
        // 연결 유지용 참조는 해당 조치를 마칠 때까지 지역 변수로 유지한다.

        // 마지막 참조 반환 경로가 세션 락을 사용할 수 있으므로 락 밖에서 반환한다.
        connectionReference.Reset();
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
