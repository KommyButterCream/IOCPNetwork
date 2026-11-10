#include "ClientSessionPool.h"

#include "../ClientSession.h"
#include "ConnectionGeneration.h"

#include <exception> // for std::terminate
#include <cstddef> // for std::size_t
#include <limits>  // for std::numeric_limits
#include <new>     // for std::bad_alloc
#include <utility> // for std::move

namespace IOCPNetwork::Detail
{
    std::unique_ptr<ClientSessionPool> ClientSessionPool::Create(
        std::uint32_t capacity,
        FinalizationHandler finalizationHandler,
        void* finalizationContext) noexcept
    {
        if (capacity == 0 || finalizationHandler == nullptr)
        {
            return {};
        }

        if (capacity > (std::numeric_limits<std::size_t>::max)() / sizeof(ClientSession))
        {
            return {};
        }

        try
        {
            std::vector<std::uint32_t> freeIndices;

            if (capacity > freeIndices.max_size())
            {
                return {};
            }

            // 이후 대여·반환에서는 스택의 메모리를 다시 확보하지 않는다.
            freeIndices.reserve(capacity);

            // private 생성자에 접근할 수 있는 풀의 멤버 함수에서 직접 생성한다.
            auto sessions = std::unique_ptr<ClientSession[]>{ new ClientSession[capacity] };

            // 처음에는 인덱스 0부터 대여하도록 역순으로 넣는다.
            for (std::uint32_t remaining = capacity; remaining != 0; --remaining)
            {
                freeIndices.push_back(remaining - 1);
            }

            return std::unique_ptr<ClientSessionPool>{
                new ClientSessionPool(
                    std::move(sessions),
                    std::move(freeIndices),
                    capacity,
                    finalizationHandler,
                    finalizationContext)
            };
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    ClientSessionPool::ClientSessionPool(
        std::unique_ptr<ClientSession[]> sessions,
        std::vector<std::uint32_t> freeIndices,
        std::uint32_t capacity,
        FinalizationHandler finalizationHandler,
        void* finalizationContext) noexcept
        : m_sessions(std::move(sessions))
        , m_capacity(capacity)
        , m_finalizationHandler(finalizationHandler)
        , m_finalizationContext(finalizationContext)
        , m_freeIndices(std::move(freeIndices))
    {
        // 외부에 풀이 공개되기 전에 각 세션의 소속 풀을 연결한다.
        for (std::uint32_t index = 0; index < m_capacity; ++index)
        {
            m_sessions[index].m_pool = this;
        }
    }

    ClientSessionPool::~ClientSessionPool()
    {
        // 모든 호출자·워커가 종료되어 풀에 동시 접근하지 않는 상태여야 한다.
        // 대여 중이거나 최종 마무리가 남은 세션을 가진 풀은 파괴할 수 없다.
        if (m_freeIndices.size() != m_capacity)
        {
            std::terminate();
        }
    }

    SessionReference ClientSessionPool::AcquireSession() noexcept
    {
        std::lock_guard<std::mutex> poolLock{ m_mutex };

        if (m_freeIndices.empty())
        {
            return {};
        }

        const std::uint64_t generation = AllocateConnectionGeneration();

        if (generation == 0)
        {
            return {};
        }

        const std::uint32_t index = m_freeIndices.back();

        ClientSession& session = m_sessions[index];

        std::lock_guard<std::mutex> sessionLock{ session.m_mutex };

        // 빈 인덱스 스택에는 정리가 완료된 세션만 들어 있어야 한다.
        if (session.m_state != ClientSession::State::Free ||
            session.m_referenceCount.load(std::memory_order_relaxed) != 0 ||
            session.m_connectionReference)
        {
            std::terminate();
        }

        m_freeIndices.pop_back();

        session.m_connectionId = ConnectionId{ index, generation };

        session.m_state = ClientSession::State::Preparing;

        // Free 슬롯을 새 연결로 대여할 때만 최초 참조를 설정한다.
        session.m_referenceCount.store(1, std::memory_order_relaxed);

        // 최초 참조 하나의 반환 책임을 연결 유지용 참조에 맡긴다.
        session.m_connectionReference = SessionReference{ &session };

        // 호출자가 초기화 작업을 마칠 때까지 사용할 별도 참조를 확보한다.
        return session.AcquireReference();
    }

    SessionReference ClientSessionPool::TryAcquireSession(ConnectionId id) noexcept
    {
        // 세대 0 또는 배열 범위를 벗어난 ID는 세션에 접근하지 않고 거절한다.
        if (!id.IsValid() || id.index >= m_capacity)
        {
            return {};
        }

        std::lock_guard<std::mutex> poolLock{ m_mutex };

        ClientSession& session = m_sessions[id.index];

        std::lock_guard<std::mutex> sessionLock{ session.m_mutex };

        // 같은 배열 위치라도 다른 연결로 재대여됐다면 거절한다.
        if (session.m_connectionId != id)
        {
            return {};
        }

        switch (session.m_state)
        {
        case ClientSession::State::Preparing:
        case ClientSession::State::Ready:
        case ClientSession::State::Draining:
        {
            // 연결 유지용 참조가 존재하는 상태에서 새 사용 참조를 확보한다.
            return session.AcquireReference();
        }

        default:
        {
            // Free·Closing·Finalizing 상태에서는 새 조회 참조를 발급하지 않는다.
            return {};
        }
        }
    }

    void ClientSessionPool::DispatchFinalization(SessionFinalization finalization) noexcept
    {
        m_finalizationHandler(m_finalizationContext, std::move(finalization));

        // 인계된 항목은 다른 스레드에서 이미 완료됐을 수 있으므로 세션에 접근하지 않는다.
    }

    void ClientSessionPool::ReturnSession(ClientSession& session, ConnectionId expectedId) noexcept
    {
        std::lock_guard<std::mutex> poolLock{ m_mutex };

        // 이 풀의 해당 슬롯을 반환하는 요청인지 먼저 확인한다.
        if (!expectedId.IsValid() ||
            expectedId.index >= m_capacity ||
            &session != &m_sessions[expectedId.index])
        {
            std::terminate();
        }

        std::lock_guard<std::mutex> sessionLock{ session.m_mutex };

        // 같은 연결의 최종 마무리가 끝난 경우에만 반환할 수 있다.
        if (session.m_connectionId != expectedId ||
            session.m_state != ClientSession::State::Finalizing ||
            session.m_referenceCount.load(std::memory_order_relaxed) != 0 ||
            session.m_connectionReference)
        {
            std::terminate();
        }

        // 정상적인 반환에서는 빈 인덱스 수가 전체 용량을 넘지 않는다.
        if (m_freeIndices.size() >= m_capacity)
        {
            std::terminate();
        }

        // 이전 연결의 ID를 무효화하고 새 연결을 대여할 수 있게 한다.
        session.m_connectionId = {};

        session.m_state = ClientSession::State::Free;

        // 생성 시 전체 용량을 확보했으므로 여기서는 추가 할당이 필요 없다.
        m_freeIndices.push_back(expectedId.index);
    }
}
