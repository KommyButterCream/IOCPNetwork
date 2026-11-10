#include "SessionFinalization.h"

#include "ClientSessionPool.h"

#include <exception> // for std::terminate
#include <utility>   // for std::exchange

namespace IOCPNetwork::Detail
{
    SessionFinalization::~SessionFinalization()
    {
        if (m_session != nullptr)
        {
            // 종료 통지·정리를 생략한 채 책임을 잃어버릴 수는 없다.
            std::terminate();
        }
    }

    SessionFinalization::SessionFinalization(
        SessionFinalization&& other) noexcept
        : m_pool(std::exchange(other.m_pool, nullptr))
        , m_session(std::exchange(other.m_session, nullptr))
        , m_connectionId(std::exchange(other.m_connectionId, ConnectionId{}))
    {
    }

    SessionFinalization& SessionFinalization::operator=(SessionFinalization&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        if (m_session != nullptr)
        {
            // 기존 최종 마무리를 끝내지 않고 다른 대상으로 덮어쓰는 내부 오류다.
            std::terminate();
        }

        m_pool = std::exchange(other.m_pool, nullptr);

        m_session = std::exchange(other.m_session, nullptr);

        m_connectionId = std::exchange(other.m_connectionId, ConnectionId{});

        return *this;
    }

    SessionFinalization::operator bool() const noexcept
    {
        return m_session != nullptr;
    }

    ClientSession* SessionFinalization::Get() const noexcept
    {
        return m_session;
    }

    void SessionFinalization::Finish() noexcept
    {
        if (m_session == nullptr)
        {
            return;
        }

        // 풀 반환 전에 자신을 비워 중복 반환을 막는다.
        ClientSessionPool* pool = std::exchange(m_pool, nullptr);

        ClientSession* session = std::exchange(m_session, nullptr);

        const ConnectionId id = std::exchange(m_connectionId, ConnectionId{});

        pool->ReturnSession(*session, id);

        // 이 시점부터 같은 슬롯이 재대여될 수 있으므로 세션에 접근하지 않는다.
    }

    SessionFinalization::SessionFinalization(
        ClientSessionPool& pool,
        ClientSession& session,
        ConnectionId id) noexcept
        : m_pool(&pool)
        , m_session(&session)
        , m_connectionId(id)
    {

    }
}
