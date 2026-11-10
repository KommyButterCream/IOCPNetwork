#include "SessionReference.h"

#include "../ClientSession.h"

#include <utility> // for std::exchange

namespace IOCPNetwork::Detail
{
    SessionReference::~SessionReference()
    {
        Reset();
    }

    SessionReference::SessionReference(SessionReference&& other) noexcept
        : m_session(std::exchange(other.m_session, nullptr))
    {
    }

    SessionReference& SessionReference::operator=(SessionReference&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        ClientSession* newSession = std::exchange(other.m_session, nullptr);

        ClientSession* oldSession = std::exchange(m_session, newSession);

        // 새 소유 상태를 확정한 뒤 기존 참조를 반환한다.
        if (oldSession != nullptr)
        {
            oldSession->ReleaseReference();
        }

        return *this;
    }

    SessionReference::operator bool() const noexcept
    {
        return m_session != nullptr;
    }

    ClientSession* SessionReference::Get() const noexcept
    {
        return m_session;
    }

    void SessionReference::Reset() noexcept
    {
        // 마지막 반환으로 최종 정리가 시작되더라도 핸들은 이미 비어 있다.
        ClientSession* session = std::exchange(m_session, nullptr);

        if (session != nullptr)
        {
            session->ReleaseReference();
        }
    }

    SessionReference::SessionReference(ClientSession* session) noexcept
        : m_session(session)
    {
    }
}
