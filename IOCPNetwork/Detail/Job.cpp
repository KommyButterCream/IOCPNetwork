#include "Job.h"

#include <cassert> // for assert
#include <utility> // for std::move, std::in_place_type
#include <variant> // for std::variant, std::monostate, std::get

namespace IOCPNetwork::Detail
{
    Job::Job(SessionReference&& session, ConnectedNotification notification) noexcept
        : m_session(std::move(session))
        , m_data(std::in_place_type<ConnectedNotification>, notification)
    {
        assert(m_session);
    }

    Job::Job(SessionReference&& session, Packet&& packet) noexcept
        : m_session(std::move(session))
        , m_data(std::in_place_type<Packet>, std::move(packet))
    {
        assert(m_session);
        assert(std::get<Packet>(m_data));
    }

    Job::Job(SessionReference&& session, std::unique_ptr<SessionTask>&& task) noexcept
        : m_session(std::move(session))
        , m_data(std::in_place_type<std::unique_ptr<SessionTask>>, std::move(task))
    {
        assert(m_session);
        assert(std::get<std::unique_ptr<SessionTask>>(m_data));
    }

    Job::Job(Job&& other) noexcept
        : m_session(std::move(other.m_session))
        , m_data(std::move(other.m_data))
    {
        other.ClearData();
    }

    Job& Job::operator=(Job&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        Reset();

        m_session = std::move(other.m_session);

        m_data = std::move(other.m_data);

        other.ClearData();

        return *this;
    }

    ClientSession* Job::GetSession() const noexcept
    {
        return m_session.Get();
    }

    JobData& Job::GetData() noexcept
    {
        return m_data;
    }

    const JobData& Job::GetData() const noexcept
    {
        return m_data;
    }

    void Job::ClearData() noexcept
    {
        m_data.emplace<std::monostate>();
    }

    void Job::Reset() noexcept
    {
        ClearData();

        m_session.Reset();
    }
}
