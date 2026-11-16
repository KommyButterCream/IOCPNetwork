#pragma once

namespace IOCPNetwork
{
    class ClientSession;
}

namespace IOCPNetwork::Detail
{
    // 현재 스레드에서 실행 중인 Job의 세션을 표시한다.
    // 세션의 수명은 실행 중인 Job과 스케줄러의 참조가 보호한다.
    class JobExecutionScope final
    {
    public:
        explicit JobExecutionScope(ClientSession& session) noexcept
            : m_session(&session)
            , m_previous(s_current)
        {
            s_current = this;
        }

        ~JobExecutionScope() noexcept
        {
            s_current = m_previous;
        }

        JobExecutionScope(const JobExecutionScope&) = delete;
        JobExecutionScope& operator=(const JobExecutionScope&) = delete;

        JobExecutionScope(JobExecutionScope&&) = delete;
        JobExecutionScope& operator=(JobExecutionScope&&) = delete;

        // 현재 실행 중인 Job이 해당 세션의 작업인지 확인한다.
        [[nodiscard]] static bool IsExecuting(const ClientSession& session) noexcept
        {
            return s_current != nullptr && s_current->m_session == &session;
        }

    private:
        // 소유권 없이 실행 대상만 기록한다.
        const ClientSession* const m_session;

        // 중첩 실행이 끝나면 이전 실행 범위를 복원한다.
        JobExecutionScope* const m_previous;

        inline static thread_local JobExecutionScope* s_current = nullptr;
    };
}
