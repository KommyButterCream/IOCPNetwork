#pragma once

namespace IOCPNetwork::Detail
{
    // 현재 스레드가 엔진 내부 실행 범위에 있음을 표시한다.
    // 워커 실행과 시작·종료 처리 중 동기식 종료 재진입을 판별한다.
    class EngineExecutionScope final
    {
    public:
        EngineExecutionScope() noexcept
            : m_previous(s_current)
        {
            s_current = this;
        }

        ~EngineExecutionScope() noexcept
        {
            s_current = m_previous;
        }

        EngineExecutionScope(const EngineExecutionScope&) = delete;
        EngineExecutionScope& operator=(const EngineExecutionScope&) = delete;

        EngineExecutionScope(EngineExecutionScope&&) = delete;
        EngineExecutionScope& operator=(EngineExecutionScope&&) = delete;

        // 현재 스레드에 활성화된 엔진 실행 범위가 있는지 확인한다.
        [[nodiscard]] static bool IsActive() noexcept
        {
            return s_current != nullptr;
        }

    private:
        // 중첩 범위가 끝나면 이전 실행 표시를 복원한다.
        EngineExecutionScope* const m_previous;

        // 스레드별 실행 표시이며 객체 소유권은 갖지 않는다.
        inline static thread_local EngineExecutionScope* s_current = nullptr;
    };
}
