#pragma once

// 엔진 실행 중 재진입한 동기식 시작·종료는 워커 조인 교착을 만들 수 있다.
// 콜백과 Job에서는 제어 스레드에 요청만 전달하고 직접 시작·종료하지 않는다.
class EngineExecutionScope final
{
public:
    EngineExecutionScope() noexcept { ++s_depth; }
    ~EngineExecutionScope() { --s_depth; }
    EngineExecutionScope(const EngineExecutionScope&) = delete;
    EngineExecutionScope& operator=(const EngineExecutionScope&) = delete;
    [[nodiscard]] static bool IsActive() noexcept { return s_depth != 0; }

private:
    inline static thread_local unsigned int s_depth = 0;
};
