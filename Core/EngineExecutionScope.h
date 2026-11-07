#pragma once
#include "../../Core/Util/Logger.h"
#include <exception> // for std::exception
#include <utility> // for std::forward

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

// 서비스 예외를 워커·타이머 경계 밖으로 전달하지 않는다. 실패 후 연결 처리는 호출자가 결정한다.
template<class Callback>
[[nodiscard]] bool InvokeServiceCallback(const char* name, Callback&& callback) noexcept
{
    EngineExecutionScope execution;
    try
    {
        std::forward<Callback>(callback)();
        return true;
    }
    catch (const std::exception& error)
    {
        LOGE("service callback %s failed: %s", name, error.what());
    }
    catch (...)
    {
        LOGE("service callback %s failed with an unknown exception", name);
    }
    return false;
}
