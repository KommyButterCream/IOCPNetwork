#pragma once

#include <functional> // for std::function

namespace IOCPNetwork
{
    class ClientSession;

    // 해당 세션의 Job 실행 순서에 맞춰 호출할 서비스 작업이다.
    using SessionTask = std::function<void(ClientSession&)>;
}
