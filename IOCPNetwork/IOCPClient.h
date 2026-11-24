#pragma once

#include "IOCPBase.h"
#include "NetworkOptions.h"
#include "NetworkTypes.h"

namespace IOCPNetwork
{
    // 서버 하나에 접속하는 클라이언트의 공개 진입점이다.
    class IOCPClient final : public IOCPBase
    {
    public:
        // 내부 구현 생성 시 메모리 확보 예외가 발생할 수 있다.
        IOCPNETWORK_API IOCPClient();

        // 연결과 워커의 종료를 완료한 뒤 내부 구현을 파괴한다.
        IOCPNETWORK_API ~IOCPClient() override;

        // 비동기 연결 요청을 시작한다. 성공은 연결 완료를 뜻하지 않는다.
        [[nodiscard]] IOCPNETWORK_API StartResult Start(
            const ClientOptions& options) noexcept;

        // 외부 제어 스레드에서 종료 완료까지 기다린다.
        [[nodiscard]] IOCPNETWORK_API StopResult Stop() noexcept;
    };
}
