#pragma once

#include "IOCPBase.h"
#include "NetworkOptions.h"
#include "NetworkTypes.h"

#include <cstddef> // for std::size_t

namespace IOCPNetwork
{
    // TCP 서버의 공개 진입점이다. 내부 자원은 기반 클래스가 단독 소유한다.
    class IOCPServer final : public IOCPBase
    {
    public:
        // 내부 구현을 생성한다. 메모리 확보 실패 시 예외가 발생할 수 있다.
        IOCPNETWORK_API IOCPServer();

        // 내부 구현의 종료 완료 후 자원을 파괴한다.
        IOCPNETWORK_API ~IOCPServer() override;

        // 서버를 시작한다. 패킷 풀 한도는 바이트 단위다.
        [[nodiscard]] IOCPNETWORK_API StartResult Start(
            const ServerOptions& options, std::size_t maxTotalSegmentBytes) noexcept;

        // 외부 제어 스레드에서 종료 완료까지 기다린다.
        [[nodiscard]] IOCPNETWORK_API StopResult Stop() noexcept;
    };
}
