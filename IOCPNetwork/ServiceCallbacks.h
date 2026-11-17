#pragma once

#include "NetworkTypes.h"
#include "Packet.h"

#include <functional> // for std::function

namespace IOCPNetwork
{
    class ClientSession;

    // 세션은 호출 동안 빌리고, Packet 은 소유권을 전달받는다.
    // 같은 세션의 핸들러와 Task 는 Job 실행 순서에 따라 직렬 실행한다.
    using PacketHandler = std::function<void(ClientSession& session, Packet packet)>;

    // 서비스가 받을 연결 관련 통지 함수를 보관한다.
    // 비어 있는 콜백은 호출하지 않는다.
    struct ConnectionEvents final
    {
        // 핸드셰이크를 마치고 서비스 통신 준비가 확정된 연결을 알린다.
        std::function<void(ClientSession& session)> connected;

        // 연결 통지가 확정된 연결의 마지막 정리 단계에서 한 번 호출한다.
        // 세션은 호출 동안만 유효하며 새 통신 작업을 접수할 수 없다.
        std::function<void(ClientSession& session, DisconnectInfo info)> disconnected;

        // 클라이언트의 연결 준비가 확정되기 전에 실패한 시도를 알린다.
        // 사용할 수 있는 세션을 전달하지 않으며 서버에서는 호출하지 않는다.
        std::function<void(DisconnectInfo info)> connectFailed;
    };
}
