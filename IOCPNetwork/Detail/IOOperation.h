#pragma once

#include <WinSock2.h> // for OVERLAPPED

#include <cstddef>     // for offsetof
#include <type_traits> // for std::is_standard_layout_v

namespace IOCPNetwork::Detail
{
    // 완료된 비동기 작업의 종류를 구분한다.
    enum class IOOperationType
    {
        Accept,
        Connect,
        Receive,
        Send
    };

    // 운영체제에 전달할 OVERLAPPED와 엔진의 작업 종류를 함께 보관한다.
    // 발행된 작업의 완료 처리가 끝날 때까지 주소를 유지해야 한다.
    struct IOOperation final
    {
        explicit IOOperation(IOOperationType operationType) noexcept
            : type(operationType)
        {
        }

        IOOperation(const IOOperation&) = delete;
        IOOperation& operator=(const IOOperation&) = delete;

        IOOperation(IOOperation&&) = delete;
        IOOperation& operator=(IOOperation&&) = delete;

        // 완료 시 받은 OVERLAPPED 주소에서 원래 작업을 찾도록 첫 멤버로 둔다.
        OVERLAPPED overlapped{};

        // 같은 저장소를 재사용하더라도 작업의 종류는 바뀌지 않는다.
        const IOOperationType type;
    };

    // 첫 멤버 주소를 이용할 수 있는 표준 레이아웃인지 검사한다.
    static_assert(std::is_standard_layout_v<IOOperation>);

    // overlapped가 객체 시작 위치에 있는지 검사한다.
    static_assert(offsetof(IOOperation, overlapped) == 0);
}
