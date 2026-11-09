#pragma once

#include <cstdint> // for std::uint32_t, std::uint64_t

namespace IOCPNetwork
{
    // 세션 배열 위치와 연결 세대로 특정 연결을 식별한다.
    struct ConnectionId
    {
        std::uint32_t index = 0;
        std::uint64_t generation = 0;

        // 발급 가능한 형태인지 확인한다. 실제 연결의 생존 여부는 검사하지 않는다.
        [[nodiscard]] constexpr bool IsValid() const noexcept
        {
            return generation != 0;
        }

        // 인덱스와 세대가 모두 같아야 같은 연결 ID 이다.
        [[nodiscard]] constexpr bool operator==(const ConnectionId& other) const noexcept = default;
    };
}
