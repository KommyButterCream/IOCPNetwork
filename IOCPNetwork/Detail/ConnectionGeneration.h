#pragma once

#include <cstdint> // for std::uint64_t

namespace IOCPNetwork::Detail
{
    // 같은 DLL 인스턴스 안에서 중복되지 않는 연결 세대를 발급한다.
    // 발급 범위를 모두 사용하면 0을 반환하며, 이전 번호를 재사용하지 않는다.
    [[nodiscard]] std::uint64_t AllocateConnectionGeneration() noexcept;
}
