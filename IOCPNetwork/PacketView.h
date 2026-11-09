#pragma once

#include "PacketTypes.h"

#include <cstddef> // for std::byte
#include <span> // for std::span

namespace IOCPNetwork
{
    // 패킷 ID와 헤더를 제외한 본문을 빌려서 읽는다.
    // 원본 저장소의 수명을 연장하지 않는다.
    struct PacketView
    {
        PacketId id = InvalidPacketId;

        std::span<const std::byte> payload;
    };
}
