#pragma once

#include <cstddef> // for std::size_t
#include <cstdint> // for std::uint16_t

namespace IOCPNetwork
{
    // 시스템, 서비스 패킷을 구분하는 전송 ID
    using PacketId = std::uint16_t;

    // 패킷 크기는 공통 헤더를 포함하고, 본문 크기는 헤더를 제외한다.
    inline constexpr std::size_t PacketHeaderSize = 4;
    inline constexpr std::size_t MaxPacketSize = 65'535;
    inline constexpr std::size_t MaxPayloadSize = MaxPacketSize - PacketHeaderSize;

    // ID 0은 사용하지 않는다.
    inline constexpr PacketId InvalidPacketId = 0;

    // 엔진 내부 시스템 패킷에 예약된 범위
    inline constexpr PacketId FirstSystemPacketId = 1;
    inline constexpr PacketId LastSystemPacketId = 999;

    // 서비스에서 생성·등록할 수 있는 패킷 ID 범위
    inline constexpr PacketId FirstServicePacketId = 1'000;
    inline constexpr PacketId LastServicePacketId = 65'535;

    // 엔진 내부 시스템 패킷에 예약된 ID인지 확인한다.
    [[nodiscard]] constexpr bool IsSystemPacketId(PacketId packetId) noexcept
    {
        return packetId >= FirstSystemPacketId && packetId <= LastSystemPacketId;
    }

    // 서비스에서 사용할 수 있는 ID인지 확인한다.
    [[nodiscard]] constexpr bool IsServicePacketId(PacketId packetId) noexcept
    {
        return packetId >= FirstServicePacketId && packetId <= LastServicePacketId;
    }
}
