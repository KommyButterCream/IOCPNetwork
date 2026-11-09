#pragma once

#include "../PacketTypes.h"

#include <cstddef> // for std::byte, std::to_integer
#include <cstdint> // for std::uint16_t
#include <optional> // for std::optional, std::nullopt
#include <span> // for std::span

namespace IOCPNetwork::Detail
{
    // 전송 바이트를 해석한 값. 구조체의 메모리를 그대로 송신하지 않는다.
    struct PacketHeader
    {
        PacketId packetId = InvalidPacketId;
        std::uint16_t packetSize = 0;
    };

    // 헤더의 형식과 현재 수신 바이트 수를 검사한 결과
    enum class PacketFrameStatus
    {
        Complete,               // 형식상 유효하고 첫 패킷 전체가 도착함
        NeedMoreData,           // 형식상 유효하지만 본문이 덜 도착함
        InvalidPacketId,        // 사용할 수 없는 ID
        InvalidPacketSize,      // 전체 길이가 헤더보다 작거나 수신 상한을 초과함
        InvalidReceiveLimit     // 엔진에 전달된 수신 상한 설정 자체가 잘못됨
    };

    // 앞의 4바이트를 해석한다. 헤더가 부족하면 결과를 반환하지 않는다.
    [[nodiscard]] constexpr std::optional<PacketHeader> ReadPacketHeader(
        std::span<const std::byte> bytes) noexcept
    {
        if (bytes.size() < PacketHeaderSize)
        {
            // 아직 헤더를 읽을 수 없다.
            return std::nullopt;
        }

        const auto packetId = static_cast<PacketId>(
            std::to_integer<std::uint16_t>(bytes[0]) |
            (std::to_integer<std::uint16_t>(bytes[1]) << 8));

        const auto packetSize = static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(bytes[2]) |
            (std::to_integer<std::uint16_t>(bytes[3]) << 8));

        return PacketHeader{ packetId, packetSize };
    }

    // 앞의 4바이트에 헤더를 기록한다. 공간이 부족하면 아무것도 변경하지 않는다.
    [[nodiscard]] constexpr bool WritePacketHeader(
        std::span<std::byte> bytes,
        PacketHeader header) noexcept
    {
        if (bytes.size() < PacketHeaderSize)
        {
            // 아직 헤더를 쓸 수 없다.
            return false;
        }

        bytes[0] = static_cast<std::byte>(header.packetId & 0xFF);
        bytes[1] = static_cast<std::byte>(header.packetId >> 8);

        bytes[2] = static_cast<std::byte>(header.packetSize & 0xFF);
        bytes[3] = static_cast<std::byte>(header.packetSize >> 8);

        return true;
    }

    // 읽은 헤더와 현재 보관 중인 전체 바이트 수로 첫 패킷의 완성 여부를 확인한다.
    [[nodiscard]] constexpr PacketFrameStatus CheckPacketFrame(
        PacketHeader header,
        std::size_t bufferedSize,
        std::size_t maxRecvPacketSize) noexcept
    {
        if (maxRecvPacketSize < PacketHeaderSize || maxRecvPacketSize > MaxPacketSize)
        {
            return PacketFrameStatus::InvalidReceiveLimit;
        }

        if (!IsSystemPacketId(header.packetId) && !IsServicePacketId(header.packetId))
        {
            return PacketFrameStatus::InvalidPacketId;
        }

        if (header.packetSize < PacketHeaderSize || header.packetSize > maxRecvPacketSize)
        {
            return PacketFrameStatus::InvalidPacketSize;
        }

        if (bufferedSize < header.packetSize)
        {
            return PacketFrameStatus::NeedMoreData;
        }

        return PacketFrameStatus::Complete;
    }
}
