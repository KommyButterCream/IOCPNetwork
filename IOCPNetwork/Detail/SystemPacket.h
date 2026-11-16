#pragma once

#include "../PacketTypes.h"

#include <cstddef> // for std::byte, std::size_t, std::to_integer
#include <cstdint> // for std::uint16_t, std::uint64_t
#include <optional> // for std::optional, std::nullopt
#include <span> // for std::span

namespace IOCPNetwork::Detail
{
    // 서버와 클라이언트가 사용하는 엔진 통신 규약의 버전이다.
    inline constexpr std::uint16_t ProtocolVersion = 1;

    // 시스템 패킷 ID는 전송 규격이므로 값을 변경하거나 재사용하지 않는다.
    enum class SystemPacketId : PacketId
    {
        HandshakeRequest = 1,
        HandshakeResponse = 2,
        HeartbeatRequest = 3,
        HeartbeatResponse = 4,
    };

    // 시스템 패킷의 열거형 값을 공통 패킷 ID로 변환한다.
    [[nodiscard]] constexpr PacketId ToPacketId(SystemPacketId packetId) noexcept
    {
        return static_cast<PacketId>(packetId);
    }

    // 아래 크기는 구조체 크기가 아니라 실제 전송할 본문 바이트 수다.
    inline constexpr std::size_t HandshakePayloadSize = 2;
    inline constexpr std::size_t HeartbeatPayloadSize = 8;

    // 요청은 클라이언트 버전, 응답은 서버 버전을 전달한다.
    // 구조체 메모리를 직접 송신하지 않고 리틀엔디언으로 기록한다.
    struct HandshakePayload
    {
        std::uint16_t protocolVersion = ProtocolVersion;
    };

    // 요청마다 새 번호를 보내고, 응답은 받은 번호를 그대로 돌려준다.
    // 0은 사용하지 않으며 현재 대기 중인 요청과 응답을 연결한다.
    struct HeartbeatPayload
    {
        std::uint64_t sequence = 0;
    };

    static_assert(IsSystemPacketId(ToPacketId(SystemPacketId::HandshakeRequest)));
    static_assert(IsSystemPacketId(ToPacketId(SystemPacketId::HandshakeResponse)));
    static_assert(IsSystemPacketId(ToPacketId(SystemPacketId::HeartbeatRequest)));
    static_assert(IsSystemPacketId(ToPacketId(SystemPacketId::HeartbeatResponse)));
    static_assert(PacketHeaderSize + HandshakePayloadSize <= MaxPacketSize);
    static_assert(PacketHeaderSize + HeartbeatPayloadSize <= MaxPacketSize);

    // 본문 길이가 정확히 일치할 때 프로토콜 버전을 읽는다.
    // 버전의 호환 여부는 핸드셰이크 처리 경로에서 판단한다.
    [[nodiscard]] constexpr std::optional<HandshakePayload> ReadHandshakePayload(
        std::span<const std::byte> payload) noexcept
    {
        if (payload.size() != HandshakePayloadSize)
        {
            return std::nullopt;
        }

        const auto version = static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(payload[0]) |
            (std::to_integer<std::uint16_t>(payload[1]) << 8));

        return HandshakePayload{ version };
    }

    // 정확한 크기의 본문에 버전을 리틀엔디언으로 기록한다.
    // 길이가 다르면 아무것도 변경하지 않는다.
    [[nodiscard]] constexpr bool WriteHandshakePayload(
        std::span<std::byte> payload, HandshakePayload value) noexcept
    {
        if (payload.size() != HandshakePayloadSize)
        {
            return false;
        }

        payload[0] = static_cast<std::byte>(value.protocolVersion & 0xFFu);

        payload[1] = static_cast<std::byte>(value.protocolVersion >> 8);

        return true;
    }

    // 정확한 크기의 본문에서 요청 번호를 읽는다.
    // 예약된 값인 0은 유효한 요청·응답 번호로 인정하지 않는다.
    [[nodiscard]] constexpr std::optional<HeartbeatPayload> ReadHeartbeatPayload(
        std::span<const std::byte> payload) noexcept
    {
        if (payload.size() != HeartbeatPayloadSize)
        {
            return std::nullopt;
        }

        std::uint64_t sequence = 0;

        for (std::size_t index = 0; index < HeartbeatPayloadSize; ++index)
        {
            const auto byteValue = std::to_integer<std::uint64_t>(payload[index]);

            sequence |= byteValue << (index * 8);
        }

        if (sequence == 0)
        {
            return std::nullopt;
        }

        return HeartbeatPayload{ sequence };
    }

    // 요청 번호를 리틀엔디언으로 기록한다.
    // 길이가 다르거나 번호가 0이면 아무것도 변경하지 않는다.
    [[nodiscard]] constexpr bool WriteHeartbeatPayload(
        std::span<std::byte> payload, HeartbeatPayload value) noexcept
    {
        if (payload.size() != HeartbeatPayloadSize || value.sequence == 0)
        {
            return false;
        }

        for (std::size_t index = 0; index < HeartbeatPayloadSize; ++index)
        {
            payload[index] = static_cast<std::byte>(
                (value.sequence >> (index * 8)) & 0xFFu);
        }

        return true;
    }

}
