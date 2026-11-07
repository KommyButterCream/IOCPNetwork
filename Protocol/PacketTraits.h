#pragma once

#include "PacketHeader.h"
#include <concepts> // for std::same_as, std::convertible_to
#include <cstddef> // for offsetof
#include <type_traits> // for std::is_standard_layout_v, std::is_trivially_copyable_v

// 고정 길이 전송 형식만 명시적으로 등록합니다. 포인터·컨테이너를 포함한 타입은 등록하지 않습니다.
// 특수화에서 static constexpr uint16_t id를 정의하며, 양쪽 서비스가 같은 전송 형식을 사용합니다.
template<typename Packet> struct PacketTraits;

template<typename Packet>
concept FixedServicePacket = std::is_standard_layout_v<Packet> &&
    std::is_trivially_copyable_v<Packet> && requires(Packet packet)
    {
        { PacketTraits<Packet>::id } -> std::convertible_to<uint16_t>;
        { packet.header } -> std::same_as<PACKET_HEADER&>;
    } && offsetof(Packet, header) == 0 && sizeof(Packet) <= PACKET_SIZE_LIMIT &&
    IsServicePacketId(PacketTraits<Packet>::id);
