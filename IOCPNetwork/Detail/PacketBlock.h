#pragma once

#include "../PacketTypes.h"

#include <atomic> // for std::atomic
#include <cstddef> // for std::byte, std::size_t
#include <cstdint> // for std::uint8_t, std::uint16_t
#include <memory> // for std::shared_ptr

namespace IOCPNetwork::Detail
{
    class PacketPool;

    // this          this + 1
    // [PacketBlock] [전송 헤더] [본문]

    // 통합 블록의 제어부다. 바로 뒤에 전송 헤더와 본문을 배치한다.
    // 저장소를 대여할 때 생성하고 풀에 반환하기 전에 소멸한다.
    struct alignas(16) PacketBlock
    {
        // Writer 또는 Packet이 가진 소유 참조 수.
        std::atomic<std::size_t> referenceCount{ 1 };

        // 블록 반환이 끝날 때까지 원래 풀의 수명을 유지한다.
        std::shared_ptr<PacketPool> pool;

        // Writer에 허용한 본문 용량이며 풀의 여유 공간은 포함하지 않는다.
        std::size_t payloadCapacity = 0;

        // 작성할 ID와 완성된 전체 전송 길이다.
        PacketId packetId = InvalidPacketId;
        std::uint16_t packetSize = 0;

        // 원래 크기 등급으로 블록을 반환할 때 사용한다.
        std::uint8_t sizeClassIndex = 0;

        // 제어부 바로 뒤에 배치된 전송 헤더의 시작 주소.
        [[nodiscard]] std::byte* GetWireData() noexcept
        {
            // sizeof(PacketBlock)만큼 뒤의 주소를 반환.
            return reinterpret_cast<std::byte*>(this + 1);
        }

        [[nodiscard]] const std::byte* GetWireData() const noexcept
        {
            // sizeof(PacketBlock)만큼 뒤의 주소를 반환.
            return reinterpret_cast<const std::byte*>(this + 1);
        }
    };

    // 호출자가 소유 중인 유효한 블록의 참조를 하나 추가한다.
    void AddPacketReference(PacketBlock* block) noexcept;

    // 호출자의 소유 참조를 반환하고 마지막 참조이면 원래 풀로 돌려준다.
    void ReleasePacketReference(PacketBlock* block) noexcept;
}
