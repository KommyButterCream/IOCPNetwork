#pragma once

#include <cstdint> // for std::uint8_t

namespace IOCPNetwork::Detail
{
    struct PacketBlock;

    // 패킷 블록의 저장소와 대여·반환을 담당한다.
    // 실제 저장소와 생성 인터페이스는 풀 구현 단계에서 추가한다.
    class PacketPool
    {
    private:
        friend void ReleasePacketReference(PacketBlock* block) noexcept;

        // 제어부가 소멸한 메모리를 원래 크기 등급으로 반환한다.
        void ReturnBlock(void* memory, std::uint8_t sizeClassIndex) noexcept;
    };
}
