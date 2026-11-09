#pragma once

#include "Export.h"
#include "PacketView.h"

namespace IOCPNetwork
{
    namespace Detail
    {
        struct PacketBlock;
    }

    class PacketWriter;

    // 완성된 패킷 저장소를 공유 소유한다.
    // 복사는 저장소 공유, 이동은 소유권 이전이며 본문을 복사하지 않는다.
    class IOCPNETWORK_API Packet
    {
    public:
        // 저장소를 소유하지 않는 빈 패킷을 만든다.
        Packet() noexcept = default;

        // 저장소 참조를 반환하고 마지막 참조이면 풀에 돌려준다.
        ~Packet();

        // 같은 저장소를 공유한다.
        Packet(const Packet& other) noexcept;
        Packet& operator=(const Packet& other) noexcept;

        // 저장소 소유권을 이전하고 원본을 비운다.
        Packet(Packet&& other) noexcept;
        Packet& operator=(Packet&& other) noexcept;

        // 저장소를 소유하고 있는지 확인한다.
        [[nodiscard]] explicit operator bool() const noexcept;

        // 패킷 ID와 읽기 전용 본문을 빌린다.
        [[nodiscard]] PacketView View() const noexcept;

        // 현재 저장소 참조를 반환하고 빈 상태가 된다.
        void Reset() noexcept;

    private:
        friend class PacketWriter;

        // Writer가 넘긴 기존 참조를 증가 없이 인수한다.
        explicit Packet(Detail::PacketBlock* block) noexcept;

        Detail::PacketBlock* m_block = nullptr;
    };
}
