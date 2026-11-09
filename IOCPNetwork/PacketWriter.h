#pragma once

#include "Export.h"

#include <cstddef> // for std::byte, std::size_t
#include <span> // for std::span

namespace IOCPNetwork
{
    namespace Detail
    {
        struct PacketBlock;
        class PacketPool;
    }

    struct PacketResult;

    // 작성 중인 패킷 저장소를 단독 소유한다.
    // Finish 성공 시 저장소를 Packet으로 이전한다.
    class IOCPNETWORK_API PacketWriter
    {
    public:
        // 저장소를 소유하지 않는 빈 Writer를 만든다.
        PacketWriter() noexcept = default;

        // 미완성 저장소를 풀에 반환한다.
        ~PacketWriter();

        // 작성 권한은 공유할 수 없다.
        PacketWriter(const PacketWriter&) = delete;
        PacketWriter& operator=(const PacketWriter&) = delete;

        // 저장소와 작성 권한을 이전하고 원본을 비운다.
        PacketWriter(PacketWriter&& other) noexcept;
        PacketWriter& operator=(PacketWriter&& other) noexcept;

        // 작성할 저장소를 소유하고 있는지 확인한다.
        [[nodiscard]] explicit operator bool() const noexcept;

        // 요청한 본문 용량만큼의 쓰기 범위를 빌린다.
        [[nodiscard]] std::span<std::byte> GetWritablePayload() noexcept;

        // 작성 길이를 검증하고 같은 저장소를 불변 Packet으로 전환한다.
        // 실패하면 Writer와 작성 내용을 유지한다.
        [[nodiscard]] PacketResult Finish(std::size_t payloadSize) noexcept;

        // 작성 중인 저장소를 반환하고 빈 상태가 된다.
        void Reset() noexcept;

    private:
        friend class Detail::PacketPool;

        // 풀이 준비한 블록의 소유권을 인수한다.
        explicit PacketWriter(Detail::PacketBlock* block) noexcept;

        Detail::PacketBlock* m_block = nullptr;
    };
}
