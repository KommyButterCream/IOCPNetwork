#pragma once

#include "PacketHeader.h"
#include "../Packet.h"

#include <cstddef>  // for std::byte, std::size_t
#include <memory>   // for std::unique_ptr
#include <optional> // for std::optional
#include <span>     // for std::span

namespace IOCPNetwork::Detail
{
    class PacketPool;

    // 패킷 추출 결과를 구분한다.
    enum class PacketExtractStatus
    {
        Complete,          // 완성된 Packet을 반환함
        NeedMoreData,      // 헤더 또는 본문이 덜 도착함
        InvalidPacketId,   // 사용할 수 없는 패킷 ID
        InvalidPacketSize, // 패킷 길이가 허용 범위를 벗어남
        NoMemory,          // 패킷 저장소 확보 실패
        InternalError      // 내부 설정 또는 패킷 생성 계약 불일치
    };

    // Complete이면 packet이 추출한 패킷을 소유하고, 그 외에는 비어 있다.
    struct [[nodiscard]] PacketExtractResult
    {
        PacketExtractStatus status = PacketExtractStatus::NeedMoreData;

        Packet packet;
    };

    // 수신한 바이트를 원형 버퍼에 보관하고 패킷 단위로 추출한다.
    // 동시 접근은 세션이 보호하며, 수신 발행부터 완료 반영까지 버퍼 상태를 유지한다.

    class RecvPacketBuffer final
    {
    public:
        // 설정이 잘못되었거나 메모리가 부족하면 nullptr를 반환한다.
        [[nodiscard]] static std::unique_ptr<RecvPacketBuffer> Create(
            std::size_t capacity,
            std::size_t maxPacketSize) noexcept;

        ~RecvPacketBuffer() = default;

        RecvPacketBuffer(const RecvPacketBuffer&) = delete;
        RecvPacketBuffer& operator=(const RecvPacketBuffer&) = delete;

        RecvPacketBuffer(RecvPacketBuffer&&) = delete;
        RecvPacketBuffer& operator=(RecvPacketBuffer&&) = delete;

        // 메모리를 유지한 채 보관 데이터를 폐기하고 읽기·쓰기 위치를 초기화한다.
        // 진행 중인 수신 I/O가 없을 때만 호출한다.
        void Clear() noexcept;

        // 다음 수신 전에 빈 버퍼의 위치를 처음으로 정리한다.
        // 미처리 데이터는 유지하며, 진행 중인 수신 I/O가 없을 때만 호출한다.
        void ResetOffsetsIfEmpty() noexcept;

        // 다음 수신 데이터를 기록할 수 있는 연속 영역을 반환한다.
        // 반환 영역은 비소유 참조이며, 버퍼가 가득 차면 빈 span을 반환한다.
        [[nodiscard]] std::span<std::byte> GetReceiveSpan() noexcept;

        // 상태가 유지된 쓰기 영역에 실제로 수신한 바이트 수를 반영한다.
        // 0이거나 쓰기 가능한 연속 영역보다 크면 상태를 유지하고 실패한다.
        [[nodiscard]] bool CommitReceivedBytes(std::size_t bytesReceived) noexcept;

        // 완성된 패킷 하나를 추출한다.
        // 성공할 때만 데이터를 소비하며, 반환된 Packet은 버퍼와 독립적으로 유지된다.
        [[nodiscard]] PacketExtractResult ExtractPacket(PacketPool& pool);

    private:
        static constexpr std::size_t BufferAlignment = 64;

        struct AlignedBufferDeleter
        {
            void operator()(std::byte* buffer) const noexcept;
        };

        using BufferStorage = std::unique_ptr<std::byte[], AlignedBufferDeleter>;

        RecvPacketBuffer(
            BufferStorage buffer,
            std::size_t capacity,
            std::size_t maxPacketSize) noexcept;

        // 읽기 위치에서 offset만큼 떨어진 데이터부터 목적지에 복사한다.
        // 요청 범위는 보관 중인 데이터 안에 있어야 하며, 읽기 위치는 변경하지 않는다.
        void CopyReadableBytes(
            std::span<std::byte> destination,
            std::size_t offset = 0) const noexcept;

        // 읽기 위치의 헤더를 해석한다. 4바이트 미만이면 결과가 없다.
        // 패킷 ID와 길이의 유효성 검사는 별도로 수행한다.
        [[nodiscard]] std::optional<PacketHeader> PeekPacketHeader() const noexcept;

        // 바이트 배열과 생성 시 확정되는 경계 정보.
        BufferStorage m_buffer;

        const std::size_t m_capacity;
        const std::size_t m_wrapMask;
        const std::size_t m_maxPacketSize;

        // 두 위치가 같을 때 빈 상태와 가득 찬 상태는 m_bufferedSize로 구분한다.
        std::size_t m_readOffset = 0;
        std::size_t m_writeOffset = 0;
        std::size_t m_bufferedSize = 0;
    };
}
