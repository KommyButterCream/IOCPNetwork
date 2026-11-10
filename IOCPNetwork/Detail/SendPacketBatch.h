#pragma once

#include <WinSock2.h> // for WSABUF

#include "../Packet.h"

#include <cstddef> // for std::size_t
#include <memory>  // for std::unique_ptr
#include <span> // for std::span

namespace IOCPNetwork::Detail
{
    class SendPacketQueue;

    // 큐에서 새 송신 배치를 구성한 결과다.
    enum class SendBatchBuildResult
    {
        Ready,          // 하나 이상의 패킷으로 배치를 구성함
        Empty,          // 큐에 패킷이 없음
        NotEmpty,       // 기존 배치가 아직 남아 있음
        PacketTooLarge  // 첫 패킷 하나가 배치 전체 바이트 상한을 초과함
    };

    // 송신할 Packet의 소유권과 남은 전송 구간을 보관한다.
    // 세션이 동시 접근을 보호하고, I/O 완료 전에는 배치를 변경하거나 파괴하지 않는다.

    class SendPacketBatch final
    {
    public:
        // 배치에서 재사용할 Packet과 WSABUF 슬롯을 확보한다.
        // 설정이 잘못되었거나 메모리가 부족하면 nullptr를 반환한다.
        [[nodiscard]] static std::unique_ptr<SendPacketBatch> Create(
            std::size_t maxPackets,
            std::size_t maxBytes) noexcept;

        ~SendPacketBatch() = default;

        SendPacketBatch(const SendPacketBatch&) = delete;
        SendPacketBatch& operator=(const SendPacketBatch&) = delete;

        SendPacketBatch(SendPacketBatch&&) = delete;
        SendPacketBatch& operator=(SendPacketBatch&&) = delete;

        // 빈 배치에 큐의 패킷을 FIFO 순서로 옮긴다.
        // 패킷 수·바이트 상한에 도달하면 나머지는 큐에 유지한다.
        [[nodiscard]] SendBatchBuildResult BuildFrom(SendPacketQueue& queue) noexcept;

        // 아직 송신 완료되지 않은 연속된 WSABUF 구간을 빌린다.
        // 호출자는 송신 API에 전달하는 용도로만 사용하고 내용을 수정하지 않는다.
        [[nodiscard]] std::span<WSABUF> GetBuffers() noexcept;

        // 성공한 송신 완료의 바이트 수를 반영하고 완료된 Packet을 반환한다.
        // 0이거나 남은 바이트 수보다 크면 상태를 유지하고 실패한다.
        [[nodiscard]] bool CommitSentBytes(std::size_t bytesSent) noexcept;

        // 남은 Packet을 반환하고 배치를 비운다. 슬롯 배열은 유지한다.
        // 진행 중인 송신 I/O가 없거나 완료가 확인된 뒤에만 호출한다.
        void Clear() noexcept;

    private:
        SendPacketBatch(
            std::unique_ptr<Packet[]> packets,
            std::unique_ptr<WSABUF[]> buffers,
            std::size_t maxPackets,
            std::size_t maxBytes) noexcept;

        // Packet은 저장소를 소유하고, 같은 인덱스의 WSABUF는 전송 구간을 참조한다.
        std::unique_ptr<Packet[]> m_packets;
        std::unique_ptr<WSABUF[]> m_buffers;

        const std::size_t m_maxPackets;
        const std::size_t m_maxBytes;

        // 유효한 슬롯 범위는 [m_firstPending, m_packetCount) 이다.
        std::size_t m_packetCount = 0;  // 이번 배치에 채운 슬롯 수, 즉 사용 범위의 끝
        std::size_t m_firstPending = 0; // 아직 완료되지 않은 첫 슬롯의 인덱스

        // 현재 배치에서 아직 송신 완료가 확인되지 않은 전체 바이트 수.
        std::size_t m_remainingBytes = 0;
    };
}
