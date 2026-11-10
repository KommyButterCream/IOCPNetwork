#pragma once

#include "../Packet.h"
#include "../NetworkTypes.h"

#include <cstddef> // for std::size_t
#include <memory>  // for std::unique_ptr

namespace IOCPNetwork::Detail
{
    // 발행 전 Packet을 원형 배열에 FIFO로 보관한다.
    // 동시 접근은 세션의 송신 상태 락으로 보호한다.
    class SendPacketQueue final
    {
    public:
        // 지정한 수의 Packet 슬롯을 확보한다.
        // 용량이 잘못되었거나 메모리가 부족하면 nullptr를 반환한다.
        [[nodiscard]] static std::unique_ptr<SendPacketQueue> Create(std::size_t capacity) noexcept;

        ~SendPacketQueue() = default;

        SendPacketQueue(const SendPacketQueue&) = delete;
        SendPacketQueue& operator=(const SendPacketQueue&) = delete;

        SendPacketQueue(SendPacketQueue&&) = delete;
        SendPacketQueue& operator=(SendPacketQueue&&) = delete;

        // 성공하면 Packet 소유권을 큐로 이전한다.
        // 빈 패킷이거나 큐가 가득 차면 원본 Packet을 유지한다.
        [[nodiscard]] SendResult Enqueue(Packet&& packet) noexcept;

        // 가장 먼저 등록한 Packet의 소유권을 반환한다.
        // 큐가 비어 있으면 빈 Packet을 반환한다.
        [[nodiscard]] Packet Dequeue() noexcept;

        // 첫 패킷의 헤더 포함 크기를 반환한다.
        // 큐가 비어 있으면 0이며, 패킷을 소비하지 않는다.
        [[nodiscard]] std::size_t GetFrontPacketSize() const noexcept;

        // 대기 중인 Packet을 모두 반환하고 큐를 비운다.
        // 슬롯 배열은 유지하여 다시 사용할 수 있다.
        void Clear() noexcept;

    private:
        SendPacketQueue(std::unique_ptr<Packet[]> packets, std::size_t capacity) noexcept;

        // 슬롯 배열은 큐가 소유하며, 각 슬롯은 Packet의 소유권을 보관한다.
        std::unique_ptr<Packet[]> m_packets;

        const std::size_t m_capacity;

        // 다음 인출 위치, 다음 등록 위치, 현재 보관 중인 패킷 수.
        std::size_t m_head = 0;
        std::size_t m_tail = 0;
        std::size_t m_count = 0;
    };
}
