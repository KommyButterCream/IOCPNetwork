#include "SendPacketQueue.h"

#include <limits>  // for std::numeric_limits
#include <new>     // for std::bad_alloc
#include <utility> // for std::move

namespace IOCPNetwork::Detail
{
    std::unique_ptr<SendPacketQueue> SendPacketQueue::Create(std::size_t capacity) noexcept
    {
        if (capacity == 0)
        {
            return {};
        }

        // 배열의 원소 수가 표현 가능한 메모리 크기를 넘지 않도록 검사한다.
        if (capacity > (std::numeric_limits<std::size_t>::max)() / sizeof(Packet))
        {
            return {};
        }

        try
        {
            auto packets = std::make_unique<Packet[]>(capacity);

            return std::unique_ptr<SendPacketQueue>{ new SendPacketQueue(std::move(packets), capacity) };
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    SendPacketQueue::SendPacketQueue(std::unique_ptr<Packet[]> packets, std::size_t capacity) noexcept
        : m_packets(std::move(packets))
        , m_capacity(capacity)
    {
    }

    SendResult SendPacketQueue::Enqueue(Packet&& packet) noexcept
    {
        if (!packet)
        {
            return SendResult::InvalidPacket;
        }

        if (m_count == m_capacity)
        {
            return SendResult::QueueFull;
        }

        // 모든 실패 조건을 확인한 뒤에만 소유권을 이전한다.
        m_packets[m_tail] = std::move(packet);

        ++m_tail;

        if (m_tail == m_capacity)
        {
            m_tail = 0;
        }

        ++m_count;

        return SendResult::Queued;
    }

    Packet SendPacketQueue::Dequeue() noexcept
    {
        if (m_count == 0)
        {
            return {};
        }

        // 슬롯이 가진 소유권을 지역 Packet으로 옮기고 슬롯을 비운다.
        Packet packet = std::move(m_packets[m_head]);

        ++m_head;

        if (m_head == m_capacity)
        {
            m_head = 0;
        }

        --m_count;

        return packet;
    }

    std::size_t SendPacketQueue::GetFrontPacketSize() const noexcept
    {
        if (m_count == 0)
        {
            return 0;
        }

        return GetPacketWireBytes(m_packets[m_head]).size();
    }

    void SendPacketQueue::Clear() noexcept
    {
        while (m_count != 0)
        {
            // Dequeue로 소유권을 가져온 뒤 이번 반복에서 참조를 반환한다.
            Packet packet = Dequeue();
        }

        m_head = 0;

        m_tail = 0;
    }
}
