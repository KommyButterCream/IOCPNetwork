#include "SendPacketBatch.h"

#include "SendPacketQueue.h"
#include "../PacketTypes.h"

#include <limits>  // for std::numeric_limits
#include <new>     // for std::bad_alloc
#include <utility> // for std::move

namespace IOCPNetwork::Detail
{
    std::unique_ptr<SendPacketBatch> SendPacketBatch::Create(
        std::size_t maxPackets,
        std::size_t maxBytes) noexcept
    {
        if (maxPackets == 0)
        {
            return {};
        }

        // WSASend에 전달할 버퍼 개수는 DWORD 범위 안에 있어야 한다.
        if (maxPackets > (std::numeric_limits<DWORD>::max)())
        {
            return {};
        }

        // 두 배열 모두 원소 수에 따른 크기가 표현 가능한 범위인지 검사한다.
        if (maxPackets > (std::numeric_limits<std::size_t>::max)() / sizeof(Packet))
        {
            return {};
        }

        if (maxPackets >
            (std::numeric_limits<std::size_t>::max)() / sizeof(WSABUF))
        {
            return {};
        }

        // 최소 헤더를 하나 보낼 수 있어야 하며, 배치 크기는 DWORD 범위로 제한한다.
        if (maxBytes < PacketHeaderSize || maxBytes > (std::numeric_limits<DWORD>::max)())
        {
            return {};
        }

        try
        {
            auto packets = std::make_unique<Packet[]>(maxPackets);

            auto buffers = std::make_unique<WSABUF[]>(maxPackets);

            return std::unique_ptr<SendPacketBatch>{
                new SendPacketBatch(
                    std::move(packets),
                    std::move(buffers),
                    maxPackets,
                    maxBytes)
            };
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    SendPacketBatch::SendPacketBatch(
        std::unique_ptr<Packet[]> packets,
        std::unique_ptr<WSABUF[]> buffers,
        std::size_t maxPackets,
        std::size_t maxBytes) noexcept
        : m_packets(std::move(packets))
        , m_buffers(std::move(buffers))
        , m_maxPackets(maxPackets)
        , m_maxBytes(maxBytes)
    {
    }

    SendBatchBuildResult SendPacketBatch::BuildFrom(SendPacketQueue& queue) noexcept
    {
        // 기존 배치를 덮어쓰거나 진행 중인 배치에 새 패킷을 추가하지 않는다.
        if (m_packetCount != 0)
        {
            return SendBatchBuildResult::NotEmpty;
        }

        while (m_packetCount < m_maxPackets)
        {
            const std::size_t packetSize = queue.GetFrontPacketSize();

            if (packetSize == 0)
            {
                break;
            }

            // 남은 공간과 비교하여 덧셈의 오버플로를 피한다.
            if (packetSize > m_maxBytes - m_remainingBytes)
            {
                if (m_packetCount == 0)
                {
                    return SendBatchBuildResult::PacketTooLarge;
                }

                break;
            }

            // 크기 검사가 끝난 패킷만 큐에서 꺼내 배치로 옮긴다.
            m_packets[m_packetCount] = queue.Dequeue();

            const auto bytes = GetPacketWireBytes(m_packets[m_packetCount]);

            WSABUF& buffer = m_buffers[m_packetCount];

            // WSABUF의 타입에 맞추지만, 실제 패킷 바이트는 수정하지 않는다.
            buffer.buf = reinterpret_cast<char*>(const_cast<std::byte*>(bytes.data()));

            buffer.len = static_cast<ULONG>(bytes.size());

            m_remainingBytes += bytes.size();

            ++m_packetCount;
        }

        if (m_packetCount == 0)
        {
            return SendBatchBuildResult::Empty;
        }

        return SendBatchBuildResult::Ready;
    }

    std::span<WSABUF> SendPacketBatch::GetBuffers() noexcept
    {
        if (m_firstPending == m_packetCount)
        {
            return {};
        }

        return { m_buffers.get() + m_firstPending, m_packetCount - m_firstPending };
    }

    SendBatchCommitResult SendPacketBatch::CommitSentBytes(std::size_t bytesSent) noexcept
    {
        if (bytesSent == 0 || bytesSent > m_remainingBytes)
        {
            return {};
        }

        std::size_t bytesToConsume = bytesSent;

        std::size_t completedPackets = 0;

        while (bytesToConsume != 0)
        {
            WSABUF& buffer = m_buffers[m_firstPending];

            if (bytesToConsume < buffer.len)
            {
                // 현재 패킷의 일부만 완료되었다.
                // Packet 소유권을 유지하고 다음 송신 시작 위치만 전진시킨다.
                buffer.buf += bytesToConsume;

                buffer.len -= static_cast<ULONG>(bytesToConsume);

                bytesToConsume = 0;
            }
            else
            {
                // 현재 패킷의 남은 구간 전체가 완료되었다.
                bytesToConsume -= buffer.len;

                buffer = {};

                m_packets[m_firstPending].Reset();

                ++m_firstPending;

                ++completedPackets;
            }
        }

        m_remainingBytes -= bytesSent;

        if (m_firstPending == m_packetCount)
        {
            // 모든 Packet은 위에서 반환됐으며, 다음 배치를 위해 위치를 초기화한다.
            Clear();
        }

        return { true, completedPackets };
    }

    void SendPacketBatch::Clear() noexcept
    {
        while (m_firstPending < m_packetCount)
        {
            // 참조 구간을 먼저 비우고 해당 Packet의 소유 참조를 반환한다.
            m_buffers[m_firstPending] = {};

            m_packets[m_firstPending].Reset();

            ++m_firstPending;
        }

        m_packetCount = 0;

        m_firstPending = 0;

        m_remainingBytes = 0;
    }
}
