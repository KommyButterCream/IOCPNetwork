#include "RecvPacketBuffer.h"

#include "PacketPool.h"

#include "../PacketError.h"
#include "../PacketResult.h"
#include "../PacketTypes.h"

#include <array>   // for std::array
#include <bit>     // for std::has_single_bit
#include <cassert> // for assert
#include <cstring> // for std::memcpy
#include <new>     // for std::align_val_t, std::nothrow, operator new, operator delete
#include <utility> // for std::move

namespace IOCPNetwork::Detail
{
    std::unique_ptr<RecvPacketBuffer> RecvPacketBuffer::Create(
        std::size_t capacity,
        std::size_t maxPacketSize) noexcept
    {
        if (maxPacketSize < PacketHeaderSize ||
            maxPacketSize > MaxPacketSize)
        {
            return {};
        }

        if (!std::has_single_bit(capacity))
        {
            return {};
        }

        // 최대 패킷과 미완성 조각을 처리할 공간을 확보한다.
        if (capacity < maxPacketSize * 2)
        {
            return {};
        }

        BufferStorage buffer{
            static_cast<std::byte*>(::operator new(
                capacity,
                std::align_val_t{ BufferAlignment },
                std::nothrow))
        };

        if (!buffer)
        {
            return {};
        }

        return std::unique_ptr<RecvPacketBuffer>{
            new (std::nothrow) RecvPacketBuffer(
                std::move(buffer),
                capacity,
                maxPacketSize)
        };
    }

    RecvPacketBuffer::RecvPacketBuffer(
        BufferStorage buffer,
        std::size_t capacity,
        std::size_t maxPacketSize) noexcept
        : m_buffer(std::move(buffer))
        , m_capacity(capacity)
        , m_wrapMask(capacity - 1)
        , m_maxPacketSize(maxPacketSize)
    {

    }

    void RecvPacketBuffer::Clear() noexcept
    {
        m_readOffset = 0;

        m_writeOffset = 0;

        m_bufferedSize = 0;
    }

    void RecvPacketBuffer::ResetOffsetsIfEmpty() noexcept
    {
        if (m_bufferedSize != 0)
        {
            return;
        }

        m_readOffset = 0;

        m_writeOffset = 0;
    }

    std::span<std::byte> RecvPacketBuffer::GetReceiveSpan() noexcept
    {
        if (m_bufferedSize == m_capacity)
        {
            return {};
        }

        std::size_t writableSize = 0;

        if (m_writeOffset >= m_readOffset)
        {
            // 빈 공간이 배열 끝과 앞쪽으로 나뉠 수 있다.
            // 이번 수신에는 쓰기 위치부터 배열 끝까지의 연속 영역만 제공한다.
            writableSize = m_capacity - m_writeOffset;
        }
        else
        {
            // 쓰기 위치가 배열 앞쪽으로 돌아온 상태다.
            // 읽기 위치부터는 미처리 데이터이므로 그 직전까지만 제공한다.
            writableSize = m_readOffset - m_writeOffset;
        }

        return { m_buffer.get() + m_writeOffset, writableSize };
    }

    bool RecvPacketBuffer::CommitReceivedBytes(std::size_t bytesReceived) noexcept
    {
        if (bytesReceived == 0)
        {
            return false;
        }

        const std::size_t writableSize = GetReceiveSpan().size();

        if (bytesReceived > writableSize)
        {
            return false;
        }

        m_writeOffset = (m_writeOffset + bytesReceived) & m_wrapMask;

        m_bufferedSize += bytesReceived;

        return true;
    }

    PacketExtractResult RecvPacketBuffer::ExtractPacket(PacketPool& pool)
    {
        const auto header = PeekPacketHeader();

        if (!header)
        {
            return { PacketExtractStatus::NeedMoreData, {} };
        }

        const PacketFrameStatus frameStatus = CheckPacketFrame(
            *header,
            m_bufferedSize,
            m_maxPacketSize);

        switch (frameStatus)
        {
            case PacketFrameStatus::Complete:
            {
                break;
            }

            case PacketFrameStatus::NeedMoreData:
            {
                return { PacketExtractStatus::NeedMoreData, {} };
            }

            case PacketFrameStatus::InvalidPacketId:
            {
                return { PacketExtractStatus::InvalidPacketId, {} };
            }

            case PacketFrameStatus::InvalidPacketSize:
            {
                return { PacketExtractStatus::InvalidPacketSize, {} };
            }

            case PacketFrameStatus::InvalidReceiveLimit:
            default:
            {
                return { PacketExtractStatus::InternalError, {} };
            }
        }

        // 헤더 검증을 통과했으므로 전체 길이에서 헤더 크기를 안전하게 뺄 수 있다.
        const std::size_t packetSize = header->packetSize;

        const std::size_t payloadSize = packetSize - PacketHeaderSize;

        auto writerResult = pool.CreatePacketWriter(header->packetId, payloadSize);

        if (!writerResult)
        {
            const PacketExtractStatus status =
                writerResult.error == PacketError::NoMemory
                ? PacketExtractStatus::NoMemory
                : PacketExtractStatus::InternalError;

            return { status, {} };
        }

        // 헤더를 건너뛰고 본문만 패킷 저장소에 복사한다.
        CopyReadableBytes(writerResult.writer.GetWritablePayload(), PacketHeaderSize);

        auto packetResult = writerResult.writer.Finish(payloadSize);

        if (!packetResult)
        {
            return { PacketExtractStatus::InternalError, {} };
        }

        // Packet이 완성된 뒤에만 원본 데이터를 소비한다.
        m_readOffset = (m_readOffset + packetSize) & m_wrapMask;

        m_bufferedSize -= packetSize;

        return { PacketExtractStatus::Complete, std::move(packetResult.packet) };
    }

    void RecvPacketBuffer::CopyReadableBytes(std::span<std::byte> destination, std::size_t offset) const noexcept
    {
        assert(offset <= m_bufferedSize);

        assert(destination.size() <= m_bufferedSize - offset);

        if (destination.empty())
        {
            return;
        }

        // 읽기 위치에 상대 오프셋을 더하고 배열 범위 안으로 되돌린다.
        const std::size_t sourceOffset = (m_readOffset + offset) & m_wrapMask;

        // 복사 시작 위치부터 배열 끝까지 연속으로 접근할 수 있는 크기다.
        const std::size_t bytesToEnd = m_capacity - sourceOffset;

        // 배열 끝을 넘지 않는 범위만 첫 번째 복사로 처리한다.
        const std::size_t firstCopySize =
            destination.size() < bytesToEnd
            ? destination.size()
            : bytesToEnd;

        std::memcpy(destination.data(), m_buffer.get() + sourceOffset, firstCopySize);

        if (firstCopySize < destination.size())
        {
            // 나머지 데이터는 배열 앞쪽에 이어져 있다.
            std::memcpy(destination.data() + firstCopySize, m_buffer.get(), destination.size() - firstCopySize);
        }
    }

    std::optional<PacketHeader> RecvPacketBuffer::PeekPacketHeader() const noexcept
    {
        if (m_bufferedSize < PacketHeaderSize)
        {
            return std::nullopt;
        }

        const std::size_t bytesToEnd = m_capacity - m_readOffset;

        if (bytesToEnd >= PacketHeaderSize)
        {
            // 헤더가 연속되어 있으면 임시 복사 없이 기존 해석 함수에 전달한다.
            return ReadPacketHeader(
                std::span<const std::byte>{
                    m_buffer.get() + m_readOffset,
                    PacketHeaderSize
                });
        }

        // 헤더가 배열 끝과 시작에 나뉜 경우에만 작은 지역 배열에 모은다.
        std::array<std::byte, PacketHeaderSize> headerBytes{};

        CopyReadableBytes(headerBytes);

        return ReadPacketHeader(headerBytes);
    }

    void RecvPacketBuffer::AlignedBufferDeleter::operator()(std::byte* buffer) const noexcept
    {
        ::operator delete(buffer, std::align_val_t{ BufferAlignment });
    }
}
