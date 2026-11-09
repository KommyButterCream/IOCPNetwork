#include "PacketWriter.h"

#include "PacketResult.h"

#include "Detail/PacketBlock.h"
#include "Detail/PacketHeader.h"

#include <cstddef> // for std::byte, std::size_t
#include <cstdint> // for std::uint16_t
#include <span> // for std::span
#include <utility> // for std::exchange

namespace IOCPNetwork
{
    PacketWriter::~PacketWriter()
    {
        Reset();
    }

    PacketWriter::PacketWriter(PacketWriter&& other) noexcept
        : m_block(std::exchange(other.m_block, nullptr))
    {
    }

    PacketWriter& PacketWriter::operator=(PacketWriter&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        Detail::PacketBlock* newBlock = std::exchange(other.m_block, nullptr);

        Detail::PacketBlock* oldBlock = std::exchange(m_block, newBlock);

        if (oldBlock != nullptr)
        {
            Detail::ReleasePacketReference(oldBlock);
        }

        return *this;
    }

    PacketWriter::operator bool() const noexcept
    {
        return m_block != nullptr;
    }

    std::span<std::byte> PacketWriter::GetWritablePayload() noexcept
    {
        if (m_block == nullptr)
        {
            return {};
        }

        return std::span<std::byte>
        {
            m_block->GetWireData() + PacketHeaderSize, m_block->payloadCapacity
        };
    }

    PacketResult PacketWriter::Finish(std::size_t payloadSize) noexcept
    {
        if (m_block == nullptr)
        {
            return PacketResult
            {
                Packet{},
                PacketError::InvalidState
            };
        }

        // 실패하면 작성 내용과 소유권을 유지한다.
        if (payloadSize > m_block->payloadCapacity)
        {
            return PacketResult
            {
                Packet{},
                PacketError::InvalidPayloadSize
            };
        }

        // 덧셈과 16비트 변환 전에 전송 규격의 상한을 확인한다.
        if (payloadSize > MaxPayloadSize)
        {
            return PacketResult
            {
                Packet{},
                PacketError::PacketTooLarge
            };
        }

        const auto packetSize = static_cast<std::uint16_t>(PacketHeaderSize + payloadSize);

        const Detail::PacketHeader header
        {
            m_block->packetId,
            packetSize
        };

        const bool written = Detail::WritePacketHeader(
            std::span<std::byte> { m_block->GetWireData(), PacketHeaderSize },
            header);

        if (!written)
        {
            return PacketResult
            {
                Packet{},
                PacketError::InvalidState
            };
        }

        m_block->packetSize = packetSize;

        // 기존 참조를 Packet으로 이전하고 Writer를 비운다.
        return PacketResult
        {
            Packet{ std::exchange(m_block, nullptr) },
            PacketError::None
        };
    }

    void PacketWriter::Reset() noexcept
    {
        Detail::PacketBlock* block =
            std::exchange(m_block, nullptr);

        if (block != nullptr)
        {
            Detail::ReleasePacketReference(block);
        }
    }

    PacketWriter::PacketWriter(Detail::PacketBlock* block) noexcept
        : m_block(block)
    {
    }
}
