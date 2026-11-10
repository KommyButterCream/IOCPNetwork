#include "Packet.h"

#include "Detail/PacketBlock.h"

#include <cstddef> // for std::byte, std::size_t
#include <span> // for std::span
#include <utility> // for std::exchange

namespace IOCPNetwork
{
    Packet::~Packet()
    {
        Reset();
    }

    Packet::Packet(const Packet& other) noexcept
        : m_block(other.m_block)
    {
        if (m_block != nullptr)
        {
            Detail::AddPacketReference(m_block);
        }
    }

    Packet& Packet::operator=(const Packet& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        // 새 저장소의 수명을 먼저 확보하고 기존 참조를 반환한다.
        Detail::PacketBlock* newBlock = other.m_block;

        if (newBlock != nullptr)
        {
            Detail::AddPacketReference(newBlock);
        }

        Detail::PacketBlock* oldBlock = std::exchange(m_block, newBlock);

        if (oldBlock != nullptr)
        {
            Detail::ReleasePacketReference(oldBlock);
        }

        return *this;
    }

    Packet::Packet(Packet&& other) noexcept
        : m_block(std::exchange(other.m_block, nullptr))
    {
    }

    Packet& Packet::operator=(Packet&& other) noexcept
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

    Packet::operator bool() const noexcept
    {
        return m_block != nullptr;
    }

    PacketView Packet::View() const noexcept
    {
        if (m_block == nullptr)
        {
            return {};
        }

        const Detail::PacketBlock& block = *m_block;

        const std::size_t payloadSize =
            static_cast<std::size_t>(block.packetSize) - PacketHeaderSize;

        return PacketView
        {
            block.packetId,
            std::span<const std::byte>{ block.GetWireData() + PacketHeaderSize, payloadSize }
        };
    }

    void Packet::Reset() noexcept
    {
        // 반환 중에는 이 핸들이 이미 빈 상태가 되도록 한다.
        Detail::PacketBlock* block = std::exchange(m_block, nullptr);

        if (block != nullptr)
        {
            Detail::ReleasePacketReference(block);
        }
    }

    Packet::Packet(Detail::PacketBlock* block) noexcept
        : m_block(block)
    {
    }
}

namespace IOCPNetwork::Detail
{
    std::span<const std::byte> GetPacketWireBytes(const Packet& packet) noexcept
    {
        if (packet.m_block == nullptr)
        {
            return {};
        }

        const PacketBlock& block = *packet.m_block;

        return { block.GetWireData(), static_cast<std::size_t>(block.packetSize) };
    }
}
