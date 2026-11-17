#include "IOCPBase.h"

#include "Detail/IOCPBaseImpl.h"

#include <cstddef> // for std::byte, std::size_t
#include <exception> // for std::terminate
#include <memory> // for std::unique_ptr
#include <span> // for std::span
#include <utility>   // for std::move

namespace IOCPNetwork
{
    IOCPBase::IOCPBase(std::unique_ptr<Detail::IOCPBaseImpl> impl) noexcept
        : m_impl(std::move(impl))
    {
        // 공개 엔진 객체는 항상 대응하는 내부 구현을 소유해야 한다.
        if (!m_impl)
        {
            std::terminate();
        }
    }

    IOCPBase::~IOCPBase() = default;

    HandlerRegistrationResult IOCPBase::RegisterPacketHandler(PacketId packetId, PacketHandler handler) noexcept
    {
        return m_impl->RegisterPacketHandler(packetId, std::move(handler));
    }

    bool IOCPBase::SetConnectionEvents(ConnectionEvents events) noexcept
    {
        return m_impl->SetConnectionEvents(std::move(events));
    }

    PacketResult IOCPBase::CreatePacket(PacketId packetId, std::span<const std::byte> payload) noexcept
    {
        return m_impl->CreatePacket(packetId, payload);
    }

    PacketWriterResult IOCPBase::CreatePacketWriter(PacketId packetId, std::size_t payloadCapacity) noexcept
    {
        return m_impl->CreatePacketWriter(packetId, payloadCapacity);
    }

    SendResult IOCPBase::Send(ConnectionId id, Packet&& packet) noexcept
    {
        return m_impl->Send(id, std::move(packet));
    }

    PostResult IOCPBase::Post(ConnectionId id, SessionTask&& task) noexcept
    {
        return m_impl->Post(id, std::move(task));
    }

    bool IOCPBase::Disconnect(ConnectionId id) noexcept
    {
        return m_impl->Disconnect(id);
    }

    Detail::IOCPBaseImpl& IOCPBase::GetImpl() noexcept
    {
        return *m_impl;
    }
}
