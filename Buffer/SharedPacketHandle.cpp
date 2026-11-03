#include "SharedPacketHandle.h"

#include "../Memory/TlsMemoryPool.h"

#include <memory>  // for std::construct_at, std::destroy_at
#include <utility> // for std::move, std::swap, std::exchange

struct SharedPacketHandle::Control
{
    volatile LONG refCount = 1;
    TlsMemoryPool* pool = nullptr;
    PacketHandle packet;
};

SharedPacketHandle::~SharedPacketHandle() noexcept
{
    Reset();
}

SharedPacketHandle::SharedPacketHandle(const SharedPacketHandle& other) noexcept : m_control(other.m_control)
{
    AddRef(m_control);
}

SharedPacketHandle& SharedPacketHandle::operator=(const SharedPacketHandle& other) noexcept
{
    SharedPacketHandle copy(other);
    std::swap(m_control, copy.m_control);

    return *this;
}

SharedPacketHandle& SharedPacketHandle::operator=(SharedPacketHandle&& other) noexcept
{
    if (this != &other)
    {
        Reset();
        m_control = std::exchange(other.m_control, nullptr);
    }

    return *this;
}

SharedPacketHandle SharedPacketHandle::Create(TlsMemoryPool& pool, PacketHandle packet)
{
    SharedPacketHandle result;
    if (!packet)
        return result;

    void* memory = pool.Acquire(sizeof(Control));

    if (!memory)
        return result;

    auto* control = std::construct_at(static_cast<Control*>(memory));

    control->pool = &pool;
    control->packet = std::move(packet);
    result.m_control = control;

    return result;
}

void SharedPacketHandle::Reset() noexcept
{
    Release(std::exchange(m_control, nullptr));
}

std::size_t SharedPacketHandle::Size() const noexcept
{
    return m_control ? m_control->packet.Size() : 0;
}

std::span<const std::byte> SharedPacketHandle::View() const noexcept
{
    return m_control ? m_control->packet.View() : std::span<const std::byte>{};
}

void SharedPacketHandle::AddRef(Control* control) noexcept
{
    if (control)
    {
        ::InterlockedIncrement(&control->refCount);
    }
}

void SharedPacketHandle::Release(Control* control) noexcept
{
    if (!control || ::InterlockedDecrement(&control->refCount) != 0)
        return;

    auto* pool = control->pool;
    std::destroy_at(control);
    pool->Release(control);
}

std::size_t SharedPacketHandle::GetControlSize() noexcept
{
    return sizeof(Control);
}
