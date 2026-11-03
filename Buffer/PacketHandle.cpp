#include "PacketHandle.h"
#include "../Memory/TlsMemoryPool.h"

PacketHandle PacketHandle::Take(TlsMemoryPool& pool, void* data, std::size_t size) noexcept
{
    PacketHandle result;

    if (data)
    {
        result.m_pool = &pool;
        result.m_data = static_cast<std::byte*>(data);
        result.m_size = size;
    }

    return result;
}

void PacketHandle::Reset() noexcept
{
    auto* pool = std::exchange(m_pool, nullptr);
    auto* data = std::exchange(m_data, nullptr);

    m_size = 0;

    if (data)
    {
        pool->Release(data);
    }
}
