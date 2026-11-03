#include "HandlerContext.h"

#include "../Protocol/PacketHeader.h"
#include "../Memory/TlsMemoryPool.h"
#include <utility> // for std::move
PacketHandle HandlerContext::CreatePacket(std::size_t size) const
{
    if (!m_packetPool || size == 0 || size > PACKET_SIZE_LIMIT)
        return {};
    return PacketHandle::Take(*m_packetPool, m_packetPool->Acquire(size), size);
}
SharedPacketHandle HandlerContext::CreateSharedPacket(PacketHandle packet) const
{
    if (!m_sharedPool)
        return {};
    return SharedPacketHandle::Create(*m_sharedPool, std::move(packet));
}
