#include "Job.h"
#include "../../Core/Util/Logger.h"
#include <utility> // for std::move

using namespace Core::Util;

bool Job::SetPacketJob(PacketHandlerFunc handler, ISession* session,
    uint16_t packetId, PacketHandle&& packet, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !session || !packet)
        return false;

    m_packetHandle = std::move(packet);
    m_packetFunc = handler;
    m_target = session;
    m_packetId = packetId;
    m_context = &context;
    m_type = JobType::PACKET;

    return true;
}

bool Job::SetDBJob(DBHandlerFunc handler, IDBConnection* connection,
    const char* data, uint32_t size, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !connection || (!data && size != 0))
        return false;

    m_dbFunc = handler;
    m_target = connection;
    m_data = data;
    m_size = size;
    m_context = &context;
    m_type = JobType::DB;

    return true;
}

bool Job::SetSystemJob(SystemHandlerFunc handler, IOCPCore* core,
    const void* data, uint32_t size, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !core || (!data && size != 0))
        return false;

    m_systemFunc = handler;
    m_target = core;
    m_data = static_cast<const char*>(data);
    m_size = size;
    m_context = &context;
    m_type = JobType::SYSTEM;

    return true;
}

PacketHandle Job::TakePacket() noexcept
{
    if (m_type != JobType::PACKET)
        return {};
    return std::move(m_packetHandle);
}

void Job::Execute()
{
    if (!m_context)
    {
        LOGE("job (type %d, packet id %u) has no handler context, skipping",
            static_cast<int>(m_type), m_packetId);
        return;
    }
    switch (m_type)
    {
    case JobType::PACKET:
        if (!m_packetHandle)
        {
            LOGE("packet job for id %u has no packet, skipping", m_packetId);
            break;
        }
        if (m_packetFunc)
        {
            const auto bytes = m_packetHandle.View();
            const bool result = m_packetFunc(static_cast<ISession*>(m_target),
                reinterpret_cast<const char*>(bytes.data()),
                static_cast<uint32_t>(bytes.size()), *m_context);
            // The callback may have transferred the packet. Do not read bytes here.
            if (!result)
                LOGW("packet handler for id %u returned failure", m_packetId);
        }
        break;
    case JobType::DB:
        if (m_dbFunc)
            m_dbFunc(static_cast<IDBConnection*>(m_target), m_data, m_size, *m_context);
        break;
    case JobType::SYSTEM:
        if (m_systemFunc)
            m_systemFunc(static_cast<IOCPCore*>(m_target), m_data, m_size, *m_context);
        break;
    default:
        break;
    }
}
