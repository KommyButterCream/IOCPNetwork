#include "../Core/EngineExecutionScope.h"
#include "Job.h"
#include "../Session/ClientSession.h"
#include "../HandlerTable/PacketHandler.h"
#include "../../Core/Util/Logger.h"
#include <utility> // for std::move

using namespace Core::Util;

bool Job::InitializePacketJob(const PacketHandler& handler, ISession* session,
    uint16_t packetId, PacketHandle&& packet, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !session || !packet || packet.Size() > UINT32_MAX)
        return false;

    m_payload = std::move(packet);
    m_packetHandler = &handler;
    m_target = session;
    m_packetId = packetId;
    m_context = &context;
    m_type = JobType::PACKET;
    m_sessionReference = static_cast<ClientSession*>(session)->AcquireReference().release();

    return true;
}

bool Job::SetDBJob(DBHandlerFunc handler, IDBConnection* connection,
    PacketHandle&& payload, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !connection || payload.Size() > UINT32_MAX)
        return false;

    m_dbFunc = handler;
    m_target = connection;
    m_payload = std::move(payload);
    m_context = &context;
    m_type = JobType::DB;

    return true;
}

bool Job::SetSystemJob(SystemHandlerFunc handler, IOCPCore* core,
    PacketHandle&& payload, const HandlerContext& context)
{
    if (m_type != JobType::NONE || !handler || !core || payload.Size() > UINT32_MAX)
        return false;

    m_systemFunc = handler;
    m_target = core;
    m_payload = std::move(payload);
    m_context = &context;
    m_type = JobType::SYSTEM;

    return true;
}

bool Job::InitializeServiceEvent(ClientSession& session, ServiceEvent event, SessionTask task)
{
    if (m_type != JobType::NONE || !task)
        return false;
    m_serviceTask = std::move(task);
    m_event = event;
    m_type = JobType::SERVICE_EVENT;
    m_sessionReference = session.AcquireReference().release();
    return true;
}

void Job::Execute()
{
	EngineExecutionScope execution;
    if (m_type == JobType::SERVICE_EVENT)
    {
        m_sessionReference->ExecuteServiceEvent(m_event, m_serviceTask);
        return;
    }
    if (m_sessionReference && m_sessionReference->IsClosing())
        return;
    if (!m_context)
    {
        LOGE("job (type %d, packet id %u) has no handler context, skipping",
            static_cast<int>(m_type), m_packetId);
        return;
    }
    const auto bytes = m_payload.View();
    const auto size = static_cast<uint32_t>(bytes.size());
    switch (m_type)
    {
    case JobType::PACKET:
        if (!m_payload)
        {
            LOGE("packet job for id %u has no packet, skipping", m_packetId);
            break;
        }
        if (m_packetHandler)
        {
            const bool result = (*m_packetHandler)(static_cast<ISession*>(m_target),
                reinterpret_cast<const char*>(bytes.data()),
                size, *m_context);
            if (!result)
                LOGW("packet handler for id %u returned failure", m_packetId);
        }
        break;
    case JobType::DB:
        if (m_dbFunc)
            m_dbFunc(static_cast<IDBConnection*>(m_target), reinterpret_cast<const char*>(bytes.data()), size, *m_context);
        break;
    case JobType::SYSTEM:
        if (m_systemFunc)
            m_systemFunc(static_cast<IOCPCore*>(m_target), bytes.data(), size, *m_context);
        break;
    default:
        break;
    }
}
