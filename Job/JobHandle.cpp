#include "JobHandle.h"
#include "Job.h"
#include "../Session/ClientSession.h"
#include "../HandlerTable/PacketHandlerTable.h"
#include <utility> // for std::move
#include "../Memory/TlsMemoryPool.h"
#include "../../Core/Util/Logger.h"
#include <memory> // for std::construct_at, std::destroy_at

using namespace Core::Util;

void JobDeleter::operator()(Job* job) const noexcept
{
    if (!job)
        return;
    LOGT("job release %p", job);
    auto* session = job->m_sessionReference;
    std::destroy_at(job);
    m_pool->Release(job);
    // Job과 패킷을 풀에 반환한 뒤 마지막 세션 참조를 놓습니다.
    if (session)
        session->ReleaseReference();
}

JobHandle MEMORY_POOL::CreateJobHandle(TlsMemoryPool& pool)
{
    void* memory = pool.Acquire(sizeof(Job));
    if (!memory)
    {
        LOGE("failed to acquire a Job block (size %zu)", sizeof(Job));
        return JobHandle(nullptr, JobDeleter{pool});
    }
    LOGT("job acquire %p", memory);
    return JobHandle(std::construct_at(static_cast<Job*>(memory)), JobDeleter{pool});
}

JobHandle CreatePacketJob(TlsMemoryPool& pool, const PacketHandlerTable& handlers,
    ISession& session, uint16_t packetId, PacketHandle&& packet, const HandlerContext& context)
{
    const auto handler = handlers.GetHandler(packetId);
    if (!handler || !packet)
    {
        LOGW("cannot create packet job for id %u: missing handler or packet", packetId);
        return JobHandle(nullptr, JobDeleter{pool});
    }
    auto job = MEMORY_POOL::CreateJobHandle(pool);
    if (!job)
        return job;
    if (!job->InitializePacketJob(*handler, &session, packetId, std::move(packet), context))
    {
        LOGE("failed to initialize packet job for id %u", packetId);
        job.reset();
    }
    return job;
}
