#pragma once
#include <memory> // for std::unique_ptr
#include <cstdint> // for uint16_t


class Job;
class TlsMemoryPool;

class JobDeleter final
{
public:
    explicit JobDeleter(TlsMemoryPool& pool) noexcept : m_pool(&pool) {}

    void operator()(Job* job) const noexcept;

    [[nodiscard]] TlsMemoryPool* GetPool() const noexcept { return m_pool; }

private:
    // The pool must outlive every JobHandle that refers to it.
    TlsMemoryPool* m_pool;
};

using JobHandle = std::unique_ptr<Job, JobDeleter>;

namespace MEMORY_POOL
{
    // The pool must support alignof(Job). Failure returns an empty handle.
    [[nodiscard]] JobHandle CreateJobHandle(TlsMemoryPool& pool);
}

class ISession;
class PacketHandle;
class PacketHandlerTable;
struct HandlerContext;

// 핸들러 조회와 Job 생성·초기화를 공통 처리한다. 큐 등록은 각 스케줄러가 담당한다.
[[nodiscard]] JobHandle CreatePacketJob(TlsMemoryPool& pool, const PacketHandlerTable& handlers,
    ISession& session, uint16_t packetId, PacketHandle&& packet, const HandlerContext& context);
