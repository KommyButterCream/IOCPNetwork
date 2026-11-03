#pragma once
#include "../Buffer/PacketHandle.h"
#include "JobDefs.h"

class SessionJobQueue;

// Job objects stay at their pool address; only their owning handles move.
// Targets and HandlerContext are borrowed and must outlive execution.
class alignas(64) Job final
{
public:
    Job() noexcept = default;
    ~Job() = default;
    Job(const Job&) = delete;
    Job& operator=(const Job&) = delete;
    Job(Job&&) = delete;
    Job& operator=(Job&&) = delete;

    // Configure once before enqueueing. Failure preserves the supplied packet.
    [[nodiscard]] bool SetPacketJob(PacketHandlerFunc handler, ISession* session,
        uint16_t packetId, PacketHandle&& packet, const HandlerContext& context);

    // Preserve legacy non-packet ownership: existing scheduler/queue paths
    // return the buffer. These setters do not adopt a PacketHandle.
    [[nodiscard]] bool SetDBJob(DBHandlerFunc handler, IDBConnection* connection,
        const char* data, uint32_t size, const HandlerContext& context);
    [[nodiscard]] bool SetSystemJob(SystemHandlerFunc handler, IOCPCore* core,
        const void* data, uint32_t size, const HandlerContext& context);

    [[nodiscard]] JobType GetType() const noexcept { return m_type; }
    // Borrowed access for existing non-packet cleanup only.
    [[nodiscard]] const char* GetNonPacketData() const noexcept
    {
        return m_type == JobType::DB || m_type == JobType::SYSTEM ? m_data : nullptr;
    }

    [[nodiscard]] PacketHandle TakePacket() noexcept;
    void Execute();

private:
    friend class SessionJobQueue;
    Job* m_next = nullptr;
    void* m_target = nullptr;
    const char* m_data = nullptr;
    union
    {
        PacketHandlerFunc m_packetFunc = nullptr;
        DBHandlerFunc m_dbFunc;
        SystemHandlerFunc m_systemFunc;
    };
    const HandlerContext* m_context = nullptr;
    uint32_t m_size = 0;
    uint16_t m_packetId = 0;
    JobType m_type = JobType::NONE;
    PacketHandle m_packetHandle;
};
