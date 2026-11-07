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

    // 빈 Job을 패킷 처리 작업으로 초기화한다. 실패하면 전달받은 핸들의 소유권을 유지한다.
    [[nodiscard]] bool InitializePacketJob(PacketHandlerFunc handler, ISession* session,
        uint16_t packetId, PacketHandle&& packet, const HandlerContext& context);

    // 빈 데이터도 허용하며, 설정에 성공한 경우에만 소유권을 이전한다.
    [[nodiscard]] bool SetDBJob(DBHandlerFunc handler, IDBConnection* connection,
        PacketHandle&& payload, const HandlerContext& context);
    [[nodiscard]] bool SetSystemJob(SystemHandlerFunc handler, IOCPCore* core,
        PacketHandle&& payload, const HandlerContext& context);

    [[nodiscard]] JobType GetType() const noexcept { return m_type; }

    [[nodiscard]] PacketHandle TakePacket() noexcept;
    void Execute();

private:
    friend class SessionJobQueue;
    Job* m_next = nullptr;
    void* m_target = nullptr;
    union
    {
        PacketHandlerFunc m_packetFunc = nullptr;
        DBHandlerFunc m_dbFunc;
        SystemHandlerFunc m_systemFunc;
    };
    const HandlerContext* m_context = nullptr;
    uint16_t m_packetId = 0;
    JobType m_type = JobType::NONE;
    PacketHandle m_payload;
};
