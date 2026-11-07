#pragma once
#include "../Buffer/PacketHandle.h"
#include "JobDefs.h"
#include "../Session/ISession.h"

class SessionJobQueue;
class PacketHandler;
class ClientSession;

// Job은 풀의 고정 주소에 두고 소유 핸들만 이동합니다.
// 세션 Job은 연결 참조를 보유합니다. DB/System 대상과 핸들러·컨텍스트는 실행 종료까지 유지합니다.
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
    [[nodiscard]] bool InitializePacketJob(const PacketHandler& handler, ISession* session,
        uint16_t packetId, PacketHandle&& packet, const HandlerContext& context);

    // 빈 데이터도 허용하며, 설정에 성공한 경우에만 소유권을 이전한다.
    [[nodiscard]] bool SetDBJob(DBHandlerFunc handler, IDBConnection* connection,
        PacketHandle&& payload, const HandlerContext& context);
    [[nodiscard]] bool SetSystemJob(SystemHandlerFunc handler, IOCPCore* core,
        PacketHandle&& payload, const HandlerContext& context);

    // 연결 이벤트 또는 외부 작업을 보관하며, 반환할 때까지 세션을 보호합니다.
    [[nodiscard]] bool InitializeServiceEvent(ClientSession& session, ServiceEvent event, SessionTask task);

    [[nodiscard]] JobType GetType() const noexcept { return m_type; }

    void Execute();

private:
    friend class SessionJobQueue;
    friend class JobDeleter;
    ClientSession* m_sessionReference = nullptr;
    ServiceEvent m_event = ServiceEvent::Task;
    SessionTask m_serviceTask;
    Job* m_next = nullptr;
    void* m_target = nullptr;
    union
    {
        const PacketHandler* m_packetHandler = nullptr;
        DBHandlerFunc m_dbFunc;
        SystemHandlerFunc m_systemFunc;
    };
    const HandlerContext* m_context = nullptr;
    uint16_t m_packetId = 0;
    JobType m_type = JobType::NONE;
    PacketHandle m_payload;
};
