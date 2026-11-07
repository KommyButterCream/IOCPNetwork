#pragma once
#include <cstdint> // for uint32_t
#include <memory> // for std::unique_ptr
#include <functional> // for std::function
#include "DisconnectReason.h"
#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"

// 풀 내 세션 번호와 연결 세대를 함께 식별합니다. 같은 프로세스의 엔진·재시작·재대여를 구분합니다.
struct ConnectionId
{
    uint64_t generation = 0;
    uint32_t sessionId = UINT32_MAX;
    explicit operator bool() const noexcept { return generation != 0; }
    bool operator==(const ConnectionId&) const = default;
};

class ISession;
// 작업은 같은 연결의 핸들러와 직렬 실행됩니다. 참조 캡처 대상은 작업이 끝날 때까지 유지합니다.
using SessionTask = std::function<void(ISession&)>;

// Queued는 엔진 큐 등록 성공이며, 상대의 수신·처리 완료를 뜻하지 않습니다.
enum class SendResult
{
    Queued,
    NotReady,
    Closed,
    QueueFull,
    InvalidPacket,
    PacketTooLarge,
    NoMemory
};

class SessionContext
{
public:
	virtual ~SessionContext() = default;
};

// 콜백·핸들러·Post 실행 중에만 빌려 쓰는 연결입니다. 외부에는 ConnectionId를 보관합니다.
// 컨텍스트는 같은 연결의 서비스 작업 안에서 접근하고, 외부 작업은 엔진의 Post로 전달합니다.
class ISession
{
public:
    virtual ~ISession() = default;
    virtual uint32_t GetSessionID() const = 0;
    virtual ConnectionId GetConnectionId() const noexcept = 0;
    // 완료를 기다리지 않는 연결 하나의 종료 요청이며, 핸들러 안에서도 호출할 수 있습니다.
    virtual void RequestDisconnect(DisconnectReason reason = DisconnectReason::LocalRequest) = 0;
    virtual bool IsEstablished() const = 0;
    // 서비스 패킷을 전송합니다. 값으로 받은 핸들은 성공·실패 모두에서 소비합니다.
    [[nodiscard]] virtual SendResult Send(PacketHandle packet) = 0;
    [[nodiscard]] virtual SendResult Send(SharedPacketHandle packet) = 0;
    // 성공하면 패킷 소유권을 이전하고, 실패하면 호출자의 패킷을 유지합니다.
    virtual bool EnqueueSendPacket(PacketHandle&& packet) = 0;
    // 값으로 전달받은 공유 참조는 성공·실패 모두에서 소비합니다.
    virtual bool EnqueueSharedSendPacket(SharedPacketHandle packet) = 0;
    virtual SessionContext* GetSessionContext() const = 0;
    // 컨텍스트 소유권을 세션으로 이전하며, 기존 컨텍스트는 삭제하고 교체합니다.
    virtual void SetSessionContext(std::unique_ptr<SessionContext> context) = 0;
};
