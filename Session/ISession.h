#pragma once
#include <cstdint> // for uint32_t
#include <memory> // for std::unique_ptr
#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"

class SessionContext
{
public:
	virtual ~SessionContext() = default;
};

// 빌려 쓰는 연결 인터페이스입니다. 포인터를 보관해도 연결 수명이 늘어나지 않습니다.
// 공개 메서드에도 엔진의 기존 스레드·수명 규약이 적용됩니다.
class ISession
{
public:
    virtual ~ISession() = default;
    virtual uint32_t GetSessionID() const = 0;
    virtual bool IsEstablished() const = 0;
    // 성공하면 패킷 소유권을 이전하고, 실패하면 호출자의 패킷을 유지합니다.
    virtual bool EnqueueSendPacket(PacketHandle&& packet) = 0;
    // 값으로 전달받은 공유 참조는 성공·실패 모두에서 소비합니다.
    virtual bool EnqueueSharedSendPacket(SharedPacketHandle packet) = 0;
    virtual SessionContext* GetSessionContext() const = 0;
    // 컨텍스트 소유권을 세션으로 이전하며, 기존 컨텍스트는 삭제하고 교체합니다.
    virtual void SetSessionContext(std::unique_ptr<SessionContext> context) = 0;
    // 현재 실행 중인 클라이언트 패킷 핸들러 안에서만 유효합니다.
    virtual PacketHandle TakeCurrentPacket() = 0;
};
