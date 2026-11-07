#pragma once
#include "../Buffer/PacketHandle.h"
#include "../Buffer/SharedPacketHandle.h"
#include <cstdint> // for uint32_t
class ISession;
namespace ENGINE_POOL { struct EnginePools; }

// 빌려 쓰는 엔진 컨텍스트입니다. 엔진 파괴·재시작 이후까지 보관하지 않습니다.
struct HandlerContext
{
    void* serviceContext = nullptr;
    [[nodiscard]] IOCP_ENGINE_API PacketHandle CreatePacket(std::size_t size) const;
    // 성공·실패 모두에서 전달받은 패킷을 소비합니다.
    [[nodiscard]] IOCP_ENGINE_API SharedPacketHandle CreateSharedPacket(PacketHandle packet) const;

private:
    friend struct ENGINE_POOL::EnginePools;
    TlsMemoryPool* m_packetPool = nullptr;
    TlsMemoryPool* m_sharedPool = nullptr;
};

// 패킷 콜백은 호출 중에만 버퍼를 빌려 사용합니다.
using PacketHandlerFunc = bool (*)(ISession* session, const char* packetData, uint32_t packetSize,
                                   const HandlerContext& context);
