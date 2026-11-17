#pragma once

#include "Export.h"
#include "PacketTypes.h"
#include "ServiceCallbacks.h"
#include "PacketResult.h"
#include "ConnectionId.h"
#include "SessionTask.h"

#include <cstddef> // for std::byte, std::size_t
#include <span>    // for std::span
#include <memory> // for std::unique_ptr

namespace IOCPNetwork
{
    namespace Detail
    {
        class IOCPBaseImpl;
    }

    // 서버·클라이언트의 공통 공개 API와 Impl 소유권을 제공한다.
    // 실제 객체는 IOCPServer 또는 IOCPClient로 생성한다.
    class IOCPBase
    {
    public:
        // 기반 포인터를 통해서도 파생 공개 객체를 올바르게 파괴한다.
        IOCPNETWORK_API virtual ~IOCPBase();

        IOCPBase(const IOCPBase&) = delete;
        IOCPBase& operator=(const IOCPBase&) = delete;

        IOCPBase(IOCPBase&&) = delete;
        IOCPBase& operator=(IOCPBase&&) = delete;

        // Start 전에 서비스 ID별 핸들러를 등록한다.
        // 등록은 Stop 이후에도 유지하며, 실행 중 변경과 중복 등록은 거절한다.
        [[nodiscard]] IOCPNETWORK_API HandlerRegistrationResult RegisterPacketHandler(
            PacketId packetId, PacketHandler handler) noexcept;

        // 정지 상태에서 연결 이벤트 구성을 교체한다. 실행 중이면 false 다.
        // 비어 있는 함수는 해당 통지를 받지 않겠다는 뜻이다.
        [[nodiscard]] IOCPNETWORK_API bool SetConnectionEvents(ConnectionEvents events) noexcept;

        // 기존 본문을 새 풀 저장소에 복사하여 서비스 패킷을 만든다.
        // payload에는 전송 헤더를 포함하지 않는다.
        [[nodiscard]] IOCPNETWORK_API PacketResult CreatePacket(
            PacketId packetId, std::span<const std::byte> payload) noexcept;

        // 서비스 패킷 본문을 직접 작성할 저장소를 확보한다.
        // 작성 후 Finish 에 실제 작성한 본문 길이를 전달한다.
        [[nodiscard]] IOCPNETWORK_API PacketWriterResult CreatePacketWriter(
            PacketId packetId, std::size_t payloadCapacity) noexcept;

        // 실행 중인 엔진에서 연결 ID를 검증하고 서비스 패킷을 송신 접수한다.
        // Queued일 때만 Packet을 인수하며, 실패하면 원본을 유지한다.
        [[nodiscard]] IOCPNETWORK_API SendResult Send(ConnectionId id, Packet&& packet) noexcept;

        // 해당 연결의 Job 큐에 서비스 작업을 접수한다.
        // Queued일 때만 Task 를 인수하며, 실패하면 원본을 유지한다.
        [[nodiscard]] IOCPNETWORK_API PostResult Post(ConnectionId id, SessionTask&& task) noexcept;

        // 연결 하나에 정상 종료를 요청하며, 종료 완료까지 기다리지 않는다.
        // true는 유효한 연결에 요청을 전달했다는 뜻이며 최초 요청 여부는 아니다.
        [[nodiscard]] IOCPNETWORK_API bool Disconnect(ConnectionId id) noexcept;

    protected:
        // 파생 공개 객체가 만든 실제 Impl의 단독 소유권을 인수한다.
        explicit IOCPBase(std::unique_ptr<Detail::IOCPBaseImpl> impl) noexcept;

        // DLL 내부의 서버·클라이언트 구현에서 소유 중인 Impl을 빌린다.
        // 소유권은 IOCPBase에 있으며 반환한 참조를 별도로 보관하지 않는다.
        [[nodiscard]] Detail::IOCPBaseImpl& GetImpl() noexcept;

    private:
        // 실제 서버 또는 클라이언트 Impl 하나를 소유한다.
        std::unique_ptr<Detail::IOCPBaseImpl> m_impl;
    };
}
