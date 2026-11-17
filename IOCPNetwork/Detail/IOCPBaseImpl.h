#pragma once

#include "../ConnectionId.h"
#include "../NetworkOptions.h"
#include "../NetworkTypes.h"
#include "../PacketResult.h"
#include "../PacketTypes.h"
#include "../PacketView.h"
#include "../ServiceCallbacks.h"
#include "../SessionTask.h"
#include "ReceiveProcessor.h"
#include "SessionReference.h"
#include "SocketHandle.h"
#include "SystemPacket.h"
#include "WinsockRuntime.h"

#include <WinSock2.h> // for SOCKET, ULONG_PTR
#include <MSWSock.h> // for LPFN_CONNECTEX

#include <condition_variable> // for std::condition_variable_any
#include <cstddef> // for std::byte, std::size_t
#include <cstdint>  // for std::uint32_t
#include <memory> // for std::shared_ptr, std::unique_ptr
#include <mutex>  // for std::mutex
#include <span> // for std::span
#include <stop_token> // for std::stop_token
#include <thread> // for std::jthread
#include <unordered_map> // for std::unordered_map

namespace IOCPNetwork::Detail
{
    class ClientSessionPool;
    class JobScheduler;
    class PacketPool;
    class IOWorkerPool;

    struct IOCompletion;
    struct JobCallbacks;
    struct IOOperation;

    // 서버·클라이언트 Impl이 공유하는 자원과 실행 흐름의 기반이다.
    // DLL 내부 구현이며 공개 헤더에는 정의를 노출하지 않는다.
    class IOCPBaseImpl
    {
    public:
        // 기반 포인터로 파생 Impl을 올바르게 파괴한다.
        virtual ~IOCPBaseImpl();

        IOCPBaseImpl(const IOCPBaseImpl&) = delete;
        IOCPBaseImpl& operator=(const IOCPBaseImpl&) = delete;

        IOCPBaseImpl(IOCPBaseImpl&&) = delete;
        IOCPBaseImpl& operator=(IOCPBaseImpl&&) = delete;

        // 정지 상태에서 서비스 패킷 핸들러를 등록한다.
        // 중복 등록이나 실패 시 기존 핸들러는 변경하지 않는다.
        [[nodiscard]] HandlerRegistrationResult RegisterPacketHandler(PacketId packetId, PacketHandler handler) noexcept;

        // 정지 상태에서만 이벤트 구성을 교체한다.
        // 교체한 이전 콜백의 캡처 자원은 상태 락 밖에서 정리한다.
        [[nodiscard]] bool SetConnectionEvents(ConnectionEvents events) noexcept;

        // 기존 본문을 복사하여 서비스 패킷을 만든다.
        [[nodiscard]] PacketResult CreatePacket(PacketId packetId, std::span<const std::byte> payload) noexcept;

        // 서비스 ID와 용량을 검증하고 풀에서 작성 저장소를 확보한다.
        [[nodiscard]] PacketWriterResult CreatePacketWriter(PacketId packetId, std::size_t payloadCapacity) noexcept;

        // ID 검증과 사용 참조 확보 후 세션의 공개 송신 경로로 전달한다.
        // 엔진 종료 중이거나 조회할 수 없는 연결이면 Closed를 반환한다.
        [[nodiscard]] SendResult Send(ConnectionId id, Packet&& packet) noexcept;

        // 연결 참조와 빈 Task 저장소를 확보한 뒤 세션의 Job 접수 경로로 전달한다.
        // 접수 성공 전에는 원본 Task를 이동하지 않는다.
        [[nodiscard]] PostResult Post(ConnectionId id, SessionTask&& task) noexcept;

        // 연결 참조와 종료 기한을 확보한 뒤 기존 정상 종료 경로를 시작한다.
        // 엔진 상태 락을 해제한 상태에서 세션의 BeginDrain을 호출한다.
        [[nodiscard]] bool Disconnect(ConnectionId id) noexcept;

    protected:
        // 실제 서버·클라이언트 Impl의 일부로 생성한다.
        IOCPBaseImpl() noexcept;

        // 공통 자원과 워커를 준비한다. 워커 수는 자동 설정 해석이 끝난 값이다.
        // 성공하면 Starting을 유지하고, 실패하면 정리 후 Stopped로 복구한다.
        [[nodiscard]] StartResult StartCommon(
            const NetworkOptions& options,
            std::size_t maxSessions,
            std::size_t maxTotalSegmentBytes) noexcept;

        // 공통 자원과 역할별 시작 준비가 모두 성공했음을 확정한다.
        void CompleteStart() noexcept;

        // 공통 시작 성공 후 역할별 준비가 실패했을 때 시작 호출자가 실행한다.
        // 발행된 I/O와 세션을 정리한 뒤 공통 자원을 해제한다.
        void RollbackTransportStart() noexcept;

        // 외부 제어 스레드에서 종료 완료까지 기다린다.
        // 엔진 내부 실행 중이면 상태를 변경하지 않고 호출을 거절한다.
        [[nodiscard]] StopResult StopCommon() noexcept;

        // Stopping 상태에서 공통 기한으로 배출하고, 초과 시 강제 종료 후 최종 반환까지 기다린다.
        // 역할별 연결 생산 경로를 먼저 중단하고 완료 후처리까지 회수해야 한다.
        void CloseSessionsAndWait() noexcept;

        // 역할별 소켓을 공통 IOCP에 등록하며 소켓 소유권은 가져오지 않는다.
        // 공통 시작 성공 후, 해당 소켓의 I/O를 발행하기 전에 호출한다.
        [[nodiscard]] bool AssociateTransportSocket(SOCKET socket, ULONG_PTR completionKey) noexcept;

        // 세션을 대여하고 소켓·수신 처리 경로를 연결한다.
        // 성공할 때만 소켓을 인수하며, 실패하면 빈 참조를 반환한다.
        [[nodiscard]] SessionReference PrepareSession(SocketHandle&& socket) noexcept;

        // 세션 락으로 소켓 닫기를 막은 상태에서 ConnectEx를 발행한다.
        // 0은 접수 성공이며, 그 외의 값은 완료 통지가 없는 즉시 실패다.
        [[nodiscard]] std::uint32_t PostSessionConnect(
            ClientSession& session, LPFN_CONNECTEX connectEx,
            const sockaddr_in& remoteAddress, IOOperation& operation) noexcept;

        // Connect 성공 후 소켓의 연결 문맥을 갱신한다.
        // 성공은 0, 종료 경합·옵션 적용 실패는 해당 오류 코드를 반환한다.
        [[nodiscard]] std::uint32_t UpdateSessionConnectContext(ClientSession& session) noexcept;

        // 소켓 연결을 마친 세션에 첫 수신을 발행한다.
        // 호출자는 사용 참조를 보유하며, 발행하지 못하면 연결을 종료한다.
        [[nodiscard]] bool StartSessionReceive(ClientSession& session) noexcept;

        // 대기 상태를 먼저 설정하고 필요한 요청 송신과 첫 수신을 시작한다.
        // expectedPacket은 서버의 요청 대기 또는 클라이언트의 응답 대기다.
        [[nodiscard]] bool StartSessionHandshake(ClientSession& session, SystemPacketId expectedPacket) noexcept;

        // 수신 후처리에서 핸드셰이크를 검사하고 연결 통지 Job을 접수한다.
        // false면 호출자가 연결을 종료하며, 세션 참조를 유지해야 한다.
        [[nodiscard]] bool HandleHandshakePacket(ClientSession& session, PacketView packet) noexcept;

        // Ready이며 수신 중지·응답 대기가 없고 송신 간격이 지났을 때 요청한다.
        // 생성·접수 실패 시 종료하며, 호출자는 세션 사용 참조를 보유한다.
        void SendHeartbeatRequest(ClientSession& session) noexcept;

        // 하트비트 요청에는 응답하고, 응답은 현재 대기 번호와 기한을 검사한다.
        // 수신 후처리에서 호출하며, false이면 ReceiveProcessor가 연결을 종료한다.
        [[nodiscard]] bool HandleHeartbeatPacket(ClientSession& session, PacketView packet) noexcept;

        // 시스템 본문을 검증하고 새 Packet을 만들어 송신 큐에 접수한다.
        // 호출자는 같은 엔진에 속한 세션의 사용 참조를 보유해야 한다.
        [[nodiscard]] SendResult SendSystemPacket(
            ClientSession& session,
            SystemPacketId packetId,
            std::span<const std::byte> payload) noexcept;

        // 실행 중인 엔진에서 ID가 일치하는 연결의 사용 참조를 확보한다.
        // 연결 상태와 세대를 확인하며, 조회 실패 시 빈 참조를 반환한다.
        [[nodiscard]] SessionReference TryAcquireSession(ConnectionId id) noexcept;

        // 작업 생산을 차단하고 역할별 I/O와 모든 세션의 종료를 완료한다.
        // 일부만 초기화된 상태에서도 안전하며, 공통 워커는 정지하지 않는다.
        virtual void StopTransportAndSessions() noexcept = 0;

        // 서버 Accept 또는 클라이언트의 Connect 완료를 처리한다.
        // 파생 Impl이 담당하며, 실패·취소 완료도 정리해야 한다.
        virtual void HandleTransportCompletion(const IOCompletion& completion) noexcept = 0;

        // 연결·서비스 패킷·종료 Job의 처리 경로를 제공한다.
        // 반환된 콜백은 스케줄러가 소유하며 워커 시작 전에 확정한다.
        [[nodiscard]] virtual JobCallbacks CreateJobCallbacks() = 0;

        // 서비스 패킷·연결·종료 통지를 공개 핸들러와 이벤트에 연결한다.
        // 서버와 클라이언트가 공통으로 사용하는 내부 콜백 구성을 만든다.
        [[nodiscard]] JobCallbacks CreateServiceJobCallbacks();

        // 역할별 Impl이 공개 통지 대상으로 확정한 연결 준비 실패를 알린다.
        // 서비스 예외는 JobScheduler의 최종 정리 경로로 전달한다.
        void NotifyConnectFailed(DisconnectInfo info);

        // Job 워커에서 서비스 Id의 핸들러를 찾아 PAcket 소유권을 전달한다.
        // 핸드러 예외는 JobScheduler 가 처리하므로 noexcetp 를 붙이지 않는다.
        void HandleServicePacket(ClientSession& session, Packet packet);

        // 시스템 패킷을 즉시 처리한다. 계속 처리할 수 있으면 true다.
        // false 또는 예외이면 ReceiveProcessor가 해당 연결을 종료한다.
        [[nodiscard]] virtual bool HandleSystemPacket(ClientSession& session, Packet packet) = 0;

    private:
        // 필수 시스템 송신 실패의 사유를 기록한다. 먼저 확정된 종료 사유는 유지한다.
        static void CloseOnSystemSendFailure(ClientSession& session, SendResult result) noexcept;

#if defined(IOCPNETWORK_HEARTBEAT_TESTING)
        friend struct HeartbeatTestAccess;
#endif
#if defined(IOCPNETWORK_TESTING)
        friend struct ClientLifecycleTestAccess;
#endif
#if defined(IOCPNETWORK_TESTING)
        friend struct BaseImplTestAccess;
#endif
        // 종료 요청의 결과이며, Acquired인 호출만 실제 종료를 수행한다.
        enum class StopEntryResult
        {
            AlreadyStopped,
            Acquired
        };

        // 외부 제어 스레드에서 시작·종료 진행을 기다리고 종료 책임을 인수한다.
        // 엔진 워커와 시작·종료 처리 내부에서는 호출하지 않는다.
        [[nodiscard]] StopEntryResult BeginStop() noexcept;

        // 엔진 전체의 실행 단계다. 세션 하나의 연결 상태와는 별개다.
        enum class State
        {
            Stopped,  // 시작 전 또는 종료·실패 복구 완료
            Starting, // 공통 자원과 역할별 통신 준비 중
            Running,  // 시작 완료
            Stopping  // 새 진입을 제한하고 종료·자원 회수 중
        };

        // 시작 경로에서 처리 자원을 생성하며, 모두 성공한 경우에만 멤버에 반영한다.
        // workerCount 자동 설정은 호출 전에 해석하고, 워커는 아직 시작하지 않는다.
        [[nodiscard]] StartResult CreateProcessingResources(
            const NetworkOptions& options,  // 워커 수·배치 설정을 포함한 공통 설정
            std::size_t maxSessions,    // JobScheduler 준비 큐의 세션 수용량
            std::size_t maxTotalSegmentBytes) noexcept;   // 패킷 풀의 전체 세그먼트 메모리 한도

        // 세션 대여 전에 Job·I/O 워커를 시작한다.
        // 일부 시작에 실패하면 이미 시작한 워커도 정지한다.
        [[nodiscard]] StartResult StartWorkers() noexcept;

        // 주기 스레드를 생성한다. Starting에서는 실제 검사 없이 기다린다.
        [[nodiscard]] bool StartMaintenance() noexcept;

        // 대기를 깨워 종료를 요청하고 실행 중인 검사까지 합류한다.
        // 시작·종료 책임자만 호출하며, 엔진 상태 락을 잡지 않아야 한다.
        void StopMaintenance() noexcept;

        // 시작 완료를 기다린 뒤 중단 요청까지 주기 검사를 반복한다.
        void RunMaintenance(std::stop_token stopToken) noexcept;

        // 실행 중인 연결을 순회하여 시간 초과 종료와 하트비트 송신을 수행한다.
        // 전용 주기 작업에서만 호출하며, 풀 해제 전에 해당 작업을 합류시켜야 한다.
        void CheckSessions() noexcept;

        // 새 작업 생산과 역할별 I/O, 모든 세션의 반환이 끝난 뒤 호출한다.
        // 워커를 합류시키고 공통 자원을 해제하며 실행 상태는 변경하지 않는다.
        void ReleaseProcessingResources() noexcept;

        // 시작 실패 복구 또는 종료의 자원 정리가 끝났음을 확정한다.
        // Stopped로 전환하고 상태 변경 대기자를 깨운다.
        void CompleteStop() noexcept;

        // 세션 대여·통신 발행 전 공통 시작 실패를 정리한다.
        // 모든 자원 정리가 끝난 뒤에만 다시 시작할 수 있게 한다.
        void RollbackCommonStart() noexcept;

        // I/O 워커의 함수 포인터 콜백을 실제 Impl 객체로 연결한다.
        static void DispatchIOCompletion(void* context, const IOCompletion& completion) noexcept;

        // 함수 포인터 콜백의 문맥을 실제 Impl로 변환한다.
        // 시스템 처리 예외는 ReceiveProcessor까지 전달한다.
        [[nodiscard]] static bool DispatchSystemPacket(void* context, ClientSession& session, Packet packet);

        // 실행 상태와 공통 설정의 변경을 보호한다.
        std::mutex m_stateMutex;

        // 시작 완료·실패 또는 종료 완료를 기다리는 호출을 깨운다.
        std::condition_variable_any m_stateChanged;

        State m_state = State::Stopped;

        // 종료 대기자가 이전 실행의 정지를 확인하기 전에 재시작하지 못하게 한다.
        // m_stateMutex로 보호하며 atomic은 필요하지 않다.
        std::size_t m_stopWaiterCount = 0;

        // 정지 상태에서만 변경하고, 실행 중에는 조회만 한다.
        // Stop 이후에도 유지하여 같은 객체의 재시작에 사용한다.
        std::unordered_map<PacketId, PacketHandler> m_packetHandlers;

        // 정지 상태에서 설정하고, 실행 중에는 변경하지 않는다.
        // Stop 이후에도 유지하여 재시작할 때 같은 구성을 사용한다.
        ConnectionEvents m_connectionEvents;

        // 시작 요청의 공통 설정을 복사해 보관한다.
        // 시작 시 검증하고, 자원 회수가 끝나기 전에는 교체하지 않는다.
        NetworkOptions m_options{};

        // 소켓·워커보다 먼저 선언하여 기본 파괴 순서에서도 더 오래 유지한다.
        // 실제 종료에서는 모든 Winsock 사용이 끝난 뒤 명시적으로 Reset한다.
        WinsockRuntime m_winsock;

        // 엔진과 외부 Packet이 공유하는 원래 패킷 풀 저장소다.
        std::shared_ptr<PacketPool> m_packetPool;

        // 공통 패킷 추출·시스템 처리·수신 재개를 담당한다.
        std::unique_ptr<ReceiveProcessor> m_receiveProcessor;

        // 세션별 Job 순서와 최종 종료 통지를 처리한다.
        std::unique_ptr<JobScheduler> m_jobScheduler;

        // 모든 세션의 최종 반환 후 스케줄러보다 먼저 파괴한다.
        std::unique_ptr<ClientSessionPool> m_sessionPool;

        // 엔진당 하나의 IOCP와 그 완료를 기다리는 워커들을 소유한다.
        // 실제 종료에서는 모든 세션 완료를 회수한 뒤 워커를 정지한다.
        std::unique_ptr<IOWorkerPool> m_ioWorkers;

        // 공통 자원을 사용하는 주기 작업이다. 자원 해제 전에 명시적으로 합류한다.
        std::jthread m_maintenanceThread;
    };
}
