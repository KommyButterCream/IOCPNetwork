#pragma once

#include "../NetworkOptions.h"
#include "../NetworkTypes.h"
#include "ReceiveProcessor.h"
#include "WinsockRuntime.h"
#include "SessionReference.h"
#include "SocketHandle.h"
#include "../ConnectionId.h"

#include <WinSock2.h> // for SOCKET, ULONG_PTR
#include <cstddef> // for std::size_t
#include <memory> // for std::shared_ptr, std::unique_ptr
#include <mutex>  // for std::mutex
#include <condition_variable> // for std::condition_variable

namespace IOCPNetwork::Detail
{
    class ClientSessionPool;
    class JobScheduler;
    class PacketPool;
    class IOWorkerPool;

    struct IOCompletion;
    struct JobCallbacks;

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

        // Stopping 상태에서 모든 연결을 종료하고 최종 반환까지 기다린다.
        // 역할별 연결 생산 경롤,ㄹ 먼ㄴ저 중단하고 완료 후처리까지 회수해야 한다.
        void CloseSessionsAndWait() noexcept;

        // 역할별 소켓을 공통 IOCP에 등록하며 소켓 소유권은 가져오지 않는다.
        // 공통 시작 성공 후, 해당 소켓의 I/O를 발행하기 전에 호출한다.
        [[nodiscard]] bool AssociateTransportSocket(SOCKET socket, ULONG_PTR completionKey) noexcept;

        // 세션을 대여하고 소켓·수신 처리 경로를 연결한다.
        // 성공할 때만 소켓을 인수하며, 실패하면 빈 참조를 반환한다.
        [[nodiscard]] SessionReference PrepareSession(SocketHandle&& socket) noexcept;

        // 소켓 연결을 마친 세션에 첫 수신을 발행한다.
        // 호출자는 사용 참조를 보유하며, 발행하지 못하면 연결을 종료한다.
        [[nodiscard]] bool StartSessionReceive(ClientSession& session) noexcept;

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

        // 시스템 패킷을 즉시 처리한다. 계속 처리할 수 있으면 true다.
        // false 또는 예외이면 ReceiveProcessor가 해당 연결을 종료한다.
        [[nodiscard]] virtual bool HandleSystemPacket(ClientSession& session, Packet packet) = 0;

    private:
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
        std::condition_variable m_stateChanged;

        State m_state = State::Stopped;

        // 종료 대기자가 이전 실행의 정지를 확인하기 전에 재시작하지 못하게 한다.
        // m_stateMutex로 보호하며 atomic은 필요하지 않다.
        std::size_t m_stopWaiterCount = 0;

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
    };
}
