#pragma once

#include "ConnectionId.h"
#include "Detail/IOOperation.h"
#include "Detail/SessionReference.h"
#include "Detail/SocketHandle.h"
#include "Packet.h"
#include "SessionTask.h"
#include "NetworkTypes.h"

#include <atomic>  // for std::atomic
#include <cstddef> // for std::size_t
#include <memory>  // for std::unique_ptr
#include <mutex>   // for std::mutex

namespace IOCPNetwork
{
    struct NetworkOptions;

    namespace Detail
    {
        class ClientSessionPool;
        class IOWorkerPool;
        class Job;
        class JobScheduler;
        class PacketPool;
        class ReceiveProcessor;
        class RecvPacketBuffer;
        class SendPacketBatch;
        class SendPacketQueue;
        class SessionJobQueue;
        class IOCPBaseImpl;

        struct IOCompletion;

        // ClientSession을 completion key로 사용하는 송수신 완료를 분기한다.
        // IOWorkerPool 콜백 형식에 맞추며 context는 사용하지 않는다.
        void DispatchSessionCompletion(void* context, const IOCompletion& completion) noexcept;

#if defined(IOCPNETWORK_TESTING)
        struct SchedulerTestAccess;
        struct ReceiveTestAccess;
        struct BaseImplTestAccess;
#endif
    }

    // 연결 하나의 상태와 사용 수명을 관리한다.
    // 객체는 풀이 소유하며, 사용 참조는 해당 슬롯의 재대여를 막는다.
    class ClientSession final
    {
    public:
        ~ClientSession();

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        ClientSession(ClientSession&&) = delete;
        ClientSession& operator=(ClientSession&&) = delete;

        // 현재 연결 ID를 값으로 반환한다.
        // 호출자는 사용 참조 등으로 세션의 수명을 보호하고 있어야 한다.
        [[nodiscard]] ConnectionId GetConnectionId() const noexcept;

        // Closing으로 전환하고 소켓·보관 패킷을 정리한 뒤 연결 유지용 참조를 반환한다.
        // 호출자는 별도의 사용 참조를 보유해야 하며, 중복 호출은 무시한다.
        void BeginClose() noexcept;

    private:
        friend class Detail::ClientSessionPool;
        friend class Detail::SessionReference;
        friend class Detail::SessionJobQueue;
        friend class Detail::JobScheduler;
        friend class Detail::ReceiveProcessor;
        friend void Detail::DispatchSessionCompletion(
            void* context,
            const Detail::IOCompletion& completion) noexcept;
        friend class Detail::IOCPBaseImpl;

#if defined(IOCPNETWORK_TESTING)
        // 공개 서비스 API가 연결되기 전 내부 접수 경로를 검증한다.
        friend struct Detail::SchedulerTestAccess;
        friend struct Detail::ReceiveTestAccess;
        friend struct Detail::BaseImplTestAccess;
#endif

        // 세션 저장소는 풀만 생성한다.
        ClientSession() noexcept;

        // 풀이 외부에 공개되기 전에 세션의 Job 저장소를 한 번 확보한다.
        // 잘못된 용량이나 메모리 부족이면 false를 반환한다.
        [[nodiscard]] bool InitializeJobQueue(
            std::size_t maxPendingJobs,
            std::size_t maxPendingPacketBytes,
            Detail::JobScheduler& scheduler) noexcept;

        // 풀이 공개되기 전에 송수신 저장소를 한 번 확보한다.
        // 실패하면 멤버를 변경하지 않아 같은 세션에서 다시 시도할 수 있다.
        [[nodiscard]] bool InitializeIOStorage(const NetworkOptions& options) noexcept;

        // 연결된 소켓을 IOCP에 등록하고 닫기 책임을 인수한다.
        // 실패하면 인자의 소유권을 유지하며, 호출자는 별도 세션 참조를 보유한다.
        [[nodiscard]] bool AttachSocket(
            Detail::SocketHandle&& socket,
            Detail::IOWorkerPool& ioWorkers,
            Detail::ReceiveProcessor& receiveProcessor) noexcept;

        // 수신 발행 여부와 발행하지 않은 이유를 구분한다.
        enum class ReceivePostResult
        {
            Posted,        // 수신 I/O가 접수되어 완료를 기다림
            AlreadyActive, // 기존 수신 또는 완료 후처리가 진행 중
            Paused,        // 미접수 패킷 또는 수신 공간 부족으로 대기
            NotReady,      // 연결 상태나 소켓이 수신 가능한 상태가 아님
            Failed         // WSARecv가 즉시 실패하여 접수되지 않음
        };

        // 수신 I/O를 최대 하나만 발행한다. 호출자는 별도 세션 참조를 보유한다.
        // 즉시 발행 실패이면 연결 종료를 요청하고 Failed를 반환한다.
        [[nodiscard]] ReceivePostResult PostReceive() noexcept;

        // 수신 완료를 확인하고 I/O 참조를 인수하여 수신량을 반영한다.
        // 성공하면 후처리용 참조를 반환하고, 실패·종료이면 정리 후 빈 참조를 반환한다.
        [[nodiscard]] Detail::SessionReference BeginReceiveCompletion(const Detail::IOCompletion& completion) noexcept;

        // 수신 후처리 중 패킷 하나를 꺼낸 결과다.
        enum class ReceivePacketStatus
        {
            Complete,     // 완성된 Packet을 반환함
            NeedMoreData, // 추가 수신이 필요함
            Paused,       // 앞선 Packet의 Job 접수를 기다림
            Stopped,      // 연결 종료로 추출을 중단함
            Failed        // 패킷 검증 또는 저장소 확보 실패
        };

        struct ReceivePacketResult
        {
            ReceivePacketStatus status;

            Packet packet;
        };

        // 수신 후처리용 참조를 유지한 상태에서 패킷 하나를 추출한다.
        // Complete일 때만 반환 결과가 Packet을 소유한다.
        [[nodiscard]] ReceivePacketResult ReadReceivedPacket(Detail::PacketPool& packetPool) noexcept;

        // 수신한 서비스 패킷의 처리 결과다.
        enum class ReceivedPacketEnqueueResult
        {
            Queued, // Job 큐에 접수함
            Paused, // 큐 포화로 세션에 보관함
            Stopped, // 연결 종료로 패킷을 폐기함
            Failed // 서비스 패킷을 처리할 수 없는 상태여서 종료 요청함
        };

        // 추출한 서비스 패킷을 인수하여 Job 접수 또는 보관을 수행한다.
        // 수신 후처리 중에만 호출하며, 모든 결과에서 인자 소유권을 소비한다.
        [[nodiscard]] ReceivedPacketEnqueueResult EnqueueReceivedPacket(Packet&& packet) noexcept;

        // 보관 패킷이 있고 다른 수신 처리가 없으면 재개 처리용 참조를 확보한다.
        // 호출자는 별도 세션 참조를 보유하며, 재개할 수 없으면 빈 참조를 반환한다.
        [[nodiscard]] Detail::SessionReference TryBeginReceiveResume() noexcept;

        // 재개 처리의 독점 상태에서 보관 패킷을 다시 Job에 접수한다.
        // 재차 포화되면 같은 패킷을 다시 보관하고 Paused를 반환한다.
        [[nodiscard]] ReceivedPacketEnqueueResult RetryPendingReceivePacket() noexcept;

        // 수신 후처리를 끝내고 필요하면 다음 수신을 발행한다.
        // true이면 보관 패킷 재개를 다시 시도하며, 호출자는 참조를 계속 유지한다.
        [[nodiscard]] bool FinishReceiveCompletion(bool resumeReceive) noexcept;

        // 상태와 송신 한도를 확인하고 Packet을 송신 큐에 접수한다.
        // Queued일 때만 인자를 이동하며, 호출자는 별도 세션 참조를 보유한다.
        [[nodiscard]] SendResult EnqueueSendPacket(Packet&& packet) noexcept;

        // 송신 발행 여부와 발행하지 않은 이유를 구분한다.
        enum class SendPostResult
        {
            Posted,        // 송신 I/O가 접수되어 완료를 기다림
            AlreadyActive, // 기존 송신 또는 완료 후처리가 진행 중
            Empty,         // 배치와 큐에 송신할 데이터가 없음
            NotReady,      // 연결 상태나 소켓이 송신 가능한 상태가 아님
            Failed         // WSASend가 즉시 실패하여 접수되지 않음
        };

        // 기존 배치의 남은 구간 또는 새 배치를 최대 하나 발행한다.
        // 호출자는 별도 세션 참조를 보유하며, 즉시 실패이면 종료·정리한다.
        [[nodiscard]] SendPostResult PostSend() noexcept;

        // 송신 완료량과 예산을 반영하고 남은 배치 또는 다음 배치를 발행한다.
        // 완료 처리 동안 기존 I/O 참조를 유지하며, 실패하면 종료를 요청한다.
        void HandleSendCompletion(const Detail::IOCompletion& completion) noexcept;

        // Closing 상태에서 대기 큐·배치를 비우고 송신 예산을 초기화한다.
        // 호출자는 m_mutex를 보유하며, 송신 I/O와 후처리가 비활성이어야 한다.
        void ClearPendingSendLocked() noexcept;

        // 연결의 수명 단계. 접근과 전환은 m_mutex로 보호한다.
        enum class State
        {
            Free,       // 정리 완료, 풀에서 대여 가능
            Preparing,  // 연결 준비·핸드셰이크 진행
            Ready,      // 일반 서비스 통신 가능
            Draining,   // 정상 종료를 위해 기존 작업과 송신을 처리 중
            Closing,    // 새 작업을 차단하고 기존 작업·I/O를 정리 중
            Finalizing  // 사용 참조 0, 최종 통지·정리 후 풀 반환 대기
        };

        // 세션 상태와 큐 접수를 함께 판단한 내부 결과다.
        enum class EnqueueJobResult
        {
            Queued,
            NotReady,
            Closed,
            QueueFull
        };

        // 세션 상태 확인·Job 접수·최초 실행 예약을 연결한다.
        // Task는 원본 함수를 함께 전달하며, 접수 실패 시 인자 소유권을 유지한다.
        [[nodiscard]] EnqueueJobResult EnqueueJob(Detail::Job&& job, SessionTask* sourceTask = nullptr) noexcept;

        // 접수된 Job의 실행 시작 여부를 종료 전환과 같은 락으로 판단한다.
        // false이면 사용자 함수를 호출하지 않고 작업 자원만 정리한다.
        [[nodiscard]] bool TryBeginJob(const Detail::Job& job) noexcept;

        // 기존 참조로 수명이 보호되는 경로에서 사용 참조 하나를 추가한다.
        // 새 작업의 허용 여부와 연결 ID 검증은 호출 경로에서 먼저 수행한다.
        [[nodiscard]] Detail::SessionReference AcquireReference() noexcept;

        // 사용 참조 하나를 반환한다.
        // 마지막 반환이면 Closing에서 Finalizing으로 전환한다.
        void ReleaseReference() noexcept;

        // 종료 통지 후 풀 반환 직전에 연결별 상태를 초기화한다.
        // 호출자는 m_mutex를 보유하며, 모든 사용 참조와 I/O가 끝나 있어야 한다.
        void ResetForReuseLocked() noexcept;

        // 자신을 소유한 풀을 참조한다. 풀 생성 시 설정하고 재대여 때도 유지한다.
        // 풀의 수명은 모든 세션의 최종 마무리가 끝날 때까지 유지된다.
        Detail::ClientSessionPool* m_pool = nullptr;

        // 엔진이 소유하는 담당 스케줄러다. 초기화 후 재대여 때도 유지한다.
        // 모든 세션 작업과 최종 마무리가 끝날 때까지 살아 있어야 한다.
        Detail::JobScheduler* m_scheduler = nullptr;

        // 엔진이 소유하는 공통 수신 처리기다. 소켓 인수 시 연결한다.
        // 세션의 모든 I/O·Job·최종 정리가 끝날 때까지 대상이 유지되어야 한다.
        Detail::ReceiveProcessor* m_receiveProcessor = nullptr;

        // 상태와 연결 정보를 보호한다. 풀과 함께 잠글 때는 풀 락을 먼저 잡는다.
        mutable std::mutex m_mutex;

        State m_state = State::Free;

        ConnectionId m_connectionId{};

        // 현재 연결의 연결 통지가 접수되었는지 기록한다.
        // 최종 종료 통지 여부를 판단하며, 풀 반환 시 초기화한다.
        bool m_connectedNotificationQueued = false;

        // 연결 유지·I/O·Job·조회 등 현재 연결을 사용하는 참조의 합계.
        std::atomic<std::size_t> m_referenceCount{ 0 };

        // 세션의 Job을 보관한다. 저장소는 풀 생성 시 확보하고 재대여 시 재사용한다.
        std::unique_ptr<Detail::SessionJobQueue> m_jobQueue;

        // 현재 연결의 소켓을 단독 소유한다.
        // 접근·이전·닫기는 m_mutex로 보호한다.
        Detail::SocketHandle m_socket;

        // 수신 저장소와 진행 상태.

        // 수신 바이트를 보관한다. 저장소는 초기화 때 확보하고 재사용한다.
        std::unique_ptr<Detail::RecvPacketBuffer> m_recvPacketBuffer;

        // 세션당 하나의 수신 I/O에 사용하는 고정 주소의 작업 저장소다.
        Detail::IOOperation m_receiveOperation{ Detail::IOOperationType::Receive };

        // 발행한 수신 I/O가 완료 처리될 때까지 세션의 재대여를 막는다.
        Detail::SessionReference m_receiveReference;

        // 수신 발행부터 완료 후처리까지 새 수신 시작을 막는다.
        // m_mutex로 보호하며, 풀 반환 여부를 판정하는 카운터는 아니다.
        bool m_receiveActive = false;

        // 수신 후처리 중 Job 워커가 요청한 재개 시도를 기억한다.
        // m_mutex로 보호하며, 후처리 종료 때 소비한다.
        bool m_receiveResumeRequested = false;

        // 완성됐지만 Job 큐에 접수하지 못한 수신 패킷 하나를 보관한다.
        // 이 패킷을 먼저 접수하기 전에는 다음 패킷을 추출하지 않는다.
        Packet m_pendingReceivePacket;

        // 송신 저장소와 진행 상태.

        // 초기화 시 확정되는 송신 패킷 크기와 접수 한도다.
        std::size_t m_maxSendPacketSize = 0;

        std::size_t m_maxPendingSendPackets = 0;

        std::size_t m_maxPendingSendBytes = 0;

        // 큐와 배치를 합산한 미완료 송신 패킷 수다.
        // m_mutex로 보호하며, 패킷 전체 송신 완료 시 감소한다.
        std::size_t m_pendingSendPackets = 0;

        // 큐와 배치에서 아직 송신 완료되지 않은 바이트 수다.
        // m_mutex로 보호하며, 부분 송신 완료량도 차감한다.
        std::size_t m_pendingSendBytes = 0;

        // 아직 송신 배치로 옮기지 않은 Packet을 FIFO로 보관한다.
        std::unique_ptr<Detail::SendPacketQueue> m_sendPacketQueue;

        // 현재 송신할 Packet과 WSABUF 배열을 보관한다.
        // 부분 송신이면 남은 구간을 유지한다.
        std::unique_ptr<Detail::SendPacketBatch> m_sendPacketBatch;

        // 세션당 하나의 송신 I/O에 사용하는 고정 주소의 작업 저장소다.
        Detail::IOOperation m_sendOperation{ Detail::IOOperationType::Send };

        // 발행한 송신 I/O가 완료 처리될 때까지 세션의 재대여를 막는다.
        Detail::SessionReference m_sendReference;

        // 송신 발행부터 완료 후처리까지 다른 경로의 송신 시작을 막는다.
        // m_mutex로 보호하며, 풀 반환 여부를 판정하는 카운터는 아니다.
        bool m_sendActive = false;

        // 연결 유지용 참조 하나를 소유한다. Closing 전환 경로에서 락 밖으로 옮겨 반환한다.
        Detail::SessionReference m_connectionReference;
    };
}
