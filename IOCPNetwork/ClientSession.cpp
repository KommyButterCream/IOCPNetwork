#include "ClientSession.h"

#include "Detail/ClientSessionPool.h"
#include "Detail/IOWorkerPool.h"
#include "Detail/Job.h"
#include "Detail/JobScheduler.h"
#include "Detail/OptionsValidation.h"
#include "Detail/ReceiveProcessor.h"
#include "Detail/RecvPacketBuffer.h"
#include "Detail/SendPacketBatch.h"
#include "Detail/SendPacketQueue.h"
#include "Detail/SessionFinalization.h"
#include "Detail/SessionJobQueue.h"
#include "Detail/SessionReference.h"
#include "NetworkOptions.h"
#include "PacketTypes.h"
#include "Detail/JobExecutionScope.h"

#include <algorithm> // for std::min
#include <exception> // for std::terminate
#include <limits>    // for std::numeric_limits
#include <new>       // for std::bad_alloc
#include <mutex>     // for std::lock_guard, std::unique_lock
#include <utility>   // for std::move, std::in_place_type
#include <optional> // for std::optional, std::nullopt
#include <variant> // for std::holds_alternative, std::get, std::get_if

namespace IOCPNetwork
{
#if defined(IOCPNETWORK_TESTING)
    namespace Detail::SessionStorageTestHooks
    {
        // 초기화 중 자원 정리를 검증하는 로컬 테스트 전용 실패 지점이다.
        bool FailStorageInitialization() noexcept;
    }

    namespace Detail::SessionSendTestHooks
    {
        // 테스트에서 완료량을 제어할 때만 실제 송신 발행을 대신한다.
        bool InterceptSend() noexcept;
    }
#endif

    ClientSession::ClientSession() noexcept = default;

    ClientSession::~ClientSession() = default;

    ConnectionId ClientSession::GetConnectionId() const noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        return m_connectionId;
    }

    SendResult ClientSession::Send(Packet&& packet) noexcept
    {
        if (!packet || !IsServicePacketId(packet.View().packetId))
        {
            return SendResult::InvalidPacket;
        }

        return EnqueueSendPacket(std::move(packet));
    }

    void ClientSession::BeginClose(DisconnectInfo info) noexcept
    {
        BeginCloseImpl(info, nullptr);
    }

    void ClientSession::BeginDrain(DisconnectInfo info, std::chrono::steady_clock::time_point deadline) noexcept
    {
        Detail::ReceiveProcessor* receiveProcessor = nullptr;
        DWORD cancelError = ERROR_SUCCESS;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            switch (m_state)
            {
            case State::Ready:
            {
                // 정상 종료 중에도 연결을 유지하며 기존 작업을 처리해야 한다.
                if (!m_connectionReference || m_receiveProcessor == nullptr)
                {
                    std::terminate();
                }

                // 종료를 처음 시작하는 경로가 사유와 전체 기한을 확정한다.
                m_disconnectInfo = info;

                m_shutdownDeadline = deadline;

                // 고정된 수신 범위의 소진 여부는 후처리에서 확인한다.
                m_receiveDrained = false;

                // 수신 완료의 바이트 반영과 같은 락 안에서 종료 기준을 확정한다.
                // 이후 완료된 수신 바이트는 버퍼의 읽기 가능 범위에 추가하지 않는다.
                m_state = State::Draining;

                // I/O 참조가 멤버에 있으면 아직 완료를 인수하지 않은 수신이다.
                // 후처리 중인 수신은 참조가 지역 변수에 있으므로 취소 대상이 아니다.
                if (m_receiveReference)
                {
                    if (!m_receiveActive || !m_socket)
                    {
                        std::terminate();
                    }

                    m_receiveCancellationRequested = true;

                    // 송신에는 영향을 주지 않고 현재 수신 작업 하나만 취소한다.
                    if (!::CancelIoEx(
                        reinterpret_cast<HANDLE>(m_socket.Get()),
                        &m_receiveOperation.overlapped))
                    {
                        const DWORD errorCode = ::GetLastError();

                        m_receiveCancellationRequested = false;

                        // 이미 완료되어 취소 대상이 없을 수 있다.
                        // 이 경우에도 기존 완료 통지가 참조를 반환하도록 기다린다.
                        if (errorCode != ERROR_NOT_FOUND)
                        {
                            cancelError = errorCode;
                        }
                    }
                }

                receiveProcessor = m_receiveProcessor;

                break;
            }

            case State::Draining:
            {
                // 기존 종료 사유는 유지하고, 더 이른 기한만 반영한다.
                if (deadline < m_shutdownDeadline)
                {
                    m_shutdownDeadline = deadline;
                }

                if (!m_connectionReference || m_receiveProcessor == nullptr)
                {
                    std::terminate();
                }

                // 중복 요청도 기존 재개 경로에서 안전하게 합쳐진다.
                receiveProcessor = m_receiveProcessor;

                break;
            }

            case State::Preparing:
            {
                // 서비스 준비 전의 연결은 정상 배출을 기다리지 않는다.
                // BeginClose가 같은 락을 사용하므로 락 밖에서 호출한다.
                break;
            }

            case State::Free:
            case State::Closing:
            case State::Finalizing:
            {
                // 이미 끝났거나 종료 중인 연결을 다시 시작하지 않는다.
                return;
            }

            default:
            {
                std::terminate();
            }
            }
        }

        if (cancelError != ERROR_SUCCESS)
        {
            // 취소 요청 자체가 실패하면 기존 즉시 종료 경로로 정리한다.
            BeginClose(DisconnectInfo{ DisconnectReason::TransportError, static_cast<std::uint32_t>(cancelError) });

            return;
        }

        if (receiveProcessor != nullptr)
        {
            // 재개 함수가 세션 락을 사용하므로 반드시 락 밖에서 호출한다.
            // 호출자가 보유한 세션 참조가 이 호출 동안 수명을 보호한다.
            receiveProcessor->ResumeReceive(*this);

            return;
        }

        BeginClose(info);
    }

    bool ClientSession::IsDrainWorkCompleteLocked() const noexcept
    {
        if (m_state != State::Draining)
        {
            return false;
        }

        // 고정된 수신 범위를 모두 처리하고 후처리까지 끝나야 한다.
        // 큐 포화로 보관한 Packet도 남아 있으면 안 된다.
        if (!m_receiveDrained ||
            m_receiveActive ||
            m_pendingReceivePacket)
        {
            return false;
        }

        if (!m_jobQueue)
        {
            std::terminate();
        }

        // 배열이 비어 있어도 워커가 인출한 Job을 실행 중일 수 있다.
        // 미완료 예산과 실행 예약까지 모두 해제되어야 한다.
        if (!m_jobQueue->IsIdle())
        {
            return false;
        }

        // Job이 접수한 응답의 부분 송신과 완료 후처리까지 기다린다.
        if (m_pendingSendPackets != 0 ||
            m_pendingSendBytes != 0 ||
            m_sendActive)
        {
            return false;
        }

        return true;
    }

    void ClientSession::TryShutdownSend() noexcept
    {
        int errorCode = 0;

        bool peerSendClosed = false;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_sendShutdownStarted)
            {
                return;
            }

            if (!IsDrainWorkCompleteLocked())
            {
                return;
            }

            // Draining 연결은 아직 소켓을 소유하고 있어야 한다.
            if (!m_socket)
            {
                std::terminate();
            }

            // 실패 후 락 밖에서 종료를 요청하는 동안에도 재시도를 막는다.
            m_sendShutdownStarted = true;

            // 상태 확인과 송신 종료를 같은 락 안에서 수행한다.
            // 다른 경로가 소켓을 닫거나 이전하는 것과 경합하지 않게 한다.
            if (::shutdown(m_socket.Get(), SD_SEND) == SOCKET_ERROR)
            {
                errorCode = ::WSAGetLastError();
            }
            else
            {
                peerSendClosed = m_peerSendClosed;
            }
        }

        if (errorCode != 0)
        {
            // BeginClose가 같은 락을 사용하므로 락 밖에서 호출한다.
            // Draining 진입 때 확정한 최초 종료 사유는 기존 정책대로 유지된다.
            BeginClose(DisconnectInfo{ DisconnectReason::TransportError, static_cast<std::uint32_t>(errorCode) });

            return;
        }

        if (peerSendClosed)
        {
            BeginClose();

            return;
        }

        static_cast<void>(PostReceive());
    }

    void ClientSession::CloseIfTimedOut(const NetworkOptions& options) noexcept
    {
        BeginCloseImpl({}, &options);
    }

    void ClientSession::BeginCloseImpl(DisconnectInfo info, const NetworkOptions* timeoutOptions) noexcept
    {
        Detail::SessionReference connectionReference;

        Detail::SocketHandle socket;

        Packet pendingReceivePacket;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 상태와 기한을 보호하는 락을 획득한 뒤 현재 시각을 구한다.
            if (timeoutOptions != nullptr)
            {
                const auto now = std::chrono::steady_clock::now();

                const auto timeoutReason = GetTimeoutReasonLocked(*timeoutOptions, now);

                if (!timeoutReason.has_value())
                {
                    return;
                }

                // 자체 기한 초과이므로 운영체제 오류 코드는 없다.
                info = DisconnectInfo{ *timeoutReason, 0, *timeoutReason == DisconnectReason::ShutdownTimeout };
            }

            switch (m_state)
            {
            case State::Free:
            case State::Closing:
            case State::Finalizing:
            {
                return;
            }

            default:
            {
                break;
            }
            }

            // 종료 전 연결에는 연결 유지용 참조가 반드시 있어야 한다.
            if (!m_connectionReference)
            {
                std::terminate();
            }

            if (m_state == State::Draining)
            {
                // 정상 종료를 시작할 때 확정한 사유와 오류 코드는 유지한다.
                // 기한 초과 여부만 추가로 반영한다.
                m_disconnectInfo.shutdownTimedOut =
                    m_disconnectInfo.shutdownTimedOut ||
                    info.shutdownTimedOut;
            }
            else
            {
                // 정상 종료를 거치지 않은 즉시 종료는 여기서 사유를 확정한다.
                m_disconnectInfo = info;
            }

            m_state = State::Closing;

            // 종료된 연결에서는 핸드셰이크 응답을 기다리지 않는다.
            m_handshake = {};

            m_heartbeat = {};

            m_receiveDeadline = {};

            m_receivePausedAt.reset();

            // Job에 접수하지 못한 완성 패킷을 종료 경로가 인수한다.
            pendingReceivePacket = std::move(m_pendingReceivePacket);

            // 종료된 연결에서는 보관 패킷 재개를 시도하지 않는다.
            m_receiveResumeRequested = false;

            // 새 I/O를 차단한 상태에서 소켓 닫기 책임을 인수한다.
            socket = std::move(m_socket);

            // 참조 수를 줄이지 않고 반환 책임만 지역 변수로 옮긴다.
            connectionReference = std::move(m_connectionReference);

            // 송신 I/O나 완료 후처리가 없으면 지금 저장소를 정리한다.
            // 진행 중이면 해당 완료·실패 처리 경로가 정리를 맡는다.
            if (!m_sendActive)
            {
                ClearPendingSendLocked();
            }
        }

        // 소켓을 닫아 대기 중인 I/O의 취소를 시작한다.
        // 완료 통지는 나중에 도착할 수 있으므로 I/O 참조는 유지한다.
        if (!socket.Close())
        {
            std::terminate();
        }

        // 풀 반환은 세션 락 밖에서 수행한다.
        pendingReceivePacket.Reset();

        // 마지막 참조 반환 경로가 세션 락을 사용할 수 있으므로 락 밖에서 반환한다.
        connectionReference.Reset();
    }

    std::optional<DisconnectReason> ClientSession::GetTimeoutReasonLocked(
        const NetworkOptions& options,
        std::chrono::steady_clock::time_point now) const noexcept
    {
        switch (m_state)
        {
        case State::Preparing:
        {
            // 아직 TCP 연결 준비 중이면 핸드셰이크 기한을 적용하지 않는다.
            if (m_handshake.phase == HandshakePhase::None)
            {
                return std::nullopt;
            }

            if (now >= m_handshake.deadline)
            {
                return DisconnectReason::HandshakeTimeout;
            }

            return std::nullopt;
        }

        case State::Ready:
        {
            // 자체 수신 중지 중에는 일반 무수신·하트비트 검사를 유예한다.
            // 대신 연속된 수신 중지 시간에 별도 상한을 적용한다.
            if (m_receivePausedAt.has_value())
            {
                if (now - *m_receivePausedAt >= options.backpressureTimeout)
                {
                    return DisconnectReason::BackpressureTimeout;
                }

                return std::nullopt;
            }

            if (m_heartbeat.responseDeadline.has_value() &&
                now >= *m_heartbeat.responseDeadline)
            {
                return DisconnectReason::HeartbeatTimeout;
            }

            if (now >= m_receiveDeadline)
            {
                return DisconnectReason::ReceiveTimeout;
            }

            return std::nullopt;
        }

        case State::Draining:
        {
            // Job 배출·응답 송신·상대 FIN 대기에 하나의 전체 기한을 적용한다.
            // 수신 데이터나 작업 진행 여부에 따라 기한을 연장하지 않는다.
            if (now >= m_shutdownDeadline)
            {
                return DisconnectReason::ShutdownTimeout;
            }

            return std::nullopt;
        }

        default:
        {
            // Free·Closing·Finalizing에는 새 시간 초과 종료를 적용하지 않는다.
            return std::nullopt;
        }
        }
    }

    bool ClientSession::InitializeJobQueue(
        std::size_t maxPendingJobs,
        std::size_t maxPendingPacketBytes,
        Detail::JobScheduler& scheduler) noexcept
    {
        // 공개되거나 이미 초기화된 세션의 실행 경로를 교체하지 않는다.
        if (m_pool != nullptr ||
            m_state != State::Free ||
            m_jobQueue ||
            m_scheduler != nullptr)
        {
            std::terminate();
        }

        auto jobQueue = Detail::SessionJobQueue::Create(maxPendingJobs, maxPendingPacketBytes);

        if (!jobQueue)
        {
            return false;
        }

        m_jobQueue = std::move(jobQueue);

        m_scheduler = &scheduler;

        return true;
    }

    bool ClientSession::InitializeIOStorage(const NetworkOptions& options) noexcept
    {
        // 공개된 세션이나 이미 저장소를 확보한 세션을 다시 초기화하지 않는다.
        if (m_pool != nullptr ||
            m_state != State::Free ||
            m_recvPacketBuffer ||
            m_sendPacketQueue ||
            m_sendPacketBatch)
        {
            std::terminate();
        }

        // 잘못된 설정은 메모리를 확보하기 전에 거절한다.
        if (Detail::ValidateOptions(options) != Detail::OptionsError::None)
        {
            return false;
        }

        auto recvPacketBuffer = Detail::RecvPacketBuffer::Create(
            options.recvRingSize, options.maxRecvPacketSize);

        if (!recvPacketBuffer)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        auto sendPacketQueue = Detail::SendPacketQueue::Create(
            options.maxPendingSendPackets);

        if (!sendPacketQueue)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        auto sendPacketBatch = Detail::SendPacketBatch::Create(
            options.maxSendBatchPackets, options.maxSendBatchBytes);

        if (!sendPacketBatch)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        // 모든 저장소 확보가 성공한 뒤에만 세션 멤버에 소유권을 넘긴다.
        m_recvPacketBuffer = std::move(recvPacketBuffer);
        m_sendPacketQueue = std::move(sendPacketQueue);
        m_sendPacketBatch = std::move(sendPacketBatch);

        m_maxSendPacketSize = options.maxSendPacketSize;

        m_maxPendingSendPackets = options.maxPendingSendPackets;

        m_maxPendingSendBytes = options.maxPendingSendBytes;

        m_receiveTimeout = options.receiveTimeout;

        m_shutdownTimeout = options.shutdownTimeout;

        return true;
    }

    bool ClientSession::AttachSocket(
        Detail::SocketHandle&& socket,
        Detail::IOWorkerPool& ioWorkers,
        Detail::ReceiveProcessor& receiveProcessor) noexcept
    {
        if (!socket)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        // 풀에서 대여한 연결 준비 상태에서 한 번만 소켓을 인수한다.
        if (m_pool == nullptr || m_state != State::Preparing || m_socket)
        {
            return false;
        }

        // 소켓 인수 전에 Job과 송수신 저장소가 준비되어 있어야 한다.
        if (!m_jobQueue || !m_recvPacketBuffer || !m_sendPacketQueue || !m_sendPacketBatch)
        {
            return false;
        }

        // 이 소켓의 완료에서 현재 세션을 찾도록 세션 주소를 등록한다.
        const auto completionKey = reinterpret_cast<ULONG_PTR>(this);

        if (!ioWorkers.AssociateSocket(socket.Get(), completionKey))
        {
            return false;
        }

        // IOCP 등록에 성공한 뒤에만 소켓의 닫기 책임을 인수한다.
        m_socket = std::move(socket);

        m_receiveProcessor = &receiveProcessor;

        return true;
    }

    bool ClientSession::BeginHandshake(
        HandshakePhase phase,
        std::chrono::steady_clock::time_point deadline) noexcept
    {
        if (phase != HandshakePhase::WaitingRequest &&
            phase != HandshakePhase::WaitingResponse)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        if (m_state != State::Preparing ||
            !m_socket ||
            m_handshake.phase != HandshakePhase::None)
        {
            return false;
        }

        if (deadline <= std::chrono::steady_clock::now())
        {
            return false;
        }

        m_handshake = HandshakeProgress{ phase, deadline };

        return true;
    }

    ClientSession::ReceivePostResult ClientSession::PostReceive() noexcept
    {
        // 락보다 먼저 선언하여 함수 반환 시 락이 풀린 뒤 참조가 반환되게 한다.
        Detail::SessionReference failedReference;

        std::unique_lock<std::mutex> lock{ m_mutex };

        // 정상 통신 또는 송신 종료 후 FIN 대기 수신을 허용한다.
        const bool waitingForPeerClose =
            m_state == State::Draining &&
            m_sendShutdownStarted &&
            m_receiveDrained;

        if ((m_state != State::Preparing &&
            m_state != State::Ready &&
            !waitingForPeerClose) || !m_socket)
        {
            return ReceivePostResult::NotReady;
        }

        // 상대 FIN을 확인한 연결에는 새 수신을 발행하지 않는다.
        if (m_peerSendClosed)
        {
            return ReceivePostResult::NotReady;
        }

        // 완료 후처리 중에도 새 수신을 발행하지 않는다.
        if (m_receiveActive)
        {
            return ReceivePostResult::AlreadyActive;
        }

        // 비활성 수신에 참조가 남아 있거나 저장소가 없으면 내부 상태 오류다.
        if (m_receiveReference || !m_recvPacketBuffer)
        {
            std::terminate();
        }

        // 앞선 패킷의 Job 접수가 끝나기 전에는 추가 수신을 시작하지 않는다.
        if (m_pendingReceivePacket)
        {
            return ReceivePostResult::Paused;
        }

        m_recvPacketBuffer->ResetOffsetsIfEmpty();

        const auto receiveSpan = m_recvPacketBuffer->GetReceiveSpan();

        if (receiveSpan.empty())
        {
            return ReceivePostResult::Paused;
        }

        // WSABUF 길이 필드에 표현 가능한 범위만 이번 수신에 사용한다.
        const auto receiveSize = (std::min)(receiveSpan.size(),
            static_cast<std::size_t>((std::numeric_limits<ULONG>::max)()));

        WSABUF buffer{};

        buffer.buf = reinterpret_cast<char*>(receiveSpan.data());

        buffer.len = static_cast<ULONG>(receiveSize);

        DWORD flags = 0;

        // 이전 수신과 완료 후처리가 끝난 상태에서만 작업 정보를 초기화한다.
        m_receiveOperation.overlapped = {};

        // 완료가 즉시 도착해도 세션을 사용할 수 있도록 발행 전에 확보한다.
        m_receiveReference = AcquireReference();

        m_receiveActive = true;
        m_receiveResumeRequested = false;

        const int result = ::WSARecv(
            m_socket.Get(),
            &buffer,
            1,
            nullptr,
            &flags,
            &m_receiveOperation.overlapped,
            nullptr);

        if (result == 0)
        {
            // 즉시 성공해도 실제 데이터 반영과 참조 반환은 IOCP 완료에서 수행한다.
            return ReceivePostResult::Posted;
        }

        const int errorCode = ::WSAGetLastError();

        if (errorCode == WSA_IO_PENDING)
        {
            return ReceivePostResult::Posted;
        }

        // 즉시 실패한 I/O에는 완료 통지가 오지 않는다.
        // 종료 전환까지 Active를 유지하여 다른 경로의 재개를 막는다.
        failedReference = std::move(m_receiveReference);

        lock.unlock();

        BeginClose(DisconnectInfo{ DisconnectReason::TransportError, static_cast<std::uint32_t>(errorCode) });

        lock.lock();

        // 접수되지 않은 작업에는 완료 통지가 오지 않으므로 여기서 정리한다.
        m_receiveActive = false;

        m_receiveResumeRequested = false;

        return ReceivePostResult::Failed;
    }

    Detail::SessionReference ClientSession::BeginReceiveCompletion(const Detail::IOCompletion& completion) noexcept
    {
        // 완료 처리 중에는 지역 참조가 세션의 수명을 보호한다.
        Detail::SessionReference reference;

        bool closeRequired = false;
        bool drainRequired = false;
        std::chrono::steady_clock::time_point drainDeadline{};

        DisconnectInfo closeInfo{ DisconnectReason::TransportError, 0 };

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 이 세션에서 발행한 수신 작업의 완료인지 확인한다.
            if (completion.completionKey != reinterpret_cast<ULONG_PTR>(this) ||
                &completion.operation != &m_receiveOperation ||
                completion.operation.type != Detail::IOOperationType::Receive)
            {
                std::terminate();
            }

            // 접수된 수신 하나에는 대응하는 I/O 참조가 하나 있어야 한다.
            if (!m_receiveActive ||
                !m_receiveReference ||
                !m_recvPacketBuffer)
            {
                std::terminate();
            }

            // 참조 수는 바꾸지 않고 완료 후처리로 반환 책임을 넘긴다.
            reference = std::move(m_receiveReference);

            const bool cancellationRequested = m_receiveCancellationRequested;

            m_receiveCancellationRequested = false;

            if (m_state == State::Closing)
            {
                // 종료 중 도착한 완료는 데이터를 처리하지 않고 참조를 정리한다.
                m_receiveActive = false;

                m_receiveResumeRequested = false;
            }
            else
            {
                // 수신 참조가 보호하는 연결은 이 상태들 중 하나여야 한다.
                if (m_state != State::Preparing &&
                    m_state != State::Ready &&
                    m_state != State::Draining)
                {
                    std::terminate();
                }

                if (m_state == State::Draining &&
                    cancellationRequested &&
                    completion.errorCode == ERROR_OPERATION_ABORTED)
                {
                    // 정상 종료를 위해 요청한 수신 취소가 완료됐다.
                    // 새 바이트를 반영하지 않고 기존 버퍼의 후처리를 이어간다.
                    return reference;
                }

                if (completion.errorCode != ERROR_SUCCESS)
                {
                    // 오류 완료는 FIN이 아니다. 정상 배출을 중단하고 정리한다.
                    closeInfo = DisconnectInfo{ DisconnectReason::TransportError, static_cast<std::uint32_t>(completion.errorCode) };

                    m_receiveResumeRequested = false;

                    closeRequired = true;
                }
                else if (completion.transferredBytes == 0)
                {
                    // 상대가 송신 방향을 정상 종료했다.
                    m_peerSendClosed = true;

                    m_receiveResumeRequested = false;

                    if (m_state == State::Draining)
                    {
                        // 상대 FIN만으로 기존 Job과 응답 송신을 취소하지 않는다.
                        // 버퍼에 이미 반영된 데이터의 후처리를 이어간다.
                        return reference;
                    }

                    if (m_state == State::Ready)
                    {
                        // FIN 이전에 접수한 Packet·Task를 취소하지 않고 배출한다.
                        drainRequired = true;
                        drainDeadline = std::chrono::steady_clock::now() + m_shutdownTimeout;
                    }

                    // 준비 단계의 FIN은 서비스 연결 실패로 즉시 정리한다.
                    closeInfo = DisconnectInfo{ DisconnectReason::PeerClosed, 0 };

                    closeRequired = !drainRequired;
                }
                else if (m_state != State::Draining &&
                    !m_recvPacketBuffer->CommitReceivedBytes(completion.transferredBytes))
                {
                    // 정상 수신의 완료량이 버퍼의 수신 가능 범위와 맞지 않는다.
                    m_receiveResumeRequested = false;

                    closeRequired = true;
                }
                else
                {
                    // Draining이면 이번 완료 바이트를 반영하지 않고 기존 데이터만 처리한다.
                    // 후처리가 끝날 때까지 Active와 반환할 세션 참조를 유지한다.

                    m_receiveResumeRequested = false;

                    return reference;
                }
            }
        }

        if (drainRequired)
        {
            // 현재 수신 후처리가 참조와 독점 실행 권한을 유지한 채 배출을 이어간다.
            BeginDrain(closeInfo, drainDeadline);
            return reference;
        }

        // BeginClose와 마지막 참조 반환은 세션 락 밖에서 수행한다.
        if (closeRequired)
        {
            BeginClose(closeInfo);

            {
                std::lock_guard<std::mutex> lock{ m_mutex };

                // Closing 전환 후에만 수신 처리의 독점 상태를 해제한다.
                m_receiveActive = false;

                m_receiveResumeRequested = false;
            }
        }

        // 마지막 참조일 수 있으므로 세션 락 밖에서 반환한다.
        reference.Reset();

        return {};
    }

    ClientSession::ReceivePacketResult ClientSession::ReadReceivedPacket(Detail::PacketPool& packetPool) noexcept
    {
        DisconnectInfo closeInfo{ DisconnectReason::TransportError, 0 };

        try
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // I/O 참조를 지역으로 인수한 수신 후처리에서만 호출한다.
            if (!m_receiveActive || m_receiveReference || !m_recvPacketBuffer)
            {
                std::terminate();
            }

            if (m_state == State::Closing)
            {
                return { ReceivePacketStatus::Stopped, {} };
            }

            if (m_state != State::Preparing &&
                m_state != State::Ready &&
                m_state != State::Draining)
            {
                std::terminate();
            }

            if (m_state == State::Draining && m_receiveDrained)
            {
                // FIN 대기 중 받은 데이터는 서비스 패킷으로 추출하지 않는다.
                return { ReceivePacketStatus::Drained, {} };
            }

            // 앞선 패킷을 건너뛰고 다음 패킷을 추출하지 않는다.
            if (m_pendingReceivePacket)
            {
                return { ReceivePacketStatus::Paused, {} };
            }

            auto result = m_recvPacketBuffer->ExtractPacket(packetPool);

            switch (result.status)
            {
            case Detail::PacketExtractStatus::Complete:
            {
                // 완성된 프레임을 추출했으므로 다음 패킷의 수신 기한을 갱신한다.
                if (m_state == State::Ready)
                {
                    m_receiveDeadline = std::chrono::steady_clock::now() + m_receiveTimeout;
                }

                return { ReceivePacketStatus::Complete, std::move(result.packet) };
            }

            case Detail::PacketExtractStatus::NeedMoreData:
            {
                if (m_state == State::Draining)
                {
                    // 종료 기준 이후 바이트로 남은 조각을 완성하지 않는다.
                    // 현재는 수신 I/O가 끝난 독점 후처리 구간이므로 위치를 초기화할 수 있다.
                    m_recvPacketBuffer->Clear();

                    m_receiveDrained = true;

                    return { ReceivePacketStatus::Drained, {} };
                }

                return { ReceivePacketStatus::NeedMoreData, {} };
            }

            case Detail::PacketExtractStatus::InvalidPacketId:
            case Detail::PacketExtractStatus::InvalidPacketSize:
            {
                closeInfo.reason = DisconnectReason::ProtocolError;

                break;
            }

            case Detail::PacketExtractStatus::NoMemory:
            {
                closeInfo.reason = DisconnectReason::ResourceExhausted;

                break;
            }

            case Detail::PacketExtractStatus::InternalError:
            {
                // 내부 생성 계약의 불일치는 상대의 프로토콜 오류로 기록하지 않는다.
                break;
            }

            default:
            {
                std::terminate();
            }
            }
        }
        catch (const std::bad_alloc&)
        {
            closeInfo.reason = DisconnectReason::ResourceExhausted;
        }
        catch (...)
        {
            // 추출 중 예외도 연결 실패로 처리한다.
            // 세션 락은 이 지점에 도달하기 전에 해제된다.
        }

        // BeginClose는 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
        BeginClose(closeInfo);

        return { ReceivePacketStatus::Failed, {} };
    }

    ClientSession::ReceivedPacketEnqueueResult ClientSession::EnqueueReceivedPacket(Packet&& packet) noexcept
    {
        // 시스템 패킷은 별도 처리 경로로 보내야 한다.
        if (!packet || !IsServicePacketId(packet.View().packetId))
        {
            std::terminate();
        }

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 수신 후처리 하나만 패킷 추출과 접수를 진행해야 한다.
            if (!m_receiveActive ||
                m_receiveReference ||
                m_pendingReceivePacket)
            {
                std::terminate();
            }
        }

        // 완료 후처리용 참조와 별도로 Job이 사용할 참조를 확보한다.
        Detail::Job job{ AcquireReference(), std::move(packet) };

        const auto result = EnqueueJob(std::move(job), nullptr, JobEnqueueSource::ReceivedPacket);

        switch (result)
        {
        case EnqueueJobResult::Queued:
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 접수 이후 종료가 시작됐다면 종료 경로의 초기화 상태를 유지한다.
            if (m_state == State::Ready && m_receivePausedAt.has_value())
            {
                const auto now = std::chrono::steady_clock::now();

                const auto pausedDuration = now - *m_receivePausedAt;

                // 자체 수신 중지 때문에 응답을 처리하지 못한 시간은 제외한다.
                if (m_heartbeat.responseDeadline.has_value())
                {
                    *m_heartbeat.responseDeadline += pausedDuration;
                }

                // 수신 재개 후 다음 패킷을 기다릴 기한과 하트비트 간격을 새로 부여한다.
                m_receiveDeadline = now + m_receiveTimeout;
                m_heartbeat.lastRequestAt = now;

                m_receivePausedAt.reset();
            }
            else if (m_state == State::Draining)
            {
                // 일반 통신의 중지 표시는 해제하되 전체 종료 기한은 유지한다.
                m_receivePausedAt.reset();
            }

            return ReceivedPacketEnqueueResult::Queued;
        }

        case EnqueueJobResult::QueueFull:
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // Draining에서는 보관을 유지하고, 즉시 종료로 전환됐다면 폐기한다.
            if (m_state != State::Ready && m_state != State::Draining)
            {
                return ReceivedPacketEnqueueResult::Stopped;
            }

            if (!m_receiveActive ||
                m_receiveReference ||
                m_pendingReceivePacket)
            {
                std::terminate();
            }

            // 접수에 실패한 Job은 원본 패킷을 그대로 소유하고 있다.
            m_pendingReceivePacket = std::move(std::get<Packet>(job.GetData()));

            // 재접수 실패가 반복돼도 최초 중지 시각을 유지한다.
            if (m_state == State::Ready && !m_receivePausedAt.has_value())
            {
                m_receivePausedAt = std::chrono::steady_clock::now();
            }

            return ReceivedPacketEnqueueResult::Paused;
        }

        case EnqueueJobResult::NotReady:
        {
            // 서비스 통신 준비 전에 받은 서비스 패킷은 허용하지 않는다.
            BeginClose(DisconnectInfo{ DisconnectReason::ProtocolError, 0 });

            return ReceivedPacketEnqueueResult::Failed;
        }

        case EnqueueJobResult::Closed:
        {
            return ReceivedPacketEnqueueResult::Stopped;
        }
        }

        std::terminate();
    }

    Detail::SessionReference ClientSession::TryBeginReceiveResume() noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        if (m_state != State::Ready && m_state != State::Draining)
        {
            return {};
        }

        if (m_state == State::Draining && m_receiveDrained)
        {
            return {};
        }

        if (m_receiveActive)
        {
            // 아직 보관 슬롯이 채워지기 전일 수도 있으므로 요청부터 기억한다.
            m_receiveResumeRequested = true;

            return {};
        }

        if (m_state == State::Ready && !m_pendingReceivePacket)
        {
            return {};
        }

        // 비활성 상태에서는 발행된 수신 I/O의 참조가 남아 있으면 안 된다.
        if (m_receiveReference || !m_recvPacketBuffer)
        {
            std::terminate();
        }

        // 재개 후처리가 끝날 때까지 세션 수명과 수신 처리의 독점 상태를 유지한다.
        auto reference = AcquireReference();

        m_receiveActive = true;

        // 이번 재개 시도가 이전 요청을 인수한다.
        m_receiveResumeRequested = false;

        return reference;
    }

    bool ClientSession::RetryPendingReceivePacket() noexcept
    {
        // 함수가 끝날 때 패킷이 남아 있어도 세션 락 밖에서 반환한다.
        Packet packet;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 재개 후처리의 독점 상태를 확보한 경로에서만 호출한다.
            if (!m_receiveActive || m_receiveReference)
            {
                std::terminate();
            }

            // 독점 상태를 확보한 이후에도 다른 경로에서 종료를 요청할 수 있다.
            if (m_state != State::Ready && m_state != State::Draining)
            {
                return false;
            }

            if (!m_pendingReceivePacket)
            {
                return true;
            }

            // 기존 접수 함수는 보관 슬롯이 빈 상태에서 패킷을 받는다.
            packet = std::move(m_pendingReceivePacket);
        }

        // 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
        return EnqueueReceivedPacket(std::move(packet)) == ReceivedPacketEnqueueResult::Queued;
    }

    bool ClientSession::FinishReceiveCompletion(bool resumeReceive) noexcept
    {
        bool retryProcessing = false;

        bool closeAfterReceive = false;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 완료 후처리 중에는 I/O 참조가 멤버가 아닌 호출자에게 있어야 한다.
            if (!m_receiveActive || m_receiveReference)
            {
                std::terminate();
            }

            if (m_state == State::Ready)
            {
                // 후처리 중 들어온 재개 요청을 상태 해제와 같은 락에서 소비한다.
                retryProcessing =
                    static_cast<bool>(m_pendingReceivePacket) &&
                    m_receiveResumeRequested;
            }
            else if (m_state == State::Draining)
            {
                if (m_receiveDrained && m_sendShutdownStarted)
                {
                    // 송신 종료 이후에는 새 데이터를 소비하며 FIN만 기다린다.
                    closeAfterReceive = m_peerSendClosed;
                    resumeReceive = !m_peerSendClosed;
                }
                else
                {
                    // 고정된 수신 범위가 남았다면 기존 후처리를 재개한다.
                    retryProcessing =
                        !m_receiveDrained &&
                        (m_receiveResumeRequested || resumeReceive);
                }
            }

            m_receiveResumeRequested = false;

            m_receiveActive = false;
        }

        if (closeAfterReceive)
        {
            // 수신 후처리 참조가 살아 있고 세션 락은 해제된 상태다.
            BeginClose();

            return false;
        }

        if (retryProcessing)
        {
            // 보관 패킷을 먼저 처리해야 하므로 새 수신부터 발행하지 않는다.
            return true;
        }

        if (resumeReceive)
        {
            // 상태·미접수 패킷·중복 발행 여부를 다시 확인한다.
            // 즉시 발행 실패의 종료 처리는 PostReceive가 담당한다.
            static_cast<void>(PostReceive());
        }

        return false;
    }

    SendResult ClientSession::EnqueueSendPacket(Packet&& packet) noexcept
    {
        if (!packet)
        {
            return SendResult::InvalidPacket;
        }

        const PacketId packetId = packet.View().packetId;

        const bool isSystemPacket = IsSystemPacketId(packetId);

        if (!isSystemPacket && !IsServicePacketId(packetId))
        {
            return SendResult::InvalidPacket;
        }

        const std::size_t packetSize = Detail::GetPacketWireBytes(packet).size();

        std::unique_lock<std::mutex> lock{ m_mutex };

        switch (m_state)
        {
        case State::Preparing:
        {
            // 서비스 통신 준비 전에는 엔진 시스템 패킷만 허용한다.
            if (!isSystemPacket)
            {
                return SendResult::NotReady;
            }

            break;
        }

        case State::Ready:
        {
            break;
        }

        case State::Draining:
        {
            // 정상 종료 중에는 현재 세션의 Job이 보내는 서비스 응답만 허용한다.
            // 외부 스레드·다른 세션의 Job·시스템 패킷 송신은 접수하지 않는다.
            if (isSystemPacket || !Detail::JobExecutionScope::IsExecuting(*this))
            {
                return SendResult::Closed;
            }

            break;
        }

        default:
        {
            return SendResult::Closed;
        }
        }

        if (!m_socket)
        {
            return SendResult::NotReady;
        }

        // 통신 가능한 세션은 송신 저장소 초기화가 완료되어 있어야 한다.
        if (!m_sendPacketQueue || !m_sendPacketBatch)
        {
            std::terminate();
        }

        if (packetSize < PacketHeaderSize || packetSize > m_maxSendPacketSize)
        {
            return SendResult::InvalidPacket;
        }

        // 사용량이 한도를 넘었다면 내부 예산 관리 오류다.
        if (m_pendingSendPackets > m_maxPendingSendPackets ||
            m_pendingSendBytes > m_maxPendingSendBytes)
        {
            std::terminate();
        }

        if (m_pendingSendPackets == m_maxPendingSendPackets ||
            packetSize > m_maxPendingSendBytes - m_pendingSendBytes)
        {
            return SendResult::QueueFull;
        }

        // Enqueue는 접수에 성공한 경우에만 Packet을 실제로 이동한다.
        const SendResult result = m_sendPacketQueue->Enqueue(std::move(packet));

        if (result != SendResult::Queued)
        {
            return result;
        }

        // 소유권 이전에 성공한 패킷만 전체 송신 사용량에 반영한다.
        ++m_pendingSendPackets;

        m_pendingSendBytes += packetSize;

        // PostSend가 같은 세션 락을 사용하므로 먼저 해제한다.
        lock.unlock();

        // 즉시 발행 실패의 종료·정리는 PostSend가 담당한다.
        // 이미 송신 중이면 기존 완료 경로가 다음 발행을 이어간다.
        static_cast<void>(PostSend());

        // 이미 Packet을 인수했으므로 이후 발행 결과와 무관하게 Queued다.
        return SendResult::Queued;
    }

    ClientSession::SendPostResult ClientSession::PostSend() noexcept
    {
        // 락보다 먼저 선언하여 락이 풀린 뒤 실패한 I/O 참조를 반환한다.
        Detail::SessionReference failedReference;

        std::unique_lock<std::mutex> lock{ m_mutex };

        // 정상 종료 중에도 이미 접수한 송신은 계속 처리할 수 있다.
        if ((m_state != State::Preparing &&
            m_state != State::Ready &&
            m_state != State::Draining) ||
            !m_socket)
        {
            return SendPostResult::NotReady;
        }

        // 완료 후처리까지 끝나기 전에는 다른 송신을 발행하지 않는다.
        if (m_sendActive)
        {
            return SendPostResult::AlreadyActive;
        }

        if (m_sendReference || !m_sendPacketQueue || !m_sendPacketBatch)
        {
            std::terminate();
        }

        auto buffers = m_sendPacketBatch->GetBuffers();

        if (buffers.empty())
        {
            const auto buildResult = m_sendPacketBatch->BuildFrom(*m_sendPacketQueue);

            switch (buildResult)
            {
            case Detail::SendBatchBuildResult::Ready:
            {
                break;
            }

            case Detail::SendBatchBuildResult::Empty:
            {
                return SendPostResult::Empty;
            }

            default:
            {
                // 빈 배치에 구성해야 하며, 허용된 패킷은 배치에 들어가야 한다.
                std::terminate();
            }
            }

            buffers = m_sendPacketBatch->GetBuffers();
        }

        if (buffers.empty())
        {
            std::terminate();
        }

        // 이전 송신과 완료 후처리가 끝난 상태에서만 초기화한다.
        m_sendOperation.overlapped = {};

        // 완료가 즉시 도착해도 세션과 배치가 유지되도록 먼저 확보한다.
        m_sendReference = AcquireReference();

        m_sendActive = true;

        const int result =
#if defined(IOCPNETWORK_TESTING)
            Detail::SessionSendTestHooks::InterceptSend() ? 0 :
#endif
            ::WSASend(
            m_socket.Get(),
            buffers.data(),
            static_cast<DWORD>(buffers.size()),
            nullptr,
            0,
            &m_sendOperation.overlapped,
            nullptr);

        if (result == 0)
        {
            // 즉시 성공해도 송신량 반영과 참조 반환은 IOCP 완료에서 수행한다.
            return SendPostResult::Posted;
        }

        const int errorCode = ::WSAGetLastError();

        if (errorCode == WSA_IO_PENDING)
        {
            return SendPostResult::Posted;
        }

        // 즉시 실패한 I/O에는 완료 통지가 오지 않는다.
        // 반환 책임을 인수하되 종료 전환까지 Active는 유지한다.
        failedReference = std::move(m_sendReference);

        lock.unlock();

        BeginClose(DisconnectInfo{ DisconnectReason::TransportError, static_cast<std::uint32_t>(errorCode) });

        lock.lock();

        // Closing 전환이 끝났고, 운영체제가 사용 중인 송신도 없다.
        m_sendActive = false;

        ClearPendingSendLocked();

        return SendPostResult::Failed;
    }

    void ClientSession::HandleSendCompletion(const Detail::IOCompletion& completion) noexcept
    {
        // 완료된 I/O의 참조를 인수하여 함수가 끝날 때까지 세션을 보호한다.
        Detail::SessionReference reference;

        bool closeRequired = false;

        DisconnectInfo closeInfo{ DisconnectReason::TransportError, 0 };

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (completion.completionKey != reinterpret_cast<ULONG_PTR>(this) ||
                &completion.operation != &m_sendOperation ||
                completion.operation.type != Detail::IOOperationType::Send)
            {
                std::terminate();
            }

            if (!m_sendActive ||
                !m_sendReference ||
                !m_sendPacketBatch ||
                !m_sendPacketQueue)
            {
                std::terminate();
            }

            // 참조 수를 변경하지 않고 반환 책임만 지역 변수로 옮긴다.
            reference = std::move(m_sendReference);

            if (m_state == State::Closing)
            {
                // 종료 중 도착한 완료는 새 송신으로 이어가지 않는다.
                m_sendActive = false;

                ClearPendingSendLocked();

                return;
            }

            if (m_state != State::Preparing &&
                m_state != State::Ready &&
                m_state != State::Draining)
            {
                std::terminate();
            }

            if (completion.errorCode != ERROR_SUCCESS ||
                completion.transferredBytes == 0)
            {
                closeInfo.nativeError = static_cast<std::uint32_t>(completion.errorCode);

                closeRequired = true;
            }
            else
            {
                const std::size_t bytesSent = completion.transferredBytes;

                // 전체 예산보다 큰 완료량은 배치에도 반영하지 않는다.
                const auto result = bytesSent <= m_pendingSendBytes
                    ? m_sendPacketBatch->CommitSentBytes(bytesSent)
                    : Detail::SendBatchCommitResult{};

                if (!result.succeeded)
                {
                    closeRequired = true;
                }
                else
                {
                    if (result.completedPackets > m_pendingSendPackets)
                    {
                        std::terminate();
                    }

                    // 바이트는 부분 완료도 반환하고, 개수는 완성된 패킷만 반환한다.
                    m_pendingSendBytes -= bytesSent;

                    m_pendingSendPackets -= result.completedPackets;
                }
            }

            if (!closeRequired)
            {
                // 배치와 예산 반영이 끝난 뒤 다음 송신을 허용한다.
                m_sendActive = false;
            }

            // 실패한 경우에는 종료 상태로 바뀔 때까지 Active를 유지한다.
        }

        if (closeRequired)
        {
            // 세션 락 밖에서 종료한다. 지역 참조가 세션의 수명을 보호한다.
            BeginClose(closeInfo);

            {
                std::lock_guard<std::mutex> lock{ m_mutex };

                // 실패 완료도 도착했으므로 배치와 대기 큐를 정리할 수 있다.
                m_sendActive = false;

                ClearPendingSendLocked();
            }

            return;
        }

        // 부분 송신이면 기존 배치부터, 모두 완료됐으면 다음 배치를 발행한다.
        // 즉시 발행 실패의 종료·정리는 PostSend가 담당한다.
        static_cast<void>(PostSend());

        // 완료 후처리 참조가 살아 있는 동안 배출 완료를 확인한다.
        TryShutdownSend();
    }

    void ClientSession::ClearPendingSendLocked() noexcept
    {
        // 운영체제가 사용 중이거나 완료 후처리 중인 배치는 비우지 않는다.
        if (m_state != State::Closing ||
            m_sendActive ||
            m_sendReference ||
            !m_sendPacketQueue ||
            !m_sendPacketBatch)
        {
            std::terminate();
        }

        m_sendPacketQueue->Clear();

        m_sendPacketBatch->Clear();

        m_pendingSendPackets = 0;

        m_pendingSendBytes = 0;
    }

    ClientSession::EnqueueJobResult ClientSession::EnqueueJob(
        Detail::Job&& job,
        SessionTask* sourceTask,
        JobEnqueueSource source) noexcept
    {
        // 다른 세션의 Job이나 빈 참조를 이 세션에 등록하지 않는다.
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        if (source == JobEnqueueSource::ReceivedPacket)
        {
            const auto* packet = std::get_if<Packet>(&job.GetData());

            // 수신 접수 경로에 Task나 연결 통지를 전달하면 내부 호출 오류다.
            if (sourceTask != nullptr ||
                packet == nullptr ||
                !*packet ||
                !IsServicePacketId(packet->View().packetId))
            {
                std::terminate();
            }
        }

        Detail::JobEnqueueResult result;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (!m_jobQueue || m_scheduler == nullptr)
            {
                std::terminate();
            }

            switch (m_state)
            {
            case State::Preparing:
            {
                // 서비스 준비를 확정하는 연결 통지를 먼저 등록한다.
                if (!isConnectNotify)
                {
                    return EnqueueJobResult::NotReady;
                }

                // 핸드셰이크가 시작되지 않았다면 준비 완료를 확정할 수 없다.
                if (m_handshake.phase == HandshakePhase::None)
                {
                    return EnqueueJobResult::NotReady;
                }

                // 응답 송신 준비 중 기한이 지났을 수 있으므로 다시 검사한다.
                if (std::chrono::steady_clock::now() >= m_handshake.deadline)
                {
                    return EnqueueJobResult::NotReady;
                }

                break;
            }

            case State::Ready:
            {
                // 이미 준비 완료된 연결에는 연결 통지를 다시 접수하지 않는다.
                if (isConnectNotify)
                {
                    return EnqueueJobResult::NotReady;
                }

                break;
            }

            case State::Draining:
            {
                // 정상 종료 기준에 포함되지 않은 새 작업은 접수하지 않는다.
                if (source != JobEnqueueSource::ReceivedPacket)
                {
                    return EnqueueJobResult::Closed;
                }

                // 고정 수신 범위를 처리하는 독점 후처리에서만 접수할 수 있다.
                if (!m_receiveActive ||
                    m_receiveReference ||
                    m_pendingReceivePacket ||
                    m_receiveDrained)
                {
                    std::terminate();
                }

                break;
            }

            default:
            {
                return EnqueueJobResult::Closed;
            }
            }

            if (sourceTask != nullptr)
            {
                // 보관 중인 수신 Packet의 재접수를 새 Task보다 우선한다.
                if (m_pendingReceivePacket)
                {
                    return EnqueueJobResult::QueueFull;
                }

                result = m_jobQueue->Enqueue(std::move(job), *sourceTask);
            }
            else
            {
                result = m_jobQueue->Enqueue(std::move(job));
            }

            if (result.status == Detail::JobEnqueueStatus::QueueFull)
            {
                // Preparing에서는 일반 Job을 받지 않아 통지용 공간이 있어야 한다.
                if (isConnectNotify)
                {
                    std::terminate();
                }

                return EnqueueJobResult::QueueFull;
            }

            if (isConnectNotify)
            {
                // 연결 통지 접수 이력은 최종 종료 통지까지 유지한다.
                m_connectedNotificationQueued = true;

                // 통지가 FIFO에 들어간 뒤에만 일반 작업 접수를 허용한다.
                m_state = State::Ready;

                // 서비스 준비가 끝났으므로 핸드셰이크 대기를 해제한다.
                m_handshake = {};

                const auto now = std::chrono::steady_clock::now();

                m_heartbeat = {};

                m_heartbeat.lastRequestAt = now;

                m_receiveDeadline = now + m_receiveTimeout;

                m_receivePausedAt.reset();
            }
        }

        if (result.readySession)
        {
            Detail::ReadyItem item{ std::in_place_type<Detail::SessionReference>, std::move(result.readySession) };

            // 세션 락을 놓은 뒤 실행 책임을 스케줄러에 전달한다.
            m_scheduler->Enqueue(std::move(item));
        }

        return EnqueueJobResult::Queued;
    }

    bool ClientSession::TryBeginJob(const Detail::Job& job) noexcept
    {
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        std::lock_guard<std::mutex> lock{ m_mutex };

        switch (m_state)
        {
        case State::Ready:
        case State::Draining:
        {
            // 이미 접수된 작업은 정상 종료의 처리 범위에 포함된다.
            return true;
        }

        case State::Closing:
        {
            // 확정된 연결 통지는 유지하고 일반 Packet·Task는 취소한다.
            return isConnectNotify;
        }

        default:
        {
            // 접수된 Job이 보호하는 세션은 이 상태에 있을 수 없다.
            std::terminate();
        }
        }
    }

    Detail::SessionReference ClientSession::AcquireReference() noexcept
    {
        std::size_t count = m_referenceCount.load(std::memory_order_relaxed);

        for (;;)
        {
            // 기존 연결의 참조를 0에서 되살리거나 카운터가 넘치게 할 수는 없다.
            if (count == 0 || count == (std::numeric_limits<std::size_t>::max)())
            {
                std::terminate();
            }

            // 값이 같아도 실패할 수 있으므로 성공할 때까지 다시 시도한다.
            if (m_referenceCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
            {
                return Detail::SessionReference{ this };
            }
        }
    }

    void ClientSession::ReleaseReference() noexcept
    {
        const std::size_t previous = m_referenceCount.fetch_sub(1, std::memory_order_acq_rel);

        // 소유하지 않은 참조를 반환한 내부 오류이다.
        if (previous == 0)
        {
            std::terminate();
        }

        if (previous != 1)
        {
            return;
        }

        Detail::SessionFinalization finalization;

        Detail::ClientSessionPool* pool = nullptr;

        // 마지막 사용 참조를 반환한 경로만 최종 마무리 상태로 전환한다.
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_state != State::Closing ||
                m_pool == nullptr ||
                m_connectionReference)
            {
                // 종료 상태·소속 풀·연결 유지용 참조가 마지막 반환 조건과 일치해야 한다.
                std::terminate();
            }

            m_state = State::Finalizing;

            pool = m_pool;

            // 참조를 다시 늘리지 않고 최종 마무리 책임을 만든다.
            finalization = Detail::SessionFinalization{
                *pool,
                *this,
                m_connectionId
            };
        }

        // 최종 항목의 전달은 세션 락을 해제한 뒤 수행한다.
        pool->DispatchFinalization(std::move(finalization));

        // 인계 후 같은 슬롯이 재대여될 수 있으므로 멤버에 다시 접근하지 않는다.
    }

    void ClientSession::ResetForReuseLocked() noexcept
    {
        // 살아 있는 참조나 I/O, 미정리된 취소 상태를 초기화로 숨기지 않는다.
        if (m_state != State::Finalizing ||
            m_referenceCount.load(std::memory_order_relaxed) != 0 ||
            m_connectionReference ||
            m_socket ||
            m_receiveReference ||
            m_sendReference ||
            m_receiveActive ||
            m_sendActive ||
            m_receiveCancellationRequested ||
            m_receiveResumeRequested ||
            m_pendingReceivePacket)
        {
            std::terminate();
        }

        if (!m_jobQueue ||
            !m_recvPacketBuffer ||
            !m_sendPacketQueue ||
            !m_sendPacketBatch)
        {
            std::terminate();
        }

        // 최종 반환 시 실행 예약과 미반환 예산이 남아 있으면 내부 오류다.
        if (!m_jobQueue->IsIdle())
        {
            std::terminate();
        }

        // 송신 자원은 앞선 종료 경로에서 이미 정리되어 있어야 한다.
        if (m_pendingSendPackets != 0 ||
            m_pendingSendBytes != 0 ||
            m_sendPacketQueue->GetFrontPacketSize() != 0 ||
            !m_sendPacketBatch->GetBuffers().empty())
        {
            std::terminate();
        }

        // 이전 연결의 불완전한 헤더·본문을 버린다. 버퍼 할당은 유지한다.
        m_recvPacketBuffer->Clear();

        // 이전 I/O의 완료 정보를 제거한다. 작업 종류와 객체 주소는 유지한다.
        m_receiveOperation.overlapped = {};

        m_sendOperation.overlapped = {};

        // 종료 통지가 끝났으므로 이전 연결 정보를 초기화한다.
        m_connectionId = {};

        m_disconnectInfo = {};

        m_shutdownDeadline = {};

        m_sendShutdownStarted = false;

        m_handshake = {};

        m_heartbeat = {};

        m_receiveDeadline = {};

        m_receivePausedAt.reset();

        m_peerSendClosed = false;

        m_receiveDrained = false;

        m_receiveCancellationRequested = false;

        m_connectedNotificationQueued = false;

        m_receiveProcessor = nullptr;
    }
}

namespace IOCPNetwork::Detail
{
    void DispatchSessionCompletion(void*, const IOCompletion& completion) noexcept
    {
        // Accept·Connect 완료는 이 진입점에서 처리하지 않는다.
        if (completion.operation.type != IOOperationType::Receive &&
            completion.operation.type != IOOperationType::Send)
        {
            std::terminate();
        }

        if (completion.completionKey == 0)
        {
            std::terminate();
        }

        auto* session = reinterpret_cast<ClientSession*>(completion.completionKey);

        switch (completion.operation.type)
        {
        case IOOperationType::Receive:
        {
            ReceiveProcessor* receiveProcessor = nullptr;

            {
                std::lock_guard<std::mutex> lock{ session->m_mutex };

                receiveProcessor = session->m_receiveProcessor;
            }

            if (receiveProcessor == nullptr)
            {
                std::terminate();
            }

            // 처리기 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
            receiveProcessor->HandleReceive(*session, completion);

            return;
        }

        case IOOperationType::Send:
        {
            session->HandleSendCompletion(completion);

            return;
        }

        default:
        {
            std::terminate();
        }
        }
    }
}
