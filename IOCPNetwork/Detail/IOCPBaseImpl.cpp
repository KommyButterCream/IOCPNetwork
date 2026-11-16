#include "IOCPBaseImpl.h"

#include "JobScheduler.h"
#include "OptionsValidation.h"
#include "PacketPool.h"
#include "ClientSessionPool.h"
#include "../ClientSession.h"
#include "IOWorkerPool.h"
#include "EngineExecutionScope.h"
#include "Job.h"

#include <array> // for std::array
#include <exception> // for std::terminate
#include <memory>    // for std::make_unique
#include <new>       // for std::bad_alloc
#include <utility>   // for std::move
#include <cstdint> // for std::uint32_t
#include <limits>  // for std::numeric_limits
#include <mutex>   // for std::mutex, std::lock_guard, std::unique_lock
#include <chrono> // for std::chrono::milliseconds, std::chrono::seconds, std::chrono::steady_clock
#include <system_error> // for std::system_error

namespace IOCPNetwork::Detail
{
#if defined(IOCPNETWORK_HEARTBEAT_TESTING)
    namespace MaintenanceTestHooks
    {
        void BeforeStart();
        void BeforeCheck() noexcept;
    }
#endif

    IOCPBaseImpl::IOCPBaseImpl() noexcept = default;

    IOCPBaseImpl::~IOCPBaseImpl() = default;

    StartResult IOCPBaseImpl::StartCommon(
        const NetworkOptions& options,
        std::size_t maxSessions,
        std::size_t maxTotalSegmentBytes) noexcept
    {
        EngineExecutionScope executionScope;

        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state == State::Running)
            {
                return { StartError::AlreadyRunning, 0 };
            }

            if (m_state != State::Stopped ||
                m_stopWaiterCount != 0)
            {
                return { StartError::Busy, 0 };
            }

            // 이 호출이 시작 작업을 맡는다.
            m_state = State::Starting;
        }

        // 자원을 확보하기 전에 설정을 확인한다.
        if (ValidateOptions(options) != OptionsError::None ||
            options.ioWorkerCount == 0 ||
            options.jobWorkerCount == 0 ||
            maxSessions == 0 ||
            maxSessions > (std::numeric_limits<std::uint32_t>::max)() ||
            maxTotalSegmentBytes == 0)
        {
            RollbackCommonStart();

            return { StartError::InvalidOptions, 0 };
        }

        const int winsockError = m_winsock.Initialize();

        if (winsockError != 0)
        {
            RollbackCommonStart();

            return { StartError::WinsockInitializationFailed, static_cast<std::uint32_t>(winsockError) };
        }

        const auto resourcesResult = CreateProcessingResources(
            options,
            maxSessions,
            maxTotalSegmentBytes);

        if (!resourcesResult)
        {
            RollbackCommonStart();

            return resourcesResult;
        }

        const auto workersResult = StartWorkers();

        if (!workersResult)
        {
            RollbackCommonStart();

            return workersResult;
        }

        if (!StartMaintenance())
        {
            RollbackCommonStart();

            return { StartError::WorkerStartFailed, 0 };
        }


        // 역할별 통신 준비가 남아 있으므로 Starting을 유지한다.
        return {};
    }

    void IOCPBaseImpl::CompleteStart() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state != State::Starting)
            {
                std::terminate();
            }

            m_state = State::Running;
        }

        // 시작이 완료됐으므로 상태를 기다리던 호출이 다시 판단한다.
        m_stateChanged.notify_all();
    }

    void IOCPBaseImpl::RollbackTransportStart() noexcept
    {
        EngineExecutionScope executionScope;

        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state != State::Starting)
            {
                std::terminate();
            }

            // 시작 호출자가 실패 복구의 종료 책임을 이어받는다.
            m_state = State::Stopping;
        }

        // 주기 순회가 자원에 접근하지 않도록 먼저 합류한다.
        StopMaintenance();

        // 완료 회수와 세션 반환에 필요한 워커는 계속 실행한다.
        StopTransportAndSessions();

        ReleaseProcessingResources();

        CompleteStop();
    }

    StopResult IOCPBaseImpl::StopCommon() noexcept
    {
        // 대기나 상태 변경 전에 잘못된 호출 위치를 거절한다.
        if (EngineExecutionScope::IsActive())
        {
            return StopResult::InvalidCallContext;
        }

        // 종료 처리 중 콜백이 이 함수를 다시 호출하는 경우도 차단한다.
        EngineExecutionScope executionScope;

        if (BeginStop() == StopEntryResult::AlreadyStopped)
        {
            return StopResult::AlreadyStopped;
        }

        // 순회와 하트비트 생산을 끝낸 뒤 세션 종료를 시작한다.
        StopMaintenance();

        // 워커가 실행 중인 상태에서 역할별 통신과 세션을 정리한다.
        StopTransportAndSessions();

        ReleaseProcessingResources();

        CompleteStop();

        return StopResult::Stopped;
    }

    void IOCPBaseImpl::CloseSessionsAndWait() noexcept
    {
        ClientSessionPool* pool = nullptr;

        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            // 정상 종료 또는 역할별 시작 실패 복구의 책임자만 호출한다.
            if (m_state != State::Stopping)
            {
                std::terminate();
            }

            pool = m_sessionPool.get();
        }

        if (pool == nullptr)
        {
            return;
        }

        // 종료 책임자가 공통 자원 해제 전까지 풀의 수명을 유지한다.
        // 상태 락을 보유하지 않고 종료 요청과 반환 대기를 수행한다.
        pool->RequestCloseAll(DisconnectInfo{ DisconnectReason::EngineStopping, 0 });
        pool->WaitUntilAllReturned();
    }

    bool IOCPBaseImpl::AssociateTransportSocket(SOCKET socket, ULONG_PTR completionKey) noexcept
    {
        std::lock_guard<std::mutex> lock{ m_stateMutex };

        // 종료 책임을 인수한 뒤에는 새 소켓을 등록하지 않는다.
        if (m_state != State::Starting &&
            m_state != State::Running)
        {
            return false;
        }

        // 공통 자원 생성이 완료된 뒤에만 등록할 수 있다.
        if (!m_ioWorkers)
        {
            return false;
        }

        return m_ioWorkers->AssociateSocket(socket, completionKey);
    }

    SessionReference IOCPBaseImpl::PrepareSession(SocketHandle&& socket) noexcept
    {
        if (!socket)
        {
            return {};
        }

        // 실패 정리와 참조 반환은 엔진 상태 락 밖에서 수행한다.
        SessionReference session;

        bool attached = false;

        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state != State::Starting &&
                m_state != State::Running)
            {
                return {};
            }

            // StartCommon 성공 이후에만 세션 준비를 허용한다.
            if (!m_sessionPool ||
                !m_ioWorkers ||
                !m_receiveProcessor)
            {
                return {};
            }

            session = m_sessionPool->AcquireSession();

            if (!session)
            {
                return {};
            }

            attached = session.Get()->AttachSocket(
                std::move(socket),
                *m_ioWorkers,
                *m_receiveProcessor);
        }

        if (!attached)
        {
            // 연결 유지용 참조도 반환해야 세션이 풀로 돌아갈 수 있다.
            session.Get()->BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            session.Reset();

            return {};
        }

        return session;
    }

    std::uint32_t IOCPBaseImpl::PostSessionConnect(
        ClientSession& session,
        LPFN_CONNECTEX connectEx,
        const sockaddr_in& remoteAddress,
        IOOperation& operation) noexcept
    {
        if (connectEx == nullptr ||
            operation.type != IOOperationType::Connect)
        {
            std::terminate();
        }

        std::lock_guard<std::mutex> lock{ session.m_mutex };

        if (session.m_state != ClientSession::State::Preparing ||
            !session.m_socket)
        {
            return static_cast<std::uint32_t>(WSA_OPERATION_ABORTED);
        }

        DWORD bytesSent = 0;

        const BOOL connected = connectEx(
            session.m_socket.Get(),
            reinterpret_cast<const sockaddr*>(&remoteAddress),
            static_cast<int>(sizeof(remoteAddress)),
            nullptr,
            0,
            &bytesSent,
            &operation.overlapped);

        if (connected == FALSE)
        {
            const int error = ::WSAGetLastError();

            if (error != WSA_IO_PENDING)
            {
                return static_cast<std::uint32_t>(error);
            }
        }

        return 0;
    }

    std::uint32_t IOCPBaseImpl::UpdateSessionConnectContext(ClientSession& session) noexcept
    {
        std::lock_guard<std::mutex> lock{ session.m_mutex };

        if (session.m_state != ClientSession::State::Preparing || !session.m_socket)
        {
            return static_cast<std::uint32_t>(WSA_OPERATION_ABORTED);
        }

        if (::setsockopt(session.m_socket.Get(), SOL_SOCKET,
            SO_UPDATE_CONNECT_CONTEXT, nullptr, 0) == SOCKET_ERROR)
        {
            return static_cast<std::uint32_t>(::WSAGetLastError());
        }

        return 0;
    }

    bool IOCPBaseImpl::StartSessionReceive(ClientSession& session) noexcept
    {
        // 첫 수신 전용
        const auto result = session.PostReceive();

        if (result == ClientSession::ReceivePostResult::Posted)
        {
            return true;
        }

        // 새 연결의 첫 수신은 실제 발행되어야 한다.
        // 즉시 실패 경로가 먼저 종료했더라도 중복 종료 요청은 안전하다.
        session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

        return false;
    }

    bool IOCPBaseImpl::StartSessionHandshake(ClientSession& session, SystemPacketId expectedPacket) noexcept
    {
        ClientSession::HandshakePhase phase;

        switch (expectedPacket)
        {
        case SystemPacketId::HandshakeRequest:
        {
            phase = ClientSession::HandshakePhase::WaitingRequest;

            break;
        }

        case SystemPacketId::HandshakeResponse:
        {
            phase = ClientSession::HandshakePhase::WaitingResponse;

            break;
        }

        default:
        {
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            return false;
        }

        }

        const auto deadline = std::chrono::steady_clock::now() + m_options.handshakeTimeout;

        if (!session.BeginHandshake(phase, deadline))
        {
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            return false;
        }

        // 클라이언트는 응답 대기를 설정한 다음 요청을 보낸다.
        if (expectedPacket == SystemPacketId::HandshakeResponse)
        {
            std::array<std::byte, HandshakePayloadSize> payload{};

            if (!WriteHandshakePayload(payload, HandshakePayload{ ProtocolVersion }))
            {
                session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

                return false;
            }

            const auto result = SendSystemPacket(
                session,
                SystemPacketId::HandshakeRequest,
                payload);

            if (result != SendResult::Queued)
            {
                CloseOnSystemSendFailure(session, result);
                return false;
            }
        }

        // 상태 설정과 요청 접수를 마친 뒤 수신을 시작한다.
        // 발행 실패 시 StartSessionReceive가 연결을 종료한다.
        return StartSessionReceive(session);
    }

    void IOCPBaseImpl::SendHeartbeatRequest(ClientSession& session) noexcept
    {
        std::uint64_t sequence = 0;

        {
            std::unique_lock<std::mutex> lock{ session.m_mutex };

            if (session.m_state != ClientSession::State::Ready)
            {
                return;
            }

            const auto now = std::chrono::steady_clock::now();

            // 순회 검사 이후 기한이 지났다면 최신 상태로 다시 종료를 판단한다.
            if (session.GetTimeoutReasonLocked(m_options, now).has_value())
            {
                lock.unlock();

                session.CloseIfTimedOut(m_options);

                return;
            }

            // 자체 수신 중지 또는 기존 응답 대기 중에는 새 요청을 예약하지 않는다.
            if (session.m_receivePausedAt.has_value() ||
                session.m_heartbeat.responseDeadline.has_value())
            {
                return;
            }

            if (now - session.m_heartbeat.lastRequestAt < m_options.heartbeatInterval)
            {
                return;
            }

            // 같은 연결에서 요청 번호를 재사용하지 않는다.
            if (session.m_heartbeat.sequence ==
                (std::numeric_limits<std::uint64_t>::max)())
            {
                lock.unlock();

                session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

                return;
            }

            sequence = ++session.m_heartbeat.sequence;

            session.m_heartbeat.lastRequestAt = now;

            // 응답이 빠르게 도착해도 찾을 수 있도록 송신 전에 기록한다.
            session.m_heartbeat.responseDeadline =
                now + m_options.heartbeatTimeout;
        }

        std::array<std::byte, HeartbeatPayloadSize> payload{};

        if (!WriteHeartbeatPayload(payload, HeartbeatPayload{ sequence }))
        {
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            return;
        }

        const auto result = SendSystemPacket(
            session,
            SystemPacketId::HeartbeatRequest,
            payload);

        if (result != SendResult::Queued)
        {
            CloseOnSystemSendFailure(session, result);
        }
    }

    bool IOCPBaseImpl::HandleHeartbeatPacket(ClientSession& session, PacketView packet) noexcept
    {
        const auto payload = ReadHeartbeatPayload(packet.payload);

        if (!payload)
        {
            return false;
        }

        {
            std::unique_lock<std::mutex> lock{ session.m_mutex };

            // 핸드셰이크 완료 전이나 종료 중에는 하트비트를 처리하지 않는다.
            if (session.m_state != ClientSession::State::Ready)
            {
                return false;
            }

            switch (packet.packetId)
            {
            case ToPacketId(SystemPacketId::HeartbeatRequest):
            {
                // 상대 요청에 대한 응답은 세션 락을 해제한 뒤 송신한다.
                break;
            }

            case ToPacketId(SystemPacketId::HeartbeatResponse):
            {
                const auto& deadline = session.m_heartbeat.responseDeadline;

                if (!deadline.has_value() ||
                    payload->sequence != session.m_heartbeat.sequence)
                {
                    return false;
                }

                // 자체 수신 중지로 응답 처리가 밀린 시간은 유예한다.
                if (session.GetTimeoutReasonLocked(m_options, std::chrono::steady_clock::now()).has_value())
                {
                    lock.unlock();

                    session.CloseIfTimedOut(m_options);

                    return false;
                }

                // 번호는 유지하고 이번 요청의 응답 대기만 해제한다.
                session.m_heartbeat.responseDeadline.reset();

                return true;
            }

            default:
            {
                return false;
            }
            }
        }

        std::array<std::byte, HeartbeatPayloadSize> response{};

        if (!WriteHeartbeatPayload(response, *payload))
        {
            return false;
        }

        // 상대가 보낸 요청 번호를 그대로 돌려준다.
        const auto result = SendSystemPacket(session, SystemPacketId::HeartbeatResponse, response);

        if (result != SendResult::Queued)
        {
            CloseOnSystemSendFailure(session, result);
        }

        return result == SendResult::Queued;
    }

    bool IOCPBaseImpl::HandleHandshakePacket(ClientSession& session, PacketView packet) noexcept
    {
        const auto payload = ReadHandshakePayload(packet.payload);

        if (!payload || payload->protocolVersion != ProtocolVersion)
        {
            return false;
        }

        bool sendResponse = false;

        {
            std::unique_lock<std::mutex> lock{ session.m_mutex };

            if (session.m_state != ClientSession::State::Preparing)
            {
                return false;
            }

            if (session.m_handshake.phase == ClientSession::HandshakePhase::None)
            {
                return false;
            }

            if (std::chrono::steady_clock::now() >= session.m_handshake.deadline)
            {
                lock.unlock();

                session.CloseIfTimedOut(m_options);

                return false;
            }

            switch (session.m_handshake.phase)
            {
            case ClientSession::HandshakePhase::WaitingRequest:
            {
                if (packet.packetId != ToPacketId(SystemPacketId::HandshakeRequest))
                {
                    return false;
                }

                sendResponse = true;

                break;
            }

            case ClientSession::HandshakePhase::WaitingResponse:
            {
                if (packet.packetId !=
                    ToPacketId(SystemPacketId::HandshakeResponse))
                {
                    return false;
                }

                break;
            }

            default:
            {
                return false;
            }
            }
        }

        // 송신 함수가 세션 락을 사용하므로 검사 구간 밖에서 호출한다.
        if (sendResponse)
        {
            std::array<std::byte, HandshakePayloadSize> response{};

            if (!WriteHandshakePayload(response, HandshakePayload{ ProtocolVersion }))
            {
                return false;
            }

            const auto result = SendSystemPacket(
                session,
                SystemPacketId::HandshakeResponse,
                response);

            if (result != SendResult::Queued)
            {
                CloseOnSystemSendFailure(session, result);

                return false;
            }
        }

        // 수신 후처리 참조가 살아 있으므로 Job용 참조를 추가할 수 있다.
        Job notification{ session.AcquireReference(), ConnectedNotification{} };

        // 연결 통지를 먼저 FIFO에 넣고 Ready로 전환한다.
        // 검사 이후 종료가 시작됐으면 기존 접수 경로가 거절된다.
        const auto result = session.EnqueueJob(std::move(notification));

        if (result != ClientSession::EnqueueJobResult::Queued)
        {
            // 최종 접수 직전 기한 초과도 ProtocolError보다 먼저 확정한다.
            session.CloseIfTimedOut(m_options);
        }

        return result == ClientSession::EnqueueJobResult::Queued;
    }

    void IOCPBaseImpl::CloseOnSystemSendFailure(ClientSession& session, SendResult result) noexcept
    {
        switch (result)
        {
        case SendResult::Queued:
        {
            return;
        }

        case SendResult::QueueFull:
        case SendResult::NoMemory:
        {
            session.BeginClose(DisconnectInfo{ DisconnectReason::ResourceExhausted, 0 });

            return;
        }

        case SendResult::Closed:
        {
            // 엔진 종료로 접수가 막혔거나 세션이 이미 Closing인 경우다.
            session.BeginClose(DisconnectInfo{ DisconnectReason::EngineStopping, 0 });

            return;
        }

        default:
        {
            // 엔진이 만든 패킷의 내부 계약 오류를 상대의 프로토콜 위반으로 기록하지 않는다.
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            return;
        }
        }
    }

    SendResult IOCPBaseImpl::SendSystemPacket(
        ClientSession& session,
        SystemPacketId packetId,
        std::span<const std::byte> payload) noexcept
    {
        // 정의된 시스템 패킷과 해당 본문 규격만 허용한다.
        switch (packetId)
        {
        case SystemPacketId::HandshakeRequest:
        case SystemPacketId::HandshakeResponse:
        {
            if (!ReadHandshakePayload(payload))
            {
                return SendResult::InvalidPacket;
            }

            break;
        }

        case SystemPacketId::HeartbeatRequest:
        case SystemPacketId::HeartbeatResponse:
        {
            if (!ReadHeartbeatPayload(payload))
            {
                return SendResult::InvalidPacket;
            }

            break;
        }

        default:
        {
            return SendResult::InvalidPacket;
        }
        }

        std::shared_ptr<PacketPool> pool;
        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            // 연결 완료는 엔진의 시작 완료보다 먼저 도착할 수 있다.
            if (m_state != State::Starting &&
                m_state != State::Running)
            {
                return SendResult::Closed;
            }

            pool = m_packetPool;
        }

        if (!pool)
        {
            return SendResult::Closed;
        }

        try
        {
            auto result = pool->CreatePacket(ToPacketId(packetId), payload);

            if (!result)
            {
                if (result.error == PacketError::NoMemory)
                {
                    return SendResult::NoMemory;
                }

                return SendResult::InvalidPacket;
            }

            return session.EnqueueSendPacket(std::move(result.packet));
        }
        catch (const std::bad_alloc&)
        {
            return SendResult::NoMemory;
        }
    }

    SessionReference IOCPBaseImpl::TryAcquireSession(ConnectionId id) noexcept
    {
        if (!id.IsValid())
        {
            return {};
        }

        std::lock_guard<std::mutex> lock{ m_stateMutex };

        // 시작 준비 중이거나 엔진 종료가 시작됐다면 새 조회를 허용하지 않는다.
        if (m_state != State::Running || !m_sessionPool)
        {
            return {};
        }

        return m_sessionPool->TryAcquireSession(id);
    }

    IOCPBaseImpl::StopEntryResult IOCPBaseImpl::BeginStop() noexcept
    {
        std::unique_lock<std::mutex> lock{ m_stateMutex };

        // 대기 중에는 재시작이 이전 종료를 추월하지 못하게 한다.
        ++m_stopWaiterCount;

        m_stateChanged.wait(lock, [this]
        {
            return m_state == State::Stopped ||
                m_state == State::Running;
        });

        // wait는 mutex를 다시 획득한 상태로 반환한다.
        --m_stopWaiterCount;

        if (m_state == State::Stopped)
        {
            return StopEntryResult::AlreadyStopped;
        }

        // 상태 확인과 종료 책임 인수를 같은 락 안에서 처리한다.
        m_state = State::Stopping;

        return StopEntryResult::Acquired;
    }

    StartResult IOCPBaseImpl::CreateProcessingResources(
        const NetworkOptions& options,
        std::size_t maxSessions,
        std::size_t maxTotalSegmentBytes) noexcept
    {
        // 시작 경로가 독점적으로 호출하며 기존 자원을 덮어쓰지 않는다.
        if (m_packetPool ||
            m_receiveProcessor ||
            m_jobScheduler ||
            m_sessionPool ||
            m_ioWorkers)
        {
            std::terminate();
        }

        if (ValidateOptions(options) != OptionsError::None ||
            options.ioWorkerCount == 0 ||
            options.jobWorkerCount == 0 ||
            maxSessions == 0 ||
            maxSessions > (std::numeric_limits<std::uint32_t>::max)() ||
            maxTotalSegmentBytes == 0)
        {
            return { StartError::InvalidOptions, 0 };
        }

        auto packetPool = PacketPool::Create(maxTotalSegmentBytes);

        if (!packetPool)
        {
            return { StartError::ResourceInitializationFailed, 0 };
        }

        try
        {
            auto callbacks = CreateJobCallbacks();

            // 패킷 Job을 조용히 버리는 구성은 허용하지 않는다.
            if (!callbacks.received)
            {
                std::terminate();
            }

            auto receiveProcessor = std::make_unique<ReceiveProcessor>(
                packetPool,
                &IOCPBaseImpl::DispatchSystemPacket,
                this);

            auto jobScheduler = JobScheduler::Create(
                maxSessions,
                options.jobWorkerCount,
                options.maxJobsPerBatch,
                options.jobBatchTime,
                std::move(callbacks),
                packetPool);

            if (!jobScheduler)
            {
                return { StartError::ResourceInitializationFailed, 0 };
            }

            auto sessionPool = ClientSessionPool::Create(
                static_cast<std::uint32_t>(maxSessions),
                options,
                &JobScheduler::EnqueueFinalization,
                jobScheduler.get(),
                *jobScheduler);

            if (!sessionPool)
            {
                return { StartError::ResourceInitializationFailed, 0 };
            }

            auto ioWorkers = IOWorkerPool::Create(
                options.ioWorkerCount,
                &IOCPBaseImpl::DispatchIOCompletion,
                this,
                packetPool);

            if (!ioWorkers)
            {
                return { StartError::ResourceInitializationFailed, 0 };
            }

            // 여기서부터는 실패하지 않는 소유권 이전만 수행한다.
            m_packetPool = std::move(packetPool);

            m_receiveProcessor = std::move(receiveProcessor);

            m_jobScheduler = std::move(jobScheduler);

            m_sessionPool = std::move(sessionPool);

            m_ioWorkers = std::move(ioWorkers);

            m_options = options;

            return {};
        }
        catch (const std::bad_alloc&)
        {
            return { StartError::NoMemory, 0 };
        }
        catch (...)
        {
            // 콜백 구성 중 발생한 나머지 예외도 시작 실패로 반환한다.
            return { StartError::ResourceInitializationFailed, 0 };
        }
    }

    StartResult IOCPBaseImpl::StartWorkers() noexcept
    {
        // 공통 자원 생성에 성공한 시작 경로에서만 호출한다.
        if (!m_jobScheduler || !m_ioWorkers)
        {
            std::terminate();
        }

        // I/O 처리가 시작되기 전에 Job 실행 준비를 마친다.
        if (!m_jobScheduler->Start())
        {
            return { StartError::WorkerStartFailed, 0 };
        }

        if (!m_ioWorkers->Start())
        {
            // I/O 워커 자체의 부분 시작 복구는 Start()가 수행한다.
            // 여기서는 먼저 시작했던 Job 워커까지 정지한다.
            m_jobScheduler->Stop();

            return { StartError::WorkerStartFailed, 0 };
        }

        return {};
    }

    bool IOCPBaseImpl::StartMaintenance() noexcept
    {
        // 시작·종료 책임자가 직렬로 호출하므로 별도 멤버 락은 필요 없다.
        if (m_maintenanceThread.joinable())
        {
            std::terminate();
        }

        try
        {
#if defined(IOCPNETWORK_HEARTBEAT_TESTING)
            MaintenanceTestHooks::BeforeStart();
#endif
            m_maintenanceThread = std::jthread(
                [this](std::stop_token stopToken)
                {
                    RunMaintenance(stopToken);
                });

            return true;
        }
        catch (const std::system_error&)
        {
            return false;
        }
        catch (const std::bad_alloc&)
        {
            return false;
        }
    }

    void IOCPBaseImpl::StopMaintenance() noexcept
    {
        if (!m_maintenanceThread.joinable())
        {
            return;
        }

        // stop_token을 사용하는 조건 변수 대기도 함께 깨어난다.
        m_maintenanceThread.request_stop();

        // 실행 중인 CheckSessions와 지역 세션 참조 반환까지 기다린다.
        m_maintenanceThread.join();
    }

    void IOCPBaseImpl::RunMaintenance(std::stop_token stopToken) noexcept
    {
        // 이 스레드 안에서 동기식 Stop을 호출해 자신을 기다리지 않도록 한다.
        EngineExecutionScope executionScope;

        std::unique_lock<std::mutex> lock{ m_stateMutex };

        // 역할별 시작이 끝나기 전에는 연결 검사를 실행하지 않는다.
        const bool startFinished = m_stateChanged.wait(
            lock,
            stopToken,
            [this]
            {
                return m_state != State::Starting;
            });

        if (!startFinished ||
            stopToken.stop_requested() ||
            m_state != State::Running)
        {
            return;
        }

        // 합류 전까지 설정을 변경하지 않으므로 한 번 읽어 사용한다.
        const auto interval = m_options.maintenanceInterval;

        for (;;)
        {
            static_cast<void>(m_stateChanged.wait_for(
                lock,
                stopToken,
                interval,
                [this]
                {
                    return m_state != State::Running;
                }));

            if (stopToken.stop_requested() ||
                m_state != State::Running)
            {
                return;
            }

            // CheckSessions가 엔진 상태 락을 사용하므로 해제하고 호출한다.
            lock.unlock();

#if defined(IOCPNETWORK_HEARTBEAT_TESTING)
            MaintenanceTestHooks::BeforeCheck();
#endif
            CheckSessions();

            lock.lock();
        }
    }

    void IOCPBaseImpl::CheckSessions() noexcept
    {
        ClientSessionPool* pool = nullptr;

        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state != State::Running || !m_sessionPool)
            {
                return;
            }

            pool = m_sessionPool.get();
        }

        // 주기 작업이 끝나기 전에는 풀을 파괴하지 않는 수명 규칙을 사용한다.
        const auto capacity = pool->GetCapacity();

        for (std::uint32_t index = 0; index < capacity; ++index)
        {
            SessionReference reference;

            {
                std::lock_guard<std::mutex> lock{ m_stateMutex };

                if (m_state != State::Running)
                {
                    return;
                }

                reference = pool->TryAcquireSessionAt(index);
            }

            if (!reference)
            {
                continue;
            }

            ClientSession& session = *reference.Get();

            // 상태·기한 검사와 Closing 전환은 세션 내부에서 함께 수행한다.
            session.CloseIfTimedOut(m_options);

            // 종료됐다면 Ready 검사에서 반환하므로 별도 상태 조회는 필요 없다.
            SendHeartbeatRequest(session);
        }
    }

    void IOCPBaseImpl::ReleaseProcessingResources() noexcept
    {
        // 공통 시작 실패도 포함한다. 이미 합류했다면 즉시 반환한다.
        StopMaintenance();

        // 반환되지 않은 세션이 있다면 워커를 먼저 정지할 수 없다.
        // 여기서는 기다리지 않고 호출 순서가 올바른지만 확인한다.
        if (m_sessionPool &&
            !m_sessionPool->WaitUntilAllReturned(std::chrono::milliseconds{ 0 }))
        {
            std::terminate();
        }

        // 역할별 I/O도 이미 회수된 상태에서 호출해야 한다.
        if (m_ioWorkers)
        {
            m_ioWorkers->Stop();
        }

        if (m_jobScheduler)
        {
            m_jobScheduler->Stop();
        }

        m_ioWorkers.reset();

        // 세션이 참조하는 스케줄러보다 먼저 세션 풀을 파괴한다.
        m_sessionPool.reset();

        m_jobScheduler.reset();

        m_receiveProcessor.reset();

        // 캐시와 처리 객체가 놓은 뒤 엔진의 풀 소유권을 놓는다.
        m_packetPool.reset();

        // 소켓과 워커의 Winsock 사용이 모두 끝난 뒤 정리한다.
        m_winsock.Reset();
    }

    void IOCPBaseImpl::CompleteStop() noexcept
    {
        {
            std::lock_guard lock{ m_stateMutex };

            // 시작 실패 복구와 정상적인 종료 경로에서만 호출한다.
            if (m_state != State::Starting &&
                m_state != State::Stopping)
            {
                std::terminate();
            }

            // 공통 자원을 남긴 채 정지 완료로 표시하면 안 된다.
            if (m_packetPool ||
                m_receiveProcessor ||
                m_jobScheduler ||
                m_sessionPool ||
                m_ioWorkers ||
                m_maintenanceThread.joinable())
            {
                std::terminate();
            }

            m_options = {};

            m_state = State::Stopped;
        }

        // 상태를 변경한 뒤 락을 풀고 대기자를 모두 깨운다.
        m_stateChanged.notify_all();
    }

    void IOCPBaseImpl::RollbackCommonStart() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_stateMutex };

            if (m_state != State::Starting)
            {
                std::terminate();
            }
        }

        // 공통 시작 실패 시에는 아직 세션이나 통신 작업이 없다.
        ReleaseProcessingResources();

        CompleteStop();
    }

    void IOCPBaseImpl::DispatchIOCompletion(void* context, const IOCompletion& completion) noexcept
    {
        if (context == nullptr)
        {
            std::terminate();
        }

        auto& impl = *static_cast<IOCPBaseImpl*>(context);

        switch (completion.operation.type)
        {
        case IOOperationType::Receive:
        case IOOperationType::Send:
        {
            // 송수신 완료의 key는 ClientSession 주소다.
            DispatchSessionCompletion(nullptr, completion);

            return;
        }

        case IOOperationType::Accept:
        case IOOperationType::Connect:
        {
            // 역할별 완료의 key는 해당 파생 Impl이 해석한다.
            impl.HandleTransportCompletion(completion);

            return;
        }

        default:
        {
            std::terminate();
        }
        }
    }

    bool IOCPBaseImpl::DispatchSystemPacket(void* context, ClientSession& session, Packet packet)
    {
        if (context == nullptr)
        {
            std::terminate();
        }

        auto& impl = *static_cast<IOCPBaseImpl*>(context);

        return impl.HandleSystemPacket(session, std::move(packet));
    }
}
