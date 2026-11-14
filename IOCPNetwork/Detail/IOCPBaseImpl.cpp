#include "IOCPBaseImpl.h"

#include "JobScheduler.h"
#include "OptionsValidation.h"
#include "PacketPool.h"
#include "ClientSessionPool.h"
#include "../ClientSession.h"
#include "IOWorkerPool.h"
#include "EngineExecutionScope.h"

#include <exception> // for std::terminate
#include <memory>    // for std::make_unique
#include <new>       // for std::bad_alloc
#include <utility>   // for std::move
#include <cstdint> // for std::uint32_t
#include <limits>  // for std::numeric_limits
#include <mutex>   // for std::mutex, std::lock_guard, std::unique_lock
#include <chrono> // for std::chrono::milliseconds

namespace IOCPNetwork::Detail
{
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
        pool->RequestCloseAll();
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
            session.Get()->BeginClose();

            session.Reset();

            return {};
        }

        return session;
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
        session.BeginClose();

        return false;
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

    void IOCPBaseImpl::ReleaseProcessingResources() noexcept
    {
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
                m_ioWorkers)
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
