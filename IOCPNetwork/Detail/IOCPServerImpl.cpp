#include "IOCPServerImpl.h"

#include "../ClientSession.h"
#include "IOWorkerPool.h"
#include "EngineExecutionScope.h"
#include "OptionsValidation.h"
#include "JobScheduler.h"

#include <WS2tcpip.h> // for InetPtonA

#include <cstdint>   // for std::uint32_t
#include <exception> // for std::terminate
#include <memory>    // for std::make_unique
#include <new>       // for std::bad_alloc
#include <utility>   // for std::move
#include <mutex>     // for std::lock_guard, std::unique_lock
#include <chrono>       // for std::chrono::milliseconds
#include <system_error> // for std::system_error
#include <thread> // for std::thread, std::thread::hardware_concurrency, std::this_thread::get_id

namespace IOCPNetwork::Detail
{
    IOCPServerImpl::~IOCPServerImpl()
    {
        const auto result = Stop();

        // 내부 워커에서 자신을 파괴하면 자기 합류 또는 사용 중 객체 파괴가 된다.
        if (result == StopResult::InvalidCallContext)
        {
            std::terminate();
        }
    }

    StartResult IOCPServerImpl::Start(const ServerOptions& options, std::size_t maxTotalSegmentBytes) noexcept
    {
        // 역할별 준비까지 포함하여 전체 시작 경로를 내부 실행으로 표시한다.
        EngineExecutionScope executionScope;

        if (ValidateOptions(options) != OptionsError::None ||
            maxTotalSegmentBytes == 0)
        {
            return { StartError::InvalidOptions, 0 };
        }

        // 문자열을 가진 ServerOptions 전체 대신 공통 실행 설정만 복사한다.
        NetworkOptions network = options.network;

        const auto detectedWorkerCount = std::thread::hardware_concurrency();

        const auto automaticWorkerCount = detectedWorkerCount == 0 ? 1u : detectedWorkerCount;

        if (network.ioWorkerCount == 0)
        {
            network.ioWorkerCount = automaticWorkerCount;
        }

        if (network.jobWorkerCount == 0)
        {
            network.jobWorkerCount = automaticWorkerCount;
        }

        const auto commonResult = StartCommon(
            network,
            options.maxConnections,
            maxTotalSegmentBytes);

        if (!commonResult)
        {
            // 시작 책임을 얻지 못했거나 공통 시작 내부에서 이미 복구했다.
            return commonResult;
        }

        const auto transportResult = StartAccepting(options);

        if (!transportResult)
        {
            // 이 호출이 공통 시작을 성공시켰으므로 역할별 실패 복구를 책임진다.
            RollbackTransportStart();

            return transportResult;
        }

        // 리슨·최초 Accept·재시도 스레드까지 모두 준비된 뒤 확정한다.
        CompleteStart();

        return {};
    }

    StopResult IOCPServerImpl::Stop() noexcept
    {
        return StopCommon();
    }

    StartResult IOCPServerImpl::StartAccepting(const ServerOptions& options) noexcept
    {
        auto result = PrepareListener(options);

        if (!result)
        {
            return result;
        }

        result = PrepareAcceptSessions();

        if (!result)
        {
            return result;
        }

        return PostInitialAccepts();
    }

    void IOCPServerImpl::StopAccepting() noexcept
    {
        // 새 발행을 차단하고 배열에 접근하는 재시도 스레드를 먼저 합류시킨다.
        StopAcceptRetryWorker();

        std::unique_lock<std::mutex> lock{ m_acceptMutex };

        // 리슨 소켓을 닫아 새 접속 수락을 중단한다.
        if (!m_listenSocket.Close())
        {
            std::terminate();
        }

        if (m_acceptSessions)
        {
            for (std::size_t index = 0; index < AcceptSessionCount; ++index)
            {
                auto& session = m_acceptSessions[index];

                // 아직 AcceptSession이 소유한 소켓만 닫는다.
                // ClientSession이나 완료 처리 지역 변수로 이동된 소켓은 건드리지 않는다.
                if (!session.m_acceptSocket.Close())
                {
                    std::terminate();
                }
            }
        }

        // 소켓을 닫아도 완료 후처리는 아직 남아 있을 수 있다.
        // 기다리는 동안 mutex를 풀어 완료 워커가 정리를 진행하게 한다.
        m_acceptCompleted.wait(
            lock,
            [this]
            {
                return m_acceptIOCount == 0;
            });

        // 재시도 스레드와 모든 Accept 후처리가 끝난 뒤 저장소를 해제한다.
        m_acceptSessions.reset();

        m_acceptEx = nullptr;
        m_getAcceptExSockaddrs = nullptr;
    }

    void IOCPServerImpl::StopTransportAndSessions() noexcept
    {
        // 완료 처리 중인 Accept가 새 ClientSession을 만드는 경로까지 끝낸다.
        StopAccepting();

        // 이제 새로운 세션이 생기지 않으므로 한 번 순회해 종료할 수 있다.
        CloseSessionsAndWait();
    }

    StartResult IOCPServerImpl::PrepareListener(const ServerOptions& options) noexcept
    {
        // 같은 시작 경로에서 준비한 자원을 덮어쓰면 안 된다.
        if (m_listenSocket || m_acceptEx || m_getAcceptExSockaddrs)
        {
            std::terminate();
        }

        if (options.bindAddress.empty() || options.port == 0)
        {
            return { StartError::InvalidOptions, 0 };
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = ::htons(options.port);

        // 이번 서버 구현은 숫자로 표현한 IPv4 주소를 사용한다.
        if (::InetPtonA(
            AF_INET,
            options.bindAddress.c_str(),
            &address.sin_addr) != 1)
        {
            return { StartError::InvalidOptions, 0 };
        }

        SocketHandle listener{
             ::WSASocketW(
                 AF_INET,
                 SOCK_STREAM,
                 IPPROTO_TCP,
                 nullptr,
                 0,
                 WSA_FLAG_OVERLAPPED)
        };

        if (!listener)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        // 다른 소켓과 같은 주소·포트를 공유하지 않도록 bind 전에 설정한다.
        const BOOL exclusiveAddressUse = TRUE;

        if (::setsockopt(
            listener.Get(),
            SOL_SOCKET,
            SO_EXCLUSIVEADDRUSE,
            reinterpret_cast<const char*>(&exclusiveAddressUse),
            static_cast<int>(sizeof(exclusiveAddressUse))) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        if (::bind(
            listener.Get(),
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<int>(sizeof(address))) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        if (::listen(listener.Get(), SOMAXCONN) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        LPFN_ACCEPTEX acceptEx = nullptr;
        GUID acceptExId = WSAID_ACCEPTEX;
        DWORD bytesReturned = 0;

        if (::WSAIoctl(
            listener.Get(),
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &acceptExId,
            static_cast<DWORD>(sizeof(acceptExId)),
            &acceptEx,
            static_cast<DWORD>(sizeof(acceptEx)),
            &bytesReturned,
            nullptr,
            nullptr) == SOCKET_ERROR)
        {
            return {
                StartError::TransportStartFailed,
                static_cast<std::uint32_t>(::WSAGetLastError())
            };
        }

        if (acceptEx == nullptr || bytesReturned != sizeof(acceptEx))
        {
            return { StartError::TransportStartFailed, WSAEOPNOTSUPP };
        }

        LPFN_GETACCEPTEXSOCKADDRS getAcceptExSockaddrs = nullptr;
        GUID getAddressesId = WSAID_GETACCEPTEXSOCKADDRS;

        bytesReturned = 0;

        if (::WSAIoctl(
            listener.Get(),
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &getAddressesId,
            static_cast<DWORD>(sizeof(getAddressesId)),
            &getAcceptExSockaddrs,
            static_cast<DWORD>(sizeof(getAcceptExSockaddrs)),
            &bytesReturned,
            nullptr,
            nullptr) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        if (getAcceptExSockaddrs == nullptr ||
            bytesReturned != sizeof(getAcceptExSockaddrs))
        {
            return { StartError::TransportStartFailed, WSAEOPNOTSUPP };
        }

        // 모든 준비가 끝난 뒤 실패하지 않는 대입만 수행한다.
        m_listenSocket = std::move(listener);

        m_acceptEx = acceptEx;

        m_getAcceptExSockaddrs = getAcceptExSockaddrs;

        return {};
    }

    StartResult IOCPServerImpl::PrepareAcceptSessions() noexcept
    {
        // 리슨 준비에 성공한 시작 경로에서 한 번만 호출한다.
        if (!m_listenSocket ||
            !m_acceptEx ||
            !m_getAcceptExSockaddrs ||
            m_acceptSessions)
        {
            std::terminate();
        }

        try
        {
            auto sessions = std::make_unique<AcceptSession[]>(AcceptSessionCount);

            // 리슨 소켓에서 발생하는 Accept 완료는 서버 Impl의 주소를 완료 키로 사용한다.
            const auto completionKey = reinterpret_cast<ULONG_PTR>(this);

            if (!AssociateTransportSocket(m_listenSocket.Get(), completionKey))
            {
                // 현재 공통 등록 API는 상세 오류를 반환하지 않는다.
                return { StartError::TransportStartFailed, 0 };
            }

            // 등록 이후에는 실패하지 않는 소유권 이전만 수행한다.
            m_acceptSessions = std::move(sessions);

            return {};
        }
        catch (const std::bad_alloc&)
        {
            return { StartError::NoMemory, 0 };
        }
    }

    StartResult IOCPServerImpl::PostInitialAccepts() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_acceptMutex };

            // 역할별 시작 경로에서 배열 준비 후 한 번만 호출한다.
            if (!m_acceptSessions ||
                !m_listenSocket ||
                !m_acceptEx ||
                m_acceptPostingEnabled ||
                m_acceptIOCount != 0)
            {
                std::terminate();
            }

            m_acceptPostingEnabled = true;
        }

        for (std::size_t index = 0; index < AcceptSessionCount; ++index)
        {
            const auto result = PostAccept(m_acceptSessions[index]);

            if (!result)
            {
                std::lock_guard<std::mutex> lock{ m_acceptMutex };

                // 먼저 접수된 작업은 남아 있다.
                // 호출자는 시작 실패 복구 경로에서 취소하고 완료를 회수한다.
                m_acceptPostingEnabled = false;

                return result;
            }
        }

        // 모든 최초 발행에 성공한 뒤 재시도 경로를 가동한다.
        return StartAcceptRetryWorker();
    }

    StartResult IOCPServerImpl::StartAcceptRetryWorker() noexcept
    {
        std::lock_guard<std::mutex> lock{ m_acceptMutex };

        if (!m_acceptPostingEnabled ||
            !m_acceptSessions ||
            m_acceptRetryThread.joinable())
        {
            std::terminate();
        }

        try
        {
            m_acceptRetryThread = std::thread(&IOCPServerImpl::AcceptRetryLoop, this);

            return {};
        }
        catch (const std::system_error&)
        {
            return { StartError::WorkerStartFailed, 0 };
        }
        catch (const std::bad_alloc&)
        {
            return { StartError::NoMemory, 0 };
        }
    }

    void IOCPServerImpl::AcceptRetryLoop() noexcept
    {
        EngineExecutionScope executionScope;

        std::unique_lock<std::mutex> lock{ m_acceptMutex };

        while (m_acceptPostingEnabled)
        {
            const bool stopping = m_acceptRetryWake.wait_for(
                lock,
                std::chrono::milliseconds{ 100 },
                [this]
                {
                    return !m_acceptPostingEnabled;
                });

            if (stopping)
            {
                break;
            }

            for (std::size_t index = 0; index < AcceptSessionCount; ++index)
            {
                auto& session = m_acceptSessions[index];

                if (session.m_state != AcceptSession::State::RetryPending)
                {
                    continue;
                }

                // 상태 확인과 발행을 같은 락 안에서 수행한다.
                // 실패하면 RetryPending을 유지하고 다음 주기를 기다린다.
                static_cast<void>(PostAcceptLocked(session));
            }
        }
    }

    void IOCPServerImpl::StopAcceptRetryWorker() noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_acceptMutex };

            // 완료 워커의 재등록과 재시도 스레드의 발행을 함께 차단한다.
            m_acceptPostingEnabled = false;
        }

        m_acceptRetryWake.notify_all();

        // 재시도 스레드는 종료 전에 같은 mutex를 다시 획득해야 한다.
        // 따라서 락을 풀고 합류한다.
        if (m_acceptRetryThread.joinable())
        {
            if (m_acceptRetryThread.get_id() == std::this_thread::get_id())
            {
                std::terminate();
            }

            m_acceptRetryThread.join();
        }
    }

    StartResult IOCPServerImpl::PostAccept(AcceptSession& session) noexcept
    {
        std::lock_guard lock(m_acceptMutex);

        return PostAcceptLocked(session);
    }

    StartResult IOCPServerImpl::PostAcceptLocked(AcceptSession& session) noexcept
    {
        // 종료 또는 시작 실패 복구가 시작되면 새 작업을 발행하지 않는다.
        if (!m_acceptPostingEnabled)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(WSA_OPERATION_ABORTED) };
        }

        // 리슨 준비와 수락용 소켓 생성이 끝난 상태에서 호출한다.
        if (!m_listenSocket || !m_acceptEx)
        {
            std::terminate();
        }

        // 최초 발행과 이전 완료 후 재등록 모두 빈 소켓에서 시작한다.
        // 소켓이 남아 있다면 같은 AcceptSession을 중복 발행한 것이다.
        if (session.m_state == AcceptSession::State::Active ||
            session.m_acceptSocket)
        {
            std::terminate();
        }

        SocketHandle socket{
            ::WSASocketW(
                AF_INET,
                SOCK_STREAM,
                IPPROTO_TCP,
                nullptr,
                0,
                WSA_FLAG_OVERLAPPED)
        };

        if (!socket)
        {
            session.m_state = AcceptSession::State::RetryPending;

            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        session.m_acceptSocket = std::move(socket);

        // 최초 발행이거나 이전 Accept의 완료 처리가 끝난 작업만 재사용한다.
        // 작업 종류는 유지하고 운영체제가 사용하는 영역만 초기화한다.
        session.m_acceptOperation.overlapped = {};

        // 완료가 발행 함수보다 먼저 실행될 수 있으므로 호출 전에 증가시킨다.
        session.m_state = AcceptSession::State::Active;

        ++m_acceptIOCount;

        DWORD bytesReceived = 0;

        const BOOL accepted = m_acceptEx(
            m_listenSocket.Get(),
            session.m_acceptSocket.Get(),
            session.m_addressBuffer.data(),
            0,
            static_cast<DWORD>(AcceptSession::AddressBufferSize),
            static_cast<DWORD>(AcceptSession::AddressBufferSize),
            &bytesReceived,
            &session.m_acceptOperation.overlapped);

        if (accepted == FALSE)
        {
            const int error = ::WSAGetLastError();

            // 비동기 발행이 접수된 상태이므로 정상 경로다.
            if (error != WSA_IO_PENDING)
            {
                // 즉시 실패에는 완료 통지가 없으므로 여기서 정리한다.
                if (!session.m_acceptSocket.Close())
                {
                    std::terminate();
                }

                session.m_state = AcceptSession::State::RetryPending;

                --m_acceptIOCount;

                if (m_acceptIOCount == 0)
                {
                    m_acceptCompleted.notify_all();
                }

                return { StartError::TransportStartFailed, static_cast<std::uint32_t>(error) };
            }
        }

        // 즉시 성공도 완료 처리는 IOCP 워커에서 수행한다.
        return {};
    }

    void IOCPServerImpl::HandleTransportCompletion(const IOCompletion& completion) noexcept
    {
        // 이 서버의 리슨 소켓에서 발행한 Accept 완료만 전달되어야 한다.
        if (completion.completionKey != reinterpret_cast<ULONG_PTR>(this) ||
            completion.operation.type != IOOperationType::Accept)
        {
            std::terminate();
        }

        auto& acceptSession = FindAcceptSession(completion.operation);

        if (completion.errorCode == ERROR_SUCCESS)
        {
            auto session = PrepareAcceptedSession(acceptSession);

            if (session)
            {
                // 실패 시 공통 함수가 연결 종료까지 처리한다.
                // 성공 시 첫 수신 I/O가 별도의 세션 참조를 소유한다.
                if (!StartSessionReceive(*session.Get()))
                {
                    session.Reset();
                }
            }
        }
        else
        {
            // 취소와 일반 실패 모두 연결 세션으로 인계하지 않는다.
            // 종료 경로가 이미 닫혔다면 Close는 그대로 성공한다.
            std::lock_guard<std::mutex> lock{ m_acceptMutex };

            if (!acceptSession.m_acceptSocket.Close())
            {
                std::terminate();
            }
        }

        // 위 블록의 지역 참조 반환과 소켓 정리가 끝난 뒤 호출한다.
        // 이후에는 Accept 배열이나 acceptSession에 접근하지 않는다.
        CompleteAcceptIO(acceptSession);
    }

    JobCallbacks IOCPServerImpl::CreateJobCallbacks()
    {
        JobCallbacks callbacks;

        // 서비스 디스패치 구현 전의 임시 거절 경로다.
        // 정상 연결은 아직 Preparing이므로 서비스 Packet Job을 접수하지 않는다.
        callbacks.received = [](ClientSession& session, Packet)
            {
                session.BeginClose();
            };

        return callbacks;
    }

    bool IOCPServerImpl::HandleSystemPacket(ClientSession&, Packet)
    {
        // 핸드셰이크·하트비트 구현 전에는 시스템 패킷을 수락하지 않는다.
        // false를 받은 ReceiveProcessor가 연결 종료를 요청한다.
        return false;
    }

    AcceptSession& IOCPServerImpl::FindAcceptSession(const IOOperation& operation) noexcept
    {
        if (operation.type != IOOperationType::Accept || !m_acceptSessions)
        {
            std::terminate();
        }

        for (std::size_t index = 0; index < AcceptSessionCount; ++index)
        {
            auto& session = m_acceptSessions[index];

            if (&session.m_acceptOperation == &operation)
            {
                return session;
            }
        }

        // 이 서버에서 발행한 Accept 완료라면 반드시 배열 안에 있어야 한다.
        std::terminate();
    }

    SessionReference IOCPServerImpl::PrepareAcceptedSession(AcceptSession& session) noexcept
    {
        SocketHandle socket;

        {
            std::lock_guard<std::mutex> lock{ m_acceptMutex };

            // 종료 경로가 먼저 소켓을 정리했을 수 있다.
            if (!session.m_acceptSocket)
            {
                return {};
            }

            // 이제부터 지역 변수가 소켓 정리 책임을 가진다.
            socket = std::move(session.m_acceptSocket);

            // 종료가 시작됐다면 연결 세션으로 인계하지 않는다.
            if (!m_acceptPostingEnabled || !m_listenSocket)
            {
                return {};
            }

            const SOCKET listenSocket = m_listenSocket.Get();

            // 수락된 소켓에 리슨 소켓의 연결 문맥을 반영한다.
            if (::setsockopt(
                socket.Get(),
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                reinterpret_cast<const char*>(&listenSocket),
                static_cast<int>(sizeof(listenSocket))) == SOCKET_ERROR)
            {
                return {};
            }
        }

        // 공통 상태 락과 세션 풀 락을 사용하는 경로는 Accept 락 밖에서 호출한다.
        // 실패하면 지역 소켓이 닫히고, 성공하면 ClientSession이 소유한다.
        return PrepareSession(std::move(socket));
    }

    void IOCPServerImpl::CompleteAcceptIO(AcceptSession& session) noexcept
    {
        std::lock_guard lock(m_acceptMutex);

        if (m_acceptIOCount == 0 ||
            session.m_state != AcceptSession::State::Active ||
            session.m_acceptSocket)
        {
            std::terminate();
        }

        // 소켓 인계 또는 실패 정리가 끝났으므로 작업 저장소를 재사용할 수 있다.
        session.m_state = AcceptSession::State::Idle;

        if (m_acceptPostingEnabled)
        {
            // 실패하면 내부에서 RetryPending 으로 남긴다.
            // 현재 워커에서 반복하지 않고 지연 재시도 경로에 맡긴다.
            static_cast<void>(PostAcceptLocked(session));
        }

        // 재발행 결과와 관계없이 이전 완료의 카운트는 정확히 한 번 반환한다.
        --m_acceptIOCount;

        if (m_acceptIOCount == 0)
        {
            m_acceptCompleted.notify_all();
        }
    }
}
