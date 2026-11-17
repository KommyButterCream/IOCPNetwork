#include "IOCPClientImpl.h"

#include "../ClientSession.h"
#include "EngineExecutionScope.h"
#include "IOWorkerPool.h"
#include "JobScheduler.h"
#include "OptionsValidation.h"
#include "SystemPacket.h"

#include <WinSock2.h> // for WSASocketW, bind, WSAIoctl, WSAGetLastError
#include <WS2tcpip.h> // for InetPtonA

#include <cstddef> // for std::size_t
#include <cstdint>   // for std::uint32_t
#include <exception> // for std::terminate
#include <mutex> // for std::lock_guard, std::unique_lock
#include <utility>   // for std::move

namespace IOCPNetwork::Detail
{
    IOCPClientImpl::~IOCPClientImpl()
    {
        if (Stop() == StopResult::InvalidCallContext)
        {
            std::terminate();
        }
    }

    StartResult IOCPClientImpl::Start(const ClientOptions& options, std::size_t maxTotalSegmentBytes) noexcept
    {
        EngineExecutionScope executionScope;

        if (ValidateOptions(options) != OptionsError::None ||
            maxTotalSegmentBytes == 0)
        {
            return { StartError::InvalidOptions, 0 };
        }

        NetworkOptions network = options.network;

        // 단일 연결 클라이언트의 기본 워커 수다.
        if (network.ioWorkerCount == 0)
        {
            network.ioWorkerCount = 1;
        }

        if (network.jobWorkerCount == 0)
        {
            network.jobWorkerCount = 1;
        }

        const auto commonResult = StartCommon(
            network,
            1,
            maxTotalSegmentBytes);

        if (!commonResult)
        {
            return commonResult;
        }

        const auto connectResult = StartConnecting(options);

        if (!connectResult)
        {
            RollbackTransportStart();

            return connectResult;
        }

        // 엔진 시작을 확정한다. TCP 연결 완료를 의미하지 않는다.
        CompleteStart();

        return {};
    }

    StopResult IOCPClientImpl::Stop() noexcept
    {
        return StopCommon();
    }

    StartResult IOCPClientImpl::StartConnecting(const ClientOptions& options) noexcept
    {
        SocketHandle socket;

        auto result = PrepareConnectSocket(options, socket);

        if (!result)
        {
            return result;
        }

        result = PrepareConnectSession(std::move(socket));

        if (!result)
        {
            return result;
        }

        return PostConnect();
    }

    StartResult IOCPClientImpl::PrepareConnectSocket(const ClientOptions& options, SocketHandle& preparedSocket) noexcept
    {
        // 시작 준비 경로에서 한 번만 호출하며 출력 핸들은 비어 있어야 한다.
        if (preparedSocket ||
            m_connectEx != nullptr ||
            m_connectReference ||
            m_connectActive)
        {
            std::terminate();
        }

        if (options.serverAddress.empty() || options.port == 0)
        {
            return { StartError::InvalidOptions, 0 };
        }

        sockaddr_in remoteAddress{};
        remoteAddress.sin_family = AF_INET;
        remoteAddress.sin_port = ::htons(options.port);

        // 서버와 동일하게 숫자로 표현한 IPv4 주소만 지원한다.
        if (::InetPtonA(
            AF_INET,
            options.serverAddress.c_str(),
            &remoteAddress.sin_addr) != 1)
        {
            return { StartError::InvalidOptions, 0 };
        }

        // 접속 대상에 와일드카드 주소를 사용할 수 없다.
        if (remoteAddress.sin_addr.s_addr == htonl(INADDR_ANY))
        {
            return { StartError::InvalidOptions, 0 };
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
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        // ConnectEx는 미리 bind된 소켓을 요구한다.
        // 로컬 주소와 포트 선택은 운영체제에 맡긴다.
        sockaddr_in localAddress{};
        localAddress.sin_family = AF_INET;
        localAddress.sin_addr.s_addr = htonl(INADDR_ANY);
        localAddress.sin_port = 0;

        if (::bind(
            socket.Get(),
            reinterpret_cast<const sockaddr*>(&localAddress),
            static_cast<int>(sizeof(localAddress))) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        LPFN_CONNECTEX connectEx = nullptr;
        GUID connectExId = WSAID_CONNECTEX;
        DWORD bytesReturned = 0;

        if (::WSAIoctl(
            socket.Get(),
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &connectExId,
            static_cast<DWORD>(sizeof(connectExId)),
            &connectEx,
            static_cast<DWORD>(sizeof(connectEx)),
            &bytesReturned,
            nullptr,
            nullptr) == SOCKET_ERROR)
        {
            return { StartError::TransportStartFailed, static_cast<std::uint32_t>(::WSAGetLastError()) };
        }

        if (connectEx == nullptr || bytesReturned != sizeof(connectEx))
        {
            return { StartError::TransportStartFailed, WSAEOPNOTSUPP };
        }

        // 모든 준비가 성공한 뒤 실패하지 않는 대입으로 결과를 반영한다.
        m_remoteAddress = remoteAddress;
        m_connectEx = connectEx;

        preparedSocket = std::move(socket);

        return {};
    }

    StartResult IOCPClientImpl::PrepareConnectSession(SocketHandle&& socket) noexcept
    {
        {
            std::lock_guard<std::mutex> lock{ m_connectMutex };

            // 공통 시작 성공 후 연결 준비 경로에서 한 번만 호출한다.
            if (!socket ||
                m_connectEx == nullptr ||
                m_connectReference ||
                m_connectActive)
            {
                std::terminate();
            }
        }

        // 공통 상태·풀·세션 락을 사용하는 경로는 Connect 락 밖에서 호출한다.
        auto reference = PrepareSession(std::move(socket));

        if (!reference)
        {
            // 공통 준비 실패 시 소켓은 호출자가 계속 소유한다.
            return { StartError::TransportStartFailed, 0 };
        }

        {
            std::lock_guard<std::mutex> lock{ m_connectMutex };

            // 참조 수를 증가시키지 않고 반환 책임을 멤버로 이전한다.
            m_connectReference = std::move(reference);
        }

        return {};
    }

    StartResult IOCPClientImpl::PostConnect() noexcept
    {
        SessionReference failedReference;

        std::uint32_t error = 0;

        {
            std::lock_guard<std::mutex> lock(m_connectMutex);

            if (!m_connectReference ||
                m_connectEx == nullptr ||
                m_connectActive)
            {
                std::terminate();
            }

            m_connectOperation.overlapped = {};

            // 완료 워커가 먼저 실행될 수 있으므로 발행 전에 설정한다.
            m_connectActive = true;

            error = PostSessionConnect(
                *m_connectReference.Get(),
                m_connectEx,
                m_remoteAddress,
                m_connectOperation);

            if (error == 0)
            {
                // 즉시 성공과 IO_PENDING은 모두 접수 성공이다.
                // 이후 실패는 StartResult가 아니라 최종 정리에서 통지한다.
                m_connectAccepted = true;

                return {};
            }

            // 즉시 실패에는 완료 통지가 없으므로 이 경로가 정리를 맡는다.
            // 정리가 끝나기 전까지 Active는 유지한다.
            failedReference = std::move(m_connectReference);
        }

        // 연결 유지용 참조와 Connect용 참조를 락 밖에서 정리한다.
        failedReference.Get()->BeginClose(DisconnectInfo{ DisconnectReason::TransportError, error });

        failedReference.Reset();

        {
            std::lock_guard<std::mutex> lock{ m_connectMutex };

            m_connectActive = false;

            m_connectCompleted.notify_all();
        }

        return { StartError::TransportStartFailed, error };
    }

    void IOCPClientImpl::HandleTransportCompletion(const IOCompletion& completion) noexcept
    {
        SessionReference reference;

        {
            std::lock_guard<std::mutex> lock{ m_connectMutex };

            // 현재 발행한 Connect 작업의 완료인지 먼저 검증한다.
            if (completion.operation.type != IOOperationType::Connect ||
                &completion.operation != &m_connectOperation ||
                !m_connectActive ||
                !m_connectReference)
            {
                std::terminate();
            }

            // 연결 소켓은 처음부터 ClientSession을 완료 키로 등록했다.
            if (completion.completionKey != reinterpret_cast<ULONG_PTR>(m_connectReference.Get()))
            {
                std::terminate();
            }

            // 참조 수는 그대로 유지하고 후처리 책임만 지역 변수로 옮긴다.
            reference = std::move(m_connectReference);
        }

        ClientSession& session = *reference.Get();

        if (completion.errorCode != ERROR_SUCCESS)
        {
            // 완료 오류 코드를 보존하며, 먼저 확정된 종료 사유는 유지한다.
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError,
                static_cast<std::uint32_t>(completion.errorCode) });
        }
        else if (const auto error = UpdateSessionConnectContext(session); error != 0)
        {
            // 완료 성공과 종료 요청이 경합했거나 문맥 적용에 실패했다.
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, error });
        }
        else
        {
            // 실패하면 공통 함수가 BeginClose까지 수행한다.
            // 성공하면 수신 I/O가 별도의 세션 참조를 보유한다.
            static_cast<void>(StartSessionHandshake(
                session,
                SystemPacketId::HandshakeResponse));
        }

        // 마지막 참조일 수 있으므로 Connect·세션 락 밖에서 반환한다.
        reference.Reset();

        // 위에서 세션이 최종 반환될 수 있으므로 이후 session에 접근하지 않는다.
        {
            std::lock_guard lock(m_connectMutex);

            m_connectActive = false;

            m_connectCompleted.notify_all();
        }
    }

    void IOCPClientImpl::StopTransportAndSessions() noexcept
    {
        SessionReference preparedReference;

        {
            std::lock_guard<std::mutex> lock{ m_connectMutex };

            // 발행 전 준비 참조에는 이를 회수할 완료 통지가 없다.
            // 발행된 참조는 완료 함수가 인수하도록 멤버에 유지한다.
            if (!m_connectActive)
            {
                preparedReference = std::move(m_connectReference);
            }
        }

        if (preparedReference)
        {
            preparedReference.Get()->BeginClose(DisconnectInfo{ DisconnectReason::EngineStopping, 0 });

            // 전체 반환 대기를 시작하기 전에 준비 참조부터 반환한다.
            preparedReference.Reset();
        }

        // 세션 소켓을 닫아 Connect·송수신 취소를 시작하고,
        // 완료와 Job 정리를 거쳐 모든 세션이 반환될 때까지 기다린다.
        CloseSessionsAndWait();

        {
            std::unique_lock<std::mutex> lock{ m_connectMutex };

            // 세션 참조 반환 직후에도 완료 함수의 마지막 통지가 남을 수 있다.
            m_connectCompleted.wait(
                lock,
                [this]
                {
                    return !m_connectActive;
                });

            if (m_connectReference)
            {
                std::terminate();
            }

            // 운영체제와 완료 후처리가 더 이상 접근하지 않는 시점이다.
            m_connectOperation.overlapped = {};
            m_remoteAddress = {};
            m_connectEx = nullptr;

            // 최종 통지와 세션 반환이 끝난 뒤 다음 시작을 준비한다.
            m_connectAccepted = false;
        }
    }


    JobCallbacks IOCPClientImpl::CreateJobCallbacks()
    {
        auto callbacks = CreateServiceJobCallbacks();

        callbacks.connectionFailed = [this](DisconnectInfo info)
            {
                bool notify = false;

                {
                    std::lock_guard<std::mutex> lock{ m_connectMutex };

                    // Start가 직접 실패를 반환하는 경로는 공개 통지하지 않는다.
                    notify = m_connectAccepted;
                }

                // 서비스 함수는 Connect 락 밖에서 호출한다.
                if (notify)
                {
                    NotifyConnectFailed(info);
                }
            };

        return callbacks;
    }

    bool IOCPClientImpl::HandleSystemPacket(ClientSession& session, Packet packet)
    {
        const auto view = packet.View();

        switch (view.packetId)
        {
        case ToPacketId(SystemPacketId::HandshakeResponse):
        {
            return HandleHandshakePacket(session, view);
        }

        case ToPacketId(SystemPacketId::HeartbeatRequest):
        case ToPacketId(SystemPacketId::HeartbeatResponse):
        {
            return HandleHeartbeatPacket(session, view);
        }

        default:
        {
            // 지원하지 않거나 클라이언트가 받을 방향이 아닌 패킷이다.
            return false;
        }
        }
    }
}
