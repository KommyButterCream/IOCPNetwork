#pragma once

#include "IOCPBaseImpl.h"
#include "IOOperation.h"
#include "SessionReference.h"
#include "SocketHandle.h"
#include <WinSock2.h> // for sockaddr_in
#include <MSWSock.h>  // for LPFN_CONNECTEX

#include <condition_variable> // for std::condition_variable
#include <mutex>              // for std::mutex

namespace IOCPNetwork::Detail
{
    // 클라이언트의 비동기 연결과 역할별 종료를 담당한다.
    // 연결 소켓과 송수신 저장소는 ClientSession이 소유한다.
    class IOCPClientImpl final : public IOCPBaseImpl
    {
    public:
        IOCPClientImpl() noexcept = default;

        // 소멸 전에 연결과 워커의 종료를 완료한다.
        ~IOCPClientImpl() override;

        // 공통 자원을 준비하고 비동기 연결 요청을 접수한다.
        [[nodiscard]] StartResult Start(const ClientOptions& options, std::size_t maxTotalSegmentBytes) noexcept;

        // 외부 제어 스레드에서 종료 완료까지 기다린다.
        [[nodiscard]] StopResult Stop() noexcept;

    private:
#if defined(IOCPNETWORK_TESTING)
        friend struct ClientLifecycleTestAccess;
#endif
        // 소켓 준비·세션 인계·Connect 발행을 순서대로 수행한다.
        [[nodiscard]] StartResult StartConnecting(const ClientOptions& options) noexcept;

        // 공통 시작 성공 후 연결 주소·소켓·ConnectEx를 준비한다.
        // 성공할 때만 출력 소켓과 멤버에 결과를 반영한다.
        [[nodiscard]] StartResult PrepareConnectSocket(const ClientOptions& options, SocketHandle& preparedSocket) noexcept;

        // 준비된 소켓을 연결 세션에 인계하고 Connect용 참조를 보관한다.
        // 성공할 때만 소켓을 인수하며, 아직 ConnectEx는 발행하지 않는다.
        [[nodiscard]] StartResult PrepareConnectSession(SocketHandle&& socket) noexcept;

        // 준비한 세션의 참조를 유지한 채 ConnectEx를 한 번 발행한다.
        // 즉시 실패하면 연결을 종료하고 Connect용 참조를 반환한다.
        [[nodiscard]] StartResult PostConnect() noexcept;

        // Connect 성공·실패·취소 완료를 처리한다.
        // 세션 참조 반환까지 마친 뒤 Active를 해제하고 종료 대기자를 깨운다.
        void HandleTransportCompletion(const IOCompletion& completion) noexcept override;

        // 연결 준비 참조를 정리하고 모든 세션의 종료 반환을 기다린다.
        // Connect 완료 후처리까지 끝난 뒤 연결별 저장소를 초기화한다.
        void StopTransportAndSessions() noexcept override;

        // 공통 서비스 핸들러와 연결 이벤트를 JobScheduler에 연결한다.
        [[nodiscard]] JobCallbacks CreateJobCallbacks() override;

        // 클라이언트가 받을 핸드셰이크·하트비트 패킷을 처리한다.
        [[nodiscard]] bool HandleSystemPacket(ClientSession& session, Packet packet) override;

    private:
        // 연결 발행·완료 후처리·종료 요청 사이의 접근을 보호한다.
        std::mutex m_connectMutex;

        // Connect 완료 후처리까지 끝났음을 종료 대기자에게 알린다.
        std::condition_variable m_connectCompleted;

        // 시작 준비 시 확정하고 연결 완료 후처리까지 유지한다.
        sockaddr_in m_remoteAddress{};

        // 연결 소켓의 서비스 제공자에서 확보한 확장 함수다.
        LPFN_CONNECTEX m_connectEx = nullptr;

        // Connect 한 건의 작업 저장소다. 완료 회수 전에는 재사용하지 않는다.
        IOOperation m_connectOperation{ IOOperationType::Connect };

        // 연결 준비부터 Connect 완료 후처리까지 세션의 재대여를 막는다.
        SessionReference m_connectReference;

        // Connect 발행부터 완료 후처리까지 true이며 mutex로 보호한다.
        bool m_connectActive = false;

        // ConnectEx 접수 성공 여부이며, 즉시 발행 실패에서는 false다.
        // m_connectMutex로 보호하고 모든 세션 반환 후 초기화한다.
        bool m_connectAccepted = false;
    };
}
