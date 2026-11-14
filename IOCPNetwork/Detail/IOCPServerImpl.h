#pragma once

#include "IOCPBaseImpl.h"
#include "SocketHandle.h"
#include "AcceptSession.h"

#include <WinSock2.h> // for SOCKET
#include <MSWSock.h>  // for LPFN_ACCEPTEX, LPFN_GETACCEPTEXSOCKADDRS
#include <cstddef>  // for std::size_t
#include <memory>  // for std::unique_ptr
#include <condition_variable> // for std::condition_variable
#include <mutex>              // for std::mutex
#include <thread> // for std::thread

namespace IOCPNetwork::Detail
{
    // 서버의 리슨 소켓과 Accept 처리를 담당한다.
    // 공통 자원과 워커 수명은 IOCPBaseImpl이 관리한다.
    class IOCPServerImpl final : public IOCPBaseImpl
    {
    public:
        IOCPServerImpl() noexcept = default;

        // 내부 멤버가 파괴되기 전에 워커와 모든 연결을 종료한다.
        ~IOCPServerImpl() override;

        // 설정을 검증하고 공통 자원·Accept 처리를 시작한다.
        // 역할별 준비 실패 시 자원 회수와 Stopped 복구까지 수행한다.
        [[nodiscard]] StartResult Start(const ServerOptions& options, std::size_t maxTotalSegmentBytes) noexcept;

        // 외부 제어 스레드에서 종료와 자원 회수 완료까지 기다린다.
        [[nodiscard]] StopResult Stop() noexcept;

    private:
        // 공통 자원이 준비된 뒤 서버의 리슨·Accept·재시도를 시작한다.
        // 실패하면 호출자가 공통 시작 실패 복구를 수행한다.
        [[nodiscard]] StartResult StartAccepting(const ServerOptions& options) noexcept;

        // 재시도 합류·소켓 취소·완료 회수를 마친 뒤 Accept 저장소를 해제한다.
        // 일부만 준비된 시작 실패 상태에서도 호출할 수 있다.
        void StopAccepting() noexcept;

        // Accept 생산과 후처리를 끝낸 뒤 모든 연결의 최종 반환을 기다린다.
        void StopTransportAndSessions() noexcept override;

        // 공통 시작 성공 후 호출하여 IPv4 리슨 소켓과 확장 함수를 준비한다.
        // 실패하면 지역 소켓을 정리하며 기존 멤버는 변경하지 않는다.
        [[nodiscard]] StartResult PrepareListener(const ServerOptions& options) noexcept;

        // 고정 Accept 배열을 준비하고 리슨 소켓을 IOCP에 등록한다.
        // 수락용 소켓 생성과 AcceptEx 발행은 PostAccept에서 수행한다.
        [[nodiscard]] StartResult PrepareAcceptSessions() noexcept;

        // 준비된 모든 AcceptSession에 최초 Accept를 발행한다.
        // 일부 실패하면 추가 발행을 막고, 이미 발행된 작업은 종료 경로에서 회수한다.
        [[nodiscard]] StartResult PostInitialAccepts() noexcept;

        // 최초 Accept 발행 성공 후 지연 재시도 스레드를 시작한다.
        [[nodiscard]] StartResult StartAcceptRetryWorker() noexcept;

        // 발행을 차단하고 재시도 스레드를 깨운 뒤 종료까지 합류한다.
        // 시작·종료를 담당하는 외부 제어 경로에서만 호출한다.
        void StopAcceptRetryWorker() noexcept;

        // 일정 간격으로 RetryPending 항목만 재발행한다.
        void AcceptRetryLoop() noexcept;

        // 빈 AcceptSession에 수락용 소켓을 만들고 AcceptEx를 발행한다.
        // 성공은 즉시 완료 또는 완료 대기를 뜻하며, 즉시 실패 시 오류를 반환한다.
        [[nodiscard]] StartResult PostAccept(AcceptSession& session) noexcept;

        // 호출자가 m_acceptMutex를 보유한 상태에서 소켓 생성·발행을 수행한다.
        // 실패하면 RetryPending으로 남기며 즉시 반복 재시도하지 않는다.
        [[nodiscard]] StartResult PostAcceptLocked(AcceptSession& session) noexcept;

        // Accept 성공·실패·취소 완료를 처리한다.
        // 소켓과 세션의 후처리가 끝난 뒤 Accept 카운트를 반환한다.
        void HandleTransportCompletion(const IOCompletion& completion) noexcept override;

        // JobScheduler 에 서비스 작업의 처리 경로를 제공하낟.
        [[nodiscard]] JobCallbacks CreateJobCallbacks() override;

        // 시스템 프로토콜 구현 전에는 수신한 시스템 패킷을 거절한다.
        [[nodiscard]] bool HandleSystemPacket(ClientSession& session, Packet packet) override;

        // 완료 작업의 주소가 일치하는 AcceptSession을 찾는다.
        // Accept 배열의 수명은 해당 완료의 I/O 카운트로 보호한다.
        [[nodiscard]] AcceptSession& FindAcceptSession(const IOOperation& operation) noexcept;

        // 성공한 Accept의 소켓을 ClientSession으로 인계한다.
        // 종료 중이거나 준비에 실패하면 소켓을 닫고 빈 참조를 반환한다.
        [[nodiscard]] SessionReference PrepareAcceptedSession(AcceptSession& session) noexcept;

        // 완료된 Accept를 재등록하거나 종료 상태로 정리한다.
        // 마지막으로 이전 작업의 카운트를 반환한다.
        void CompleteAcceptIO(AcceptSession& session) noexcept;

        // 서버 하나가 동시에 대기시킬 Accept 개수다. 워커별 개수가 아니다.
        static constexpr std::size_t AcceptSessionCount = 16;

        // Accept 발행과 종료 시 발행 차단·소켓 취소를 직렬화한다.
        std::mutex m_acceptMutex;

        // 발행된 Accept의 완료 후처리가 모두 끝날 때까지 기다린다.
        std::condition_variable m_acceptCompleted;

        // 아래 두 값은 m_acceptMutex로 보호한다.
        bool m_acceptPostingEnabled = false;
        std::size_t m_acceptIOCount = 0;

        // 서버 시작부터 역할별 종료 완료까지 소유한다.
        SocketHandle m_listenSocket;

        // 시작 시 한 번 확보하고 모든 Accept 완료 회수 후 해제한다.
        std::unique_ptr<AcceptSession[]> m_acceptSessions;

        // 리슨 소켓의 서비스 제공자에서 얻은 확장 함수다.
        LPFN_ACCEPTEX m_acceptEx = nullptr;
        LPFN_GETACCEPTEXSOCKADDRS m_getAcceptExSockaddrs = nullptr;

        // 재시도 대기를 종료 시 즉시 깨운다. m_acceptMutex와 함께 사용한다.
        std::condition_variable m_acceptRetryWake;

        // Accept 배열을 해제하기 전에 반드시 명시적으로 합류한다.
        std::thread m_acceptRetryThread;
    };
}
