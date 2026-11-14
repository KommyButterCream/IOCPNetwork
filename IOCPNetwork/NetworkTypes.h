#pragma once

#include <cstdint> // for std::uint32_t

namespace IOCPNetwork
{
    // 엔진 시작 요청이 실패한 원인을 구분한다.
    enum class StartError
    {
        None,
        InvalidOptions,                 // 설정값이 유효하지 않음
        AlreadyRunning,                 // 이미 실행 중
        Busy,                           // 시작 또는 종료가 진행 중
        NoMemory,                       // 메모리 확보 실패
        WinsockInitializationFailed,    // Winsock 초기화 실패
        ResourceInitializationFailed,   // IOCP 등 공통 자원 생성 실패
        WorkerStartFailed,              // 워커 시작 실패
        TransportStartFailed            // 서버 리슨·수락 준비 또는 클라이언트 접속 발행 실패
    };

    // 시작 결과와 관련 운영체제 오류를 전달한다.
    // 성공은 엔진 시작을 의미하며, 클라이언트의 연결·핸드셰이크 완료와는 별개다.
    struct [[nodiscard]] StartResult
    {
        StartError error = StartError::None;

        // 관련 운영체제 오류를 확보한 경우에만 설정한다.
        std::uint32_t nativeError = 0;

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return error == StartError::None;
        }
    };

    // 동기식 종료 결과다. 거절된 호출은 종료를 예약하지 않는다.
    enum class StopResult
    {
        Stopped,           // 이번 호출이 종료 책임을 인수하여 종료를 완료했다.
        AlreadyStopped,    // 정지된 상태를 확인했다.
        InvalidCallContext // 엔진 내부 실행 중이어서 동기식 종료를 거절했다.
    };

    // 송신 접수 결과다. Queued는 상대의 수신이나 처리 완료를 의미하지 않는다.
    enum class SendResult
    {
        Queued,         // 송신 책임을 엔진이 인수했다.
        NotReady,       // 서비스 통신 준비가 끝나지 않았다.
        Closed,         // 연결이 종료 중이거나 대상 연결이 유효하지 않다.
        QueueFull,      // 송신 개수 또는 바이트 한도가 부족하다.
        InvalidPacket,  // 빈 Packet·잘못된 ID·길이 등 송신 인자가 유효하지 않다.
        NoMemory        // 패킷 생성 등 접수 준비에 필요한 메모리를 확보하지 못했다.
    };

    // 세션 작업의 접수 결과다. Queued는 작업 실행 완료를 의미하지 않는다.
    enum class PostResult
    {
        Queued,         // 작업 소유권을 엔진이 인수했다.
        NotReady,       // 서비스 통신 준비가 끝나지 않았다.
        Closed,         // 연결이 종료 중이거나 대상 연결이 유효하지 않다.
        QueueFull,      // 작업 한도가 부족하거나 보관 중인 수신 Packet의 복구가 우선이다.
        InvalidTask,    // 전달한 작업이 비어 있다.
        NoMemory        // 엔진 내부 Task 저장소를 확보하지 못했다.
    };

    // 연결 종료를 시작하거나 강제 정리로 전환하게 된 원인이다.
    enum class DisconnectReason
    {
        LocalRequest,        // 서비스가 연결 종료를 요청했다.
        PeerClosed,          // 상대가 송신 방향을 정상 종료했다.
        EngineStopping,      // 엔진 종료로 연결을 정리한다.
        TransportError,      // 소켓 또는 I/O 오류가 발생했다.
        ProtocolError,       // 헤더·ID·시스템 패킷 처리 규약을 위반했다.
        HandshakeTimeout,    // 연결 준비 제한 시간을 초과했다.
        ReceiveTimeout,      // 유효한 패킷을 받지 못한 시간이 제한을 초과했다.
        BackpressureTimeout, // 수신 일시정지 유예 시간을 초과했다.
        ShutdownTimeout,     // 정상 종료의 전체 처리 기한을 초과했다.
        ResourceExhausted,   // 연결 유지에 필요한 메모리 등 자원이 부족하다.
        ServiceException     // 패킷 핸들러·Task·연결 통지에서 예외가 발생했다.
    };

    // 서비스에 전달할 종료 사유와 운영체제 오류 정보를 보관한다.
    struct DisconnectInfo
    {
        DisconnectReason reason = DisconnectReason::LocalRequest;

        // 관련 운영체제 오류가 없으면 0이다.
        std::uint32_t nativeError = 0;
    };
}
