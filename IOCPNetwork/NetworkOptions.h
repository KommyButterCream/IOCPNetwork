#pragma once

#include "PacketTypes.h"

#include <chrono> // for std::chrono::milliseconds
#include <cstddef> // for std::size_t
#include <cstdint> // for std::uint16_t, std::uint32_t
#include <string> // for std::string

namespace IOCPNetwork
{
    // 서버·클라이언트가 공유하는 용량과 실행 설정이다.
    struct NetworkOptions
    {
        // 0이면 엔진 역할과 논리 CPU 수를 기준으로 자동 결정한다.
        std::uint32_t ioWorkerCount = 0;
        std::uint32_t jobWorkerCount = 0;

        // 패킷 크기는 공통 헤더를 포함한다.
        std::size_t maxRecvPacketSize = MaxPacketSize;
        std::size_t maxSendPacketSize = MaxPacketSize;
        std::size_t recvRingSize = 128 * 1024;

        // Job 한도는 큐·인출 배치·실행 중인 일반 작업을 합산한다.
        std::size_t maxPendingJobs = 256;
        std::size_t maxPendingPacketBytes = 1024 * 1024;

        // 송신 한도는 대기 큐와 진행 중인 배치를 합산한다.
        std::size_t maxPendingSendPackets = 256;
        std::size_t maxPendingSendBytes = 1024 * 1024;

        // 같은 세션이 한 차례에 처리할 Job의 상한이다.
        std::size_t maxJobsPerBatch = 32;
        std::chrono::milliseconds jobBatchTime{ 2 };

        // 한 번의 WSASend에 묶을 패킷 수와 바이트 상한이다.
        std::uint32_t maxSendBatchPackets = 32;
        std::size_t maxSendBatchBytes = 256 * 1024;
    };

    // 서버의 수신 주소와 최대 연결 수를 지정한다.
    struct ServerOptions
    {
        NetworkOptions network;

        std::string bindAddress = "0.0.0.0";
        std::uint16_t port = 0;

        // 서비스가 필요한 최대 연결 수를 명시한다.
        std::uint32_t maxConnections = 0;
    };

    // 클라이언트가 연결할 서버 주소를 지정한다.
    struct ClientOptions
    {
        NetworkOptions network;

        std::string serverAddress;
        std::uint16_t port = 0;
    };
}
