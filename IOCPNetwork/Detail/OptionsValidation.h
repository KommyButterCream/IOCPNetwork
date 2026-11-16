#pragma once

#include "../NetworkOptions.h"

#include <bit> // for std::has_single_bit
#include <chrono> // for std::chrono::milliseconds, std::chrono::hours
#include <cstddef> // for std::size_t
#include <cstdint> // for std::uint32_t
#include <limits> // for std::numeric_limits

namespace IOCPNetwork::Detail
{
    // 설정 검사에서 처음 발견한 오류를 나타낸다.
    enum class OptionsError
    {
        None,
        InvalidAddress,
        InvalidPort,
        InvalidConnectionCount,
        InvalidPacketSize,
        InvalidRecvRing,
        InvalidJobLimit,
        InvalidSendLimit,
        InvalidBatch,
        InvalidTimeout
    };

    // 공통 설정의 범위와 항목 사이의 제약을 검사한다.
    [[nodiscard]] inline OptionsError ValidateOptions(const NetworkOptions& options) noexcept
    {
        if (options.maxRecvPacketSize < PacketHeaderSize ||
            options.maxRecvPacketSize > MaxPacketSize ||
            options.maxSendPacketSize < PacketHeaderSize ||
            options.maxSendPacketSize > MaxPacketSize)
        {
            return OptionsError::InvalidPacketSize;
        }

        // 수신 링은 2의 거듭제곱 크기이며 최대 수신 패킷의 두 배 이상이어야 한다.
        if (!std::has_single_bit(options.recvRingSize) ||
            options.recvRingSize < options.maxRecvPacketSize * 2)
        {
            return OptionsError::InvalidRecvRing;
        }

        if (options.maxPendingJobs == 0 ||
            options.maxPendingJobs == (std::numeric_limits<std::size_t>::max)() ||
            options.maxPendingPacketBytes < options.maxRecvPacketSize)
        {
            return OptionsError::InvalidJobLimit;
        }

        if (options.maxPendingSendPackets == 0 ||
            options.maxPendingSendBytes < options.maxSendPacketSize)
        {
            return OptionsError::InvalidSendLimit;
        }

        if (options.maxJobsPerBatch == 0 ||
            options.jobBatchTime <= std::chrono::milliseconds::zero() ||
            options.maxSendBatchPackets == 0 ||
            options.maxSendBatchBytes < options.maxSendPacketSize ||
            options.maxSendBatchBytes >
            (std::numeric_limits<std::uint32_t>::max)())
        {
            return OptionsError::InvalidBatch;
        }

        // 이번 시간 설정은 0으로 비활성화하지 않으며, 최대 하루까지 허용한다.
        const auto isValidDuration =
            [](std::chrono::milliseconds duration) noexcept
            {
                return duration > std::chrono::milliseconds::zero() &&
                    duration <= std::chrono::hours{ 24 };
            };

        if (!isValidDuration(options.maintenanceInterval) ||
            !isValidDuration(options.handshakeTimeout) ||
            !isValidDuration(options.heartbeatInterval) ||
            !isValidDuration(options.heartbeatTimeout) ||
            !isValidDuration(options.receiveTimeout) ||
            !isValidDuration(options.backpressureTimeout) ||
            !isValidDuration(options.shutdownTimeout))
        {
            return OptionsError::InvalidTimeout;
        }

        // 검사 주기가 관리 대상의 간격이나 제한 시간보다 길면 안 된다.
        if (options.maintenanceInterval > options.handshakeTimeout ||
            options.maintenanceInterval > options.heartbeatInterval ||
            options.maintenanceInterval > options.heartbeatTimeout ||
            options.maintenanceInterval > options.receiveTimeout ||
            options.maintenanceInterval > options.backpressureTimeout)
        {
            return OptionsError::InvalidTimeout;
        }

        // 정상적인 하트비트 요청·응답을 기다릴 시간은 확보한다.
        if (options.receiveTimeout < options.heartbeatInterval + options.heartbeatTimeout)
        {
            return OptionsError::InvalidTimeout;
        }

        return OptionsError::None;
    }

    // 서버 전용 설정을 검사한 뒤 공통 설정을 검사한다.
    [[nodiscard]] inline OptionsError ValidateOptions(const ServerOptions& options) noexcept
    {
        if (options.bindAddress.empty())
        {
            return OptionsError::InvalidAddress;
        }

        if (options.port == 0)
        {
            return OptionsError::InvalidPort;
        }

        if (options.maxConnections == 0)
        {
            return OptionsError::InvalidConnectionCount;
        }

        return ValidateOptions(options.network);
    }

    // 클라이언트 전용 설정을 검사한 뒤 공통 설정을 검사한다.
    [[nodiscard]] inline OptionsError ValidateOptions(const ClientOptions& options) noexcept
    {
        if (options.serverAddress.empty())
        {
            return OptionsError::InvalidAddress;
        }

        if (options.port == 0)
        {
            return OptionsError::InvalidPort;
        }

        return ValidateOptions(options.network);
    }
}
