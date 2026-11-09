#pragma once

#include "../NetworkOptions.h"

#include <bit> // for std::has_single_bit
#include <chrono> // for std::chrono::milliseconds
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
        InvalidBatch
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

        // std::has_single_bit : 2의 제곱 수인지 (power of 2) 확인
        if (!std::has_single_bit(options.recvRingSize) ||
            options.recvRingSize < options.maxRecvPacketSize * 2)
        {
            return OptionsError::InvalidRecvRing;
        }

        if (options.maxPendingJobs == 0 ||
            options.maxPendingJobs == std::numeric_limits<std::size_t>::max() ||
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
            std::numeric_limits<std::uint32_t>::max())
        {
            return OptionsError::InvalidBatch;
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
