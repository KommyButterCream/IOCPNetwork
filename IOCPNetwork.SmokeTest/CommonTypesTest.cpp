#include <IOCPNetwork/Detail/OptionsValidation.h>
#include <IOCPNetwork/Detail/PacketHeader.h>

#include <array> // for std::array
#include <chrono> // for std::chrono::milliseconds
#include <cstddef> // for std::byte, std::size_t
#include <cstdint> // for std::uint16_t, std::uint32_t
#include <initializer_list> // for braced range-for lists
#include <iostream> // for std::cout, std::cerr
#include <limits> // for std::numeric_limits
#include <span> // for std::span

namespace
{
    using namespace IOCPNetwork;
    using namespace IOCPNetwork::Detail;

    // Release에서도 모든 검사를 실행하고 실패한 항목을 출력한다.
    class TestContext
    {
    public:
        void Check(bool passed, const char* name)
        {
            ++m_checkCount;

            if (!passed)
            {
                ++m_failureCount;

                std::cerr << "FAIL: " << name << '\n';
            }
        }

        [[nodiscard]] int Report() const
        {
            std::cout << (m_failureCount == 0 ? "PASS: " : "FAIL: ")
                << "Common types: " << m_checkCount << " checks, "
                << m_failureCount << " failures.\n";

            return m_failureCount;
        }

    private:
        int m_checkCount = 0;
        int m_failureCount = 0;
    };

    void TestOptions(TestContext& test)
    {
        test.Check(ValidateOptions(NetworkOptions{}) == OptionsError::None,
            "Default common options");

        // 독립된 정상 설정에서 한 필드만 변경해 오류 원인을 분리한다.
        const auto checkSize = [&test](
            std::size_t NetworkOptions::* member,
            std::size_t value,
            OptionsError expected,
            const char* name)
        {
            NetworkOptions options;

            options.*member = value;

            test.Check(ValidateOptions(options) == expected, name);
        };

        for (const auto member :
            { &NetworkOptions::maxRecvPacketSize, &NetworkOptions::maxSendPacketSize })
        {
            checkSize(member, PacketHeaderSize - 1,
                OptionsError::InvalidPacketSize, "Packet smaller than header");

            checkSize(member, MaxPacketSize + 1,
                OptionsError::InvalidPacketSize, "Packet exceeds wire range");
        }

        checkSize(&NetworkOptions::recvRingSize, 0,
            OptionsError::InvalidRecvRing, "Zero receive ring");

        checkSize(&NetworkOptions::recvRingSize, 128 * 1024 - 1,
            OptionsError::InvalidRecvRing, "Non power-of-two ring");

        checkSize(&NetworkOptions::recvRingSize, 64 * 1024,
            OptionsError::InvalidRecvRing, "Receive ring too small");

        checkSize(&NetworkOptions::maxPendingJobs, 0,
            OptionsError::InvalidJobLimit, "Zero job capacity");

        checkSize(&NetworkOptions::maxPendingJobs,
            std::numeric_limits<std::size_t>::max(),
            OptionsError::InvalidJobLimit, "Reserved job slot overflow");

        checkSize(&NetworkOptions::maxPendingPacketBytes, MaxPacketSize - 1,
            OptionsError::InvalidJobLimit, "Insufficient packet job budget");

        checkSize(&NetworkOptions::maxPendingSendPackets, 0,
            OptionsError::InvalidSendLimit, "Zero send capacity");

        checkSize(&NetworkOptions::maxPendingSendBytes, MaxPacketSize - 1,
            OptionsError::InvalidSendLimit, "Insufficient send budget");

        checkSize(&NetworkOptions::maxJobsPerBatch, 0,
            OptionsError::InvalidBatch, "Zero job batch");

        checkSize(&NetworkOptions::maxSendBatchBytes, MaxPacketSize - 1,
            OptionsError::InvalidBatch, "Insufficient send batch bytes");

        checkSize(&NetworkOptions::maxSendBatchBytes,
            static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 1,
            OptionsError::InvalidBatch, "Send completion byte range overflow");

        for (const int milliseconds : { 0, -1 })
        {
            NetworkOptions options;

            options.jobBatchTime = std::chrono::milliseconds{ milliseconds };

            test.Check(ValidateOptions(options) == OptionsError::InvalidBatch,
                "Nonpositive job time budget");
        }

        NetworkOptions options;

        options.maxSendBatchPackets = 0;

        test.Check(ValidateOptions(options) == OptionsError::InvalidBatch,
            "Zero send batch count");

        // 최소 경계는 포함한다. 기본 워커 수 0은 자동 선택이므로 유효하다.
        NetworkOptions minimum;

        minimum.maxRecvPacketSize = PacketHeaderSize;
        minimum.maxSendPacketSize = PacketHeaderSize;
        minimum.recvRingSize = PacketHeaderSize * 2;
        minimum.maxPendingJobs = 1;
        minimum.maxPendingPacketBytes = PacketHeaderSize;
        minimum.maxPendingSendPackets = 1;
        minimum.maxPendingSendBytes = PacketHeaderSize;
        minimum.maxJobsPerBatch = 1;
        minimum.jobBatchTime = std::chrono::milliseconds{ 1 };
        minimum.maxSendBatchPackets = 1;
        minimum.maxSendBatchBytes = PacketHeaderSize;

        test.Check(ValidateOptions(minimum) == OptionsError::None,
            "Minimum valid capacities");

        ServerOptions server;

        server.port = 9000;
        server.maxConnections = 1;

        test.Check(ValidateOptions(server) == OptionsError::None, "Valid server");

        server.bindAddress.clear();

        test.Check(ValidateOptions(server) == OptionsError::InvalidAddress,
            "Empty bind address");

        server.bindAddress = "0.0.0.0";
        server.port = 0;

        test.Check(ValidateOptions(server) == OptionsError::InvalidPort,
            "Unset server port");

        server.port = 9000;
        server.maxConnections = 0;

        test.Check(ValidateOptions(server) == OptionsError::InvalidConnectionCount,
            "Zero connections");

        server.maxConnections = 1;
        server.network.maxPendingJobs = 0;

        test.Check(ValidateOptions(server) == OptionsError::InvalidJobLimit,
            "Server common options validation");

        ClientOptions client;

        client.serverAddress = "127.0.0.1";
        client.port = 9000;

        test.Check(ValidateOptions(client) == OptionsError::None, "Valid client");

        client.serverAddress.clear();

        test.Check(ValidateOptions(client) == OptionsError::InvalidAddress,
            "Empty server address");

        client.serverAddress = "127.0.0.1";
        client.port = 0;

        test.Check(ValidateOptions(client) == OptionsError::InvalidPort,
            "Unset client port");

        client.port = 9000;
        client.network.maxPendingJobs = 0;

        test.Check(ValidateOptions(client) == OptionsError::InvalidJobLimit,
            "Client common options validation");
    }

    void TestPacketHeader(TestContext& test)
    {
        // 왕복 변환만으로 놓칠 수 있는 바이트 순서 오류를 정답 바이트로 검사한다.
        const std::array<std::byte, 4> expected
        {
            std::byte{ 0x34 }, std::byte{ 0x12 },
            std::byte{ 0x78 }, std::byte{ 0x56 }
        };

        std::array<std::byte, 6> bytes;

        bytes.fill(std::byte{ 0xCC });

        test.Check(WritePacketHeader(bytes, { 0x1234, 0x5678 }), "Write header");

        for (std::size_t index = 0; index < expected.size(); ++index)
        {
            test.Check(bytes[index] == expected[index], "Little endian byte");
        }

        test.Check(bytes[4] == std::byte{ 0xCC } && bytes[5] == std::byte{ 0xCC },
            "Preserve following bytes");

        const auto header = ReadPacketHeader(expected);

        test.Check(header.has_value() &&
            header->packetId == 0x1234 && header->packetSize == 0x5678,
            "Read known wire bytes");

        for (std::size_t size = 0; size < PacketHeaderSize; ++size)
        {
            std::array<std::byte, 4> shortBuffer;

            shortBuffer.fill(std::byte{ 0xCC });

            const auto before = shortBuffer;

            test.Check(!ReadPacketHeader(
                std::span<const std::byte>{ shortBuffer }.first(size)).has_value(),
                "Incomplete header read");

            test.Check(!WritePacketHeader(
                std::span<std::byte>{ shortBuffer }.first(size), { 1000, 4 }),
                "Short header write");

            test.Check(shortBuffer == before, "Failed write preserves bytes");
        }

        test.Check(!ReadPacketHeader(std::span<const std::byte>{}).has_value(),
            "Empty null span read");

        test.Check(!WritePacketHeader(std::span<std::byte>{}, { 1000, 4 }),
            "Empty null span write");

        std::array<std::byte, 4> maximumBytes{};

        test.Check(WritePacketHeader(maximumBytes,
            { LastServicePacketId, static_cast<std::uint16_t>(MaxPacketSize) }),
            "Write maximum header");

        for (const auto byte : maximumBytes)
        {
            test.Check(byte == std::byte{ 0xFF }, "Maximum wire byte");
        }

        const auto maximum = ReadPacketHeader(maximumBytes);

        test.Check(maximum.has_value() &&
            maximum->packetId == LastServicePacketId &&
            maximum->packetSize == MaxPacketSize, "Read maximum header");
    }

    void TestPacketBoundaries(TestContext& test)
    {
        test.Check(!IsSystemPacketId(0) && !IsServicePacketId(0), "Reserved ID zero");

        test.Check(IsSystemPacketId(1) && IsSystemPacketId(999) &&
            !IsSystemPacketId(1000), "System ID boundaries");

        test.Check(!IsServicePacketId(999) && IsServicePacketId(1000) &&
            IsServicePacketId(65535), "Service ID boundaries");

        test.Check(CheckPacketFrame({ 0, 4 }, 4, MaxPacketSize) ==
            PacketFrameStatus::InvalidPacketId, "Reject ID zero");

        for (std::uint16_t size = 0; size < PacketHeaderSize; ++size)
        {
            test.Check(CheckPacketFrame({ 1000, size }, 4, MaxPacketSize) ==
                PacketFrameStatus::InvalidPacketSize, "Length below header");
        }

        test.Check(CheckPacketFrame({ 1000, 4 }, 4, MaxPacketSize) ==
            PacketFrameStatus::Complete, "Empty payload");

        test.Check(CheckPacketFrame({ 1000, 8 }, 8, 8) ==
            PacketFrameStatus::Complete, "Exact receive limit");

        test.Check(CheckPacketFrame({ 1000, 9 }, 9, 8) ==
            PacketFrameStatus::InvalidPacketSize, "Above receive limit");

        test.Check(CheckPacketFrame({ 1000, 4 }, 4, PacketHeaderSize - 1) ==
            PacketFrameStatus::InvalidReceiveLimit, "Receive limit too small");

        test.Check(CheckPacketFrame({ 1000, 4 }, 4, MaxPacketSize + 1) ==
            PacketFrameStatus::InvalidReceiveLimit, "Receive limit too large");

        const PacketHeader maximum
        {
            LastServicePacketId, static_cast<std::uint16_t>(MaxPacketSize)
        };

        test.Check(CheckPacketFrame(maximum, MaxPacketSize - 1, MaxPacketSize) ==
            PacketFrameStatus::NeedMoreData, "Maximum packet missing last byte");

        test.Check(CheckPacketFrame(maximum, MaxPacketSize, MaxPacketSize) ==
            PacketFrameStatus::Complete, "Maximum packet complete");
    }

    void TestPartialReceive(TestContext& test)
    {
        const std::array<std::byte, 8> packet
        {
            std::byte{ 0xE8 }, std::byte{ 0x03 },
            std::byte{ 0x08 }, std::byte{ 0x00 },
            std::byte{ 0x11 }, std::byte{ 0x22 },
            std::byte{ 0x33 }, std::byte{ 0x44 }
        };

        // 수신된 길이를 한 바이트씩 늘려 헤더와 본문의 분할 도착을 표현한다.
        for (std::size_t received = 0; received <= packet.size(); ++received)
        {
            const auto available = std::span<const std::byte>{ packet }.first(received);

            const auto header = ReadPacketHeader(available);

            if (received < PacketHeaderSize)
            {
                test.Check(!header.has_value(), "Fragmented header");

                continue;
            }

            test.Check(header.has_value(), "Header before full payload");

            if (!header.has_value())
            {
                continue;
            }

            const auto expected = received < packet.size()
                ? PacketFrameStatus::NeedMoreData : PacketFrameStatus::Complete;

            test.Check(CheckPacketFrame(*header, available.size(), MaxPacketSize) ==
                expected, "Fragmented payload");
        }

        // 첫 패킷은 6바이트, 두 번째는 본문 없는 4바이트 패킷이다.
        const std::array<std::byte, 10> stream
        {
            std::byte{ 0xE8 }, std::byte{ 0x03 },
            std::byte{ 0x06 }, std::byte{ 0x00 },
            std::byte{ 0xAA }, std::byte{ 0xBB },
            std::byte{ 0xE9 }, std::byte{ 0x03 },
            std::byte{ 0x04 }, std::byte{ 0x00 }
        };

        const std::span<const std::byte> available{ stream };

        const auto first = ReadPacketHeader(available);

        test.Check(first.has_value(), "First coalesced header");

        if (!first.has_value())
        {
            return;
        }

        const auto firstStatus = CheckPacketFrame(*first, available.size(), MaxPacketSize);

        test.Check(firstStatus == PacketFrameStatus::Complete && first->packetSize == 6,
            "First frame consumes only its declared size");

        if (firstStatus != PacketFrameStatus::Complete || first->packetSize != 6)
        {
            return;
        }

        const auto remaining = available.subspan(first->packetSize);

        const auto second = ReadPacketHeader(remaining);

        test.Check(second.has_value() && second->packetId == 1001 &&
            second->packetSize == 4, "Second coalesced header");

        if (!second.has_value())
        {
            return;
        }

        test.Check(CheckPacketFrame(*second, remaining.size(), MaxPacketSize) ==
            PacketFrameStatus::Complete, "Second coalesced frame");

        const auto partialStream = available.first(8);

        test.Check(CheckPacketFrame(*first, partialStream.size(), MaxPacketSize) ==
            PacketFrameStatus::Complete, "Complete first frame with partial next header");

        test.Check(!ReadPacketHeader(
            partialStream.subspan(first->packetSize)).has_value(), "Partial next header");
    }
}

// 공개 DLL 호출 테스트와 같은 실행 파일에서 내부 값 타입 검사를 실행한다.
int RunCommonTypesTests()
{
    TestContext test;

    TestOptions(test);

    TestPacketHeader(test);

    TestPacketBoundaries(test);

    TestPartialReceive(test);

    return test.Report();
}
