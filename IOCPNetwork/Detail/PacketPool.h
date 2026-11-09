#pragma once

#include "../PacketResult.h"

#include "PacketBlock.h"

#include <array> // for std::array
#include <bit> // for std::bit_ceil, std::countr_zero
#include <cstddef> // for std::byte, std::size_t
#include <cstdint> // for std::uint8_t
#include <mutex> // for std::mutex
#include <optional> // for std::optional, std::nullopt
#include <memory> // for std::shared_ptr, std::enable_shared_from_this
#include <span> // for std::span

namespace IOCPNetwork::Detail
{
    // 크기 등급은 제어부를 제외한 전송 헤더와 본문의 용량
    inline constexpr std::size_t PacketMinClassCapacity = 64;
    inline constexpr std::size_t PacketMaxClassCapacity = 64 * 1024;
    inline constexpr std::size_t PacketSizeClassCount = 11;

    // 제어부의 정렬 조건을 실제 블록 배치에도 적용한다.
    inline constexpr std::size_t PacketBlockAlignment = alignof(PacketBlock);

    // 새 세그먼트 하나의 목표 크기이며 실제 크기는 배치에 맞춰 계산한다.
    inline constexpr std::size_t PacketSegmentTargetSize = 256 * 1024;

    // 64바이트부터 4KiB까지의 7개 등급만 워커 캐시에 보관한다.
    inline constexpr std::size_t PacketCacheSizeClassCount = 7;

    // 워커 하나의 등급 하나가 보관할 수 있는 최대 블록 수
    inline constexpr std::size_t PacketCacheMaxBlocksPerClass = 64;

    // 등급별 빈 블록의 실제 메모리 합계 상한이다.
    inline constexpr std::size_t PacketCacheMaxBytesPerClass = 32 * 1024;

    // 유효한 전체 전송 크기에 맞는 가장 작은 등급을 선택한다.
    [[nodiscard]] constexpr std::optional<std::uint8_t>
        FindPacketSizeClass(std::size_t packetSize) noexcept
    {
        if (packetSize < PacketHeaderSize || packetSize > MaxPacketSize)
        {
            return std::nullopt;
        }

        // 필요한 최소 크기 계산
        const std::size_t requiredCapacity =
            packetSize < PacketMinClassCapacity ? PacketMinClassCapacity : packetSize;

        // 필요한 용량 이상인 가장 작은 2의 거듭제곱을 구한다.
        const std::size_t classCapacity = std::bit_ceil(requiredCapacity);

        // 2의 거듭제곱인 두 용량의 지수 차이로 등급 인덱스를 구한다.
        return static_cast<std::uint8_t>(
            std::countr_zero(classCapacity) - std::countr_zero(PacketMinClassCapacity));
    }

    // 검증된 등급 인덱스에 해당하는 전송 공간의 용량을 반환한다.
    [[nodiscard]] constexpr std::size_t
        GetPacketClassCapacity(std::uint8_t sizeClassIndex) noexcept
    {
        return PacketMinClassCapacity << sizeClassIndex;
    }

    // 같은 세그먼트 안에서 다음 블록까지의 바이트 간격을 반환한다.
    [[nodiscard]] constexpr std::size_t
        GetPacketBlockStride(std::uint8_t sizeClassIndex) noexcept
    {
        // [PacketBlock][헤더][본문]이 들어갈 전체 크기를 계산한다.
        const std::size_t size = sizeof(PacketBlock) + GetPacketClassCapacity(sizeClassIndex);

        return (size + PacketBlockAlignment - 1) & ~(PacketBlockAlignment - 1);
    }

    // 개수와 실제 블록 바이트 한도를 모두 만족하는 캐시 용량을 구한다.
    // 캐시 대상이 아닌 등급은 0을 반환한다.
    [[nodiscard]] constexpr std::size_t GetPacketCacheCapacity(std::uint8_t sizeClassIndex) noexcept
    {
        if (sizeClassIndex >= PacketCacheSizeClassCount)
        {
            return 0;
        }

        const std::size_t byteLimitedCount = PacketCacheMaxBytesPerClass / GetPacketBlockStride(sizeClassIndex);

        return byteLimitedCount < PacketCacheMaxBlocksPerClass ? byteLimitedCount : PacketCacheMaxBlocksPerClass;
    }

    class PacketPoolCache;

    // 패킷 블록의 저장소와 대여·반환을 담당한다.
    class PacketPool : public std::enable_shared_from_this<PacketPool>
    {
    public:
        // 모든 등급의 세그먼트 메모리 합계 상한을 지정한다. 생성 실패 시 빈 포인터를 반환한다.
        [[nodiscard]] static std::shared_ptr<PacketPool> Create(std::size_t maxTotalSegmentBytes) noexcept;

        // 모든 Packet과 캐시의 소유 참조가 사라진 뒤 저장소를 해제한다.
        ~PacketPool();

        PacketPool(const PacketPool&) = delete;
        PacketPool& operator=(const PacketPool&) = delete;

        PacketPool(PacketPool&&) = delete;
        PacketPool& operator=(PacketPool&&) = delete;

        // 요청한 본문 용량을 직접 작성할 수 있는 Writer를 만든다.
        [[nodiscard]] PacketWriterResult CreatePacketWriter(PacketId packetId, std::size_t payloadCapacity);

        // 기존 본문 바이트를 새 저장소에 복사해 완성된 Packet을 만든다.
        [[nodiscard]] PacketResult CreatePacket(PacketId packetId, std::span<const std::byte> payload);

    private:
        friend class PacketPoolCache;
        friend void ReleasePacketReference(PacketBlock* block) noexcept;

        // 비어 있는 블록의 앞부분에만 배치하는 연결 정보다.
        struct FreeBlock
        {
            FreeBlock* next = nullptr;
        };

        // 같은 크기 등급의 빈 블록을 연결한 전달용 묶음이다.
        // 자체적으로 블록을 반환하지 않으므로 수신 측이 인수해야 한다.
        struct BlockBatch
        {
            FreeBlock* first = nullptr;
            FreeBlock* last = nullptr;

            std::size_t count = 0;
        };

        // 확보한 메모리 영역의 시작에 배치하는 관리 정보이다.
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4324) // 뒤따르는 블록의 정렬을 위해 끝 패딩을 허용한다.
#endif
        struct alignas(PacketBlockAlignment) Segment
        {
            Segment* next = nullptr;
        };
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

        // 한 크기 등급의 공용 빈 블록과 확보 영역을 관리한다.
        struct SizeClassStorage
        {
            std::mutex mutex;

            FreeBlock* freeBlocks = nullptr;
            Segment* segments = nullptr;
        };

        explicit PacketPool(std::size_t maxTotalSegmentBytes) noexcept;

        // 현재 스레드의 같은 풀 캐시를 우선 사용한다. 확보 실패 시 nullptr를 반환한다.
        [[nodiscard]] void* AcquireBlock(std::uint8_t sizeClassIndex);

        // 최대 요청 개수까지 대여한다. 실제 개수는 요청보다 작을 수 있다.
        [[nodiscard]] BlockBatch AcquireBatch(std::uint8_t sizeClassIndex, std::size_t maxCount);

        // 호출자가 해당 등급의 락을 잡은 상태에서 빈 블록을 확충한다.
        [[nodiscard]] bool AddSegment(std::uint8_t sizeClassIndex);

        // 제어부가 소멸한 메모리를 같은 풀의 캐시 또는 공용 목록으로 반환한다.
        void ReturnBlock(void* memory, std::uint8_t sizeClassIndex) noexcept;

        // 빈 블록 묶음을 공용 목록에 연결하고 전달받은 묶음을 비운다.
        void ReturnBatch(std::uint8_t sizeClassIndex, BlockBatch& batch) noexcept;

        std::array<SizeClassStorage, PacketSizeClassCount> m_sizeClasses;

        // 모든 등급이 공유하는 세그먼트 확보 예산이다.
        const std::size_t m_maxTotalSegmentBytes;

        std::mutex m_budgetMutex;

        // m_budgetMutex를 잡은 상태에서만 읽거나 변경한다.
        std::size_t m_allocatedSegmentBytes = 0;
    };

    // 워커 하나가 같은 스레드에서 사용하는 빈 블록 캐시다.
    // 캐시가 보관한 메모리의 원래 풀을 공유 소유한다.
    class PacketPoolCache
    {
    public:
        // 생성한 스레드의 TLS에 등록한다. 빈 풀과 중복 등록은 예외로 거절한다.
        explicit PacketPoolCache(std::shared_ptr<PacketPool> pool);

        // 같은 스레드에서 TLS를 해제하고 남은 빈 블록을 공용 풀로 반환한다.
        ~PacketPoolCache();

        PacketPoolCache(const PacketPoolCache&) = delete;
        PacketPoolCache& operator=(const PacketPoolCache&) = delete;

        PacketPoolCache(PacketPoolCache&&) = delete;
        PacketPoolCache& operator=(PacketPoolCache&&) = delete;

    private:
        friend class PacketPool;

        // 해당 워커만 접근하므로 락이나 atomic 없이 관리한다.
        struct SizeClassCache
        {
            PacketPool::FreeBlock* freeBlocks = nullptr;
            std::size_t count = 0;
        };

        // 캐시에서 하나를 대여하고 비어 있으면 공용 풀에서 묶음으로 충전한다.
        [[nodiscard]] void* AcquireBlock(std::uint8_t sizeClassIndex);

        // 반환할 블록을 캐시에 보관하고 상한에 도달했으면 먼저 일부를 반환한다.
        void ReturnBlock(void* memory, std::uint8_t sizeClassIndex) noexcept;

        // 캐시의 앞쪽 블록을 최대 요청 개수까지 공용 풀로 돌려준다.
        void FlushBlocks(std::uint8_t sizeClassIndex, std::size_t maxCount) noexcept;

        // 빈 블록을 모두 반환할 때까지 원래 풀을 유지한다.
        std::shared_ptr<PacketPool> m_pool;

        std::array<SizeClassCache, PacketCacheSizeClassCount> m_sizeClasses;
    };
}
