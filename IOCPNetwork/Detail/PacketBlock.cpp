#include "PacketBlock.h"

#include "PacketPool.h"

#include <atomic> // for std::memory_order_relaxed, std::memory_order_acq_rel
#include <cstddef> // for std::size_t
#include <cstdint> // for std::uint8_t
#include <exception> // for std::terminate
#include <limits> // for std::numeric_limits
#include <memory> // for std::destroy_at
#include <utility> // for std::move

namespace IOCPNetwork::Detail
{
    static_assert(!FindPacketSizeClass(0).has_value());

    static_assert(!FindPacketSizeClass(MaxPacketSize + 1).has_value());

    static_assert(FindPacketSizeClass(4).value() == 0);

    static_assert(FindPacketSizeClass(64).value() == 0);

    static_assert(FindPacketSizeClass(65).value() == 1);

    static_assert(FindPacketSizeClass(128).value() == 1);

    static_assert(FindPacketSizeClass(129).value() == 2);

    static_assert(FindPacketSizeClass(MaxPacketSize).value() == 10);

    static_assert(GetPacketClassCapacity(10) == PacketMaxClassCapacity);

    static_assert(GetPacketBlockStride(0) >= sizeof(PacketBlock) + PacketMinClassCapacity);

    static_assert(GetPacketBlockStride(10) % PacketBlockAlignment == 0);

    static_assert(GetPacketBlockStride(0) < sizeof(PacketBlock) + PacketMinClassCapacity + PacketBlockAlignment);

    static_assert(GetPacketBlockStride(10) < sizeof(PacketBlock) + PacketMaxClassCapacity + PacketBlockAlignment);

    static_assert(GetPacketClassCapacity(static_cast<std::uint8_t>(PacketCacheSizeClassCount - 1)) == 4 * 1024);

    static_assert(GetPacketCacheCapacity(7) == 0);

    static_assert(GetPacketCacheCapacity(10) == 0);

    static_assert([]() constexpr
        {
            for (std::size_t index = 0; index < PacketCacheSizeClassCount; ++index)
            {
                const auto sizeClassIndex = static_cast<std::uint8_t>(index);

                const std::size_t capacity = GetPacketCacheCapacity(sizeClassIndex);

                if (capacity == 0 || capacity > PacketCacheMaxBlocksPerClass)
                {
                    return false;
                }

                if (capacity * GetPacketBlockStride(sizeClassIndex) > PacketCacheMaxBytesPerClass)
                {
                    return false;
                }
            }

            return true;
        }());

    void AddPacketReference(PacketBlock* block) noexcept
    {
        std::size_t count = block->referenceCount.load(std::memory_order_relaxed);

        for (;;)
        {
            // 종료된 참조의 부활과 카운터 넘침은 내부 계약 위반이다.
            if (count == 0 ||
                count == std::numeric_limits<std::size_t>::max())
            {
                std::terminate();
            }

            // CAS 를 통해 안전한 증가 시도
            if (block->referenceCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
            {
                return;
            }
        }
    }

    void ReleasePacketReference(PacketBlock* block) noexcept
    {
        const std::size_t previousCount =
            block->referenceCount.fetch_sub(1, std::memory_order_acq_rel);

        if (previousCount != 1)
        {
            return;
        }

        // 제어부를 소멸해도 반환 대상 풀이 살아 있도록 지역 참조로 옮긴다.
        auto pool = std::move(block->pool);

        void* memory = static_cast<void*>(block);

        const std::uint8_t sizeClassIndex = block->sizeClassIndex;

        // 제어부의 객체 수명을 끝낸 뒤 메모리를 재사용 가능 상태로 돌린다.
        std::destroy_at(block);

        pool->ReturnBlock(memory, sizeClassIndex);

        // 지역 pool이 소멸하면서 풀 소유 참조를 해제한다. 마지막 참조이면 풀도 소멸한다.
    }
}
