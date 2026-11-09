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
        const std::size_t previousCount = block->referenceCount.fetch_sub(1, std::memory_order_acq_rel);

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
