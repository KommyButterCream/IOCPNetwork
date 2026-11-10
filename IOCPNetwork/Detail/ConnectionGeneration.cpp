#include "ConnectionGeneration.h"

#include <atomic> // for std::atomic
#include <limits> // for std::numeric_limits

namespace IOCPNetwork::Detail
{
    namespace
    {
        // 마지막으로 발급한 세대다. 엔진 시작·종료 시 초기화하지 않는다.
        std::atomic<std::uint64_t> lastGeneration{ 0 };
    }

    std::uint64_t AllocateConnectionGeneration() noexcept
    {
        std::uint64_t current = lastGeneration.load(std::memory_order_relaxed);

        for (;;)
        {
            if (current == (std::numeric_limits<std::uint64_t>::max)())
            {
                return 0;
            }

            const std::uint64_t next = current + 1;

            // 값이 같아도 실패할 수 있으므로 성공할 때까지 다시 시도한다.
            if (lastGeneration.compare_exchange_weak(current, next,
                std::memory_order_relaxed, std::memory_order_relaxed))
            {
                return next;
            }
        }
    }
}
