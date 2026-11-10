#include "PacketPool.h"

#include <algorithm> // for std::min
#include <cstddef> // for std::byte, std::size_t
#include <cstdint> // for std::uint8_t
#include <memory> // for std::shared_ptr, std::construct_at, std::destroy_at
#include <mutex> // for std::lock_guard
#include <new> // for std::align_val_t, std::nothrow, std::bad_alloc, operator new/delete
#include <cstring> // for std::memcpy
#include <span> // for std::span
#include <utility> // for std::move
#include <exception> // for std::terminate
#include <stdexcept> // for std::invalid_argument, std::logic_error

namespace IOCPNetwork::Detail
{
    namespace
    {
        // 현재 스레드에 등록된 캐시의 비소유 주소
        thread_local PacketPoolCache* currentPacketPoolCache = nullptr;
    }

    std::shared_ptr<PacketPool> PacketPool::Create(std::size_t maxTotalSegmentBytes) noexcept
    {
        // 가장 작은 등급의 블록 하나와 세그먼트 관리 정보도 담지 못한다.
        constexpr std::size_t minimumBytes = sizeof(Segment) + GetPacketBlockStride(0);

        if (maxTotalSegmentBytes < minimumBytes)
        {
            return {};
        }

        try
        {
            return std::shared_ptr<PacketPool>(new PacketPool(maxTotalSegmentBytes));
        }
        catch (const std::bad_alloc&)
        {
            return {};
        }
    }

    PacketPool::~PacketPool()
    {
        // 활성 블록과 캐시가 없으므로 다른 스레드가 저장소에 접근하지 않는다.
        for (auto& storage : m_sizeClasses)
        {
            Segment* segment = storage.segments;

            while (segment != nullptr)
            {
                Segment* next = segment->next;

                std::destroy_at(segment);

                ::operator delete(
                    static_cast<void*>(segment),
                    std::align_val_t{ PacketBlockAlignment });

                segment = next;
            }
        }
    }

    PacketWriterResult PacketPool::CreatePacketWriter(PacketId packetId, std::size_t payloadCapacity)
    {
        if (!IsSystemPacketId(packetId) &&
            !IsServicePacketId(packetId))
        {
            return PacketWriterResult
            {
                PacketWriter{},
                PacketError::InvalidPacketId
            };
        }

        // 헤더 크기를 더하기 전에 본문 용량의 상한을 검사한다.
        if (payloadCapacity > MaxPayloadSize)
        {
            return PacketWriterResult
            {
                PacketWriter{},
                PacketError::PacketTooLarge
            };
        }

        const auto sizeClassIndex = FindPacketSizeClass(PacketHeaderSize + payloadCapacity);

        if (!sizeClassIndex.has_value())
        {
            return PacketWriterResult
            {
                PacketWriter{},
                PacketError::PacketTooLarge
            };
        }

        // 블록 대여 전에 기존 풀의 공유 소유권을 확보한다.
        auto pool = shared_from_this();

        void* memory = AcquireBlock(*sizeClassIndex);

        if (memory == nullptr)
        {
            return PacketWriterResult
            {
                PacketWriter{},
                PacketError::NoMemory
            };
        }

        // 빈 메모리에 제어부를 생성한다. 본문 전체를 초기화하지는 않는다.
        PacketBlock* block = std::construct_at(static_cast<PacketBlock*>(memory));

        block->pool = std::move(pool);
        block->payloadCapacity = payloadCapacity;
        block->packetId = packetId;
        block->sizeClassIndex = *sizeClassIndex;

        return PacketWriterResult
        {
            PacketWriter{ block },
            PacketError::None
        };
    }

    PacketResult PacketPool::CreatePacket(PacketId packetId, std::span<const std::byte> payload)
    {
        auto writerResult = CreatePacketWriter(packetId, payload.size());

        if (!writerResult)
        {
            return PacketResult
            {
                Packet{},
                writerResult.error
            };
        }

        const auto destination = writerResult.writer.GetWritablePayload();

        // 빈 본문도 유효하다. 크기가 0이면 메모리 복사를 호출하지 않는다.
        if (!payload.empty())
        {
            std::memcpy(
                destination.data(),
                payload.data(),
                payload.size());
        }

        return writerResult.writer.Finish(payload.size());
    }

    PacketPool::PacketPool(std::size_t maxTotalSegmentBytes) noexcept
        : m_maxTotalSegmentBytes(maxTotalSegmentBytes)
    {
    }

    void* PacketPool::AcquireBlock(std::uint8_t sizeClassIndex)
    {
        PacketPoolCache* cache = currentPacketPoolCache;

        if (cache != nullptr &&
            cache->m_pool.get() == this &&
            GetPacketCacheCapacity(sizeClassIndex) != 0) // 캐시 사용 대상이 아니면 0을 반환한다.
        {
            // 캐시 사용 경로
            return cache->AcquireBlock(sizeClassIndex);
        }

        // 캐시가 없거나 다른 풀·큰 등급이면 공용 경로를 사용한다.
        const BlockBatch batch = AcquireBatch(sizeClassIndex, 1);

        if (batch.first == nullptr)
        {
            return nullptr;
        }

        FreeBlock* block = batch.first;

        void* memory = static_cast<void*>(block);

        // 실제 사용을 시작하는 블록만 연결 정보의 수명을 끝낸다.
        std::destroy_at(block);

        return memory;
    }

    PacketPool::BlockBatch PacketPool::AcquireBatch(std::uint8_t sizeClassIndex, std::size_t maxCount)
    {
        if (maxCount == 0)
        {
            return {};
        }

        auto& storage = m_sizeClasses[sizeClassIndex];

        std::lock_guard storageLock(storage.mutex);

        // 하나도 없을 때만 세그먼트를 확장한다.
        if (storage.freeBlocks == nullptr)
        {
            if (!AddSegment(sizeClassIndex))
            {
                return {};
            }
        }

        BlockBatch batch;

        batch.first = storage.freeBlocks;
        batch.last = batch.first;
        batch.count = 1;

        // 요청 상한 또는 현재 목록의 끝까지 이동한다.
        while (batch.count < maxCount && batch.last->next != nullptr)
        {
            batch.last = batch.last->next;

            ++batch.count;
        }

        // 남은 목록은 공용 저장소에 두고, 가져갈 묶음의 연결을 끊는다.
        storage.freeBlocks = batch.last->next;

        batch.last->next = nullptr;

        return batch;
    }

    bool PacketPool::AddSegment(std::uint8_t sizeClassIndex)
    {
        // 등급 인덱스는 내부에서 검증된 값만 전달된다.
        auto& storage = m_sizeClasses[sizeClassIndex];

        const std::size_t stride = GetPacketBlockStride(sizeClassIndex);

        const std::size_t headerSize = sizeof(Segment);

        // 목표 크기 안에 들어가는 같은 등급의 블록 수를 계산한다.
        const std::size_t targetBlockCount = (PacketSegmentTargetSize - headerSize) / stride;

        // 호출자는 등급 락을 보유한다. 락 순서는 등급 락 -> 예산 락이다.
        std::lock_guard budgetLock(m_budgetMutex);

        const std::size_t remainingBudgetBytes = m_maxTotalSegmentBytes - m_allocatedSegmentBytes;

        if (remainingBudgetBytes < headerSize)
        {
            return false;
        }

        const std::size_t affordableBlockCount = (remainingBudgetBytes - headerSize) / stride;

        const std::size_t blockCount = std::min(targetBlockCount, affordableBlockCount);

        if (blockCount == 0)
        {
            return false;
        }

        // blockCount가 남은 예산으로 제한되어 있으므로 계산 결과도 예산 이하다.
        const std::size_t segmentBytes = headerSize + blockCount * stride;

        void* memory = ::operator new(
            segmentBytes,
            std::align_val_t{ PacketBlockAlignment },
            std::nothrow);

        if (memory == nullptr)
        {
            return false;
        }

        Segment* segment = std::construct_at(static_cast<Segment*>(memory));

        auto* bytes = static_cast<std::byte*>(memory);

        FreeBlock* freeBlocks = storage.freeBlocks;

        for (std::size_t index = 0; index < blockCount; ++index)
        {
            auto* address = reinterpret_cast<FreeBlock*>(
                bytes + headerSize + index * stride);

            FreeBlock* block = std::construct_at(address);

            block->next = freeBlocks;

            freeBlocks = block;
        }

        // 할당과 블록 구성이 끝난 세그먼트만 공용 저장소에 연결한다.
        segment->next = storage.segments;

        storage.segments = segment;
        storage.freeBlocks = freeBlocks;

        m_allocatedSegmentBytes += segmentBytes;

        return true;
    }

    void PacketPool::ReturnBlock(void* memory, std::uint8_t sizeClassIndex) noexcept
    {
        PacketPoolCache* cache = currentPacketPoolCache;

        if (cache != nullptr &&
            cache->m_pool.get() == this &&
            GetPacketCacheCapacity(sizeClassIndex) != 0)
        {
            cache->ReturnBlock(memory, sizeClassIndex);

            return;
        }

        FreeBlock* block = std::construct_at(static_cast<FreeBlock*>(memory));

        BlockBatch batch
        {
            block,
            block,
            1
        };

        ReturnBatch(sizeClassIndex, batch);
    }

    void PacketPool::ReturnBatch(std::uint8_t sizeClassIndex, BlockBatch& batch) noexcept
    {
        if (batch.first == nullptr)
        {
            return;
        }

        auto& storage = m_sizeClasses[sizeClassIndex];

        std::lock_guard storageLock(storage.mutex);

        // 묶음의 마지막 블록 뒤에 기존 공용 목록을 연결한다.
        batch.last->next = storage.freeBlocks;

        storage.freeBlocks = batch.first;

        // 공용 목록으로 인계한 주소를 전달 객체에 남기지 않는다.
        batch = {};
    }

    PacketPoolCache::PacketPoolCache(std::shared_ptr<PacketPool> pool)
        : m_pool(std::move(pool))
    {
        if (!m_pool)
        {
            throw std::invalid_argument("PacketPoolCache requires a valid pool.");
        }

        if (currentPacketPoolCache != nullptr)
        {
            throw std::logic_error("A packet pool cache is already registered on this thread.");
        }

        // 모든 검사가 끝난 뒤에만 TLS에 등록한다.
        currentPacketPoolCache = this;
    }

    PacketPoolCache::~PacketPoolCache()
    {
        // 생성한 스레드에서 등록된 캐시를 소멸해야 한다.
        if (currentPacketPoolCache != this)
        {
            std::terminate();
        }

        // 먼저 등록을 해제해 종료 중인 캐시를 다시 선택하지 않게 한다.
        currentPacketPoolCache = nullptr;

        for (std::size_t index = 0; index < m_sizeClasses.size(); ++index)
        {
            FlushBlocks(static_cast<std::uint8_t>(index),
                m_sizeClasses[index].count);
        }

        // 모든 빈 블록 반환 후 멤버 m_pool이 해제된다.
    }

    void* PacketPoolCache::AcquireBlock(std::uint8_t sizeClassIndex)
    {
        auto& cache = m_sizeClasses[sizeClassIndex];

        if (cache.freeBlocks == nullptr)
        {
            const std::size_t capacity = GetPacketCacheCapacity(sizeClassIndex);

            const std::size_t refillCount = (capacity + 1) / 2;

            const PacketPool::BlockBatch batch = m_pool->AcquireBatch(sizeClassIndex, refillCount);

            if (batch.first == nullptr)
            {
                return nullptr;
            }

            // 캐시 자체가 비어 있으므로 받은 묶음을 그대로 인수한다.
            cache.freeBlocks = batch.first;
            cache.count = batch.count;
        }

        PacketPool::FreeBlock* block = cache.freeBlocks;

        cache.freeBlocks = block->next;

        --cache.count;

        void* memory = static_cast<void*>(block);

        // 실제 사용하는 블록만 FreeBlock에서 PacketBlock으로 전환할 준비를 한다.
        std::destroy_at(block);

        return memory;
    }

    void PacketPoolCache::ReturnBlock(void* memory, std::uint8_t sizeClassIndex) noexcept
    {
        auto& cache = m_sizeClasses[sizeClassIndex];

        const std::size_t capacity = GetPacketCacheCapacity(sizeClassIndex);

        // 새 블록을 넣기 전에 공간을 만들어 상한을 넘지 않게 한다.
        if (cache.count >= capacity)
        {
            const std::size_t flushCount = (capacity + 1) / 2;

            FlushBlocks(sizeClassIndex, flushCount);
        }

        PacketPool::FreeBlock* block = std::construct_at(static_cast<PacketPool::FreeBlock*>(memory));

        block->next = cache.freeBlocks;

        cache.freeBlocks = block;

        ++cache.count;
    }

    void PacketPoolCache::FlushBlocks(std::uint8_t sizeClassIndex, std::size_t maxCount) noexcept
    {
        auto& cache = m_sizeClasses[sizeClassIndex];

        if (maxCount == 0 || cache.freeBlocks == nullptr)
        {
            return;
        }

        PacketPool::BlockBatch batch;

        batch.first = cache.freeBlocks;
        batch.last = batch.first;
        batch.count = 1;

        // 반환할 범위의 마지막 블록을 찾는다.
        while (batch.count < maxCount && batch.last->next != nullptr)
        {
            batch.last = batch.last->next;

            ++batch.count;
        }

        // 남길 목록을 먼저 보관한 뒤 반환 묶음을 분리한다.
        cache.freeBlocks = batch.last->next;

        batch.last->next = nullptr;

        cache.count -= batch.count;

        m_pool->ReturnBatch(sizeClassIndex, batch);
    }
}
