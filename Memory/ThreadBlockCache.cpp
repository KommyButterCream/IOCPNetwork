#include "ThreadBlockCache.h"

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;

namespace MemoryPoolDetail
{
	namespace
	{
		// 빠른 슬롯 점유 비트맵. Finalize 가 슬롯을 반납하므로 레지스트리를
		// 만들고 지우기를 반복해도 슬롯이 마르지 않는다.
		volatile LONG64 g_slotMask = 0;

		// 슬롯이 재사용될 때 예전 레지스트리의 캐시 포인터를 잘못 집는 일이
		// 없도록 세대를 붙인다. 세대가 다르면 포인터를 아예 건드리지 않는다.
		volatile LONG g_generationCounter = 0;

		// 스레드당 슬롯. 핫 경로는 여기서 포인터 하나를 읽는 게 전부다.
		//
		// thread_local 이 배열 전체에 걸려 있다는 점이 핵심이다.
		// t_slots 는 하나가 아니라 스레드 수만큼 존재하고, 각 스레드는 자기
		// 것만 본다. 그래서 (스레드 x 풀) 2차원 대응을 1차원 배열로 처리한다.
		struct FastSlot
		{
			void* cache = nullptr;
			uint32_t generation = 0;
			uint32_t reserved = 0;
		};

		thread_local FastSlot t_slots[ThreadCacheRegistry::MAX_FAST_SLOTS];

		uint32_t AcquireSlot()
		{
			for (;;)
			{
				const LONG64 current = ::ReadAcquire64(&g_slotMask);

				uint32_t slot = ThreadCacheRegistry::MAX_FAST_SLOTS;
				for (uint32_t i = 0; i < ThreadCacheRegistry::MAX_FAST_SLOTS; ++i)
				{
					if ((current & (1LL << i)) == 0)
					{
						slot = i;
						break;
					}
				}

				if (slot == ThreadCacheRegistry::MAX_FAST_SLOTS)
					return UINT32_MAX;

				const LONG64 desired = current | (1LL << slot);
				if (::InterlockedCompareExchange64(&g_slotMask, desired, current) == current)
					return slot;
			}
		}

		void ReleaseSlot(uint32_t slot)
		{
			if (slot >= ThreadCacheRegistry::MAX_FAST_SLOTS)
				return;

			for (;;)
			{
				const LONG64 current = ::ReadAcquire64(&g_slotMask);
				const LONG64 desired = current & ~(1LL << slot);
				if (::InterlockedCompareExchange64(&g_slotMask, desired, current) == current)
					return;
			}
		}
	}

	// ---------------------------------------------------------------------
	// ThreadBlockCache
	// ---------------------------------------------------------------------

	bool ThreadBlockCache::Refill(uint32_t bin)
	{
		const GlobalBlockPool::ChunkRef chunk = m_pool->PopChunk(bin);
		if (!chunk.head)
		{
			LOGE("bin %u (block size %u) exhausted, cannot refill the thread cache",
				bin, m_table->Spec(bin).blockSize);
			return false;
		}

		// 리스트가 비었을 때만 이 함수를 부르므로 이어붙일 필요가 없다.
		FreeList& list = m_lists[bin];
		list.head = chunk.head;
		list.count = chunk.count;

		return true;
	}

	void ThreadBlockCache::Flush(uint32_t bin, bool all)
	{
		FreeList& list = m_lists[bin];
		if (list.count == 0 || list.head == nullptr)
			return;

		const uint32_t batch = m_table->Spec(bin).batchBlocks;

		do
		{
			uint32_t take = batch;
			if (take > list.count)
				take = list.count;

			if (take == 0)
				break;

			// 묶음의 꼬리를 찾는다. take-1 번 링크를 따라간다.
			// 이 비용은 take 번의 해제마다 한 번이므로 해제 1건당 링크 추적
			// 1회로 상각된다. 대신 없어진 것이 배타 락 획득이다.
			BlockHeader* head = list.head;
			BlockHeader* tail = head;
			for (uint32_t i = 1; i < take; ++i)
				tail = tail->next;

			list.head = tail->next;
			list.count -= take;
			tail->next = nullptr;

			m_pool->PushChunk(bin, head, take);

		} while (all && list.count > 0);
	}

	void ThreadBlockCache::FlushAll()
	{
		for (uint32_t bin = 0; bin < m_table->binCount; ++bin)
			Flush(bin, true);
	}

	// ---------------------------------------------------------------------
	// ThreadCacheRegistry
	// ---------------------------------------------------------------------

	ThreadCacheRegistry::~ThreadCacheRegistry()
	{
		Finalize();
	}

	uint8_t ThreadCacheRegistry::OwnerTag() const
	{
		// 빠른 슬롯을 받은 레지스트리만 블록에 자기 표식을 남긴다.
		// 슬롯이 없는 풀들은 서로 구분할 방법이 없으므로 검사 대상에서 뺀다.
		return (m_slotIndex < MAX_FAST_SLOTS)
			? static_cast<uint8_t>(m_slotIndex)
			: POOL_ID_NONE;
	}

	bool ThreadCacheRegistry::Initialize(GlobalBlockPool& pool, const BinTable& table)
	{
		if (m_flsIndex != FLS_OUT_OF_INDEXES)
			return false;

		::InitializeSRWLock(&m_lock);

		m_pool = &pool;
		m_table = &table;

		// 빠른 슬롯을 못 받아도 초기화를 실패시키지 않는다.
		// 메모리 풀이 "자리가 없어서" 못 만들어지면 그 위의 모든 것이 무너진다.
		// 이 경우 thread_local 배열 대신 FLS 조회로 스레드 캐시를 찾는다.
		// 함수 호출 한 번이 더 붙을 뿐 동작과 락 특성은 완전히 같다.
		m_slotIndex = AcquireSlot();
		if (m_slotIndex == UINT32_MAX)
		{
			LOGW("no fast cache slot left (maximum %u per process), falling back to FLS lookup",
				MAX_FAST_SLOTS);
		}

		m_generation = static_cast<uint32_t>(::InterlockedIncrement(&g_generationCounter));

		// 스레드가 죽을 때 캐시를 회수하기 위한 등록.
		// 이게 없으면 스레드가 죽을 때마다 그 캐시에 남은 블록이 그대로 샌다.
		m_flsIndex = ::FlsAlloc(&ThreadCacheRegistry::OnThreadExit);
		if (m_flsIndex == FLS_OUT_OF_INDEXES)
		{
			LOGE("FlsAlloc failed (error %lu). thread caches would leak on thread exit",
				::GetLastError());
			ReleaseSlot(m_slotIndex);
			m_slotIndex = UINT32_MAX;
			return false;
		}

		return true;
	}

	void ThreadCacheRegistry::Finalize()
	{
		// 1. 스레드 종료 콜백을 먼저 닫는다.
		//    아래에서 캐시를 해제한 뒤에 콜백이 돌면 해제된 메모리를 만지게 된다.
		if (m_flsIndex != FLS_OUT_OF_INDEXES)
		{
			::FlsFree(m_flsIndex);
			m_flsIndex = FLS_OUT_OF_INDEXES;
		}

		// 2. 남아있는 캐시를 모두 회수한다.
		//    이 시점에 캐시가 남아 있다는 것은 그 스레드가 아직 살아 있다는
		//    뜻이므로, 호출 계약(모든 워커 종료 후 Finalize)이 지켜졌는지
		//    확인하는 지표이기도 하다.
		for (;;)
		{
			::AcquireSRWLockExclusive(&m_lock);
			ThreadBlockCache* cache = m_head;
			::ReleaseSRWLockExclusive(&m_lock);

			if (!cache)
				break;

			if (!Detach(cache))
			{
				// 다른 스레드(대개 종료 중인 워커의 FLS 콜백)가 이 캐시를 회수하는
				// 중이다. 그쪽이 FlushAll 을 끝내고 목록에서 빼야 머리가 바뀌므로
				// 여기서 곧바로 다시 읽으면 같은 포인터만 계속 나온다.
				//
				// 그 flush 가 끝나기 전에 세그먼트를 반납하면 남의 스레드가
				// 해제된 메모리에 블록을 밀어넣게 되므로 기다리는 것이 맞다.
				// 대신 코어를 태우지 않도록 양보한다.
				::SwitchToThread();
			}
		}

		if (m_slotIndex != UINT32_MAX)
		{
			ReleaseSlot(m_slotIndex);
			m_slotIndex = UINT32_MAX;
		}

		// 세대를 무효화해서 남아있는 슬롯이 이 레지스트리의 캐시를 가리키지 않게 한다.
		m_generation = 0;
		m_pool = nullptr;
		m_table = nullptr;
	}

	ThreadBlockCache* ThreadCacheRegistry::GetOrCreate()
	{
		if (m_slotIndex < MAX_FAST_SLOTS)
		{
			FastSlot& slot = t_slots[m_slotIndex];
			if (slot.generation == m_generation)
				return static_cast<ThreadBlockCache*>(slot.cache);
		}
		else if (m_flsIndex != FLS_OUT_OF_INDEXES)
		{
			ThreadBlockCache* cache = static_cast<ThreadBlockCache*>(::FlsGetValue(m_flsIndex));
			if (cache)
				return cache;
		}

		return Attach();
	}

	ThreadBlockCache* ThreadCacheRegistry::Attach()
	{
		ThreadBlockCache* cache = static_cast<ThreadBlockCache*>(
			::HeapAlloc(::GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(ThreadBlockCache)));

		if (!cache)
		{
			LOGE("failed to allocate a thread block cache for thread %lu", ::GetCurrentThreadId());
			return nullptr;
		}

		cache->m_pool = m_pool;
		cache->m_table = m_table;
		cache->m_owner = this;
		cache->m_threadId = ::GetCurrentThreadId();

		::AcquireSRWLockExclusive(&m_lock);
		cache->m_nextRegistered = m_head;
		m_head = cache;
		::ReleaseSRWLockExclusive(&m_lock);

		if (m_slotIndex < MAX_FAST_SLOTS)
		{
			t_slots[m_slotIndex].cache = cache;
			t_slots[m_slotIndex].generation = m_generation;
		}

		// 스레드당 한 번만 호출된다.
		if (m_flsIndex != FLS_OUT_OF_INDEXES)
			::FlsSetValue(m_flsIndex, cache);

		return cache;
	}

	bool ThreadCacheRegistry::Detach(ThreadBlockCache* cache)
	{
		if (!cache)
			return false;

		// 스레드 종료 콜백과 Finalize 가 같은 캐시를 두고 겹칠 수 있다.
		// 진 쪽은 false 를 돌려준다. 목록에서 빠지지 않았다는 뜻이다.
		if (::InterlockedCompareExchange(&cache->m_detached, 1, 0) != 0)
			return false;

		// 목록에서 빼는 것은 FlushAll 뒤여야 한다. 먼저 빼면 Finalize 가
		// 이 캐시를 못 보고 지나가서, flush 가 끝나기 전에 세그먼트를 반납한다.
		cache->FlushAll();

		::AcquireSRWLockExclusive(&m_lock);

		ThreadBlockCache** link = &m_head;
		while (*link)
		{
			if (*link == cache)
			{
				*link = cache->m_nextRegistered;
				break;
			}
			link = &(*link)->m_nextRegistered;
		}

		const uint32_t binCount = m_table ? m_table->binCount : 0;
		for (uint32_t bin = 0; bin < binCount; ++bin)
		{
			m_retiredAcquire[bin] += cache->m_acquireCount[bin];
			m_retiredRelease[bin] += cache->m_releaseCount[bin];
		}

		::ReleaseSRWLockExclusive(&m_lock);

		// 이 스레드의 슬롯이 방금 회수한 캐시를 가리키고 있으면 지운다.
		// (Finalize 가 다른 스레드의 캐시를 회수하는 경우에는 일치하지 않는다)
		if (m_slotIndex < MAX_FAST_SLOTS && t_slots[m_slotIndex].cache == cache)
		{
			t_slots[m_slotIndex].cache = nullptr;
			t_slots[m_slotIndex].generation = 0;
		}

		if (m_flsIndex != FLS_OUT_OF_INDEXES && cache->m_threadId == ::GetCurrentThreadId())
			::FlsSetValue(m_flsIndex, nullptr);

		::HeapFree(::GetProcessHeap(), 0, cache);
		return true;
	}

	void ThreadCacheRegistry::NoteOrphanRelease(uint32_t bin)
	{
		::AcquireSRWLockExclusive(&m_lock);
		m_retiredRelease[bin] += 1;
		::ReleaseSRWLockExclusive(&m_lock);
	}

	void WINAPI ThreadCacheRegistry::OnThreadExit(PVOID value)
	{
		ThreadBlockCache* cache = static_cast<ThreadBlockCache*>(value);
		if (!cache || !cache->m_owner)
			return;

		cache->m_owner->Detach(cache);
	}

	void ThreadCacheRegistry::SumCounters(uint32_t bin, uint64_t& acquire, uint64_t& release,
		uint32_t& cached) const
	{
		// retired 도 Detach 가 배타 락 아래에서 갱신하므로 같은 락 안에서 읽어야
		// 한다. 밖에서 읽으면 그 사이 회수된 캐시의 지표가 통째로 누락된다.
		::AcquireSRWLockShared(&m_lock);

		acquire = m_retiredAcquire[bin];
		release = m_retiredRelease[bin];
		cached = 0;

		for (const ThreadBlockCache* cache = m_head; cache != nullptr; cache = cache->m_nextRegistered)
		{
			acquire += cache->m_acquireCount[bin];
			release += cache->m_releaseCount[bin];
			cached += cache->m_lists[bin].count;
		}
		::ReleaseSRWLockShared(&m_lock);
	}
}
