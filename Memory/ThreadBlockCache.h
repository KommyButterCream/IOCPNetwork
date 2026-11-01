#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>

#include "BlockLayout.h"
#include "GlobalBlockPool.h"
#include "SizeBin.h"

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

// 스레드 하나가 빈별로 들고 있는 프리 리스트와, 그 캐시들의 수명 관리.
//
//   ThreadBlockCache    자료구조. 블록 "포인터" 만 들고 있고 메모리는 소유하지 않는다
//   ThreadCacheRegistry 수명 관리. 스레드마다 캐시를 만들고 죽을 때 회수한다
//
// 핵심은 이동 단위가 다르다는 것이다.
//
//   사용 중  <-- 1개 -->  ThreadBlockCache  <-- batch개 -->  GlobalBlockPool
//               락 0                          Interlocked
//
// batch 가 128 이면 128번의 할당 중 127번은 전역을 건드리지 않는다.

namespace MemoryPoolDetail
{
	class ThreadCacheRegistry;

	class ThreadBlockCache
	{
	public:
		// 핫 경로. 여기서 nullptr 이 나오면 전역까지 고갈된 것이다.
		BlockHeader* Pop(uint32_t bin)
		{
			FreeList& list = m_lists[bin];

			if (list.head == nullptr && !Refill(bin))
				return nullptr;

			BlockHeader* header = list.head;
			list.head = header->next;
			--list.count;

			++m_acquireCount[bin];
			return header;
		}

		// 핫 경로. 다른 스레드가 할당한 블록이어도 그냥 이 스레드의 리스트로 넣는다.
		// 블록에 주인이 없으므로 이게 성립한다.
		void Push(uint32_t bin, BlockHeader* header)
		{
			FreeList& list = m_lists[bin];

			header->next = list.head;
			list.head = header;
			++list.count;

			++m_releaseCount[bin];

			if (list.count > m_table->Spec(bin).cacheCapBlocks)
				Flush(bin, false);
		}

		// 남은 블록을 전부 전역으로 되돌린다. 스레드 종료 / Finalize 경로.
		// 이걸 빼먹으면 그대로 누수다.
		void FlushAll();

		uint32_t CachedCount (uint32_t bin) const { return m_lists[bin].count; }
		uint64_t AcquireCount(uint32_t bin) const { return m_acquireCount[bin]; }
		uint64_t ReleaseCount(uint32_t bin) const { return m_releaseCount[bin]; }
		DWORD    OwnerThreadId() const { return m_threadId; }

	private:
		friend class ThreadCacheRegistry;

		bool Refill(uint32_t bin);
		void Flush(uint32_t bin, bool all);

	private:
		// 핫 데이터를 앞에 몰아둔다. 실제로 쓰는 빈 수만큼만 L1 에 올라온다.
		FreeList m_lists[MAX_BIN_COUNT];

		GlobalBlockPool* m_pool = nullptr;
		const BinTable*  m_table = nullptr;
		ThreadCacheRegistry* m_owner = nullptr;
		ThreadBlockCache* m_nextRegistered = nullptr;

		DWORD m_threadId = 0;
		volatile LONG m_detached = 0;

		// 원자적 연산 없이 갱신한다. 스레드 캐시를 두는 이유의 절반이 이거다.
		// 집계는 레지스트리가 순회해서 한다.
		uint64_t m_acquireCount[MAX_BIN_COUNT] = {};
		uint64_t m_releaseCount[MAX_BIN_COUNT] = {};
	};

	// ------------------------------------------------------------------

	// 한 풀에 속한 스레드 캐시들의 수명 관리자. TlsMemoryPool 이 하나 소유한다.
	//
	// 스레드 종료 시 캐시를 회수하지 않으면 그 스레드가 들고 있던 블록이
	// 그대로 샌다. FlsAlloc 의 콜백이 그 일을 한다.
	//
	// 조회는 두 단계다.
	//   어느 배열인가   실행 중인 스레드가 암묵적으로 결정 (thread_local)
	//   배열의 몇 번째  어느 풀인가로 명시적으로 결정 (m_slotIndex)
	class IOCP_ENGINE_API ThreadCacheRegistry
	{
	public:
		// thread_local 배열로 빠르게 찾을 수 있는 풀 개수.
		// 이 수를 넘어도 실패하지 않고 FLS 조회로 물러난다(느릴 뿐 동작은 같다).
		static constexpr uint32_t MAX_FAST_SLOTS = 64;

	public:
		ThreadCacheRegistry() = default;
		~ThreadCacheRegistry();

		ThreadCacheRegistry(const ThreadCacheRegistry&) = delete;
		ThreadCacheRegistry& operator=(const ThreadCacheRegistry&) = delete;

		bool Initialize(GlobalBlockPool& pool, const BinTable& table);
		void Finalize();

		// 핫 경로 진입점. 이 스레드의 캐시를 얻고, 없으면 만든다.
		ThreadBlockCache* GetOrCreate();

		// 스레드 캐시를 못 만든 상태에서 반납된 블록을 회계에 남긴다.
		// 이걸 빼먹으면 블록은 돌아갔는데 누수로 보고된다.
		void NoteOrphanRelease(uint32_t bin);

		// 살아있는 캐시들과 이미 회수된 캐시들의 지표를 합산한다.
		void SumCounters(uint32_t bin, uint64_t& acquire, uint64_t& release, uint32_t& cached) const;

		// 빠른 슬롯을 받았는지. 못 받았으면 블록에 소유 표식을 남기지 않는다.
		uint8_t OwnerTag() const;

	private:
		ThreadBlockCache* Attach();

		// 목록에서 실제로 빼냈으면 true. 다른 스레드가 이미 회수 중이면 false.
		bool Detach(ThreadBlockCache* cache);
		static void WINAPI OnThreadExit(PVOID value);

	private:
		ThreadBlockCache* m_head = nullptr;
		mutable SRWLOCK   m_lock = SRWLOCK_INIT;

		GlobalBlockPool* m_pool = nullptr;
		const BinTable*  m_table = nullptr;

		DWORD    m_flsIndex = FLS_OUT_OF_INDEXES;
		uint32_t m_slotIndex = UINT32_MAX;
		uint32_t m_generation = 0;

		// 종료된 스레드의 지표를 잃지 않도록 여기에 누적한다.
		uint64_t m_retiredAcquire[MAX_BIN_COUNT] = {};
		uint64_t m_retiredRelease[MAX_BIN_COUNT] = {};
	};
}
