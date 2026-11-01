#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>

#include "BlockLayout.h"
#include "SizeBin.h"

#ifndef IOCP_ENGINE_API
#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif
#endif

// 블록의 실체를 소유하는 계층.
//
//   - 세그먼트(VirtualAlloc)를 잡아 stride 간격으로 자른다
//   - 자른 블록을 batch 개씩 묶음(chunk)으로 엮어 빈별 SLIST 에 보관한다
//   - 스레드 캐시가 묶음 단위로 가져가고 되돌려 놓는다
//
// 이 클래스는 스레드 캐시를 모른다. 의존은 캐시 -> 풀 한 방향뿐이다.
//
// 동기화
//   PopChunk / PushChunk : Interlocked SLIST. 락 없음
//   세그먼트 확장         : SRWLOCK. 정상 운영 중에는 호출되지 않음
//
// 묶음(chunk)이 어떻게 표현되는지는 이 클래스의 구현 사항이다.
// 바깥에는 ChunkRef(머리 포인터 + 개수)만 노출한다.

namespace MemoryPoolDetail
{
	class IOCP_ENGINE_API GlobalBlockPool
	{
	public:
		// head 가 nullptr 이면 실패
		struct ChunkRef
		{
			BlockHeader* head = nullptr;
			uint32_t count = 0;
		};

		// 빈 하나의 운영 지표
		struct BinStats
		{
			uint32_t blocksCreated = 0;   // 세그먼트에서 실제로 만들어낸 블록 수
			uint32_t blocksInPool = 0;    // 지금 SLIST 에 남아 있는 블록 수
			uint32_t peakOutOfPool = 0;   // 동시에 밖에 나가 있던 블록 수의 상한
			uint32_t growthCount = 0;     // 런타임 확장 횟수
			uint32_t failCount = 0;       // 세그먼트 확보 실패 횟수
			uint32_t segmentCount = 0;
			uint64_t committedBytes = 0;   // 이 빈이 OS 에서 잡은 총 바이트
		};

	public:
		GlobalBlockPool() = default;
		~GlobalBlockPool();

		GlobalBlockPool(const GlobalBlockPool&) = delete;
		GlobalBlockPool& operator=(const GlobalBlockPool&) = delete;

		bool Initialize(const BinTable& table, uint8_t ownerTag);

		// releaseSegments 가 false 면 VirtualFree 를 하지 않는다.
		//
		// 밖에 나가 있는 블록이 남은 채로 끝낼 때 쓴다. 세그먼트를 돌려주면
		// 그 블록들이 얹혀 있던 주소가 커밋 해제되고, 늦게 도착한 반납이
		// 그 주소를 읽는 순간 접근 위반으로 프로세스가 죽는다.
		//
		// 실측(tools/latepool 시험 2) : 브로드캐스트용 SharedSendPacket 은
		// 서술자 자신이 송신 큐 풀의 블록이다. StopServer 뒤에 마지막 참조가
		// 떨어지면 refCount 를 읽는 첫 줄에서 0xC0000005 로 죽었다.
		//
		// 이건 의도된 누수다. 그 상황은 이미 오류로 보고된 뒤이고(위쪽에서
		// "finalize with unreleased blocks" 를 남긴다), 종료 중인 프로세스에서
		// 페이지를 붙들고 있는 것과 죽는 것 중에는 붙들고 있는 쪽이 낫다.
		// 장부(세그먼트 목록, 빈 배열)는 그대로 반납하므로 새는 것은
		// 세그먼트뿐이다.
		void Finalize(bool releaseSegments = true);

		// 이 풀이 OS 에서 잡을 수 있는 총 바이트의 상한. 0 이면 무제한.
		//
		// 상한이 없으면 GrowLocked 이 VirtualAlloc 을 거부당할 때까지 세그먼트를
		// 계속 늘린다. 잡 큐에 깊이 상한이 없으므로(실측: 느린 핸들러에서
		// 세션 하나가 150ms 에 6513개, 초당 약 2.8MB) 느린 핸들러 하나가
		// 프로세스를 OOM 까지 끌고 갈 수 있다.
		//
		// 상한에 걸리면 확장을 거부하고 Acquire 가 nullptr 을 반환한다.
		// 그 경로는 이미 끝까지 구현되어 있다 —
		// CreateJob 실패는 SubmitPacketJob 이 패킷을 버리고 false 를 돌려주고,
		// CreatePacket 실패는 ReadPacket 이 OutOfMemory 를 반환해 세션을 정리한다.
		// 새 실패 경로가 생기는 것이 아니라, 죽는 대신 실패하게 된다.
		//
		// 빈별이 아니라 풀별인 이유는, 운영자가 통제하고 싶은 것이 "이 풀이
		// 쓰는 메모리" 이지 "4K 빈이 쓰는 메모리" 가 아니기 때문이다.
		// 빈별로 두면 한쪽은 남는데 다른 쪽이 말라 실패하는 상황을 튜닝으로만
		// 피해야 한다.
		//
		// 초기 Preallocate 은 이 상한을 넘더라도 통과시킨다. 설정이 상한보다
		// 크면 기동 시점에 알아야지, 나중에 조용히 마르면 안 된다.
		void SetCommitLimit(uint64_t maxCommittedBytes);
		uint64_t GetCommitLimit() const;

		// 지금 이 풀이 OS 에서 잡고 있는 총 바이트. 모든 빈의 합이다.
		uint64_t GetCommittedBytes() const;

		// 상한에 걸려 확장을 거부한 횟수. 0 이 아니면 상한이 실제로 물렸다는 뜻이다.
		uint64_t GetCommitLimitHitCount() const;

		// 설정에 적힌 만큼 미리 만들어 둔다. 세그먼트 최소 크기 때문에
		// 요청보다 많이 만들어질 수 있다.
		bool Preallocate(uint32_t bin, uint32_t blocks);

		// 묶음 하나를 꺼낸다. 재고가 없으면 세그먼트를 늘려서라도 채운다.
		ChunkRef PopChunk(uint32_t bin);

		// 묶음 하나를 되돌린다. head 는 count 개짜리 사슬의 머리여야 한다.
		void PushChunk(uint32_t bin, BlockHeader* head, uint32_t count);

		bool GetStats(uint32_t bin, BinStats& out) const;

		const BinTable& Table() const { return *m_table; }

	private:
		struct Bin;   // .cpp 에 정의

		// growLock 을 잡은 채 호출한다.
		bool GrowLocked(uint32_t bin, uint32_t minBlocks, bool isInitial);
		bool RecordSegmentLocked(Bin& target, void* segment, size_t bytes);

		void CarveAndPush(Bin& target, uint32_t bin, void* segment, uint32_t blocks);
		void NotePopped(Bin& target, uint32_t count);

	private:
		const BinTable* m_table = nullptr;
		Bin*  m_bins = nullptr;
		void* m_binsRaw = nullptr;      // m_bins 의 정렬 전 원본 포인터
		uint8_t m_ownerTag = POOL_ID_NONE;
		bool m_initialized = false;

		// 풀 전체의 커밋 회계. 빈별 committedBytes 의 합과 같아야 한다.
		// 확장은 growLock 안에서만 일어나므로 갱신은 그 락이 지킨다.
		// 읽기는 락 밖에서도 오므로 Interlocked 로 읽는다.
		volatile LONG64 m_committedBytes = 0;
		volatile LONG64 m_commitLimitHits = 0;
		uint64_t m_maxCommittedBytes = 0;   // 0 = 무제한
	};
}
