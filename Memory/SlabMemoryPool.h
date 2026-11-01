#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <Windows.h>
#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API SlabMemoryPool
{
	// Define Structure
public:
	// Slab 메모리 설정을 위한 구조체
	// 메모리 풀 초기화에 사용
	struct SlabConfig
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
	};

	// 매직 넘버: 메모리 해제 시 헤더 훼손 여부 검증용
	static constexpr uint32_t HEADER_MAGIC = 0xDEADBEEF;

	// 슬랩별 운영 지표.
	// 할당/해제마다 로그를 찍는 대신 이 값들을 누적한다.
	// 로그 한 줄보다 정보량이 많고 비용은 사실상 0 이다.
	//   - peakAllocated : 동시 사용 최고치. 초기 blockCount 산정 근거
	//   - growthCount   : 런타임 확장 횟수. 0 이 아니면 초기값이 부족했다는 뜻
	//   - totalAcquire / totalRelease : 차이가 곧 미반환 블록 수
	struct SlabStats
	{
		uint32_t blockSize = 0;
		uint32_t blockCount = 0;
		uint32_t allocatedCount = 0;
		uint32_t peakAllocated = 0;
		uint32_t growthCount = 0;
		uint32_t acquireFailCount = 0;
		uint64_t totalAcquire = 0;
		uint64_t totalRelease = 0;
	};

public:
	SlabMemoryPool();
	~SlabMemoryPool();

public:
	// payloadAlignment 는 이 구현이 지원하지 않는다. 항상 16바이트로 잡는다.
	// TlsMemoryPool 과 시그니처를 맞춰 두어야 EngineMemoryPool 스위치를
	// 양쪽으로 뒤집어도 호출부가 그대로 컴파일된다.
	bool Initialize(const SlabConfig* configs, uint32_t slabCount, uint32_t payloadAlignment = 16);
	void Finalize();

	void* Acquire(size_t size);
	void Release(const void* payload);

	uint32_t GetSlabCount() const { return m_slabCount; }

	// TlsMemoryPool 과 이름을 맞춰 두어야 EngineMemoryPool 스위치를 양쪽으로
	// 뒤집어도 호출부가 그대로 컴파일된다. 이 클래스는 A/B 비교용으로만 남아 있다.
	using BinConfig = SlabConfig;
	using BinStats = SlabStats;
	uint32_t GetBinCount() const { return m_slabCount; }
	bool GetBinStats(uint32_t index, SlabStats& out) const { return GetSlabStats(index, out); }
	bool GetSlabStats(uint32_t slabIndex, SlabStats& outStats) const;

	// 전체 슬랩 지표를 로그로 한 번에 덤프한다.
	// 종료 시점이나 주기적으로 호출해서 풀 사이징과 누수를 판단한다.
	void LogStats(const char* poolName) const;

private:
	// Slab 메모리 풀의 링크드 리스트 노드
	struct alignas(16) BlockHeader
	{
		uint32_t bucketIndex = UINT32_MAX;
		uint32_t magic = HEADER_MAGIC;
		BlockHeader* next = nullptr;
	};

	// False Sharing 을 방지하기 위해 Padding 을 넣고 캐시 라인 64바이트로 정렬
	static constexpr size_t CACHE_LINE_SIZE = 64;
	static constexpr size_t MEMORY_ALIGNMENT = 16; // IOCP 요구 정렬
	struct alignas(CACHE_LINE_SIZE) Slab
	{
		SRWLOCK lock = SRWLOCK_INIT;               // 경합 지점이므로 구조체 최상단에 배치
		BlockHeader* freeList = nullptr;

		uint32_t allocatedCount = 0;
		uint32_t blockSize = 0;
		uint32_t stride = 0;
		uint32_t blockCount = 0;

		void* initialMemory = nullptr;       // 초기 할당 영역
		void** extraMemoryBlocks = nullptr;
		uint32_t extraMemoryCount = 0;
		uint32_t extraMemoryCapacity = 0;

		// 운영 지표. 모두 슬랩 락 보유 중에만 갱신하므로 별도 원자적 연산이 없다.
		uint32_t peakAllocated = 0;
		uint32_t growthCount = 0;
		uint32_t acquireFailCount = 0;
		uint64_t totalAcquire = 0;
		uint64_t totalRelease = 0;

		Slab()
		{
		}
	};

	Slab* m_slabs = nullptr;
	uint32_t m_slabCount = 0;
	bool m_initialized = false;

	uint32_t FindSlabIndex(size_t memorySize) const;

	// 새 추가 메모리 블록을 할당한 뒤 프리 리스트에 연결
	bool AllocateExtraBlocks(Slab& slab, uint32_t blockCount);

	// Align helpers
	static size_t AlignUp(size_t memorySize, size_t alignment)
	{
		// 입력받은 메모리 크기 memorySize 이상이면서
		// alignment 의 배수를 만족하는 크기로 반환한다.
		return (memorySize + (alignment - 1)) & ~(alignment - 1);
	}

	bool VerifyAllMemoryReleased() const;
};
