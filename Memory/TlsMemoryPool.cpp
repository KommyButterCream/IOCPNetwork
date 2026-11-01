#include "TlsMemoryPool.h"

#include <stdio.h>

#include "../Diagnostics/EngineAssert.h"

#include "../../Core/Util/Logger.h"

using namespace Core::Util;
using namespace MemoryPoolDetail;

namespace
{
	// 로그에서 바이트를 눈으로 읽을 수 있게 바꾼다.
	// 4202496 보다 "4.0 MB" 가 설정을 손볼 때 훨씬 빨리 눈에 들어온다.
	void FormatBytes(uint64_t bytes, char* out, size_t outSize)
	{
		if (bytes >= 1024ull * 1024ull)
			::sprintf_s(out, outSize, "%.1f MB", static_cast<double>(bytes) / (1024.0 * 1024.0));
		else if (bytes >= 1024ull)
			::sprintf_s(out, outSize, "%.1f KB", static_cast<double>(bytes) / 1024.0);
		else
			::sprintf_s(out, outSize, "%llu B", bytes);
	}
}

TlsMemoryPool::~TlsMemoryPool()
{
	Finalize();
}

// ---------------------------------------------------------------------------
// 초기화 / 종료
// ---------------------------------------------------------------------------

bool TlsMemoryPool::Initialize(const BinConfig* configs, uint32_t configCount, uint32_t payloadAlignment)
{
	if (m_initialized)
		return false;

	if (!configs || configCount == 0)
	{
		LOGE("Initialize called with no configuration");
		return false;
	}

	if (!IsPowerOfTwo(payloadAlignment) || payloadAlignment < 16 || payloadAlignment > 4096)
	{
		LOGE("payload alignment %u is invalid. it must be a power of two between 16 and 4096",
			payloadAlignment);
		return false;
	}

	// 1. 설정에서 빈 범위를 뽑는다. 항목 수가 아니라 최소~최대가 정한다.
	size_t smallest = SIZE_MAX;
	size_t largest = 0;

	for (uint32_t i = 0; i < configCount; ++i)
	{
		if (configs[i].blockSize == 0)
		{
			LOGE("configuration %u has a zero block size", i);
			return false;
		}

		if (configs[i].blockSize < smallest) smallest = configs[i].blockSize;
		if (configs[i].blockSize > largest)  largest = configs[i].blockSize;
	}

	if (smallest < MIN_BLOCK_SIZE)
	{
		// 묶음 꼬리표를 블록 페이로드에 얹기 때문에 최소 크기가 필요하다.
		LOGW("smallest configured block size %zu is below the minimum %u, raising it",
			smallest, MIN_BLOCK_SIZE);
		smallest = MIN_BLOCK_SIZE;
	}

	m_table.minBinLog = CeilLog2(smallest);
	const uint32_t maxBinLog = CeilLog2(largest);
	m_table.binCount = maxBinLog - m_table.minBinLog + 1;

	if (m_table.binCount > MAX_BIN_COUNT)
	{
		LOGE("configuration spans %u bins (%zu .. %zu) but the maximum is %u",
			m_table.binCount, smallest, largest, MAX_BIN_COUNT);
		m_table = BinTable{};
		return false;
	}

	m_table.minMask = (static_cast<size_t>(1) << m_table.minBinLog) - 1;
	m_table.payloadAlignment = payloadAlignment;
	m_table.headerSize = HeaderSizeFor(payloadAlignment);

	for (uint32_t bin = 0; bin < m_table.binCount; ++bin)
	{
		const uint32_t blockSize =
			static_cast<uint32_t>(static_cast<size_t>(1) << (m_table.minBinLog + bin));

		m_table.specs[bin] = MakeBinSpec(blockSize, m_table.headerSize, payloadAlignment);
	}

	// 2. 스레드 캐시 레지스트리. 빠른 슬롯을 못 받아도 실패시키지 않는다.
	if (!m_registry.Initialize(m_global, m_table))
	{
		m_table = BinTable{};
		return false;
	}

	// 3. 전역 저장소. 블록에 남길 소유 표식은 레지스트리의 슬롯 번호를 쓴다.
	if (!m_global.Initialize(m_table, m_registry.OwnerTag()))
	{
		m_registry.Finalize();
		m_table = BinTable{};
		return false;
	}

	m_initialized = true;

	// 4. 설정에 적힌 만큼 미리 만들어 둔다.
	//    여러 설정이 같은 빈으로 접힐 수 있으므로 누적한다.
	uint32_t requested[MAX_BIN_COUNT] = {};
	for (uint32_t i = 0; i < configCount; ++i)
	{
		const uint32_t bin = m_table.BinOf(configs[i].blockSize);
		if (bin < m_table.binCount)
			requested[bin] += configs[i].blockCount;
	}

	for (uint32_t bin = 0; bin < m_table.binCount; ++bin)
	{
		if (requested[bin] == 0)
			continue;

		if (!m_global.Preallocate(bin, requested[bin]))
		{
			LOGE("failed to preallocate %u blocks for bin %u (block size %u)",
				requested[bin], bin, m_table.specs[bin].blockSize);
			Finalize();
			return false;
		}
	}

	LOGI("pool %u initialized : %u bins (%u .. %u bytes), payload alignment %u, header %u",
		m_registry.OwnerTag(), m_table.binCount,
		m_table.specs[0].blockSize, m_table.specs[m_table.binCount - 1].blockSize,
		m_table.payloadAlignment, m_table.headerSize);

	return true;
}

void TlsMemoryPool::Finalize()
{
	if (!m_initialized)
	{
		m_registry.Finalize();
		m_global.Finalize();
		return;
	}

	// 1. 스레드 캐시를 먼저 전부 회수한다. 남은 블록이 전역으로 돌아온다.
	m_registry.Finalize();

	// 2. 통계 덤프 + 누수 판정. 캐시가 다 비워진 뒤라야 수치가 맞는다.
	LogStats("finalize");

	uint64_t totalAcquire = 0;
	uint64_t totalRelease = 0;
	for (uint32_t bin = 0; bin < m_table.binCount; ++bin)
	{
		uint64_t acquire = 0;
		uint64_t release = 0;
		uint32_t cached = 0;
		m_registry.SumCounters(bin, acquire, release, cached);
		totalAcquire += acquire;
		totalRelease += release;
	}

	const LONG64 bypassAcquire = ::ReadAcquire64(&m_bypassAcquire);
	const LONG64 bypassRelease = ::ReadAcquire64(&m_bypassRelease);

	const bool blocksStillOut = (totalAcquire != totalRelease) || (bypassAcquire != bypassRelease);

	if (blocksStillOut)
	{
		LOGE("finalize with unreleased blocks : pooled %llu outstanding, bypass %lld outstanding. see the stats above",
			totalAcquire - totalRelease, bypassAcquire - bypassRelease);
		ENGINE_BREAK_IF_DEBUGGER();
	}

	// 3. 세그먼트를 OS 로 반납한다.
	//
	// 밖에 나가 있는 블록이 남아 있으면 반납하지 않는다. 그 블록들이 얹혀
	// 있는 주소를 커밋 해제하면, 늦게 도착한 반납이 그 주소를 읽는 순간
	// 프로세스가 죽는다. (사정은 GlobalBlockPool::Finalize 선언부 주석)
	//
	// 여기서 죽으면 안 되는 이유는 이 경로가 종료 경로이기 때문이다.
	// 이미 오류로 보고된 상태에서 크래시까지 얹으면, 원인(어느 블록이
	// 안 돌아왔는가)이 담긴 로그를 읽을 기회마저 사라진다.
	m_global.Finalize(!blocksStillOut);

	// 같은 객체를 다시 Initialize 할 수 있으므로 우회 카운터도 되돌린다.
	// 남겨두면 다음 수명의 Finalize 가 이전 수명의 불균형을 보고한다.
	::InterlockedExchange64(&m_bypassAcquire, 0);
	::InterlockedExchange64(&m_bypassRelease, 0);

	m_table = BinTable{};
	m_initialized = false;
}

// ---------------------------------------------------------------------------
// 할당 / 해제
// ---------------------------------------------------------------------------

void TlsMemoryPool::SetCommitLimit(uint64_t maxCommittedBytes)
{
	m_global.SetCommitLimit(maxCommittedBytes);
}

uint64_t TlsMemoryPool::GetCommitLimit() const
{
	return m_global.GetCommitLimit();
}

uint64_t TlsMemoryPool::GetCommittedBytes() const
{
	return m_global.GetCommittedBytes();
}

uint64_t TlsMemoryPool::GetCommitLimitHitCount() const
{
	return m_global.GetCommitLimitHitCount();
}

void* TlsMemoryPool::Acquire(size_t size)
{
	if (!m_initialized || size == 0)
		return nullptr;

	const uint32_t bin = m_table.BinOf(size);
	if (bin >= m_table.binCount)
		return AcquireBypass(size);

	ThreadBlockCache* cache = m_registry.GetOrCreate();
	if (!cache)
		return nullptr;

	BlockHeader* header = cache->Pop(bin);
	if (!header)
		return nullptr;

	if (header->magic != MAGIC_FREE)
	{
		ENGINE_CORRUPTION("free list holds a block with magic 0x%08X (expected 0x%08X) at %p. the block was overrun or handed out twice",
			header->magic, MAGIC_FREE, static_cast<void*>(header));
		return nullptr;
	}

	header->magic = MAGIC_LIVE;
	header->next = nullptr;

	return PayloadOf(header, m_table.headerSize);
}

void TlsMemoryPool::Release(const void* payload)
{
	if (!payload)
		return;

	if (!m_initialized)
	{
		ENGINE_VIOLATION("Release called after finalize (payload %p)", payload);
		return;
	}

	BlockHeader* header = HeaderOf(payload, m_table.headerSize);

	if (header->magic == MAGIC_FREE)
	{
		ENGINE_CORRUPTION("double free : payload %p is already on a free list", payload);
		return;
	}

	if (header->magic != MAGIC_LIVE)
	{
		ENGINE_CORRUPTION("block header corrupted : payload %p, magic 0x%08X (expected 0x%08X). overrun or a pointer that did not come from this pool",
			payload, header->magic, MAGIC_LIVE);
		return;
	}

	// 빠른 슬롯을 못 받은 풀들은 서로 구분할 수 없으므로 이 검사를 건너뛴다.
	const uint8_t ownerTag = m_registry.OwnerTag();
	if (header->poolId != POOL_ID_NONE && header->poolId != ownerTag)
	{
		ENGINE_VIOLATION("payload %p belongs to pool %u but was released to pool %u",
			payload, header->poolId, ownerTag);
		return;
	}

	header->magic = MAGIC_FREE;

	if (header->binIndex == BIN_BYPASS)
	{
		ReleaseBypass(header);
		return;
	}

	const uint32_t bin = header->binIndex;
	if (bin >= m_table.binCount)
	{
		ENGINE_CORRUPTION("block header has bin index %u but the pool has %u bins : payload %p",
			bin, m_table.binCount, payload);
		return;
	}

	ThreadBlockCache* cache = m_registry.GetOrCreate();
	if (!cache)
	{
		// 캐시를 못 만들었으면 블록 하나짜리 묶음으로 전역에 직접 돌려준다.
		// 여기서 포기하면 블록이 그대로 샌다.
		header->next = nullptr;
		m_global.PushChunk(bin, header, 1);

		// 캐시가 없으니 m_releaseCount 를 올릴 곳도 없다. 레지스트리에 직접
		// 남기지 않으면 블록은 돌아갔는데 Finalize 가 누수로 보고한다.
		m_registry.NoteOrphanRelease(bin);
		return;
	}

	cache->Push(bin, header);
}

// ---------------------------------------------------------------------------
// 최대 빈을 넘는 요청 — OS 우회
// ---------------------------------------------------------------------------

void* TlsMemoryPool::AcquireBypass(size_t size)
{
	// 풀링하지 않고 OS 에서 직접 받는다.
	// 헤더는 그대로 붙이므로 Release 는 우회 블록인지 여부만 보면 된다.
	const size_t rawBytes = m_table.headerSize + size + m_table.payloadAlignment;

	void* raw = ::HeapAlloc(::GetProcessHeap(), 0, rawBytes);
	if (!raw)
	{
		LOGE("bypass allocation of %zu bytes failed", size);
		return nullptr;
	}

	char* payload = reinterpret_cast<char*>(
		AlignUpSize(reinterpret_cast<uintptr_t>(raw) + m_table.headerSize, m_table.payloadAlignment));

	BlockHeader* header = reinterpret_cast<BlockHeader*>(payload - m_table.headerSize);
	header->magic = MAGIC_LIVE;
	header->binIndex = BIN_BYPASS;
	header->poolId = m_registry.OwnerTag();
	header->reserved = 0;
	header->next = static_cast<BlockHeader*>(raw);   // 해제용 원본 주소

	::InterlockedIncrement64(&m_bypassAcquire);

	return payload;
}

void TlsMemoryPool::ReleaseBypass(BlockHeader* header)
{
	void* raw = header->next;
	::InterlockedIncrement64(&m_bypassRelease);
	::HeapFree(::GetProcessHeap(), 0, raw);
}

// ---------------------------------------------------------------------------
// 진단
// ---------------------------------------------------------------------------

uint32_t TlsMemoryPool::GetBinBlockSize(uint32_t bin) const
{
	if (!m_initialized || bin >= m_table.binCount)
		return 0;

	return m_table.specs[bin].blockSize;
}

uint32_t TlsMemoryPool::GetBinIndex(size_t size) const
{
	if (!m_initialized || size == 0)
		return UINT32_MAX;

	return m_table.BinOf(size);
}

bool TlsMemoryPool::GetBinStats(uint32_t bin, BinStats& outStats) const
{
	if (!m_initialized || bin >= m_table.binCount)
		return false;

	GlobalBlockPool::BinStats global;
	if (!m_global.GetStats(bin, global))
		return false;

	uint64_t acquire = 0;
	uint64_t release = 0;
	uint32_t cached = 0;
	m_registry.SumCounters(bin, acquire, release, cached);

	const BinSpec& spec = m_table.specs[bin];

	outStats = BinStats{};
	outStats.blockSize = spec.blockSize;
	outStats.cacheCapBlocks = spec.cacheCapBlocks;
	outStats.batchBlocks = spec.batchBlocks;

	outStats.blockCount = global.blocksCreated;
	outStats.blocksInPool = global.blocksInPool;
	outStats.growthCount = global.growthCount;
	outStats.acquireFailCount = global.failCount;
	outStats.peakAllocated = global.peakOutOfPool;
	outStats.segmentCount = global.segmentCount;
	outStats.committedBytes = global.committedBytes;

	outStats.totalAcquire = acquire;
	outStats.totalRelease = release;
	outStats.allocatedCount = static_cast<uint32_t>(acquire - release);
	outStats.blocksInCache = cached;

	return true;
}

void TlsMemoryPool::LogStats(const char* poolName) const
{
	if (!m_initialized)
		return;

	const char* name = poolName ? poolName : "pool";
	const uint8_t ownerTag = m_registry.OwnerTag();

	for (uint32_t bin = 0; bin < m_table.binCount; ++bin)
	{
		BinStats stats;
		if (!GetBinStats(bin, stats))
			continue;

		// 아무 일도 없었던 빈은 건너뛴다. 로그가 의미 있는 줄만 남게 한다.
		if (stats.totalAcquire == 0 && stats.blockCount == 0)
			continue;

		const char* marker = (stats.allocatedCount != 0) ? " OUTSTANDING" : "";

		char committed[32] = {};
		FormatBytes(stats.committedBytes, committed, sizeof(committed));

		LOGI("[%s] pool %u bin %u : size %6u | created %6u  pool %6u  cache %6u  live %6u  peak %6u | acquire %llu  release %llu | growth %u  fail %u | cap %u batch %u | seg %u  committed %s%s",
			name, ownerTag, bin, stats.blockSize,
			stats.blockCount, stats.blocksInPool, stats.blocksInCache,
			stats.allocatedCount, stats.peakAllocated,
			stats.totalAcquire, stats.totalRelease,
			stats.growthCount, stats.acquireFailCount,
			stats.cacheCapBlocks, stats.batchBlocks,
			stats.segmentCount, committed, marker);
	}

	const LONG64 bypassAcquire = ::ReadAcquire64(&m_bypassAcquire);
	if (bypassAcquire != 0)
	{
		const LONG64 bypassRelease = ::ReadAcquire64(&m_bypassRelease);
		LOGI("[%s] pool %u bypass (over the largest bin) : acquire %lld  release %lld  live %lld",
			name, ownerTag, bypassAcquire, bypassRelease, bypassAcquire - bypassRelease);
	}
}
