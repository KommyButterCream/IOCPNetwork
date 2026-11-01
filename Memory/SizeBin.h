#pragma once

#include <intrin.h>
#include <stdint.h>

#include "BlockLayout.h"

// 크기 구간(bin) 을 정의하고, 요청 크기를 빈 번호로 바꾼다.
// 상태가 없고 전부 inline 이다. 설정 해석이 이 파일 하나로 격리된다.
//
// 빈은 설정에서 자동으로 도출된다.
//   가장 작은 blockSize 를 담는 2의 거듭제곱이 0번,
//   가장 큰 blockSize 를 담는 2의 거듭제곱이 마지막 번호.
//
//   {64,N} .. {32768,N}  ->  64 128 256 512 1K 2K 4K 8K 16K 32K   (10개)
//   {128, N}             ->  128                                   (1개)
//   {64,N}, {32768,N}    ->  위와 같은 10개. 항목 수가 아니라 범위가 정한다.

namespace MemoryPoolDetail
{
	// 세그먼트 하나의 목표 크기. 빈의 stride 로 나누어 블록 수를 정한다.
	constexpr size_t SEGMENT_TARGET_BYTES = 256 * 1024;

	// 빈 하나가 스레드 캐시에서 차지할 수 있는 최대 바이트.
	//
	// 블록 개수가 아니라 바이트 상한이라는 점이 중요하다. 개수로 고정하면
	// (예: 전 빈 128개) 64KB 빈만으로 스레드당 8MB 를 잡아먹어서 L2 를 통째로
	// 밀어낸다. 캐시 적중률이 목적인데 그러면 역효과다.
	constexpr uint32_t CACHE_BYTES_PER_BIN = 64 * 1024;

	// 바이트 상한과 별개로 프리 리스트가 지나치게 길어지는 것을 막는다.
	constexpr uint32_t CACHE_CAP_BLOCK_LIMIT = 512;

	// 이 값보다 적게 담기는 빈은 스레드 캐시를 두지 않고 전역으로 직행한다.
	// 두세 개 쟁여봐야 분기 비용만 늘고 얻는 게 없다.
	constexpr uint32_t CACHE_CAP_MIN_BLOCKS = 4;

	// ChunkNode 를 블록 페이로드에 얹기 때문에 블록이 최소 이 크기는 되어야 한다.
	constexpr uint32_t MIN_BLOCK_SIZE = 32;

	// 지원하는 빈 최대 개수. 32B ~ 32B<<23 까지 커버한다.
	constexpr uint32_t MAX_BIN_COUNT = 24;

	// x64 가 아닌 구성에서도 컴파일되도록 감싼다.
	inline bool ScanReverse(unsigned long* index, size_t value)
	{
#if defined(_M_X64) || defined(_M_ARM64)
		return ::_BitScanReverse64(index, static_cast<unsigned __int64>(value)) != 0;
#else
		const unsigned long high = static_cast<unsigned long>(value >> 32);
		if (high != 0 && ::_BitScanReverse(index, high))
		{
			*index += 32;
			return true;
		}
		return ::_BitScanReverse(index, static_cast<unsigned long>(value)) != 0;
#endif
	}

	inline bool IsPowerOfTwo(uint32_t value)
	{
		return value != 0 && (value & (value - 1)) == 0;
	}

	// 2의 거듭제곱 올림 후의 지수. CeilLog2(64) == 6, CeilLog2(65) == 7
	inline uint32_t CeilLog2(size_t value)
	{
		if (value <= 1)
			return 0;

		unsigned long index = 0;
		ScanReverse(&index, value - 1);
		return static_cast<uint32_t>(index) + 1u;
	}

	// 빈 하나의 불변 상수. 초기화 때 정해지고 이후 바뀌지 않는다.
	struct BinSpec
	{
		uint32_t blockSize = 0;
		uint32_t stride = 0;             // 칸 하나의 총 크기 (헤더 포함)
		uint32_t cacheCapBlocks = 0;     // 0 이면 스레드 캐시를 두지 않는다
		uint32_t batchBlocks = 0;        // 전역 <-> 캐시 이동 단위
		uint32_t blocksPerSegment = 0;
		uint32_t reserved = 0;
	};

	// 빈 전체의 서술자. 값 타입이라 풀이 하나 들고 두 계층에 const 참조로 넘긴다.
	struct BinTable
	{
		uint32_t binCount = 0;
		uint32_t minBinLog = 0;
		uint32_t headerSize = 0;
		uint32_t payloadAlignment = 16;
		size_t   minMask = 0;
		BinSpec  specs[MAX_BIN_COUNT] = {};

		// 크기 -> 빈 번호. 메모리 접근이 없다.
		//
		// minMask 를 or 하기 때문에 입력이 0 이어도 BitScanReverse 가 안전하고,
		// 최소 빈보다 작은 요청은 자연스럽게 0번으로 접힌다.
		// 반환값이 binCount 이상이면 최대 빈을 넘은 요청이다.
		uint32_t BinOf(size_t size) const
		{
			unsigned long index = 0;
			ScanReverse(&index, (size - 1) | minMask);
			return static_cast<uint32_t>(index) + 1u - minBinLog;
		}

		const BinSpec& Spec(uint32_t bin) const { return specs[bin]; }
	};

	// 빈 하나의 상수를 blockSize 에서 유도한다.
	inline BinSpec MakeBinSpec(uint32_t blockSize, uint32_t headerSize, uint32_t alignment)
	{
		BinSpec spec;
		spec.blockSize = blockSize;
		spec.stride = static_cast<uint32_t>(AlignUpSize(headerSize + blockSize, alignment));

		uint32_t cap = CACHE_BYTES_PER_BIN / blockSize;
		if (cap > CACHE_CAP_BLOCK_LIMIT) cap = CACHE_CAP_BLOCK_LIMIT;
		if (cap < CACHE_CAP_MIN_BLOCKS)  cap = 0;

		spec.cacheCapBlocks = cap;

		// cap/4 는 히스테리시스 폭이다. Flush 후 3/4 cap 이 남아 있어야
		// 경계에서 Refill/Flush 를 반복하지 않는다.
		spec.batchBlocks = (cap >= CACHE_CAP_MIN_BLOCKS) ? (cap / 4) : 1;

		size_t blocks = SEGMENT_TARGET_BYTES / spec.stride;
		if (blocks < static_cast<size_t>(spec.batchBlocks) * 2)
			blocks = static_cast<size_t>(spec.batchBlocks) * 2;
		if (blocks < 2)
			blocks = 2;

		spec.blocksPerSegment = static_cast<uint32_t>(blocks);
		return spec;
	}
}
