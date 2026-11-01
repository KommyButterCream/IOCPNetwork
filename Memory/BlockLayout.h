#pragma once

#include <stdint.h>

// 블록 한 칸이 메모리에서 어떤 모양인지만 정의한다.
// 상태도 없고 동기화도 없다. 전부 inline 이므로 어느 계층에서 불러도 비용이 없다.
//
//   세그먼트를 stride 간격으로 자른 칸 하나
//
//   +----------------------+---------------------------------+
//   |  BlockHeader         |  payload (호출부가 받는 주소)     |
//   |  headerSize 바이트    |  blockSize 바이트                |
//   +----------------------+---------------------------------+
//   ^                      ^
//   칸의 시작               payloadAlignment 로 정렬된다
//
// headerSize 는 sizeof(BlockHeader) 가 아니라 정렬 크기만큼 올림한 값이다.
// 헤더를 정렬 크기로 부풀려야 페이로드 주소가 정렬을 만족한다.
// (Job 처럼 alignas(64) 인 타입을 담는 풀은 헤더가 64바이트가 된다)

namespace MemoryPoolDetail
{
	// 살아있는 블록과 반환된 블록을 다른 값으로 표시해서 이중 해제를 그 자리에서 잡는다.
	constexpr uint32_t MAGIC_LIVE = 0xA110C8EDu;
	constexpr uint32_t MAGIC_FREE = 0xF2EEB10Cu;

	// 최대 빈을 넘어 OS 로 우회한 블록 표시
	constexpr uint8_t BIN_BYPASS = 0xFFu;

	// 빠른 슬롯을 받지 못한 풀의 블록 표시. 이 값이면 소유 풀 검사를 건너뛴다.
	constexpr uint8_t POOL_ID_NONE = 0xFFu;

	// 페이로드 바로 앞에 놓이는 헤더.
	// Release 는 이 헤더만 읽어서 크기 인자 없이 O(1) 로 원래 빈을 찾는다.
	struct BlockHeader
	{
		uint32_t magic;         // MAGIC_LIVE / MAGIC_FREE
		uint8_t  binIndex;      // BIN_BYPASS 면 OS 우회 블록
		uint8_t  poolId;        // 다른 풀에 반납하는 실수를 잡는다
		uint16_t reserved;
		BlockHeader* next;      // 프리 리스트 링크. 우회 블록에서는 HeapAlloc 원본 주소
	};

	static_assert(sizeof(BlockHeader) == 16, "block header must stay 16 bytes");

	inline size_t AlignUpSize(size_t value, size_t alignment)
	{
		return (value + (alignment - 1)) & ~(alignment - 1);
	}

	inline uint32_t HeaderSizeFor(uint32_t payloadAlignment)
	{
		return static_cast<uint32_t>(AlignUpSize(sizeof(BlockHeader), payloadAlignment));
	}

	inline void* PayloadOf(BlockHeader* header, uint32_t headerSize)
	{
		return reinterpret_cast<char*>(header) + headerSize;
	}

	inline BlockHeader* HeaderOf(const void* payload, uint32_t headerSize)
	{
		return reinterpret_cast<BlockHeader*>(
			const_cast<char*>(static_cast<const char*>(payload)) - headerSize);
	}

	// 스레드 캐시가 빈마다 하나씩 갖는 프리 리스트.
	// 사슬 자체는 BlockHeader::next 에 있으므로 여기엔 머리와 개수만 둔다.
	struct FreeList
	{
		BlockHeader* head;
		uint32_t count;
		uint32_t reserved;
	};

	static_assert(sizeof(FreeList) == 16, "free list must stay 16 bytes so the hot array is compact");
}
