#pragma once

#include <stdint.h>

#include "../Buffer/PreDefine.h"

// 엔진이 기동할 때 만드는 메모리 풀 세 개의 설정.
//
// 왜 밖으로 꺼내는가
//   예전에는 빈 목록이 StartServer / StartClient 안에 상수로 박혀 있었다.
//   그러면 서비스가 자기 트래픽에 맞출 방법이 없다. 실제로 벌어진 일:
//
//     - 채팅형 서비스는 패킷의 99% 가 64~256 바이트인데, 32K 빈까지
//       전부 선할당해서 쓰지 않는 메모리를 들고 있었다.
//     - 반대로 클라 프리셋은 maxRecvPacketSize 를 65535 로 두면서 빈은
//       32K 까지만 있었다. 그 사이 크기는 풀을 통째로 우회해서 패킷마다
//       힙을 쳤고, 로그도 남지 않았다. (지금은 아래 커버리지 검사가 막는다)
//
//   두 경우 모두 "엔진이 모르는 것을 엔진이 정하고 있었다" 는 하나의 원인이다.
//
// 무엇을 열고 무엇을 닫는가
//   패킷 풀은 빈 목록을 통째로 연다. 담기는 것이 서비스의
//   데이터라 크기 분포를 아는 쪽이 서비스뿐이다.
//
//   잡 풀과 송신 큐 엔트리 풀은 개수와 상한만 연다. 그쪽 블록 크기는
//   sizeof(Job) / sizeof(SendPacketEntry) 로 엔진이 정하는 값이고,
//   서비스가 고를 수 있는 것처럼 보이게 두면 틀린 값을 넣을 자리만 생긴다.
//   (Job 은 alignas(64) 라 정렬 요구까지 따라붙는다)

// 빈 목록을 직접 정하는 풀의 설정. 패킷 풀이 쓴다.
struct PoolBinSetConfig
{
	// 빈 개수 상한.
	//
	// 풀 자체의 상한은 24개(32B ~ 32B<<23)지만, 그건 "범위" 가 만드는 빈 수고
	// 이건 "설정 항목" 수다. 항목은 범위를 정하는 양 끝과 선할당량을 주는
	// 용도라 그렇게 많이 필요하지 않다.
	static constexpr uint32_t MAX_ENTRIES = 16;

	struct Entry
	{
		// 이 크기를 담을 수 있는 2의 거듭제곱 빈을 만든다는 뜻이다.
		// 정확한 블록 크기가 아니다.
		uint32_t blockSize = 0;

		// 그 빈에 미리 만들어 둘 블록 수. 부족하면 런타임에 확장되고,
		// 그 사실은 LogStats 의 growth 값으로 드러난다.
		uint32_t blockCount = 0;
	};

	Entry    entries[MAX_ENTRIES] = {};
	uint32_t entryCount = 0;

	// 이 풀이 OS 에서 잡을 수 있는 총 바이트. 0 이면 무제한.
	// 튜닝 손잡이가 아니라 폭주 차단기다. (PreDefine.h 주석 참고)
	uint64_t commitLimitBytes = 0;

	// 항목을 하나 더한다. 가득 찼거나 값이 0 이면 아무것도 하지 않고 false.
	//
	// 반환값을 검사하지 않으면 조용히 빠진 빈이 생기므로, 프리셋도 검사한다.
	bool Add(uint32_t blockSize, uint32_t blockCount)
	{
		if (entryCount >= MAX_ENTRIES)
			return false;

		if (blockSize == 0 || blockCount == 0)
			return false;

		entries[entryCount].blockSize = blockSize;
		entries[entryCount].blockCount = blockCount;
		++entryCount;
		return true;
	}

	// 가장 큰 blockSize. 커버리지 검사가 이걸 본다.
	uint32_t LargestBlockSize() const
	{
		uint32_t largest = 0;
		for (uint32_t i = 0; i < entryCount; ++i)
		{
			if (entries[i].blockSize > largest)
				largest = entries[i].blockSize;
		}
		return largest;
	}

	bool IsValid() const
	{
		// 빈이 하나도 없는 풀은 모든 할당이 힙으로 빠진다. 풀을 만든 의미가 없다.
		if (entryCount == 0 || entryCount > MAX_ENTRIES)
			return false;

		for (uint32_t i = 0; i < entryCount; ++i)
		{
			if (entries[i].blockSize == 0 || entries[i].blockCount == 0)
				return false;
		}

		return true;
	}
};

// 블록 크기가 엔진에 고정된 풀의 설정. 잡 풀과 송신 큐 엔트리 풀이 쓴다.
struct PoolReserveConfig
{
	// 미리 만들어 둘 블록 수. 상한이 아니라 출발점이다.
	uint32_t blockCount = 1024;

	// 0 이면 무제한.
	uint64_t commitLimitBytes = 0;

	bool IsValid() const { return blockCount != 0; }
};

struct EnginePoolConfig
{
	// 수신 패킷과 송신 패킷이 담긴다. 서비스 트래픽의 크기 분포가 그대로 여기다.
	PoolBinSetConfig packet;

	// Job 객체. 블록 크기는 엔진이 sizeof(Job) 으로 정한다.
	PoolReserveConfig job;

	// SendPacketEntry. 블록 크기는 엔진이 sizeof(SendPacketEntry) 로 정한다.
	PoolReserveConfig sendQueue;

	bool IsValid() const
	{
		return packet.IsValid()
			&& job.IsValid() && sendQueue.IsValid();
	}
};

// 기본 프리셋. 서비스는 여기서 출발해 자기 값으로 조정하면 된다.
//
// SessionBufferPreset 과 같은 방침이다 — 엔진에 "MMORPG", "스트리밍" 같은
// 도메인 어휘를 넣지 않고 역할 이름만 쓴다.
namespace EnginePoolPreset
{
	// 서버: 제어 메시지를 받고, 큰 데이터를 민다.
	//
	// 이 값들은 예전에 StartServer 안에 박혀 있던 것 그대로다. 기본값을
	// 바꾸는 것은 이 작업의 목적이 아니라서 옮기기만 했다.
	inline EnginePoolConfig Server()
	{
		EnginePoolConfig config;

		config.packet.Add(64, 1024);
		config.packet.Add(128, 1024);
		config.packet.Add(256, 1024);
		config.packet.Add(512, 1024);
		config.packet.Add(MEMORY_SIZE_1K, 1024);
		config.packet.Add(MEMORY_SIZE_2K, 1024);
		config.packet.Add(MEMORY_SIZE_4K, 1024);
		config.packet.Add(MEMORY_SIZE_8K, 512);
		config.packet.Add(MEMORY_SIZE_16K, 512);
		config.packet.Add(MEMORY_SIZE_32K, 512);
		config.packet.commitLimitBytes = POOL_COMMIT_LIMIT_PACKET;

		config.job.blockCount = 1024;
		config.job.commitLimitBytes = POOL_COMMIT_LIMIT_JOB;

		config.sendQueue.blockCount = SEND_QUEUE_ENTRY_COUNT;
		config.sendQueue.commitLimitBytes = POOL_COMMIT_LIMIT_SENDQUEUE;

		return config;
	}

	// 클라이언트: 큰 데이터를 받고, 제어 메시지를 민다.
	//
	// 패킷 풀의 마지막 빈이 64K 인 것이 서버와 다르다. 클라 버퍼 프리셋의
	// maxRecvPacketSize 가 PACKET_SIZE_LIMIT(65535) 라서, 32K 에서 끊으면
	// 32769~65535 바이트 패킷이 전부 풀을 우회한다. 한 번 실제로 그랬다.
	//
	// 큰 빈의 개수가 적은 것은 의도다. 블록 하나가 비싸고(64K x 64 = 4MB)
	// 부족하면 런타임에 확장된다. growth 가 0 이 아니면 그때 늘리면 된다.
	inline EnginePoolConfig Client()
	{
		EnginePoolConfig config;

		config.packet.Add(64, 1024);
		config.packet.Add(128, 1024);
		config.packet.Add(256, 1024);
		config.packet.Add(512, 1024);
		config.packet.Add(MEMORY_SIZE_1K, 512);
		config.packet.Add(MEMORY_SIZE_2K, 512);
		config.packet.Add(MEMORY_SIZE_4K, 512);
		config.packet.Add(MEMORY_SIZE_8K, 256);
		config.packet.Add(MEMORY_SIZE_16K, 128);
		config.packet.Add(MEMORY_SIZE_32K, 128);
		config.packet.Add(MEMORY_SIZE_64K, 64);
		config.packet.commitLimitBytes = POOL_COMMIT_LIMIT_PACKET;

		config.job.blockCount = 1024;
		config.job.commitLimitBytes = POOL_COMMIT_LIMIT_JOB;

		config.sendQueue.blockCount = SEND_QUEUE_ENTRY_COUNT;
		config.sendQueue.commitLimitBytes = POOL_COMMIT_LIMIT_SENDQUEUE;

		return config;
	}
}
