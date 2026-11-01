#pragma once

#include "EngineMemoryPool.h"

#include "../../Core/Util/Logger.h"

#include "../Buffer/SendPacketEntry.h"
#include "../Job/Job.h"
#include "../Protocol/PacketHeader.h"
#include "../Protocol/PacketID.h"

#include <new>

using namespace Core::Util;

// 주의
// 이 파일의 함수들은 패킷/Job 하나당 호출된다.
// 즉 초당 수십만~수백만 번 실행되는 경로이므로 여기에 로그를 남기면
// 그것만으로 처리량이 한 자리 수로 떨어진다.
// (실측: 로그 억제 221k pkt/s -> 로그 켜짐 13k pkt/s)
//
// 그래서 개별 할당/해제는 LOGT(릴리스에서 컴파일 제거) 로만 남기고,
// 실제 운영 지표는 EngineMemoryPool 이 누적하는 카운터로 본다.
//   EngineMemoryPool::LogStats() -> 슬랩별 peak / 확장 횟수 / 미반환 수
// 실패 원인 로그도 EngineMemoryPool::Acquire 안에서 남긴다.

namespace MEMORY_POOL
{
	// Utility helpers for Packet objects

	inline void* CreatePacket(EngineMemoryPool& pool, size_t size)
	{
		// 메모리 풀에서 크기에 맞는 메모리를 찾아서 반환
		void* memory = pool.Acquire(size);

		LOGT("packet acquire %p size %zu", memory, size);

		return memory;
	}

	inline void ReleasePacket(EngineMemoryPool& packetPool, const void* memory)
	{
		// 사용이 끝난 메모리를 메모리풀에 반환
		if (!memory)
			return;

		LOGT("packet release %p", memory);

		packetPool.Release(memory);
	}

	// Utility helpers for SendPacketEntry objects

	// 송신 큐 엔트리 하나를 받아 온다.
	//
	// 전용 풀(SendPacketPool)이 하던 일을 EngineMemoryPool 이 대신한다.
	// 고갈 로그는 남기지 않는다 — 이 함수는 송신 패킷 하나당 호출되고,
	// 실패 원인(빈 고갈 / 크기 초과)은 Acquire 안에서 이미 남는다.
	inline SendPacketEntry* CreateSendPacketEntry(EngineMemoryPool& pool)
	{
		void* memory = pool.Acquire(sizeof(SendPacketEntry));
		if (!memory)
			return nullptr;

		LOGT("send entry acquire %p", memory);

		// 기본 멤버 초기자로 전부 지운다. 풀에서 나온 블록은 앞선 사용자의
		// 값을 그대로 들고 있으므로, 지우지 않으면 next 가 이미 반납된
		// 엔트리를 가리킨 채 큐에 들어간다.
		return new (memory) SendPacketEntry();
	}

	inline void ReleaseSendPacketEntry(EngineMemoryPool& pool, SendPacketEntry* entry)
	{
		if (!entry)
			return;

		LOGT("send entry release %p", entry);

		pool.Release(static_cast<const void*>(entry));
	}

	// Utility helpers for Job objects

	inline Job* CreateJob(EngineMemoryPool& pool)
	{
		// Job 메모리 버퍼를 버퍼풀로부터 반환
		void* memory = pool.Acquire(sizeof(Job));

		if (!memory)
		{
			// 실패의 구체적 원인(슬랩 고갈 / 크기 초과)은 Acquire 가 남긴다.
			LOGE("failed to acquire a Job block (size %zu)", sizeof(Job));
			return nullptr;
		}

		LOGT("job acquire %p", memory);

		return static_cast<Job*>(memory);
	}

	inline void ReleaseJob(EngineMemoryPool& pool, Job* job)
	{
		// 사용이 끝난 Job 메모리 버퍼를 버퍼풀에 반환
		if (!job)
		{
			LOGE("ReleaseJob called with nullptr");
			return;
		}

		LOGT("job release %p", job);

		pool.Release(static_cast<const void*>(job));
	}
}
