#pragma once

#include "TlsMemoryPool.h"

#include "../../Core/Util/Logger.h"

#include "../Buffer/SendPacketEntry.h"
#include "../Buffer/PacketHandle.h"
#include <new> // for placement new
#include <memory> // for std::destroy_at

using namespace Core::Util;

namespace MEMORY_POOL
{
	// Utility helpers for Packet objects

	inline void* CreatePacket(TlsMemoryPool& pool, size_t size)
	{
		// 메모리 풀에서 크기에 맞는 메모리를 찾아서 반환
		void* memory = pool.Acquire(size);

		LOGT("packet acquire %p size %zu", memory, size);

		return memory;
	}

	[[nodiscard]]
	inline PacketHandle CreatePacketHandle(TlsMemoryPool& pool, size_t size)
	{
		// 메모리 풀에서 크기에 맞는 메모리를 찾아서 반환
		return PacketHandle::Take(pool, CreatePacket(pool, size), size);
	}

	inline void ReleasePacket(TlsMemoryPool& packetPool, const void* memory)
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
	// 전용 풀(SendPacketPool)이 하던 일을 TlsMemoryPool 이 대신한다.
	// 고갈 로그는 남기지 않는다 — 이 함수는 송신 패킷 하나당 호출되고,
	// 실패 원인(빈 고갈 / 크기 초과)은 Acquire 안에서 이미 남는다.
	inline SendPacketEntry* CreateSendPacketEntry(TlsMemoryPool& pool)
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

	inline void ReleaseSendPacketEntry(TlsMemoryPool& pool, SendPacketEntry* entry)
	{
		if (!entry)
			return;

		LOGT("send entry release %p", entry);

		std::destroy_at(entry);
		pool.Release(static_cast<const void*>(entry));
	}

}
