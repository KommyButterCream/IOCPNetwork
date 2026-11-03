#pragma once

#include "TlsMemoryPool.h"

#include "../../Core/Util/Logger.h"

#include "../Buffer/PacketHandle.h"

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
}
