#pragma once

#include "TlsMemoryPool.h"

#include "../../Core/Util/Logger.h"

#include "../Buffer/PacketHandle.h"
#include <memory> // for std::construct_at
#include <type_traits> // for std::is_trivially_copyable_v, std::is_default_constructible_v

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

	// 풀의 기본 정렬 안에 들어가는 단순 패킷을 버퍼에 직접 생성한다.
	// 별도 소멸 처리가 필요한 타입은 받지 않으며, 생성 예외 시에도 핸들이 버퍼를 반환한다.
	template<class Packet>
		requires (std::is_trivially_copyable_v<Packet> && std::is_default_constructible_v<Packet> && alignof(Packet) <= 16)
	[[nodiscard]] inline PacketHandle CreatePacketHandle(TlsMemoryPool& pool)
	{
		auto packet = CreatePacketHandle(pool, sizeof(Packet));
		if (packet)
			std::construct_at(reinterpret_cast<Packet*>(packet.MutableView().data()));
		return packet;
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
