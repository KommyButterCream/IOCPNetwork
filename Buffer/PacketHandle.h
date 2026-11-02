#pragma once

#include <cstddef> // for std::byte, std::size_t
#include <span>    // for std::span
#include <utility> // for std::move, std::exchange

#include "../Memory/TlsMemoryPool.h"

// 풀에서 받은 패킷 메모리를 단독 소유한다.
// 풀은 이 핸들보다 오래 살아 있어야 한다.
class PacketHandle final
{
private:
	TlsMemoryPool* m_pool = nullptr;
	std::byte* m_data = nullptr;
	std::size_t m_size = 0;

public:
	PacketHandle() noexcept = default;

	~PacketHandle() noexcept
	{
		Reset();
	}

	// 패킷 소유자는 하나여야 하므로
	// 복사 생성자 & 복사 대입 연산자 제거
	PacketHandle(const PacketHandle&) = delete;
	PacketHandle& operator=(const PacketHandle&) = delete;

	// 패킷 소유자는 하나여야 하므로
	// 이동 생성자 & 이동 대입 연산자만 구현
	PacketHandle(PacketHandle&& other) noexcept
		: m_pool(std::exchange(other.m_pool, nullptr))
		, m_data(std::exchange(other.m_data, nullptr))
		, m_size(std::exchange(other.m_size, 0))
	{

	}

	PacketHandle& operator=(PacketHandle&& other) noexcept
	{
		if (this != &other)
		{
			Reset();

			m_pool = std::exchange(other.m_pool, nullptr);
			m_data = std::exchange(other.m_data, nullptr);
			m_size = std::exchange(other.m_size, 0);
		}

		return *this;
	}

	// pool에서 획득한 메모리의 소유권을 인수한다.
	// 호출 후 기존 소유자는 이 메모리를 반환하면 안 된다.
	// data가 유효하면 size는 실제 할당 범위를 넘지 않아야 한다.
	[[nodiscard]]
	static PacketHandle Take(TlsMemoryPool& pool, void* data, std::size_t size) noexcept
	{
		PacketHandle packetHandle;

		if (data)
		{
			packetHandle.m_pool = &pool;
			packetHandle.m_data = static_cast<std::byte*>(data);
			packetHandle.m_size = size;
		}

		return packetHandle;
	}
	
	// 소유 중인 패킷을 반환하고 빈 상태가 된다.
	void Reset() noexcept
	{
		TlsMemoryPool* pool = std::exchange(m_pool, nullptr);
		std::byte* data = std::exchange(m_data, nullptr);
		m_size = 0;

		if (data)
		{
			pool->Release(data);
		}
	}

	[[nodiscard]]
	explicit operator bool() const noexcept
	{
		return m_data != nullptr;
	}

	[[nodiscard]]
	std::size_t Size() const noexcept
	{
		return m_size;
	}

	// View 는 소유권을 갖지 않는다.
	// 핸들이 메모리를 반환하면 이 뷰도 더 이상 사용할 수 없다.
	[[nodiscard]]
	std::span<const std::byte> View() const noexcept
	{
		if (!m_data)
		{
			return {};
		}

		return { m_data, m_size };
	}

	[[nodiscard]]
	std::span<std::byte> MutableView() noexcept
	{
		if (!m_data)
		{
			return {};
		}

		return { m_data, m_size };
	}
};