#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>

#include "PacketHandle.h"

#include "../Memory/TlsMemoryPool.h"

#include <memory>  // for std::construct_at, std::destroy_at
#include <utility> // for std::exchange, std::move, std::swap
#include <span>    // for std::span
#include <cstddef> // for std::byte, std::size_t

class ClientSession;

class SharedPacketHandle final
{

private:
	struct Control
	{
		volatile LONG refCount = 1;
		TlsMemoryPool* selfPool = nullptr;
		PacketHandle packetHandle;
	};

public:
	SharedPacketHandle() noexcept = default;

	~SharedPacketHandle() noexcept
	{
		Reset();
	}

	SharedPacketHandle(const SharedPacketHandle& other) noexcept
		: m_control(other.m_control)
	{
		AddRef(m_control);
	}

	SharedPacketHandle& operator=(const SharedPacketHandle& other) noexcept
	{
		SharedPacketHandle copy(other);
		std::swap(m_control, copy.m_control);

		return *this;
	}

	SharedPacketHandle(SharedPacketHandle&& other) noexcept
		: m_control(std::exchange(other.m_control, nullptr))
	{

	}

	SharedPacketHandle& operator=(SharedPacketHandle&& other) noexcept
	{
		if (this != &other)
		{
			Reset();
			m_control = std::exchange(other.m_control, nullptr);
		}

		return *this;
	}

	// 성공·실패 모두 packet의 소유권을 인수한다.
	// 실패하면 전달된 패킷은 자동 반환한다.
	[[nodiscard]]
	static SharedPacketHandle Create(TlsMemoryPool& controlPool, PacketHandle packetHandle)
	{
		SharedPacketHandle result;

		if (!packetHandle)
		{
			return result;
		}

		void* memory = controlPool.Acquire(sizeof(Control));
		if (!memory)
		{
			return result;
		}

		Control* control = std::construct_at(static_cast<Control*>(memory));

		control->selfPool = &controlPool;
		control->packetHandle = std::move(packetHandle);

		result.m_control = control;

		return result;
	}

	void Reset() noexcept
	{
		Release(std::exchange(m_control, nullptr));
	}

	[[nodiscard]]
	explicit operator bool() const noexcept
	{
		return m_control != nullptr;
	}

	[[nodiscard]]
	std::size_t Size() const noexcept
	{
		return m_control ? m_control->packetHandle.Size() : 0;
	}

	[[nodiscard]]
	std::span<const std::byte> View() const noexcept
	{
		return m_control ? m_control->packetHandle.View() : std::span<const std::byte>{};
	}

private:
	static void AddRef(Control* control) noexcept
	{
		if (control)
		{
			::InterlockedIncrement(&control->refCount);
		}
	}

	static void Release(Control* control) noexcept
	{
		if (!control)
		{
			return;
		}

		if (::InterlockedDecrement(&control->refCount) != 0)
		{
			return;
		}

		TlsMemoryPool* pool = control->selfPool;

		// Control 의 PacketHandle 소멸자가 패킷을 반환한다.
		std::destroy_at(control);

		pool->Release(control);
	}

	static void ReleaseCallback(const void*, void* context) noexcept
	{
		Release(static_cast<Control*>(context));
	}

	void DetachForQueue() noexcept
	{
		m_control = nullptr;
	}

private:
	friend class ClientSession;
	Control* m_control = nullptr;
};
