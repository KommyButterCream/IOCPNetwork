#pragma once

#include <WinSock2.h> // for SOCKET, closesocket
#include <utility> // for std::exchange

// 소켓 하나를 소유하고 닫는다. Release로 소유권을 세션에 인계할 수 있다.
class UniqueSocket final
{
public:
	explicit UniqueSocket(SOCKET socket = INVALID_SOCKET) noexcept : m_socket(socket) {}
	~UniqueSocket() { Reset(); }

	UniqueSocket(const UniqueSocket&) = delete;
	UniqueSocket& operator=(const UniqueSocket&) = delete;
	UniqueSocket(UniqueSocket&& other) noexcept : m_socket(other.Release()) {}

	UniqueSocket& operator=(UniqueSocket&& other) noexcept
	{
		if (this != &other) Reset(other.Release());
		return *this;
	}
	void Reset(SOCKET socket = INVALID_SOCKET) noexcept
	{
		const SOCKET previous = std::exchange(m_socket, socket);
		if (previous != INVALID_SOCKET) ::closesocket(previous);
	}

	[[nodiscard]] SOCKET Get() const noexcept { return m_socket; }
	[[nodiscard]] SOCKET Release() noexcept { return std::exchange(m_socket, INVALID_SOCKET); }

private:
	SOCKET m_socket;
};
