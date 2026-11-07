#pragma once

#include <WinSock2.h> // for SOCKET, closesocket
#include <utility> // for std::exchange

// 연결을 준비하는 동안 소켓을 소유하고, 세션이 인수하기 전의 실패를 정리한다.
class UniqueSocket final
{
public:
	explicit UniqueSocket(SOCKET socket) noexcept : m_socket(socket) {}
	~UniqueSocket()
	{
		if (m_socket != INVALID_SOCKET)
			::closesocket(m_socket);
	}

	UniqueSocket(const UniqueSocket&) = delete;
	UniqueSocket& operator=(const UniqueSocket&) = delete;
	UniqueSocket(UniqueSocket&& other) noexcept : m_socket(other.Release()) {}

	[[nodiscard]] SOCKET Get() const noexcept { return m_socket; }
	[[nodiscard]] SOCKET Release() noexcept { return std::exchange(m_socket, INVALID_SOCKET); }

private:
	SOCKET m_socket;
};
