#pragma once

#include <WinSock2.h> // for SOCKET, INVALID_SOCKET, closesocket

#include <exception> // for std::terminate
#include <utility>   // for std::exchange

namespace IOCPNetwork::Detail
{
    // 소켓 하나의 닫기 책임을 단독 소유한다.
    // Winsock 초기화가 유지되는 범위에서 사용하며 동시 접근은 호출자가 보호한다.
    class SocketHandle final
    {
    public:
        SocketHandle() noexcept = default;

        // 전달받은 소켓의 닫기 책임을 인수한다.
        // 호출자는 이후 같은 소켓을 직접 닫지 않는다.
        explicit SocketHandle(SOCKET socket) noexcept
            : m_socket(socket)
        {
        }

        ~SocketHandle()
        {
            if (!Close())
            {
                std::terminate();
            }
        }

        SocketHandle(const SocketHandle&) = delete;
        SocketHandle& operator=(const SocketHandle&) = delete;

        SocketHandle(SocketHandle&& other) noexcept
            : m_socket(other.Detach())
        {
        }

        SocketHandle& operator=(SocketHandle&& other) noexcept
        {
            if (this != &other)
            {
                // 기존 소켓을 정리한 뒤 새 소켓의 소유권을 인수한다.
                if (!Close())
                {
                    std::terminate();
                }

                m_socket = other.Detach();
            }

            return *this;
        }

        // 소유권 이전 없이 Windows API에 전달할 소켓 값을 빌린다.
        [[nodiscard]] SOCKET Get() const noexcept
        {
            return m_socket;
        }

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return m_socket != INVALID_SOCKET;
        }

        // 소켓을 닫지 않고 닫기 책임을 호출자에게 넘긴다.
        [[nodiscard]] SOCKET Detach() noexcept
        {
            return std::exchange(m_socket, INVALID_SOCKET);
        }

        // 소켓을 닫고 비운다. 이미 비어 있으면 성공한다.
        // 닫기에 실패하면 소유권을 유지하고 false를 반환한다.
        [[nodiscard]] bool Close() noexcept
        {
            if (m_socket == INVALID_SOCKET)
            {
                return true;
            }

            if (::closesocket(m_socket) == SOCKET_ERROR)
            {
                return false;
            }

            m_socket = INVALID_SOCKET;

            return true;
        }

    private:
        SOCKET m_socket = INVALID_SOCKET;
    };
}
