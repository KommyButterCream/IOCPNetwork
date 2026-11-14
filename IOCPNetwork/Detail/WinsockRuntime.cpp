#include "WinsockRuntime.h"

#include <WinSock2.h> // for WSADATA, WSAStartup, WSACleanup, MAKEWORD, WSAVERNOTSUPPORTED

#include <exception> // for std::terminate

namespace IOCPNetwork::Detail
{
    WinsockRuntime::~WinsockRuntime()
    {
        Reset();
    }

    int WinsockRuntime::Initialize() noexcept
    {
        if (m_initialized)
        {
            return 0;
        }

        constexpr int STARTUP_SUCCESS = 0;

        WSADATA data{};

        const int errorCode = ::WSAStartup(MAKEWORD(2, 2), &data);

        if (errorCode != STARTUP_SUCCESS)
        {
            // 초기화 실패에는 WSACleanup 호출 책임이 생기지 않는다.
            return errorCode;
        }

        // WSAStartup 성공 시점부터 정리 책임을 소유한다.
        m_initialized = true;

        if (data.wVersion != MAKEWORD(2, 2))
        {
            // 요청한 버전을 사용할 수 없으면 이번 초기화를 되돌린다.
            Reset();

            return WSAVERNOTSUPPORTED;
        }

        return STARTUP_SUCCESS;
    }

    void WinsockRuntime::Reset() noexcept
    {
        if (!m_initialized)
        {
            return;
        }

        constexpr int CLEANUP_SUCCESS = 0;

        // 정상적인 초기화·정리 짝이 깨졌다면 내부 수명 관리 오류다.
        if (::WSACleanup() != CLEANUP_SUCCESS)
        {
            std::terminate();
        }

        m_initialized = false;
    }
}
