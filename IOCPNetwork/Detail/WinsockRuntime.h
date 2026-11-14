#pragma once

namespace IOCPNetwork::Detail
{
    // 성공한 Winsock 초기화 한 번에 대한 정리 책임을 소유한다.
    // 초기화·정리 호출은 엔진 시작·종료 경로에서 직렬화한다.
    class WinsockRuntime final
    {
    public:
        WinsockRuntime() noexcept = default;

        // 초기화 책임이 남아 있으면 반환한다.
        ~WinsockRuntime();

        WinsockRuntime(const WinsockRuntime&) = delete;
        WinsockRuntime& operator=(const WinsockRuntime&) = delete;

        WinsockRuntime(WinsockRuntime&&) = delete;
        WinsockRuntime& operator=(WinsockRuntime&&) = delete;

        // Winsock 2.2를 초기화한다. 성공은 0, 실패는 Winsock 오류 코드다.
        // 이미 초기화된 경우에는 추가 참조 없이 성공한다.
        [[nodiscard]] int Initialize() noexcept;

        // 성공한 초기화의 정리 책임을 반환한다. 반복 호출은 무시한다.
        // 모든 소켓·I/O·Winsock 사용 작업이 끝난 뒤 호출해야 한다.
        void Reset() noexcept;

    private:
        // 이 객체가 WSACleanup 호출 책임을 소유하는지 나타낸다.
        bool m_initialized = false;
    };
}
