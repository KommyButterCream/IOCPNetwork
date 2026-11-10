#pragma once

#include "../ConnectionId.h"

namespace IOCPNetwork
{
    class ClientSession;

    namespace Detail
    {
        class ClientSessionPool;

        // Finalizing 세션의 최종 마무리 책임을 단독 소유한다.
        // 사용 참조를 증가시키지 않으며, 정리 후 Finish로 풀에 반환한다.

        class SessionFinalization final
        {
        public:
            // 마무리할 세션이 없는 빈 객체를 만든다.
            SessionFinalization() noexcept = default;

            // 미완료 책임을 버리면 내부 오류로 처리한다.
            ~SessionFinalization();

            SessionFinalization(const SessionFinalization&) = delete;
            SessionFinalization& operator=(const SessionFinalization&) = delete;

            // 최종 마무리 책임을 이전하고 원본을 비운다.
            SessionFinalization(SessionFinalization&& other) noexcept;

            // 빈 목적지에만 책임을 이전한다. 미완료 책임을 덮어쓰지 않는다.
            SessionFinalization& operator=(SessionFinalization&& other) noexcept;

            // 최종 마무리 책임을 보유하는지 확인한다.
            [[nodiscard]] explicit operator bool() const noexcept;

            // 최종 정리 대상 세션을 빌린다. 빈 객체이면 nullptr를 반환한다.
            [[nodiscard]] ClientSession* Get() const noexcept;

            // 최종 통지와 자원 정리가 끝난 뒤 풀에 반환하고 자신을 비운다.
            // 반환 이후에는 빌려둔 세션 포인터에 접근하지 않는다.
            void Finish() noexcept;

        private:
            friend class IOCPNetwork::ClientSession;

            // 마지막 참조 반환 경로가 Finalizing 전환 후 생성한다.
            SessionFinalization(
                ClientSessionPool& pool,
                ClientSession& session,
                ConnectionId id) noexcept;

            ClientSessionPool* m_pool = nullptr;
            ClientSession* m_session = nullptr;

            ConnectionId m_connectionId{};
        };
    }
}
