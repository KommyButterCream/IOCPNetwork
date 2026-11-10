#pragma once

namespace IOCPNetwork
{
    class ClientSession;

    namespace Detail
    {
        class ClientSessionPool;

        // 세션 사용 참조 하나를 소유하여 해당 슬롯의 재대여를 막는다.
        // 세션 객체를 삭제하지 않으며, 소멸할 때 사용 참조를 반환한다.

        class SessionReference final
        {
        public:
            // 세션을 참조하지 않는 빈 핸들을 만든다.
            SessionReference() noexcept = default;

            // 소유 중인 사용 참조를 반환한다.
            ~SessionReference();

            SessionReference(const SessionReference&) = delete;
            SessionReference& operator=(const SessionReference&) = delete;

            // 참조 카운트를 늘리지 않고 소유권을 이전한다.
            SessionReference(SessionReference&& other) noexcept;

            // 기존 참조를 반환하고 다른 핸들의 소유권을 이전받는다.
            SessionReference& operator=(SessionReference&& other) noexcept;

            // 사용 참조를 소유하고 있는지 확인한다.
            [[nodiscard]] explicit operator bool() const noexcept;

            // 보호 중인 세션을 빌린다. 빈 핸들이면 nullptr를 반환한다.
            [[nodiscard]] ClientSession* Get() const noexcept;

            // 사용 참조를 반환하고 빈 핸들이 된다.
            void Reset() noexcept;

        private:
            friend class IOCPNetwork::ClientSession;
            friend class ClientSessionPool;

            // 호출자가 이미 확보한 사용 참조 하나를 증가 없이 인수한다.
            explicit SessionReference(ClientSession* session) noexcept;

            ClientSession* m_session = nullptr;
        };
    }
}
