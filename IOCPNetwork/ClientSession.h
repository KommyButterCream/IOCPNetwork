#pragma once

#include "ConnectionId.h"
#include "Detail/SessionReference.h"

#include <atomic>  // for std::atomic
#include <cstddef> // for std::size_t
#include <mutex>   // for std::mutex

namespace IOCPNetwork
{
    namespace Detail
    {
        class ClientSessionPool;
    }

    // 연결 하나의 상태와 사용 수명을 관리한다.
    // 객체는 풀이 소유하며, 사용 참조는 해당 슬롯의 재대여를 막는다.
    class ClientSession final
    {
    public:
        ~ClientSession();

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        ClientSession(ClientSession&&) = delete;
        ClientSession& operator=(ClientSession&&) = delete;

        // 현재 연결 ID를 값으로 반환한다.
        // 호출자는 사용 참조 등으로 세션의 수명을 보호하고 있어야 한다.
        [[nodiscard]] ConnectionId GetConnectionId() const noexcept;

        // Closing으로 전환하고 연결 유지용 참조를 반환한다.
        // 호출자는 별도의 사용 참조를 보유해야 하며, 중복 호출은 무시한다.
        void BeginClose() noexcept;

    private:
        friend class Detail::ClientSessionPool;
        friend class Detail::SessionReference;

        // 세션 저장소는 풀만 생성한다.
        ClientSession() noexcept;

        // 연결의 수명 단계. 접근과 전환은 m_mutex로 보호한다.
        enum class State
        {
            Free,       // 정리 완료, 풀에서 대여 가능
            Preparing,  // 연결 준비·핸드셰이크 진행
            Ready,      // 일반 서비스 통신 가능
            Draining,   // 정상 종료를 위해 기존 작업과 송신을 처리 중
            Closing,    // 새 작업을 차단하고 기존 작업·I/O를 정리 중
            Finalizing  // 사용 참조 0, 최종 통지·정리 후 풀 반환 대기
        };

        // 기존 참조로 수명이 보호되는 경로에서 사용 참조 하나를 추가한다.
        // 새 작업의 허용 여부와 연결 ID 검증은 호출 경로에서 먼저 수행한다.
        [[nodiscard]] Detail::SessionReference AcquireReference() noexcept;

        // 사용 참조 하나를 반환한다.
        // 마지막 반환이면 Closing에서 Finalizing으로 전환한다.
        void ReleaseReference() noexcept;

        // 자신을 소유한 풀을 참조한다. 풀 생성 시 설정하고 재대여 때도 유지한다.
        // 풀의 수명은 모든 세션의 최종 마무리가 끝날 때까지 유지된다.
        Detail::ClientSessionPool* m_pool = nullptr;

        // 상태와 연결 정보를 보호한다. 풀과 함께 잠글 때는 풀 락을 먼저 잡는다.
        mutable std::mutex m_mutex;

        State m_state = State::Free;

        ConnectionId m_connectionId{};

        // 연결 유지·I/O·Job·조회 등 현재 연결을 사용하는 참조의 합계.
        std::atomic<std::size_t> m_referenceCount{ 0 };

        // 연결 유지용 참조 하나를 소유한다. Closing 전환 경로에서 락 밖으로 옮겨 반환한다.
        Detail::SessionReference m_connectionReference;
    };
}
