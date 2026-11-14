#pragma once

#include "SessionReference.h"
#include "../ConnectionId.h"
#include "SessionFinalization.h"

#include <chrono> // for std::chrono::milliseconds
#include <condition_variable> // for std::condition_variable
#include <cstddef> // for std::size_t
#include <cstdint> // for std::uint32_t
#include <memory>  // for std::unique_ptr
#include <mutex>   // for std::mutex
#include <vector>  // for std::vector

namespace IOCPNetwork
{
    class ClientSession;
    struct NetworkOptions;

    namespace Detail
    {
        class JobScheduler;

        // 세션 배열과 대여 가능한 인덱스를 소유한다.
        // 모든 세션의 사용·최종 정리가 끝난 뒤에만 풀을 파괴한다.
        class ClientSessionPool final
        {
        public:
            // 최종 마무리 책임을 인수하여 실행 경로에 전달한다.
            // 예외를 던지거나 항목을 버리지 않으며, 이후 반드시 Finish까지 진행한다.
            using FinalizationHandler = void (*)(void* context, SessionFinalization finalization) noexcept;

            // 세션 배열과 각 세션의 Job·송수신 저장소를 확보하고 최종 마무리 경로를 설정한다.
            // 설정이 잘못되었거나 메모리가 부족하면 nullptr를 반환한다.
            [[nodiscard]] static std::unique_ptr<ClientSessionPool> Create(
                std::uint32_t capacity,
                const NetworkOptions& options,
                FinalizationHandler finalizationHandler,
                void* finalizationContext,
                JobScheduler& scheduler) noexcept;

            ~ClientSessionPool();

            ClientSessionPool(const ClientSessionPool&) = delete;
            ClientSessionPool& operator=(const ClientSessionPool&) = delete;

            ClientSessionPool(ClientSessionPool&&) = delete;
            ClientSessionPool& operator=(ClientSessionPool&&) = delete;

            // 빈 세션에 새 연결 ID를 부여하고 초기화 실행용 참조를 반환한다.
            // 빈 슬롯이 없거나 연결 세대를 발급할 수 없으면 빈 핸들을 반환한다.
            [[nodiscard]] SessionReference AcquireSession() noexcept;

            // ID와 상태를 검증하면서 기존 연결의 사용 참조를 확보한다.
            // 일치하는 연결이 없거나 참조 획득이 허용되지 않으면 빈 핸들을 반환한다.
            [[nodiscard]] SessionReference TryAcquireSession(ConnectionId id) noexcept;

            // 모든 세션의 최종 마무리와 풀 반환을 기다린다. 새 대여 차단은 호출자가 담당한다.
            // 시간 초과는 false이며, 세션을 강제로 반환하거나 스케줄러를 정지하지 않는다.
            [[nodiscard]] bool WaitUntilAllReturned(std::chrono::milliseconds timeout) noexcept;

        private:
            friend class SessionFinalization;
            friend class IOCPNetwork::ClientSession;

            ClientSessionPool(
                std::unique_ptr<ClientSession[]> sessions,
                std::vector<std::uint32_t> freeIndices,
                std::uint32_t capacity,
                FinalizationHandler finalizationHandler,
                void* finalizationContext) noexcept;

            // 풀·세션 락을 잡지 않은 상태에서 최종 마무리 책임을 인계한다.
            void DispatchFinalization(SessionFinalization finalization) noexcept;

            // 최종 통지와 연결별 자원 정리가 끝난 세션을 반환한다.
            // 반환 이후 호출자는 해당 세션에 다시 접근하지 않는다.
            void ReturnSession(ClientSession& session, ConnectionId expectedId) noexcept;

            // 세션 객체의 주소는 풀의 수명 동안 바뀌지 않는다.
            std::unique_ptr<ClientSession[]> m_sessions;

            const std::uint32_t m_capacity;

            // 생성 시 확정한 전달 경로다. 문맥 대상은 모든 마무리가 끝날 때까지 유지한다.
            const FinalizationHandler m_finalizationHandler;

            void* const m_finalizationContext;

            // 대여·조회·반환을 보호하며, 세션 락보다 먼저 획득한다.
            std::mutex m_mutex;

            // 별도 카운터 없이 빈 인덱스 수로 반환 완료를 확인한다.
            std::condition_variable m_returnedCondition;

            // back()이 다음 대여할 인덱스다. 반환 시 push_back()으로 복원한다.
            std::vector<std::uint32_t> m_freeIndices;
        };
    }
}
