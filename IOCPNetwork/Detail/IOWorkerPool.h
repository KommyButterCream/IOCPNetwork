#pragma once

#include "IOOperation.h"

#include <cstddef> // for std::size_t
#include <memory>  // for std::unique_ptr
#include <thread>  // for std::thread

namespace IOCPNetwork::Detail
{
    // 완료된 I/O 하나의 정보다. 완료 처리 함수 안에서만 사용한다.
    // operation은 실제 발행한 작업을 참조하며 소유권을 갖지 않는다.
    struct IOCompletion final
    {
        ULONG_PTR completionKey;

        IOOperation& operation;

        DWORD transferredBytes;

        // 성공이면 ERROR_SUCCESS, 실패이면 해당 I/O의 오류 코드다.
        DWORD errorCode;
    };

    // 하나의 IOCP와 그 완료를 기다리는 워커들을 소유한다.
    // 서비스 Job 실행은 JobScheduler가 담당한다.
    class IOWorkerPool final
    {
    public:
        // 엔진 내부의 완료 처리 함수. 여러 워커가 동시에 호출할 수 있다.
        // 함수 밖으로 예외를 전파하지 않으며 완료 정보를 보관하지 않는다.
        using CompletionHandler =
            void (*)(void* context, const IOCompletion& completion) noexcept;

        // IOCP와 워커 저장소를 확보한다. 스레드는 Start에서 시작한다.
        // 잘못된 설정이나 자원 확보 실패이면 nullptr를 반환한다.
        [[nodiscard]] static std::unique_ptr<IOWorkerPool> Create(
            std::size_t workerCount,
            CompletionHandler completionHandler,
            void* completionContext) noexcept;

        // 모든 I/O 완료 처리를 끝낸 뒤 파괴한다.
        // 남은 워커를 정지하고 IOCP 핸들을 닫는다.
        ~IOWorkerPool();

        IOWorkerPool(const IOWorkerPool&) = delete;
        IOWorkerPool& operator=(const IOWorkerPool&) = delete;

        IOWorkerPool(IOWorkerPool&&) = delete;
        IOWorkerPool& operator=(IOWorkerPool&&) = delete;

        // 워커를 시작한다. 중복 시작이나 부분 시작 실패이면 false를 반환한다.
        // 성공 반환 전에는 이 IOCP에 완료될 I/O를 발행하지 않는다.
        [[nodiscard]] bool Start() noexcept;

        // 새 I/O 발행과 기존 I/O 완료 처리를 모두 끝낸 뒤 호출한다.
        // 워커 밖에서 호출하며 모든 워커가 반환할 때까지 기다린다.
        void Stop() noexcept;

        // 소켓을 이 IOCP에 연결한다. 소켓 소유권은 가져오지 않는다.
        // completionKey는 0이 아니어야 하며 I/O 발행 전에 등록한다.
        [[nodiscard]] bool AssociateSocket(SOCKET socket, ULONG_PTR completionKey) noexcept;

    private:
#if defined(IOCPNETWORK_TESTING)
        friend struct IOWorkerTestAccess;
#endif

        IOWorkerPool(
            HANDLE completionPort,
            std::unique_ptr<std::thread[]> workers,
            std::size_t workerCount,
            CompletionHandler completionHandler,
            void* completionContext) noexcept;

        // 같은 IOCP에서 완료를 인출하여 엔진에 전달한다.
        // 종료용 완료 통지를 받으면 반환한다.
        void WorkerLoop() noexcept;

        // 이 객체가 단독 소유하며 워커 합류 후 닫는다.
        HANDLE m_completionPort = nullptr;

        // 생성 시 배열을 확보하고 Start에서 각 스레드를 실행한다.
        std::unique_ptr<std::thread[]> m_workers;

        const std::size_t m_workerCount;

        // 생성 시 확정한 엔진 내부 완료 전달 경로다.
        const CompletionHandler m_completionHandler;

        // 대상 객체는 모든 워커가 종료할 때까지 살아 있어야 한다.
        void* const m_completionContext;

        // 시작·정지를 담당하는 외부 제어 경로에서만 접근한다.
        // Start·Stop·파괴는 외부에서 직렬화하며 워커는 이 값을 읽지 않는다.
        bool m_running = false;
    };
}
