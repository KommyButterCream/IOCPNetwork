#include "IOWorkerPool.h"

#include "PacketPool.h"
#include "EngineExecutionScope.h"

#include <Windows.h> // for CreateIoCompletionPort, CloseHandle,
                     // GetQueuedCompletionStatus, GetLastError
                     // PostQueuedCompletionStatus
#include <exception> // for std::terminate
#include <limits>  // for std::numeric_limits
#include <memory>  // for std::make_unique, std::unique_ptr
#include <new>     // for std::bad_alloc
#include <utility> // for std::move
#include <system_error> // for std::system_error
#include <thread> // for std::thread, std::this_thread::get_id

namespace IOCPNetwork::Detail
{
#if defined(IOCPNETWORK_TESTING)
    namespace IOTestHooks
    {
        // 테스트 실행 파일에서만 제공하는 스레드 생성 실패 주입 지점이다.
        void BeforeWorkerStart(std::size_t index);
    }
#endif

    std::unique_ptr<IOWorkerPool> IOWorkerPool::Create(
        std::size_t workerCount,
        CompletionHandler completionHandler,
        void* completionContext,
        std::shared_ptr<PacketPool> packetPool) noexcept
    {
        // 워커·완료 처리 함수·캐시가 사용할 풀은 반드시 있어야 한다.
        if (workerCount == 0 ||
            completionHandler == nullptr ||
            !packetPool)
        {
            return {};
        }

        constexpr auto maxSize = (std::numeric_limits<std::size_t>::max)();

        constexpr auto maxConcurrency = (std::numeric_limits<DWORD>::max)();

        // 배열 크기 계산과 Windows API 인자 변환 범위를 확인한다.
        if (workerCount > maxSize / sizeof(std::thread) || workerCount > maxConcurrency)
        {
            return {};
        }

        HANDLE completionPort = nullptr;

        try
        {
            // 스레드 객체 저장소만 확보한다. 실행 스레드는 아직 없다.
            auto workers = std::make_unique<std::thread[]>(workerCount);

            // 소켓 연결 없이 새 IOCP를 생성한다.
            completionPort = ::CreateIoCompletionPort(
                INVALID_HANDLE_VALUE,              // 지금은 소켓을 연결하지 않음
                nullptr,                           // 기존 IOCP가 아닌 새 IOCP를 생성
                0,                                 // 지금은 소켓을 연결하지 않으므로 key를 사용하지 않음
                static_cast<DWORD>(workerCount));   // IOCP의 동시 실행 제한값

            if (completionPort == nullptr)
            {
                return {};
            }

            // 생성 성공 이후 IOCP와 워커 배열의 소유권은 반환 객체에 있다.
            return std::unique_ptr<IOWorkerPool>{
                new IOWorkerPool(
                    completionPort,
                    std::move(workers),
                    workerCount,
                    completionHandler,
                    completionContext,
                    std::move(packetPool))
            };
        }
        catch (const std::bad_alloc&)
        {
            // 객체 생성 실패 시 이미 확보한 IOCP도 정리한다.
            if (completionPort != nullptr)
            {
                ::CloseHandle(completionPort);
            }

            return {};
        }
    }

    IOWorkerPool::~IOWorkerPool()
    {
        // IOCP를 닫기 전에 완료 대기 중인 워커부터 모두 합류시킨다.
        Stop();

        if (m_completionPort != nullptr)
        {
            const BOOL closed = ::CloseHandle(m_completionPort);

            if (!closed)
            {
                std::terminate();
            }

            m_completionPort = nullptr;
        }
    }

    bool IOWorkerPool::Start() noexcept
    {
        // 실행 중인 워커를 새 스레드로 덮어쓰지 않는다.
        if (m_running)
        {
            return false;
        }

        // 정상적인 생성 경로에서는 IOCP와 워커 저장소가 존재한다.
        if (m_completionPort == nullptr || !m_workers)
        {
            std::terminate();
        }

        // 이전 실행의 워커가 모두 합류한 상태에서만 다시 시작한다.
        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            if (m_workers[index].joinable())
            {
                std::terminate();
            }
        }

        // 부분 시작 실패도 Stop의 공통 정리 경로를 사용하도록 먼저 설정한다.
        m_running = true;

        try
        {
            for (std::size_t index = 0; index < m_workerCount; ++index)
            {
#if defined(IOCPNETWORK_TESTING)
                IOTestHooks::BeforeWorkerStart(index);
#endif
                m_workers[index] = std::thread([this] { WorkerLoop(); });
            }

            return true;
        }
        catch (const std::system_error&)
        {
            // 스레드 생성 실패 시 앞서 시작한 워커도 모두 정지하고 합류한다.
            Stop();

            return false;
        }
        catch (const std::bad_alloc&)
        {
            // 스레드 실행 정보의 메모리 확보 실패도 같은 경로로 복구한다.
            Stop();

            return false;
        }
    }

    void IOWorkerPool::Stop() noexcept
    {
        // 생성 직후나 이미 정리된 상태에서는 할 일이 없다.
        if (!m_running)
        {
            return;
        }

        const auto callerId = std::this_thread::get_id();

        // 자기 자신을 join하는 호출은 허용하지 않는다.
        // 종료 통지를 넣기 전에 모든 워커를 먼저 확인한다.
        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            const auto& worker = m_workers[index];

            if (worker.joinable() && worker.get_id() == callerId)
            {
                std::terminate();
            }
        }

        // 부분 시작 실패를 고려해 실제 생성된 스레드 수만큼 통지한다.
        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            if (!m_workers[index].joinable())
            {
                continue;
            }

            const BOOL posted = ::PostQueuedCompletionStatus(
                m_completionPort,
                0,
                0,
                nullptr);

            // 통지에 실패한 채 join하면 대기 중인 워커가 끝나지 않을 수 있다.
            if (!posted)
            {
                std::terminate();
            }
        }

        // 모든 종료 통지를 넣은 뒤 각 워커가 반환할 때까지 기다린다.
        for (std::size_t index = 0; index < m_workerCount; ++index)
        {
            auto& worker = m_workers[index];

            if (worker.joinable())
            {
                worker.join();
            }
        }

        // 이후 Start에서 같은 IOCP와 스레드 저장소를 재사용할 수 있다.
        m_running = false;
    }

    bool IOWorkerPool::AssociateSocket(SOCKET socket, ULONG_PTR completionKey) noexcept
    {
        // 유효한 소켓과 실제 완료 대상을 나타내는 key만 등록한다.
        // key 0은 엔진의 제어용 통지를 위해 예약한다.
        if (socket == INVALID_SOCKET ||
            completionKey == 0 ||
            m_completionPort == nullptr)
        {
            return false;
        }

        // 기존 IOCP에 소켓을 연결한다. 새 IOCP를 생성하지 않는다.
        const HANDLE associatedPort = ::CreateIoCompletionPort(
            reinterpret_cast<HANDLE>(socket),
            m_completionPort,
            completionKey,
            0);

        return associatedPort == m_completionPort;
    }

    IOWorkerPool::IOWorkerPool(
        HANDLE completionPort,
        std::unique_ptr<std::thread[]> workers,
        std::size_t workerCount,
        CompletionHandler completionHandler,
        void* completionContext,
        std::shared_ptr<PacketPool> packetPool) noexcept
        : m_completionPort(completionPort)
        , m_workers(std::move(workers))
        , m_workerCount(workerCount)
        , m_completionHandler(completionHandler)
        , m_completionContext(completionContext)
        , m_packetPool(std::move(packetPool))
    {
    }

    void IOWorkerPool::WorkerLoop() noexcept
    {
        // 완료 처리와 콜백 실행 중 동기식 종료 호출을 판별한다.
        EngineExecutionScope executionScope;

        // 이 워커에서 생성·반환하는 패킷에 사용할 TLS 캐시다.
        // 종료 통지를 받고 반환하면 같은 스레드에서 자동 정리된다.
        PacketPoolCache packetCache{ m_packetPool };

        for (;;)
        {
            DWORD transferredBytes = 0;

            ULONG_PTR completionKey = 0;

            OVERLAPPED* overlapped = nullptr;

            const BOOL succeeded = ::GetQueuedCompletionStatus(
                m_completionPort,
                &transferredBytes,
                &completionKey,
                &overlapped,
                INFINITE);

            // 실패 원인은 다른 Windows API를 호출하기 전에 확보한다.
            const DWORD errorCode = succeeded ? ERROR_SUCCESS : ::GetLastError();

            if (overlapped == nullptr)
            {
                // Stop에서 등록할 종료용 통지는 성공·key 0·바이트 0으로 구분한다.
                if (succeeded &&
                    completionKey == 0 &&
                    transferredBytes == 0)
                {
                    return;
                }

                // 정상 종료에서는 워커 합류 전에 IOCP를 닫지 않는다.
                // 완료 대상 없는 대기 실패나 알 수 없는 제어 통지는 치명적 오류로 처리한다.
                std::terminate();
            }

            // 실제 I/O에는 AssociateSocket에서 등록한 유효한 key가 있어야 한다.
            if (completionKey == 0)
            {
                std::terminate();
            }

            // 발행 시 전달했던 첫 멤버 주소로 원래 작업을 찾는다.
            auto* operation = reinterpret_cast<IOOperation*>(overlapped);

            const IOCompletion completion{
                completionKey,
                *operation,
                transferredBytes,
                errorCode
            };

            // 실패·취소 완료도 전달하여 I/O 자원과 세션 참조를 정리한다.
            m_completionHandler(m_completionContext, completion);

            // 처리 함수에서 작업이 재발행되거나 세션이 반환됐을 수 있다.
            // 전달한 작업과 세션에는 여기서 다시 접근하지 않는다.
        }
    }
}
