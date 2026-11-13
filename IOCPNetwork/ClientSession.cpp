#include "ClientSession.h"

#include "Detail/ClientSessionPool.h"
#include "Detail/IOWorkerPool.h"
#include "Detail/Job.h"
#include "Detail/JobScheduler.h"
#include "Detail/OptionsValidation.h"
#include "Detail/ReceiveProcessor.h"
#include "Detail/RecvPacketBuffer.h"
#include "Detail/SendPacketBatch.h"
#include "Detail/SendPacketQueue.h"
#include "Detail/SessionFinalization.h"
#include "Detail/SessionJobQueue.h"
#include "Detail/SessionReference.h"
#include "NetworkOptions.h"
#include "PacketTypes.h"

#include <algorithm> // for std::min
#include <exception> // for std::terminate
#include <limits>    // for std::numeric_limits
#include <mutex>     // for std::lock_guard, std::unique_lock
#include <utility>   // for std::move, std::in_place_type
#include <variant>   // for std::holds_alternative, std::get

namespace IOCPNetwork
{
#if defined(IOCPNETWORK_TESTING)
    namespace Detail::SessionStorageTestHooks
    {
        // 초기화 중 자원 정리를 검증하는 로컬 테스트 전용 실패 지점이다.
        bool FailStorageInitialization() noexcept;
    }

    namespace Detail::SessionSendTestHooks
    {
        // 테스트에서 완료량을 제어할 때만 실제 송신 발행을 대신한다.
        bool InterceptSend() noexcept;
    }
#endif

    ClientSession::ClientSession() noexcept = default;

    ClientSession::~ClientSession() = default;

    ConnectionId ClientSession::GetConnectionId() const noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        return m_connectionId;
    }

    void ClientSession::BeginClose() noexcept
    {
        Detail::SessionReference connectionReference;

        Detail::SocketHandle socket;

        Packet pendingReceivePacket;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            switch (m_state)
            {
            case State::Free:
            case State::Closing:
            case State::Finalizing:
            {
                return;
            }

            default:
            {
                break;
            }
            }

            // 종료 전 연결에는 연결 유지용 참조가 반드시 있어야 한다.
            if (!m_connectionReference)
            {
                std::terminate();
            }

            m_state = State::Closing;

            // Job에 접수하지 못한 완성 패킷을 종료 경로가 인수한다.
            pendingReceivePacket = std::move(m_pendingReceivePacket);

            // 종료된 연결에서는 보관 패킷 재개를 시도하지 않는다.
            m_receiveResumeRequested = false;

            // 새 I/O를 차단한 상태에서 소켓 닫기 책임을 인수한다.
            socket = std::move(m_socket);

            // 참조 수를 줄이지 않고 반환 책임만 지역 변수로 옮긴다.
            connectionReference = std::move(m_connectionReference);

            // 송신 I/O나 완료 후처리가 없으면 지금 저장소를 정리한다.
            // 진행 중이면 해당 완료·실패 처리 경로가 정리를 맡는다.
            if (!m_sendActive)
            {
                ClearPendingSendLocked();
            }
        }

        // 소켓을 닫아 대기 중인 I/O의 취소를 시작한다.
        // 완료 통지는 나중에 도착할 수 있으므로 I/O 참조는 유지한다.
        if (!socket.Close())
        {
            std::terminate();
        }

        // 풀 반환은 세션 락 밖에서 수행한다.
        pendingReceivePacket.Reset();

        // 마지막 참조 반환 경로가 세션 락을 사용할 수 있으므로 락 밖에서 반환한다.
        connectionReference.Reset();
    }

    bool ClientSession::InitializeJobQueue(
        std::size_t maxPendingJobs,
        std::size_t maxPendingPacketBytes,
        Detail::JobScheduler& scheduler) noexcept
    {
        // 공개되거나 이미 초기화된 세션의 실행 경로를 교체하지 않는다.
        if (m_pool != nullptr ||
            m_state != State::Free ||
            m_jobQueue ||
            m_scheduler != nullptr)
        {
            std::terminate();
        }

        auto jobQueue = Detail::SessionJobQueue::Create(maxPendingJobs, maxPendingPacketBytes);

        if (!jobQueue)
        {
            return false;
        }

        m_jobQueue = std::move(jobQueue);

        m_scheduler = &scheduler;

        return true;
    }

    bool ClientSession::InitializeIOStorage(const NetworkOptions& options) noexcept
    {
        // 공개된 세션이나 이미 저장소를 확보한 세션을 다시 초기화하지 않는다.
        if (m_pool != nullptr ||
            m_state != State::Free ||
            m_recvPacketBuffer ||
            m_sendPacketQueue ||
            m_sendPacketBatch)
        {
            std::terminate();
        }

        // 잘못된 설정은 메모리를 확보하기 전에 거절한다.
        if (Detail::ValidateOptions(options) != Detail::OptionsError::None)
        {
            return false;
        }

        auto recvPacketBuffer = Detail::RecvPacketBuffer::Create(
            options.recvRingSize, options.maxRecvPacketSize);

        if (!recvPacketBuffer)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        auto sendPacketQueue = Detail::SendPacketQueue::Create(
            options.maxPendingSendPackets);

        if (!sendPacketQueue)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        auto sendPacketBatch = Detail::SendPacketBatch::Create(
            options.maxSendBatchPackets, options.maxSendBatchBytes);

        if (!sendPacketBatch)
        {
            return false;
        }

#if defined(IOCPNETWORK_TESTING)
        if (Detail::SessionStorageTestHooks::FailStorageInitialization())
        {
            return false;
        }
#endif

        // 모든 저장소 확보가 성공한 뒤에만 세션 멤버에 소유권을 넘긴다.
        m_recvPacketBuffer = std::move(recvPacketBuffer);
        m_sendPacketQueue = std::move(sendPacketQueue);
        m_sendPacketBatch = std::move(sendPacketBatch);

        m_maxSendPacketSize = options.maxSendPacketSize;

        m_maxPendingSendPackets = options.maxPendingSendPackets;

        m_maxPendingSendBytes = options.maxPendingSendBytes;

        return true;
    }

    bool ClientSession::AttachSocket(
        Detail::SocketHandle&& socket,
        Detail::IOWorkerPool& ioWorkers,
        Detail::ReceiveProcessor& receiveProcessor) noexcept
    {
        if (!socket)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock{ m_mutex };

        // 풀에서 대여한 연결 준비 상태에서 한 번만 소켓을 인수한다.
        if (m_pool == nullptr || m_state != State::Preparing || m_socket)
        {
            return false;
        }

        // 소켓 인수 전에 Job과 송수신 저장소가 준비되어 있어야 한다.
        if (!m_jobQueue || !m_recvPacketBuffer || !m_sendPacketQueue || !m_sendPacketBatch)
        {
            return false;
        }

        // 이 소켓의 완료에서 현재 세션을 찾도록 세션 주소를 등록한다.
        const auto completionKey = reinterpret_cast<ULONG_PTR>(this);

        if (!ioWorkers.AssociateSocket(socket.Get(), completionKey))
        {
            return false;
        }

        // IOCP 등록에 성공한 뒤에만 소켓의 닫기 책임을 인수한다.
        m_socket = std::move(socket);

        m_receiveProcessor = &receiveProcessor;

        return true;
    }

    ClientSession::ReceivePostResult ClientSession::PostReceive() noexcept
    {
        // 락보다 먼저 선언하여 함수 반환 시 락이 풀린 뒤 참조가 반환되게 한다.
        Detail::SessionReference failedReference;

        std::unique_lock<std::mutex> lock{ m_mutex };

        // 핸드셰이크 준비 중이거나 서비스 통신 중인 연결만 새 수신을 허용한다.
        if ((m_state != State::Preparing && m_state != State::Ready) || !m_socket)
        {
            return ReceivePostResult::NotReady;
        }

        // 완료 후처리 중에도 새 수신을 발행하지 않는다.
        if (m_receiveActive)
        {
            return ReceivePostResult::AlreadyActive;
        }

        // 비활성 수신에 참조가 남아 있거나 저장소가 없으면 내부 상태 오류다.
        if (m_receiveReference || !m_recvPacketBuffer)
        {
            std::terminate();
        }

        // 앞선 패킷의 Job 접수가 끝나기 전에는 추가 수신을 시작하지 않는다.
        if (m_pendingReceivePacket)
        {
            return ReceivePostResult::Paused;
        }

        m_recvPacketBuffer->ResetOffsetsIfEmpty();

        const auto receiveSpan = m_recvPacketBuffer->GetReceiveSpan();

        if (receiveSpan.empty())
        {
            return ReceivePostResult::Paused;
        }

        // WSABUF 길이 필드에 표현 가능한 범위만 이번 수신에 사용한다.
        const auto receiveSize = (std::min)(receiveSpan.size(),
            static_cast<std::size_t>((std::numeric_limits<ULONG>::max)()));

        WSABUF buffer{};

        buffer.buf = reinterpret_cast<char*>(receiveSpan.data());

        buffer.len = static_cast<ULONG>(receiveSize);

        DWORD flags = 0;

        // 이전 수신과 완료 후처리가 끝난 상태에서만 작업 정보를 초기화한다.
        m_receiveOperation.overlapped = {};

        // 완료가 즉시 도착해도 세션을 사용할 수 있도록 발행 전에 확보한다.
        m_receiveReference = AcquireReference();

        m_receiveActive = true;
        m_receiveResumeRequested = false;

        const int result = ::WSARecv(
            m_socket.Get(),
            &buffer,
            1,
            nullptr,
            &flags,
            &m_receiveOperation.overlapped,
            nullptr);

        if (result == 0)
        {
            // 즉시 성공해도 실제 데이터 반영과 참조 반환은 IOCP 완료에서 수행한다.
            return ReceivePostResult::Posted;
        }

        const int errorCode = ::WSAGetLastError();

        if (errorCode == WSA_IO_PENDING)
        {
            return ReceivePostResult::Posted;
        }

        // 즉시 실패한 I/O에는 완료 통지가 오지 않는다.
        // 종료 전환까지 Active를 유지하여 다른 경로의 재개를 막는다.
        failedReference = std::move(m_receiveReference);

        lock.unlock();

        BeginClose();

        lock.lock();

        // 접수되지 않은 작업에는 완료 통지가 오지 않으므로 여기서 정리한다.
        m_receiveActive = false;

        m_receiveResumeRequested = false;

        return ReceivePostResult::Failed;
    }

    Detail::SessionReference ClientSession::BeginReceiveCompletion(const Detail::IOCompletion& completion) noexcept
    {
        // 완료 처리 중에는 지역 참조가 세션의 수명을 보호한다.
        Detail::SessionReference reference;

        bool closeRequired = false;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 이 세션에서 발행한 수신 작업의 완료인지 확인한다.
            if (completion.completionKey != reinterpret_cast<ULONG_PTR>(this) ||
                &completion.operation != &m_receiveOperation ||
                completion.operation.type != Detail::IOOperationType::Receive)
            {
                std::terminate();
            }

            // 접수된 수신 하나에는 대응하는 I/O 참조가 하나 있어야 한다.
            if (!m_receiveActive ||
                !m_receiveReference ||
                !m_recvPacketBuffer)
            {
                std::terminate();
            }

            // 참조 수는 바꾸지 않고 완료 후처리로 반환 책임을 넘긴다.
            reference = std::move(m_receiveReference);

            if (m_state == State::Closing)
            {
                // 종료 중 도착한 완료는 데이터를 처리하지 않고 참조를 정리한다.
                m_receiveActive = false;

                m_receiveResumeRequested = false;
            }
            else
            {
                // 수신 참조가 보호하는 연결은 이 상태들 중 하나여야 한다.
                if (m_state != State::Preparing &&
                    m_state != State::Ready &&
                    m_state != State::Draining)
                {
                    std::terminate();
                }

                if (completion.errorCode != ERROR_SUCCESS ||
                    completion.transferredBytes == 0)
                {
                    // 수신 실패 또는 상대의 정상적인 송신 종료다.
                    // 바이트 수를 버퍼에 반영하지 않고 연결 종료를 요청한다.
                    m_receiveResumeRequested = false;

                    closeRequired = true;
                }
                else if (!m_recvPacketBuffer->CommitReceivedBytes(completion.transferredBytes))
                {
                    // 완료 바이트 수가 버퍼의 수신 가능 범위와 맞지 않는다.
                    m_receiveResumeRequested = false;

                    closeRequired = true;
                }
                else
                {
                    // 패킷 추출 후처리가 끝날 때까지 Active는 유지한다.
                    // 반환한 참조가 이후 후처리 전체의 세션 수명을 보호한다.

                    m_receiveResumeRequested = false;

                    return reference;
                }
            }
        }

        // BeginClose와 마지막 참조 반환은 세션 락 밖에서 수행한다.
        if (closeRequired)
        {
            BeginClose();

            {
                std::lock_guard<std::mutex> lock{ m_mutex };

                // Closing 전환 후에만 수신 처리의 독점 상태를 해제한다.
                m_receiveActive = false;

                m_receiveResumeRequested = false;
            }
        }

        // 마지막 참조일 수 있으므로 세션 락 밖에서 반환한다.
        reference.Reset();

        return {};
    }

    ClientSession::ReceivePacketResult ClientSession::ReadReceivedPacket(Detail::PacketPool& packetPool) noexcept
    {
        try
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // I/O 참조를 지역으로 인수한 수신 후처리에서만 호출한다.
            if (!m_receiveActive || m_receiveReference || !m_recvPacketBuffer)
            {
                std::terminate();
            }

            if (m_state == State::Closing)
            {
                return { ReceivePacketStatus::Stopped, {} };
            }

            if (m_state != State::Preparing &&
                m_state != State::Ready &&
                m_state != State::Draining)
            {
                std::terminate();
            }

            // 앞선 패킷을 건너뛰고 다음 패킷을 추출하지 않는다.
            if (m_pendingReceivePacket)
            {
                return { ReceivePacketStatus::Paused, {} };
            }

            auto result = m_recvPacketBuffer->ExtractPacket(packetPool);

            switch (result.status)
            {
            case Detail::PacketExtractStatus::Complete:
            {
                return { ReceivePacketStatus::Complete, std::move(result.packet) };
            }

            case Detail::PacketExtractStatus::NeedMoreData:
            {
                return { ReceivePacketStatus::NeedMoreData, {} };
            }

            default:
            {
                // 잘못된 헤더나 메모리 부족이면 연결을 종료한다.
                break;
            }
            }
        }
        catch (...)
        {
            // 추출 중 예외도 연결 실패로 처리한다.
            // 세션 락은 이 지점에 도달하기 전에 해제된다.
        }

        // BeginClose는 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
        BeginClose();

        return { ReceivePacketStatus::Failed, {} };
    }

    ClientSession::ReceivedPacketEnqueueResult ClientSession::EnqueueReceivedPacket(Packet&& packet) noexcept
    {
        // 시스템 패킷은 별도 처리 경로로 보내야 한다.
        if (!packet || !IsServicePacketId(packet.View().packetId))
        {
            std::terminate();
        }

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 수신 후처리 하나만 패킷 추출과 접수를 진행해야 한다.
            if (!m_receiveActive ||
                m_receiveReference ||
                m_pendingReceivePacket)
            {
                std::terminate();
            }
        }

        // 완료 후처리용 참조와 별도로 Job이 사용할 참조를 확보한다.
        Detail::Job job{ AcquireReference(), std::move(packet) };

        const auto result = EnqueueJob(std::move(job));

        switch (result)
        {
        case EnqueueJobResult::Queued:
        {
            return ReceivedPacketEnqueueResult::Queued;
        }

        case EnqueueJobResult::QueueFull:
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 접수 실패 이후 종료가 시작됐다면 보관하지 않고 폐기한다.
            if (m_state != State::Ready)
            {
                return ReceivedPacketEnqueueResult::Stopped;
            }

            if (!m_receiveActive ||
                m_receiveReference ||
                m_pendingReceivePacket)
            {
                std::terminate();
            }

            // 접수에 실패한 Job은 원본 패킷을 그대로 소유하고 있다.
            m_pendingReceivePacket = std::move(std::get<Packet>(job.GetData()));

            return ReceivedPacketEnqueueResult::Paused;
        }

        case EnqueueJobResult::NotReady:
        {
            // 서비스 통신 준비 전에 받은 서비스 패킷은 허용하지 않는다.
            BeginClose();

            return ReceivedPacketEnqueueResult::Failed;
        }

        case EnqueueJobResult::Closed:
        {
            return ReceivedPacketEnqueueResult::Stopped;
        }
        }

        std::terminate();
    }

    Detail::SessionReference ClientSession::TryBeginReceiveResume() noexcept
    {
        std::lock_guard<std::mutex> lock{ m_mutex };

        if (m_state != State::Ready)
        {
            return {};
        }

        if (m_receiveActive)
        {
            // 아직 보관 슬롯이 채워지기 전일 수도 있으므로 요청부터 기억한다.
            m_receiveResumeRequested = true;

            return {};
        }

        if (!m_pendingReceivePacket)
        {
            return {};
        }

        // 비활성 상태에서는 발행된 수신 I/O의 참조가 남아 있으면 안 된다.
        if (m_receiveReference || !m_recvPacketBuffer)
        {
            std::terminate();
        }

        // 재개 후처리가 끝날 때까지 세션 수명과 수신 처리의 독점 상태를 유지한다.
        auto reference = AcquireReference();

        m_receiveActive = true;

        // 이번 재개 시도가 이전 요청을 인수한다.
        m_receiveResumeRequested = false;

        return reference;
    }

    ClientSession::ReceivedPacketEnqueueResult ClientSession::RetryPendingReceivePacket() noexcept
    {
        // 함수가 끝날 때 패킷이 남아 있어도 세션 락 밖에서 반환한다.
        Packet packet;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 재개 후처리의 독점 상태를 확보한 경로에서만 호출한다.
            if (!m_receiveActive || m_receiveReference)
            {
                std::terminate();
            }

            // 독점 상태를 확보한 이후에도 다른 경로에서 종료를 요청할 수 있다.
            if (m_state != State::Ready)
            {
                return ReceivedPacketEnqueueResult::Stopped;
            }

            if (!m_pendingReceivePacket)
            {
                std::terminate();
            }

            // 기존 접수 함수는 보관 슬롯이 빈 상태에서 패킷을 받는다.
            packet = std::move(m_pendingReceivePacket);
        }

        // 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
        return EnqueueReceivedPacket(std::move(packet));
    }

    bool ClientSession::FinishReceiveCompletion(bool resumeReceive) noexcept
    {
        bool retryPending = false;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            // 완료 후처리 중에는 I/O 참조가 멤버가 아닌 호출자에게 있어야 한다.
            if (!m_receiveActive || m_receiveReference)
            {
                std::terminate();
            }

            // 후처리 중 들어온 재개 요청을 상태 해제와 같은 락에서 소비한다.
            retryPending =
                m_state == State::Ready &&
                static_cast<bool>(m_pendingReceivePacket) &&
                m_receiveResumeRequested;

            m_receiveResumeRequested = false;

            m_receiveActive = false;
        }

        if (retryPending)
        {
            // 보관 패킷을 먼저 처리해야 하므로 새 수신부터 발행하지 않는다.
            return true;
        }

        if (resumeReceive)
        {
            // 상태·미접수 패킷·중복 발행 여부를 다시 확인한다.
            // 즉시 발행 실패의 종료 처리는 PostReceive가 담당한다.
            static_cast<void>(PostReceive());
        }

        return false;
    }

    SendResult ClientSession::EnqueueSendPacket(Packet&& packet) noexcept
    {
        if (!packet)
        {
            return SendResult::InvalidPacket;
        }

        const PacketId packetId = packet.View().packetId;

        const bool isSystemPacket = IsSystemPacketId(packetId);

        if (!isSystemPacket && !IsServicePacketId(packetId))
        {
            return SendResult::InvalidPacket;
        }

        const std::size_t packetSize = Detail::GetPacketWireBytes(packet).size();

        std::unique_lock<std::mutex> lock{ m_mutex };

        switch (m_state)
        {
        case State::Preparing:
        {
            // 서비스 통신 준비 전에는 엔진 시스템 패킷만 허용한다.
            if (!isSystemPacket)
            {
                return SendResult::NotReady;
            }

            break;
        }

        case State::Ready:
        {
            break;
        }

        default:
        {
            return SendResult::Closed;
        }
        }

        if (!m_socket)
        {
            return SendResult::NotReady;
        }

        // 통신 가능한 세션은 송신 저장소 초기화가 완료되어 있어야 한다.
        if (!m_sendPacketQueue || !m_sendPacketBatch)
        {
            std::terminate();
        }

        if (packetSize < PacketHeaderSize || packetSize > m_maxSendPacketSize)
        {
            return SendResult::InvalidPacket;
        }

        // 사용량이 한도를 넘었다면 내부 예산 관리 오류다.
        if (m_pendingSendPackets > m_maxPendingSendPackets ||
            m_pendingSendBytes > m_maxPendingSendBytes)
        {
            std::terminate();
        }

        if (m_pendingSendPackets == m_maxPendingSendPackets ||
            packetSize > m_maxPendingSendBytes - m_pendingSendBytes)
        {
            return SendResult::QueueFull;
        }

        // Enqueue는 접수에 성공한 경우에만 Packet을 실제로 이동한다.
        const SendResult result = m_sendPacketQueue->Enqueue(std::move(packet));

        if (result != SendResult::Queued)
        {
            return result;
        }

        // 소유권 이전에 성공한 패킷만 전체 송신 사용량에 반영한다.
        ++m_pendingSendPackets;

        m_pendingSendBytes += packetSize;

        // PostSend가 같은 세션 락을 사용하므로 먼저 해제한다.
        lock.unlock();

        // 즉시 발행 실패의 종료·정리는 PostSend가 담당한다.
        // 이미 송신 중이면 기존 완료 경로가 다음 발행을 이어간다.
        static_cast<void>(PostSend());

        // 이미 Packet을 인수했으므로 이후 발행 결과와 무관하게 Queued다.
        return SendResult::Queued;
    }

    ClientSession::SendPostResult ClientSession::PostSend() noexcept
    {
        // 락보다 먼저 선언하여 락이 풀린 뒤 실패한 I/O 참조를 반환한다.
        Detail::SessionReference failedReference;

        std::unique_lock<std::mutex> lock{ m_mutex };

        // 정상 종료 중에도 이미 접수한 송신은 계속 처리할 수 있다.
        if ((m_state != State::Preparing &&
            m_state != State::Ready &&
            m_state != State::Draining) ||
            !m_socket)
        {
            return SendPostResult::NotReady;
        }

        // 완료 후처리까지 끝나기 전에는 다른 송신을 발행하지 않는다.
        if (m_sendActive)
        {
            return SendPostResult::AlreadyActive;
        }

        if (m_sendReference || !m_sendPacketQueue || !m_sendPacketBatch)
        {
            std::terminate();
        }

        auto buffers = m_sendPacketBatch->GetBuffers();

        if (buffers.empty())
        {
            const auto buildResult = m_sendPacketBatch->BuildFrom(*m_sendPacketQueue);

            switch (buildResult)
            {
            case Detail::SendBatchBuildResult::Ready:
            {
                break;
            }

            case Detail::SendBatchBuildResult::Empty:
            {
                return SendPostResult::Empty;
            }

            default:
            {
                // 빈 배치에 구성해야 하며, 허용된 패킷은 배치에 들어가야 한다.
                std::terminate();
            }
            }

            buffers = m_sendPacketBatch->GetBuffers();
        }

        if (buffers.empty())
        {
            std::terminate();
        }

        // 이전 송신과 완료 후처리가 끝난 상태에서만 초기화한다.
        m_sendOperation.overlapped = {};

        // 완료가 즉시 도착해도 세션과 배치가 유지되도록 먼저 확보한다.
        m_sendReference = AcquireReference();

        m_sendActive = true;

        const int result =
#if defined(IOCPNETWORK_TESTING)
            Detail::SessionSendTestHooks::InterceptSend() ? 0 :
#endif
            ::WSASend(
            m_socket.Get(),
            buffers.data(),
            static_cast<DWORD>(buffers.size()),
            nullptr,
            0,
            &m_sendOperation.overlapped,
            nullptr);

        if (result == 0)
        {
            // 즉시 성공해도 송신량 반영과 참조 반환은 IOCP 완료에서 수행한다.
            return SendPostResult::Posted;
        }

        const int errorCode = ::WSAGetLastError();

        if (errorCode == WSA_IO_PENDING)
        {
            return SendPostResult::Posted;
        }

        // 즉시 실패한 I/O에는 완료 통지가 오지 않는다.
        // 반환 책임을 인수하되 종료 전환까지 Active는 유지한다.
        failedReference = std::move(m_sendReference);

        lock.unlock();

        BeginClose();

        lock.lock();

        // Closing 전환이 끝났고, 운영체제가 사용 중인 송신도 없다.
        m_sendActive = false;

        ClearPendingSendLocked();

        return SendPostResult::Failed;
    }

    void ClientSession::HandleSendCompletion(const Detail::IOCompletion& completion) noexcept
    {
        // 완료된 I/O의 참조를 인수하여 함수가 끝날 때까지 세션을 보호한다.
        Detail::SessionReference reference;

        bool closeRequired = false;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (completion.completionKey != reinterpret_cast<ULONG_PTR>(this) ||
                &completion.operation != &m_sendOperation ||
                completion.operation.type != Detail::IOOperationType::Send)
            {
                std::terminate();
            }

            if (!m_sendActive ||
                !m_sendReference ||
                !m_sendPacketBatch ||
                !m_sendPacketQueue)
            {
                std::terminate();
            }

            // 참조 수를 변경하지 않고 반환 책임만 지역 변수로 옮긴다.
            reference = std::move(m_sendReference);

            if (m_state == State::Closing)
            {
                // 종료 중 도착한 완료는 새 송신으로 이어가지 않는다.
                m_sendActive = false;

                ClearPendingSendLocked();

                return;
            }

            if (m_state != State::Preparing &&
                m_state != State::Ready &&
                m_state != State::Draining)
            {
                std::terminate();
            }

            if (completion.errorCode != ERROR_SUCCESS ||
                completion.transferredBytes == 0)
            {
                closeRequired = true;
            }
            else
            {
                const std::size_t bytesSent = completion.transferredBytes;

                // 전체 예산보다 큰 완료량은 배치에도 반영하지 않는다.
                const auto result = bytesSent <= m_pendingSendBytes
                    ? m_sendPacketBatch->CommitSentBytes(bytesSent)
                    : Detail::SendBatchCommitResult{};

                if (!result.succeeded)
                {
                    closeRequired = true;
                }
                else
                {
                    if (result.completedPackets > m_pendingSendPackets)
                    {
                        std::terminate();
                    }

                    // 바이트는 부분 완료도 반환하고, 개수는 완성된 패킷만 반환한다.
                    m_pendingSendBytes -= bytesSent;

                    m_pendingSendPackets -= result.completedPackets;
                }
            }

            if (!closeRequired)
            {
                // 배치와 예산 반영이 끝난 뒤 다음 송신을 허용한다.
                m_sendActive = false;
            }

            // 실패한 경우에는 종료 상태로 바뀔 때까지 Active를 유지한다.
        }

        if (closeRequired)
        {
            // 세션 락 밖에서 종료한다. 지역 참조가 세션의 수명을 보호한다.
            BeginClose();

            {
                std::lock_guard<std::mutex> lock{ m_mutex };

                // 실패 완료도 도착했으므로 배치와 대기 큐를 정리할 수 있다.
                m_sendActive = false;

                ClearPendingSendLocked();
            }

            return;
        }

        // 부분 송신이면 기존 배치부터, 모두 완료됐으면 다음 배치를 발행한다.
        // 즉시 발행 실패의 종료·정리는 PostSend가 담당한다.
        static_cast<void>(PostSend());
    }

    void ClientSession::ClearPendingSendLocked() noexcept
    {
        // 운영체제가 사용 중이거나 완료 후처리 중인 배치는 비우지 않는다.
        if (m_state != State::Closing ||
            m_sendActive ||
            m_sendReference ||
            !m_sendPacketQueue ||
            !m_sendPacketBatch)
        {
            std::terminate();
        }

        m_sendPacketQueue->Clear();

        m_sendPacketBatch->Clear();

        m_pendingSendPackets = 0;

        m_pendingSendBytes = 0;
    }

    ClientSession::EnqueueJobResult ClientSession::EnqueueJob(Detail::Job&& job, SessionTask* sourceTask) noexcept
    {
        // 다른 세션의 Job이나 빈 참조를 이 세션에 등록하지 않는다.
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        Detail::JobEnqueueResult result;

        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (!m_jobQueue || m_scheduler == nullptr)
            {
                std::terminate();
            }

            switch (m_state)
            {
            case State::Preparing:
            {
                // 서비스 준비를 확정하는 연결 통지를 먼저 등록한다.
                if (!isConnectNotify)
                {
                    return EnqueueJobResult::NotReady;
                }

                break;
            }

            case State::Ready:
            {
                // 같은 연결의 준비 완료 통지를 두 번 등록하는 것은 내부 오류다.
                if (isConnectNotify)
                {
                    std::terminate();
                }

                break;
            }

            default:
            {
                return EnqueueJobResult::Closed;
            }
            }

            if (sourceTask != nullptr)
            {
                result = m_jobQueue->Enqueue(std::move(job), *sourceTask);
            }
            else
            {
                result = m_jobQueue->Enqueue(std::move(job));
            }

            if (result.status == Detail::JobEnqueueStatus::QueueFull)
            {
                // Preparing에서는 일반 Job을 받지 않아 통지용 공간이 있어야 한다.
                if (isConnectNotify)
                {
                    std::terminate();
                }

                return EnqueueJobResult::QueueFull;
            }

            if (isConnectNotify)
            {
                // 연결 통지 접수 이력은 최종 종료 통지까지 유지한다.
                m_connectedNotificationQueued = true;

                // 통지가 FIFO에 들어간 뒤에만 일반 작업 접수를 허용한다.
                m_state = State::Ready;
            }
        }

        if (result.readySession)
        {
            Detail::ReadyItem item{ std::in_place_type<Detail::SessionReference>, std::move(result.readySession) };

            // 세션 락을 놓은 뒤 실행 책임을 스케줄러에 전달한다.
            m_scheduler->Enqueue(std::move(item));
        }

        return EnqueueJobResult::Queued;
    }

    bool ClientSession::TryBeginJob(const Detail::Job& job) noexcept
    {
        if (job.GetSession() != this)
        {
            std::terminate();
        }

        const bool isConnectNotify = std::holds_alternative<Detail::ConnectedNotification>(job.GetData());

        std::lock_guard<std::mutex> lock{ m_mutex };

        switch (m_state)
        {
        case State::Ready:
        case State::Draining:
        {
            // 이미 접수된 작업은 정상 종료의 처리 범위에 포함된다.
            return true;
        }

        case State::Closing:
        {
            // 확정된 연결 통지는 유지하고 일반 Packet·Task는 취소한다.
            return isConnectNotify;
        }

        default:
        {
            // 접수된 Job이 보호하는 세션은 이 상태에 있을 수 없다.
            std::terminate();
        }
        }
    }

    Detail::SessionReference ClientSession::AcquireReference() noexcept
    {
        std::size_t count = m_referenceCount.load(std::memory_order_relaxed);

        for (;;)
        {
            // 기존 연결의 참조를 0에서 되살리거나 카운터가 넘치게 할 수는 없다.
            if (count == 0 || count == (std::numeric_limits<std::size_t>::max)())
            {
                std::terminate();
            }

            // 값이 같아도 실패할 수 있으므로 성공할 때까지 다시 시도한다.
            if (m_referenceCount.compare_exchange_weak(
                count,
                count + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
            {
                return Detail::SessionReference{ this };
            }
        }
    }

    void ClientSession::ReleaseReference() noexcept
    {
        const std::size_t previous = m_referenceCount.fetch_sub(1, std::memory_order_acq_rel);

        // 소유하지 않은 참조를 반환한 내부 오류이다.
        if (previous == 0)
        {
            std::terminate();
        }

        if (previous != 1)
        {
            return;
        }

        Detail::SessionFinalization finalization;

        Detail::ClientSessionPool* pool = nullptr;

        // 마지막 사용 참조를 반환한 경로만 최종 마무리 상태로 전환한다.
        {
            std::lock_guard<std::mutex> lock{ m_mutex };

            if (m_state != State::Closing ||
                m_pool == nullptr ||
                m_connectionReference)
            {
                // 종료 상태·소속 풀·연결 유지용 참조가 마지막 반환 조건과 일치해야 한다.
                std::terminate();
            }

            m_state = State::Finalizing;

            pool = m_pool;

            // 참조를 다시 늘리지 않고 최종 마무리 책임을 만든다.
            finalization = Detail::SessionFinalization{
                *pool,
                *this,
                m_connectionId
            };
        }

        // 최종 항목의 전달은 세션 락을 해제한 뒤 수행한다.
        pool->DispatchFinalization(std::move(finalization));

        // 인계 후 같은 슬롯이 재대여될 수 있으므로 멤버에 다시 접근하지 않는다.
    }

    void ClientSession::ResetForReuseLocked() noexcept
    {
        // 살아 있는 참조나 I/O를 초기화로 숨기지 않는다.
        if (m_state != State::Finalizing ||
            m_referenceCount.load(std::memory_order_relaxed) != 0 ||
            m_connectionReference ||
            m_socket ||
            m_receiveReference ||
            m_sendReference ||
            m_receiveActive ||
            m_sendActive ||
            m_receiveResumeRequested ||
            m_pendingReceivePacket)
        {
            std::terminate();
        }

        if (!m_jobQueue ||
            !m_recvPacketBuffer ||
            !m_sendPacketQueue ||
            !m_sendPacketBatch)
        {
            std::terminate();
        }

        // 최종 반환 시 실행 예약과 미반환 예산이 남아 있으면 내부 오류다.
        if (!m_jobQueue->IsIdle())
        {
            std::terminate();
        }

        // 송신 자원은 앞선 종료 경로에서 이미 정리되어 있어야 한다.
        if (m_pendingSendPackets != 0 ||
            m_pendingSendBytes != 0 ||
            m_sendPacketQueue->GetFrontPacketSize() != 0 ||
            !m_sendPacketBatch->GetBuffers().empty())
        {
            std::terminate();
        }

        // 이전 연결의 불완전한 헤더·본문을 버린다. 버퍼 할당은 유지한다.
        m_recvPacketBuffer->Clear();

        // 이전 I/O의 완료 정보를 제거한다. 작업 종류와 객체 주소는 유지한다.
        m_receiveOperation.overlapped = {};

        m_sendOperation.overlapped = {};

        // 종료 통지가 끝났으므로 이전 연결 정보를 초기화한다.
        m_connectionId = {};

        m_connectedNotificationQueued = false;

        m_receiveProcessor = nullptr;
    }
}

namespace IOCPNetwork::Detail
{
    void DispatchSessionCompletion(void*, const IOCompletion& completion) noexcept
    {
        // Accept·Connect 완료는 이 진입점에서 처리하지 않는다.
        if (completion.operation.type != IOOperationType::Receive &&
            completion.operation.type != IOOperationType::Send)
        {
            std::terminate();
        }

        if (completion.completionKey == 0)
        {
            std::terminate();
        }

        auto* session = reinterpret_cast<ClientSession*>(completion.completionKey);

        switch (completion.operation.type)
        {
        case IOOperationType::Receive:
        {
            ReceiveProcessor* receiveProcessor = nullptr;

            {
                std::lock_guard<std::mutex> lock{ session->m_mutex };

                receiveProcessor = session->m_receiveProcessor;
            }

            if (receiveProcessor == nullptr)
            {
                std::terminate();
            }

            // 처리기 내부에서 세션 락을 사용하므로 락 밖에서 호출한다.
            receiveProcessor->HandleReceive(*session, completion);

            return;
        }

        case IOOperationType::Send:
        {
            session->HandleSendCompletion(completion);

            return;
        }

        default:
        {
            std::terminate();
        }
        }
    }
}
