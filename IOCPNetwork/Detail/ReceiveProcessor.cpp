#include "ReceiveProcessor.h"

#include "../ClientSession.h"
#include "../PacketTypes.h"
#include "IOWorkerPool.h"
#include "PacketPool.h"

#include <exception> // for std::terminate
#include <memory> // for std::shared_ptr
#include <new>       // for std::bad_alloc
#include <utility>   // for std::move

namespace IOCPNetwork::Detail
{
    ReceiveProcessor::ReceiveProcessor(
        std::shared_ptr<PacketPool> packetPool,
        SystemPacketHandler systemPacketHandler,
        void* systemContext) noexcept
        : m_packetPool(std::move(packetPool))
        , m_systemPacketHandler(systemPacketHandler)
        , m_systemContext(systemContext)
    {
        if (!m_packetPool || m_systemPacketHandler == nullptr)
        {
            std::terminate();
        }
    }

    void ReceiveProcessor::HandleReceive(ClientSession& session, const IOCompletion& completion) noexcept
    {
        // 새 데이터가 도착했을 때 호출된다.
        auto reference = session.BeginReceiveCompletion(completion);

        if (!reference)
        {
            return;
        }

        Process(session, std::move(reference), false);
    }

    void ReceiveProcessor::ResumeReceive(ClientSession& session) noexcept
    {
        // Job 큐에 여유가 생겨서 백프레셔가 풀렸을 때 호출된다.
        auto reference = session.TryBeginReceiveResume();

        if (!reference)
        {
            return;
        }

        Process(session, std::move(reference), true);
    }

    void ReceiveProcessor::Process(
        ClientSession& session,
        SessionReference reference,
        bool retryPending) noexcept
    {
        if (reference.Get() != &session)
        {
            std::terminate();
        }

        for (;;)
        {
            bool readPackets = true;

            if (retryPending)
            {
                // 재시도하는 경우라면 보관하던 Pending Packet 부터 처리한다.
                readPackets = session.RetryPendingReceivePacket();
            }

            bool resumeReceive = false;

            if (readPackets)
            {
                // Recv 버퍼로부터 패킷을 읽고 Job 을 만들어 JobQueue 에 넣는다.
                resumeReceive = ReadPackets(session);
            }

            // 이번 후처리를 마무리하고 다음 행동 결정
            // 성공·포화·종료·예외 결과 모두 같은 후처리 종료 지점을 통과한다.
            const bool retryRequested = session.FinishReceiveCompletion(resumeReceive);

            // 정상 종료 조건이 완성됐는지 검사
            session.TryShutdownSend();

            // Pending Packet 없어서 처리 해야 할 게 없으면 빠져나간다.
            if (!retryRequested)
            {
                return;
            }

            // Pending Packet 을 처리 해야 하므로 Recv 처리 권한을 다시 확보한다.
            // 다른 워커가 먼저 재개했으면 그 경로에 맡긴다.
            auto resumedReference = session.TryBeginReceiveResume();

            if (!resumedReference)
            {
                return;
            }

            // 새 참조가 확보된 상태에서 이전 후처리 참조를 교체한다.
            reference = std::move(resumedReference);

            retryPending = true;
        }
    }

    bool ReceiveProcessor::ReadPackets(ClientSession& session) noexcept
    {
        try
        {
            for (;;)
            {
                auto result = session.ReadReceivedPacket(*m_packetPool);

                switch (result.status)
                {
                case ClientSession::ReceivePacketStatus::NeedMoreData:
                {
                    return true;
                }

                case ClientSession::ReceivePacketStatus::Drained:
                {
                    // 정상 종료 기준의 수신 처리는 끝났다.
                    // 기존 Job·응답 송신은 유지하고 새 서비스 수신은 요청하지 않는다.
                    return false;
                }

                case ClientSession::ReceivePacketStatus::Paused:
                case ClientSession::ReceivePacketStatus::Stopped:
                case ClientSession::ReceivePacketStatus::Failed:
                {
                    return false;
                }

                case ClientSession::ReceivePacketStatus::Complete:
                {
                    break;
                }

                default:
                {
                    std::terminate();
                }
                }

                const PacketId packetId = result.packet.View().packetId;

                if (IsSystemPacketId(packetId))
                {
                    // 세션 락을 잡지 않은 상태에서 엔진 시스템 처리를 호출한다.
                    if (!m_systemPacketHandler(m_systemContext, session, std::move(result.packet)))
                    {
                        session.BeginClose(DisconnectInfo{ DisconnectReason::ProtocolError, 0 });

                        return false;
                    }

                    continue;
                }

                const auto enqueueResult = session.EnqueueReceivedPacket(std::move(result.packet));

                if (enqueueResult != ClientSession::ReceivedPacketEnqueueResult::Queued)
                {
                    return false;
                }
            }
        }
        catch (const std::bad_alloc&)
        {
            session.BeginClose(DisconnectInfo{ DisconnectReason::ResourceExhausted, 0 });

            return false;
        }
        catch (...)
        {
            // 엔진 시스템 처리 예외는 상대의 잘못된 패킷과 구분한다.
            session.BeginClose(DisconnectInfo{ DisconnectReason::TransportError, 0 });

            return false;
        }
    }
}
