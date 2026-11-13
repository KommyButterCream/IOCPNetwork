#include "ReceiveProcessor.h"

#include "../ClientSession.h"
#include "../PacketTypes.h"
#include "IOWorkerPool.h"
#include "PacketPool.h"

#include <exception> // for std::terminate
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
                const auto result = session.RetryPendingReceivePacket();

                readPackets = result == ClientSession::ReceivedPacketEnqueueResult::Queued;
            }

            bool resumeReceive = false;

            if (readPackets)
            {
                resumeReceive = ReadPackets(session);
            }

            // 성공·포화·종료·예외 결과 모두 같은 후처리 종료 지점을 통과한다.
            const bool retryRequested = session.FinishReceiveCompletion(resumeReceive);

            if (!retryRequested)
            {
                return;
            }

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
                        session.BeginClose();

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
        catch (...)
        {
            // 시스템 처리 예외도 연결 종료로 바꾸고 공통 후처리로 돌아간다.
            session.BeginClose();

            return false;
        }
    }
}
