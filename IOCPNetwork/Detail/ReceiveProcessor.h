#pragma once

#include "../Packet.h"
#include "SessionReference.h"

#include <memory> // for std::shared_ptr

namespace IOCPNetwork
{
    class ClientSession;

    namespace Detail
    {
        class PacketPool;
        struct IOCompletion;

        // I/O 완료와 백프레셔 재개가 공유하는 패킷 처리 경로다.
        // 엔진이 소유하며 연결된 모든 세션과 워커보다 오래 유지한다.
        class ReceiveProcessor final
        {
        public:
            // 엔진 내부 시스템 패킷을 즉시 처리한다.
            // 계속 처리할 수 있으면 true, 연결을 종료해야 하면 false를 반환한다.
            using SystemPacketHandler = bool (*)(
                void* context,
                ClientSession& session,
                Packet packet);

            ReceiveProcessor(
                std::shared_ptr<PacketPool> packetPool,
                SystemPacketHandler systemPacketHandler,
                void* systemContext) noexcept;

            ReceiveProcessor(const ReceiveProcessor&) = delete;
            ReceiveProcessor& operator=(const ReceiveProcessor&) = delete;

            ReceiveProcessor(ReceiveProcessor&&) = delete;
            ReceiveProcessor& operator=(ReceiveProcessor&&) = delete;

            // 해당 세션에서 발행한 수신 I/O의 완료를 처리한다.
            void HandleReceive(ClientSession& session, const IOCompletion& completion) noexcept;

            // 호출자가 세션 참조를 유지한 상태에서 보관 패킷 재개를 시도한다.
            void ResumeReceive(ClientSession& session) noexcept;

        private:
            // 후처리용 참조를 소유하고 추출·정리·필요한 재시도를 연결한다.
            void Process(
                ClientSession& session,
                SessionReference reference,
                bool retryPending) noexcept;

            // 패킷을 순서대로 처리한다. 추가 수신이 필요한 때만 true를 반환한다.
            bool ReadPackets(ClientSession& session) noexcept;

            // 세션마다 공유 참조를 복사하지 않고 처리기 하나가 풀을 유지한다.
            const std::shared_ptr<PacketPool> m_packetPool;

            const SystemPacketHandler m_systemPacketHandler;

            // 모든 수신 처리가 끝날 때까지 대상 객체가 유지되어야 한다.
            void* const m_systemContext;
        };
    }
}
