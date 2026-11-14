#pragma once

#include "IOOperation.h"
#include "SocketHandle.h"

#include <WinSock2.h> // for sockaddr_storage

#include <array>   // for std::array
#include <cstddef> // for std::byte, std::size_t

namespace IOCPNetwork::Detail
{
    class IOCPServerImpl;

    // Accept 한 건의 소켓·I/O 작업·주소 버퍼를 보관한다.
    // 서버가 배열로 소유하며, 완료 처리 후 같은 객체를 재사용한다.
    class AcceptSession final
    {
    public:
        // 저장소만 초기화한다. 소켓 생성과 Accept 발행은 별도로 수행한다.
        AcceptSession() noexcept = default;

        // 서버가 모든 Accept 완료 처리를 끝낸 뒤 파괴해야 한다.
        ~AcceptSession() = default;

        AcceptSession(const AcceptSession&) = delete;
        AcceptSession& operator=(const AcceptSession&) = delete;

        AcceptSession(AcceptSession&&) = delete;
        AcceptSession& operator=(AcceptSession&&) = delete;

    private:
        friend class IOCPServerImpl;

        // 서버의 m_acceptMutex로 보호한다.
        enum class State
        {
            Idle,         // 최초 발행 전 또는 종료로 재등록하지 않는 상태
            Active,       // I/O 발행부터 완료 후처리까지 진행 중
            RetryPending  // 발행 실패로 지연 재시도를 기다리는 상태
        };

        State m_state = State::Idle;

        // AcceptEx가 주소 하나를 기록할 공간이다.
        // 실제 주소 저장소 외에 Windows가 요구하는 16바이트를 확보한다.
        static constexpr std::size_t AddressBufferSize = sizeof(sockaddr_storage) + 16;

        // Accept 완료를 구분하며, 발행 후 완료 처리까지 주소를 유지한다.
        IOOperation m_acceptOperation{ IOOperationType::Accept };

        // 접속 수락에 사용할 소켓의 닫기 책임을 소유한다.
        // 성공 시 ClientSession으로 이동하고 다른 수락용 소켓을 준비한다.
        SocketHandle m_acceptSocket;

        // 로컬·원격 주소를 위한 출력 버퍼다. 초기 수신 데이터는 받지 않는다.
        std::array<std::byte, AddressBufferSize * 2> m_addressBuffer{};
    };
}
