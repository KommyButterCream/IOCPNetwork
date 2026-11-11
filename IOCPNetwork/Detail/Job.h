#pragma once

#include "../Packet.h"
#include "../SessionTask.h"
#include "SessionReference.h"

#include <memory>  // for std::unique_ptr
#include <variant> // for std::variant, std::monostate, std::get

namespace IOCPNetwork::Detail
{
    // 서비스의 연결 콜백을 실행해야 한다는 표식이다.
    struct ConnectedNotification final
    {
    };

    // 빈 상태, 연결 통지, 수신 패킷, 서비스 작업 중 하나를 보관한다.
    using JobData = std::variant<
        std::monostate,                    // 빈 Job 데이터
        ConnectedNotification,            // 연결 콜백 실행 요청
        Packet,                           // 수신 패킷 처리 요청
        std::unique_ptr<SessionTask>>;      // Post()로 받은 서비스 작업

    // 작업 데이터와 세션 사용 참조를 함께 소유한다.
    // 복사 없이 큐와 실행 배치 사이에서 이동한다.
    class Job final
    {
    public:
        // 원형 배열의 비어 있는 슬롯을 만든다.
        Job() noexcept = default;

        // 작업 데이터를 먼저 정리하고 세션 참조를 반환한다.
        ~Job() = default;

        Job(const Job&) = delete;
        Job& operator=(const Job&) = delete;

        // 연결 통지와 이미 확보한 세션 참조를 인수한다.
        Job(SessionReference&& session, ConnectedNotification notification) noexcept;

        // 수신 패킷과 이미 확보한 세션 참조를 인수한다.
        Job(SessionReference&& session, Packet&& packet) noexcept;

        // Task 저장소와 이미 확보한 세션 참조를 인수한다.
        // 접수 준비 단계에서는 저장소의 함수가 비어 있어도 된다.
        Job(SessionReference&& session, std::unique_ptr<SessionTask>&& task) noexcept;

        // 참조 카운트 증가 없이 작업 소유권을 이전한다.
        Job(Job&& other) noexcept;

        // 기존 작업을 정리한 뒤 다른 작업의 소유권을 이전받는다.
        Job& operator=(Job&& other) noexcept;

        // 작업이 보호 중인 세션을 빌린다. 빈 Job이면 nullptr을 반환한다.
        [[nodiscard]] ClientSession* GetSession() const noexcept;

        // 실행 또는 소유권 이전을 위해 작업 데이터에 접근한다.
        [[nodiscard]] JobData& GetData() noexcept;

        // 작업 종류와 접수 예산을 확인하기 위해 데이터를 읽는다.
        [[nodiscard]] const JobData& GetData() const noexcept;

        // 작업 데이터만 정리하고 세션 참조는 유지한다.
        void ClearData() noexcept;

        // 작업 데이터를 먼저 정리한 뒤 세션 참조를 반환한다.
        void Reset() noexcept;

    private:
        // 데이터 정리가 끝날 때까지 세션을 보호한다.
        SessionReference m_session;

        // 멤버 소멸 역순에 따라 세션 참조보다 먼저 정리된다.
        JobData m_data;
    };
}
