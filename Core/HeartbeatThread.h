#pragma once

#include <stdint.h>

#include "HeartbeatConfig.h"
#include "../../Core/Concurrency/ThreadBase.h"

class SessionManager;

// 걸기가 실패해 비어 버린 accept 슬롯을 다시 post 하는 일을 받는다.
//
// AcceptEx 는 accept 완료 핸들러 안에서만 다시 걸린다. 그래서 PostAccept 가
// 실패하면 그 슬롯은 재차 post 될 계기를 스스로 만들지 못한다 — 다음
// PostAccept 를 부를 완료 통지가 애초에 그 걸기의 것이기 때문이다.
// 자원 부족(WSAENOBUFS 등)은 보통 여러 슬롯에 동시에 오므로, 최악의 경우
// 서버가 살아 있는 채로 새 접속을 영구히 받지 못한다.
//
// 그 계기를 이 스레드의 주기가 대신 만들어 준다. 실제 post 는 IOCPServer 만
// 할 수 있으므로(리슨 소켓, AcceptEx 함수 포인터) 함수 포인터로 받는다.
// (엔진이 CloseSocketFunc 등에서 쓰는 방식과 같다)
//
// 이 스레드가 단일이라는 점이 안전 조건이다. 세션 상태는 원자 변수가 아니라
// 평범한 멤버라서, 두 스레드가 같은 슬롯을 비어 있다고 보고 동시에 post 하면
// 겹쳐 걸린다.
//
// 부르는 쪽 규칙: 막히지 않아야 한다. 이 호출은 하트비트 발송과 좀비 정리와
// 같은 스레드를 쓴다.
typedef void (*AcceptRepostFunc)(void* context);

class HeartbeatThread final : public Core::Concurrency::ThreadBase
{
public:
	HeartbeatThread(SessionManager* sessionManager, const HeartbeatConfig& config,
		AcceptRepostFunc acceptRepostFunc = nullptr, void* acceptRepostContext = nullptr);
	~HeartbeatThread() override = default;

	HeartbeatThread(const HeartbeatThread&) = delete;
	HeartbeatThread& operator=(const HeartbeatThread&) = delete;

	void SetCheckInterval(uint64_t checkInterval_ms);
	void SetHeartbeatTimeout(uint64_t heartbeatTimeout_ms);

protected:
	void Run() override;

private:
	SessionManager* m_sessionManager = nullptr;
	uint64_t m_checkInterval_ms = 0;
	uint64_t m_heartbeatTimeout_ms = 0;

	// 송신이 밀려 있는 세션에만 쓰는 더 긴 타임아웃.
	// (HeartbeatConfig::stalledPeerTimeout_ms 주석 참고)
	uint64_t m_stalledPeerTimeout_ms = 0;

	AcceptRepostFunc m_acceptRepostFunc = nullptr;
	void* m_acceptRepostContext = nullptr;
};
