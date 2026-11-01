#pragma once

#include <stdint.h>

#include "ClientLivenessConfig.h"
#include "../../Core/Concurrency/ThreadBase.h"

class ClientSession;

// 서버가 조용해진 것을 클라이언트가 스스로 알아차리는 스레드.
//
// 왜 별도 스레드여야 하는가
//   클라이언트에는 시간을 재는 주체가 없었다. 가진 스레드는 GQCS 워커와
//   ClientSessionScheduler 둘뿐인데, 워커는 완료 통지가 와야 깨어나고
//   (서버가 죽으면 영영 오지 않는다) 스케줄러는 잡 큐에 무한 블록한다.
//   그래서 "아무것도 오지 않는다" 를 관측할 수 있는 자리가 없었다.
//
//   스케줄러에 얹지 않은 이유도 같다. 백프레셔가 걸렸거나 서비스 핸들러가
//   오래 붙들려 있으면 타이머까지 함께 멈춘다. 감시자가 감시 대상과 같은
//   스레드에 있으면 안 된다.
//
// 무엇을 보는가
//   ClientSession::GetLastRecvTick() 하나다. 서버의 하트비트는 서비스
//   트래픽과 무관하게 주기적으로 나가므로(ClientSessionPool::
//   SendHeartbeatRequests), 이 값은 조용한 연결에서도 그 주기마다 갱신된다.
//   즉 이 스레드가 재는 것은 "서비스가 데이터를 보냈는가" 가 아니라
//   "서버의 하트비트가 도착했는가" 다.
//
// 무장 시점
//   ESTABLISHED 이후에만 잰다. 하트비트는 established 세션에만 나가므로
//   그 전에는 기준이 없다. 그래서 인증 응답을 받은 자리에서 Arm 을 부른다.
class ClientWatchdogThread final : public Core::Concurrency::ThreadBase
{
public:
	ClientWatchdogThread(ClientSession* session, const ClientLivenessConfig& config);
	~ClientWatchdogThread() override = default;

	ClientWatchdogThread(const ClientWatchdogThread&) = delete;
	ClientWatchdogThread& operator=(const ClientWatchdogThread&) = delete;

	// 서버가 인증 응답으로 알려준 하트비트 주기로 감시를 시작한다.
	// 0 이면 설정의 하한값만 쓴다.
	void Arm(uint32_t serverHeartbeatInterval_ms);

	// 감시를 멈춘다. 스레드는 그대로 돈다 (재접속 없이 같은 객체를 다시
	// 쓰는 경로가 없으므로 실제로는 종료 직전에만 불린다).
	void Disarm();

	// 이 스레드가 연결을 끊은 적이 있는가. 하네스와 진단용이다.
	bool HasFiredTimeout() const;

protected:
	void Run() override;

private:
	ClientSession* m_session = nullptr;
	ClientLivenessConfig m_config;

	// Arm 이 계산해 넣는다. 0 이면 감시하지 않는다.
	// 두 값 모두 Arm(다른 스레드)과 Run(이 스레드)이 함께 보므로 원자로 읽고 쓴다.
	volatile LONGLONG m_idleTimeout_ms = 0;
	volatile LONG m_fired = 0;

	// 수신 백프레셔로 멈춰 있던 마지막 시각. 이 스레드만 읽고 쓴다.
	//
	// 멈춰 있는 동안은 아무것도 도착하지 않는 것이 정상이므로 그 구간을
	// 침묵으로 세지 않고, 재개한 뒤에는 낡은 lastRecvTick 대신 이 값에서
	// 다시 센다. (자세한 사정은 Run 의 해당 자리)
	uint64_t m_resumeBaselineTick = 0;
};
