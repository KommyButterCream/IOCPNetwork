#pragma once

#include <stdint.h>

// 클라이언트가 "서버가 죽었다" 를 스스로 판정하는 방법.
//
// 왜 필요한가
//   서버가 정상 종료하면 FIN 이 오고 recv 0 으로 잡힌다. 그런데 랜선이
//   빠지거나 서버 프로세스가 행되거나 VM 이 멈추면 아무것도 오지 않고,
//   아무 오류도 나지 않는다. 클라는 영원히 기다린다. 스트리밍 뷰어라면
//   정지된 마지막 프레임을 붙들고 앉아 있게 된다.
//
//   예전에는 이걸 감지할 수단이 하나도 없었다.
//     - 하트비트는 서버 -> 클라 단방향이라 클라는 받아서 답만 했다
//     - 클라 소켓에는 TCP keepalive 도 걸지 않았다 (서버는 걸고 있었다)
//     - 클라에는 시간을 재는 스레드 자체가 없었다
//       (ClientSessionScheduler 는 잡 큐에 무한 블록한다)
//
// 두 장치를 함께 쓴다. 겹치는 것이 아니라 잡는 대상이 다르다.
//
//   TCP keepalive : 경로 단절과 호스트 사망을 커널이 잡는다. 유휴 상태에서도
//                   동작한다. 대신 프로세스 행은 못 잡는다 — probe 에
//                   응답하는 것은 상대 커널이지 애플리케이션이 아니다.
//
//   유휴 타임아웃 : 서버의 하트비트가 끊긴 것을 본다. 상대 '애플리케이션' 이
//                   살아서 응답하는지를 묻는 유일한 장치라, 인코더 교착이나
//                   GPU 행처럼 커널만 살아 있는 장애를 여기서만 잡을 수 있다.
struct ClientLivenessConfig
{
	// 서버 하트비트를 연속으로 몇 번 놓치면 끊을 것인가. 0 이면 유휴
	// 타임아웃을 쓰지 않는다 (예전 동작).
	//
	// 기본 3 은 서버 기본값(5초 주기 / 15초 타임아웃)과 같은 비율이다.
	// 1 로 두면 안 된다 — 한 번의 지연만으로 끊긴다.
	//
	// 알려진 한계: 이 판정은 "수신 완료를 마지막으로 처리한 시각" 을 본다.
	// 수신 백프레셔로 우리가 스스로 멈춘 구간은 빼고 세지만(그 처리는
	// ClientWatchdogThread::Run 에 있다), GQCS 워커가 전부 서비스 콜백
	// (OnReceive 등) 안에 이 시간만큼 붙들려 있으면 완료를 꺼내지 못해
	// 멀쩡한 연결을 끊을 수 있다. 무거운 일은 SubmitPacketJob 으로 잡
	// 큐에 넘기라는 규약이 여기서도 전제다.
	uint32_t missedHeartbeatLimit = 3;

	// 유휴 타임아웃의 하한.
	//
	// 실제 타임아웃은 서버가 인증 응답으로 알려준 주기 x missedHeartbeatLimit
	// 이고, 그 값이 이보다 작으면 이 값을 쓴다. 서버 주기가 아주 짧게
	// 설정된 경우에 잠깐의 스케줄링 지연으로 끊기는 것을 막는다.
	//
	// 서버가 주기를 알려주지 않으면(구형 서버, 또는 좀비 정리를 끈 서버)
	// 이 값만 쓴다.
	uint64_t minIdleTimeout_ms = 15'000;

	// 소켓에 TCP keepalive 를 건다. 서버가 수락한 소켓에 거는 것과 같은 설정이다.
	bool useKeepAlive = true;

	// 유휴가 이만큼 이어지면 probe 를 시작하고, 그 간격으로 반복한다.
	// Windows 는 재시도 횟수를 10 으로 고정하므로 대략
	// keepAliveTime + 10 x keepAliveInterval 만에 소켓이 오류로 끝난다.
	uint32_t keepAliveTime_ms = 10'000;
	uint32_t keepAliveInterval_ms = 1'000;

	bool IsValid() const
	{
		// 1 은 허용하지 않는다. 하트비트 한 번을 놓치는 것만으로 끊기는 것은
		// 타임아웃이 아니라 오작동이다. 끄려면 0 을 쓴다.
		if (missedHeartbeatLimit == 1)
			return false;

		if (missedHeartbeatLimit != 0 && minIdleTimeout_ms == 0)
			return false;

		if (useKeepAlive && (keepAliveTime_ms == 0 || keepAliveInterval_ms == 0))
			return false;

		return true;
	}

	// 서버가 알려준 주기로부터 실제 유휴 타임아웃을 정한다.
	// serverHeartbeatInterval_ms 가 0 이면 하한만 쓴다.
	uint64_t ResolveIdleTimeout(uint32_t serverHeartbeatInterval_ms) const
	{
		if (missedHeartbeatLimit == 0)
			return 0;

		const uint64_t derived =
			static_cast<uint64_t>(serverHeartbeatInterval_ms) * missedHeartbeatLimit;

		return (derived > minIdleTimeout_ms) ? derived : minIdleTimeout_ms;
	}
};
