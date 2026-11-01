#pragma once

#include <stdint.h>

// 세션이 살아 있는지 묻는 주기와, 답이 없을 때 기다려 줄 시간.
//
// 예전에는 IOCPServer::StartServer 안에 상수 두 개로 박혀 있었다. 버퍼,
// 접속 정책, 메모리 풀은 전부 설정 구조체로 열려 있는데 이것만 상수였다.
// 적정치가 서비스마다 다른 것은 나머지와 똑같다 — 실시간 스트리밍은
// 15초짜리 정지 화면을 견딜 수 없고, 배치성 서비스는 15초가 너무 짧다.
struct HeartbeatConfig
{
	// 점검 주기. 이 간격마다 하트비트를 보내고 좀비를 훑는다.
	uint64_t checkInterval_ms = 5'000;

	// 아무 소식이 없을 때 세션을 끊기까지 기다리는 시간.
	//
	// checkInterval_ms 의 정수배가 아니어도 되지만, 최소 2주기는 되어야
	// 한다. 1주기면 하트비트 한 번을 놓치는 것만으로 끊긴다.
	uint64_t timeout_ms = 15'000;

	// 상대가 "죽은" 것이 아니라 "느린" 것으로 보일 때 대신 쓰는 시간.
	//
	// 왜 따로 두는가.
	//   수신 백프레셔가 걸린 상대는 아무것도 읽지 않는다. 그래서 우리가
	//   보낸 하트비트 요청도 읽지 않고, 응답도 오지 않는다. 그 침묵은
	//   죽은 상대의 침묵과 구분되지 않는다.
	//
	//   그런데 구분할 단서가 하나 있다 — 우리 송신 큐에 데이터가 밀려
	//   있다는 것. 상대의 TCP 수신 윈도가 닫혀 있다는 뜻이고, 윈도를
	//   닫아 두려면 상대의 커널이 살아서 ACK 를 하고 있어야 한다.
	//   즉 "느리지만 살아 있다" 는 증거다.
	//
	//   실측 : 클라 잡 큐를 256(recvPauseJobDepth)까지 채워 25초 멈춰
	//   두면, 정상 동작 중인 세션이 정확히 15초에 끊겼다. (tools/bpzombie)
	//   백프레셔는 "유실도 끊김도 없이 속도만 맞춘다" 가 목적인데
	//   느린 소비자를 살리려는 장치가 느린 소비자를 죽이고 있었다.
	//
	// 왜 무한이 아닌가.
	//   윈도가 닫힌 채로 멈춘 상대에게 Windows TCP 는 zero-window probe 를
	//   기본적으로 무한히 보낸다. 그래서 "밀려 있으면 봐준다" 만으로는
	//   멈춘 피어 하나가 세션 슬롯을 영원히 붙든다. 유예는 주되 끝은 있어야
	//   한다.
	//
	// timeout_ms 이상이어야 한다. 0 이면 유예 없이 timeout_ms 를 그대로 쓴다.
	uint64_t stalledPeerTimeout_ms = 60'000;

	bool IsValid() const
	{
		if (checkInterval_ms == 0)
			return false;

		// 끄는 것은 허용한다 (0 = 좀비 정리 안 함). 켠다면 최소 2주기.
		if (timeout_ms != 0 && timeout_ms < checkInterval_ms * 2)
			return false;

		// 유예가 본 타임아웃보다 짧으면 유예가 아니다.
		if (stalledPeerTimeout_ms != 0 && stalledPeerTimeout_ms < timeout_ms)
			return false;

		return true;
	}

	// 실제로 쓸 유예 시간. 0 이면 유예를 두지 않는다는 뜻이므로 본 값을 쓴다.
	uint64_t EffectiveStalledTimeout() const
	{
		return (stalledPeerTimeout_ms != 0) ? stalledPeerTimeout_ms : timeout_ms;
	}
};
