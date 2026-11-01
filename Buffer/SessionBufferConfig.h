#pragma once

#include <stdint.h>

#include "PreDefine.h"

#include "../Protocol/PacketHeader.h"

// 세션 하나가 들고 갈 버퍼 크기 설정.
//
// 이 값들의 적정치는 역할마다 크게 다르다. 스트리밍처럼 한쪽이 큰 데이터를
// 밀고 다른 쪽은 제어 메시지만 보내는 서비스에서는 서버와 클라의 적정
// 수신 버퍼가 한 자릿수 배 이상 벌어진다. 상수 하나로 양쪽을 덮으면
// 반드시 한쪽이 손해를 본다. (서버가 클라 사정에 맞춰 큰 링을 들고 있거나,
// 클라가 서버 사정에 맞춘 작은 링 때문에 큰 패킷을 못 받거나)
//
// 그래서 역할별로 따로 준다.
struct SessionBufferConfig
{
	// 헤더가 주장할 수 있는 수신 패킷 크기의 상한.
	// 이보다 큰 값을 적어 보내면 프로토콜 위반으로 보고 세션을 끊는다.
	uint32_t maxRecvPacketSize = MEMORY_SIZE_4K;

	// 세션당 수신 링 크기.
	//
	// 2의 거듭제곱이어야 한다 (위치 계산을 마스크로 한다).
	// maxRecvPacketSize 의 2배 이상이어야 한다 — PrepareWrite 가 남은
	// 조각을 앞으로 당긴 뒤에도 최대 패킷 하나가 통째로 들어가야 한다.
	// 실용적으로는 4배를 권한다.
	uint32_t recvRingSize = MEMORY_SIZE_16K;

	// 보낼 수 있는 패킷 크기의 상한.
	//
	// 주의: 이건 우리 수신 한도가 아니라 "상대의 maxRecvPacketSize" 다.
	// 상대가 받을 수 없는 크기를 보내면 상대가 프로토콜 위반으로 보고
	// 연결을 끊는데, 보낸 쪽은 원인 모를 피어 끊김만 보게 된다.
	// 그래서 보내는 시점에 여기서 막아 호출부에 알린다.
	uint32_t maxSendPacketSize = PACKET_SIZE_LIMIT;

	// 세션당 송신 큐 깊이 = 동시에 담아 둘 수 있는 패킷 수의 상한.
	//
	// 2의 거듭제곱이 아니어도 된다. 예전에는 큐가 고정 크기 링이라 위치를
	// 마스크로 계산했고 그래서 거듭제곱을 요구했다. 지금은 엔트리를
	// 연결 리스트로 엮으므로 이 값은 개수 상한으로만 쓰인다.
	//
	// 세션마다 미리 잡는 메모리도 없다. 예전에는 깊이 x 8바이트짜리 포인터
	// 배열을 기동 시점에 전부 잡았고(4096 이면 32KB) 한 칸도 쓰지 않는
	// 세션이 같은 값을 냈다. 이제는 실제로 담긴 만큼만 엔트리 풀에서 나온다.
	//
	// 깊을수록 느린 피어의 버스트를 더 흡수하지만, 그만큼 오래된 데이터를
	// 붙들고 있게 된다. 실시간성이 중요하면 얕게 잡는 편이 낫다.
	uint32_t sendQueueDepth = BLOCK_COUNT_4K;

	// --- 수신 백프레셔 수위 ---
	//
	// 잡 큐에는 깊이 상한이 없다. 핸들러가 유입보다 느리면 잡이 무한히
	// 쌓이고, 잡 하나가 패킷 하나를 붙들고 있으므로 메모리도 함께 자란다.
	// 풀 커밋 상한이 그 끝을 막지만, 상한에 닿는다는 것은 이미 패킷을
	// 버리고 세션을 끊는다는 뜻이다. 그건 마지막 방어선이지 조절이 아니다.
	//
	// 조절은 그 앞단에서 한다. 잡 큐가 깊어지면 다음 WSARecv 를 걸지 않는다.
	// 그러면 커널 수신 버퍼가 차고 TCP 수신 윈도가 닫혀서 보내는 쪽이
	// 스스로 막힌다. 유실도 끊김도 없이 속도만 맞춰진다.
	//
	//   recvPauseJobDepth  : 이 깊이 이상이면 다음 수신을 걸지 않는다
	//   recvResumeJobDepth : 이 깊이 이하로 빠지면 다시 건다
	//
	// 두 값을 벌려 두는 이유는 진동을 막기 위해서다. 하나의 경계만 쓰면
	// 경계 근처에서 매 잡마다 멈춤/재개가 반복되고, 재개 한 번이 WSARecv
	// 발행 한 번이므로 그 자체가 비용이다.
	//
	// 기본값의 근거 (실측, 8세션 부하, 핸들러 지연을 바꿔 가며)
	//   지연   0us -> 최고 깊이   18
	//   지연  10us -> 최고 깊이   25
	//   지연  25us -> 최고 깊이 1341     <- 무릎
	//   지연 250us -> 최고 깊이 6513
	//   정상 운영 구간의 수위는 11~31 이었다.
	// 재개 64 는 정상 수위의 두 배라 평상시에는 아예 닿지 않고,
	// 멈춤 256 은 무릎 위쪽이라 "정말 밀렸을 때만" 걸린다.
	//
	// 0 을 주면 백프레셔를 쓰지 않는다 (예전 동작 그대로).
	uint32_t recvPauseJobDepth = 256;
	uint32_t recvResumeJobDepth = 64;

	bool IsValid() const
	{
		if (maxRecvPacketSize < sizeof(PACKET_HEADER) || maxRecvPacketSize > PACKET_SIZE_LIMIT)
			return false;

		if (maxSendPacketSize < sizeof(PACKET_HEADER) || maxSendPacketSize > PACKET_SIZE_LIMIT)
			return false;

		if (!IsPowerOfTwo(recvRingSize))
			return false;

		// 조각 + 최대 패킷이 동시에 들어가야 한다.
		if (recvRingSize < maxRecvPacketSize * 2)
			return false;

		// 거듭제곱 제약은 없다. 0 만 막는다 — 한 칸도 못 담는 큐는
		// 아무것도 보낼 수 없는 세션과 같다.
		if (sendQueueDepth == 0)
			return false;

		// 백프레셔를 쓴다면 재개 수위가 멈춤 수위보다 낮아야 한다.
		// 같거나 뒤집히면 이력 구간이 없어져서, 경계에서 멈춤과 재개가
		// 매 잡마다 번갈아 일어난다. 끄려면 recvPauseJobDepth 를 0 으로 둔다.
		if (recvPauseJobDepth != 0 && recvResumeJobDepth >= recvPauseJobDepth)
			return false;

		return true;
	}

private:
	static bool IsPowerOfTwo(uint32_t value)
	{
		return value != 0 && (value & (value - 1)) == 0;
	}
};

// 기본 프리셋. 서비스는 여기서 출발해 자기 값으로 조정하면 된다.
//
// 엔진에 "FPS", "MMORPG" 같은 도메인 어휘를 넣지 않으려고 역할 이름만 쓴다.
// 서비스별 권장값은 서비스가 직접 정해서 넣는다.
namespace SessionBufferPreset
{
	// 서버: 제어 메시지를 받고, 큰 데이터를 민다.
	inline SessionBufferConfig Server()
	{
		SessionBufferConfig config;
		config.maxRecvPacketSize = MEMORY_SIZE_4K;
		config.recvRingSize = MEMORY_SIZE_16K;
		config.maxSendPacketSize = PACKET_SIZE_LIMIT;
		config.sendQueueDepth = BLOCK_COUNT_4K;
		config.recvPauseJobDepth = 256;
		config.recvResumeJobDepth = 64;
		return config;
	}

	// 클라이언트: 큰 데이터를 받고, 제어 메시지를 민다.
	//
	// maxRecvPacketSize 가 65536 이 아니라 65535 인 것은 헤더의 packetSize 가
	// uint16_t 이기 때문이다. 64KB 짜리 패킷은 애초에 표현할 수 없다.
	inline SessionBufferConfig Client()
	{
		SessionBufferConfig config;
		config.maxRecvPacketSize = PACKET_SIZE_LIMIT;
		config.recvRingSize = MEMORY_SIZE_256K;
		config.maxSendPacketSize = MEMORY_SIZE_4K;
		config.sendQueueDepth = BLOCK_COUNT_4K;

		// 서버와 같은 값이다.
		//
		// 처음에는 64/16 으로 뒀다. 근거는 "붙들리는 바이트를 맞춘다" 였다 —
		// 서버는 4KB 제어 메시지를, 클라는 64KB 데이터를 받으므로 같은 256 이
		// 서버에선 1MB 지만 클라에선 16MB 라는 계산이었다.
		//
		// 실측이 그 값을 부정했다. 클라가 전력으로 왕복시킬 때 잡 큐의 최고
		// 수위가 3회 실행에서 45 / 48 / 78 이었다. 고수위 64 는 그 정상
		// 구간 한가운데다. 실제로 한 번은 백프레셔가 걸렸다(pause_count=1).
		//
		// 그건 이 값들의 설계 의도를 어긴다. 백프레셔는 정상 부하에서는
		// 닿지 않고 소비자가 진짜로 밀렸을 때만 걸려야 한다. 정상 구간
		// 안에 두면 평소에 수신을 멈췄다 걸었다 하면서 비용만 낸다.
		//
		// 메모리 계산 자체가 틀린 것은 아니지만 전제가 틀렸다. 그 계산은
		// 모든 패킷이 최대 크기라고 가정했는데, maxRecvPacketSize 는 상한일
		// 뿐이고 실제 트래픽은 대부분 그보다 훨씬 작다.
		//
		// 지연에 민감한 서비스(라이브 스트리밍 등)는 낮추는 편이 낫다.
		// 256 x 64KB 를 다 채우면 16MB 를 들고 있게 되고, 그건 붙들린
		// 메모리이자 그대로 지연이다. 그때는 자기 수위를 재고 정하면 된다.
		config.recvPauseJobDepth = 256;
		config.recvResumeJobDepth = 64;
		return config;
	}
}
