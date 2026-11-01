#pragma once

#include <stdint.h>

#include "PacketHeader.h"
#include "PacketID.h"
#include "../Session/DisconnectReason.h"

// 2 : 인증 응답에 서버의 하트비트 주기를 실었다.
//
//     클라이언트가 "서버가 죽었다" 를 스스로 판정하려면 언제까지 기다려야
//     하는지를 알아야 하는데, 그 근거인 주기는 서버 설정이다. 상수로 복제하면
//     서버가 주기를 바꾸는 순간 클라가 멀쩡한 연결을 끊는다.
//
//     버전을 올린 이유는 패킷 크기가 바뀌었기 때문이다. 서버는 인증 요청의
//     버전을 크기 검사 다음, 응답 전에 보므로 (IOCPServer::HandleSystemPacket)
//     구형 클라이언트는 PROTOCOL_MISMATCH 라는 정확한 사유를 받는다.
constexpr uint16_t IOCP_ENGINE_PROTOCOL_VERSION = 2;

enum class SYSTEM_AUTH_RESULT : uint16_t
{
	SUCCESS = 0,
	FAILED = 1,
	INVALID_STATE = 2,
	PROTOCOL_MISMATCH = 3,

	// 서비스가 붙잡아 둘 접속 수 상한을 넘었다.
	// 세션 풀이 아예 비면 소켓을 즉시 닫을 수밖에 없어 클라는 RST 만 본다.
	// 이 값은 그보다 앞선 여유 구간에서 이유를 알려주기 위한 것이다.
	SERVER_FULL = 4,
};

// 인증 결과를 종료 사유로 옮긴다.
//
// 둘을 따로 두는 이유는 계층이 다르기 때문이다 — SYSTEM_AUTH_RESULT 는
// 선로에 나가는 값이라 함부로 못 바꾸고, DisconnectReason 은 서비스에
// 보여 주는 값이라 인증 말고도 사유가 많다. 그 사이를 여기서 잇는다.
inline DisconnectReason ToDisconnectReason(SYSTEM_AUTH_RESULT authResult)
{
	switch (authResult)
	{
	case SYSTEM_AUTH_RESULT::SERVER_FULL:        return DisconnectReason::AuthRejectedServerFull;
	case SYSTEM_AUTH_RESULT::PROTOCOL_MISMATCH:  return DisconnectReason::AuthRejectedProtocolMismatch;
	case SYSTEM_AUTH_RESULT::INVALID_STATE:      return DisconnectReason::AuthRejectedInvalidState;

		// SUCCESS 가 여기로 오는 것은 부르는 쪽 실수다. 거절이 아닌 값을
		// 거절 사유로 바꿀 수는 없으니 뭉뚱그린 값을 돌려준다.
	case SYSTEM_AUTH_RESULT::SUCCESS:
	case SYSTEM_AUTH_RESULT::FAILED:
	default:
		return DisconnectReason::AuthRejectedOther;
	}
}

#pragma pack(push, 1)

struct CS_SYSTEM_AUTH_REQUEST_PACKET
{
	PACKET_HEADER header = PACKET_HEADER(PACKET_ID::CS_SYSTEM_AUTH_REQUEST, sizeof(CS_SYSTEM_AUTH_REQUEST_PACKET));
	uint16_t protocolVersion = IOCP_ENGINE_PROTOCOL_VERSION;

	CS_SYSTEM_AUTH_REQUEST_PACKET() = default;
};

struct SC_SYSTEM_AUTH_RESPONSE_PACKET
{
	PACKET_HEADER header = PACKET_HEADER(PACKET_ID::SC_SYSTEM_AUTH_RESPONSE, sizeof(SC_SYSTEM_AUTH_RESPONSE_PACKET));
	uint16_t authResult = static_cast<uint16_t>(SYSTEM_AUTH_RESULT::SUCCESS);
	uint16_t protocolVersion = IOCP_ENGINE_PROTOCOL_VERSION;

	// 이 서버가 하트비트 요청을 보내는 주기(ms).
	//
	// 클라이언트의 유휴 타임아웃이 여기서 파생된다. 하트비트는 서비스
	// 트래픽과 무관하게 나가므로, 이 주기가 곧 "조용한 연결에서도 무언가
	// 도착해야 하는 간격" 의 상한이다. 클라는 여기에 배수를 곱해 "몇 번
	// 연속으로 놓치면 죽은 것으로 본다" 를 정한다.
	//
	// 0 이면 서버가 하트비트를 쓰지 않는다는 뜻이고, 클라는 유휴 타임아웃
	// 대신 설정의 하한값만 쓴다.
	uint32_t heartbeatIntervalMs = 0;

	SC_SYSTEM_AUTH_RESPONSE_PACKET() = default;
};

struct SC_SYSTEM_HEARTBEAT_REQUEST_PACKET
{
	PACKET_HEADER header = PACKET_HEADER(PACKET_ID::SC_SYSTEM_HEARTBEAT_REQUEST, sizeof(SC_SYSTEM_HEARTBEAT_REQUEST_PACKET));
	uint64_t tick = 0;

	SC_SYSTEM_HEARTBEAT_REQUEST_PACKET() = default;
};

struct CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET
{
	PACKET_HEADER header = PACKET_HEADER(PACKET_ID::CS_SYSTEM_HEARTBEAT_RESPONSE, sizeof(CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET));
	uint64_t tick = 0;

	CS_SYSTEM_HEARTBEAT_RESPONSE_PACKET() = default;
};

#pragma pack(pop)
