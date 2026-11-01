#pragma once

#include <stdint.h>

// 세션이 왜 끝났는가.
//
// 왜 필요한가
//   예전에는 OnClientDisconnect(ISession*) 하나였다. 서비스가 알 수 있는
//   것은 "끝났다" 뿐이라, 해야 할 일이 정반대인 상황들이 구분되지 않았다.
//
//     만석으로 거절됨   -> 잠시 뒤 다시 붙어야 한다
//     프로토콜 불일치   -> 다시 붙어 봐야 똑같다. 사용자에게 알려야 한다
//     네트워크가 끊김   -> 즉시 재접속
//     내가 StopClient   -> 아무것도 하면 안 된다
//
//   엔진은 이 넷을 전부 알고 있었지만 로그로만 남기고 버렸다. SERVER_FULL
//   이라는 인증 결과를 따로 만든 이유가 "RST 만 보면 만석과 장애를 구분할
//   수 없다" 였는데, 정작 그 사유가 서비스까지 가지 않았다.
//
// 기록 규칙 : 먼저 쓴 사유가 이긴다.
//   종료는 보통 연쇄로 일어난다. 프로토콜 위반으로 끊기로 결정하면 곧이어
//   소켓이 닫히고 걸려 있던 I/O 가 10054 로 실패한다. 나중 것을 쓰면 근본
//   원인이 매번 SocketError 로 덮인다. (BaseSession::NoteDisconnectReason)
enum class DisconnectReason : uint32_t
{
	// 사유를 남긴 경로가 없다. 새 종료 경로를 만들면서 표시를 빠뜨렸다는 뜻이라
	// 이 값이 서비스까지 올라오면 엔진 쪽 누락이다.
	Unknown = 0,

	// --- 우리가 내린 결정 ---

	// StopClient / StopServer. 서비스가 스스로 내린 것이므로 재접속 대상이 아니다.
	LocalShutdown,

	// 서비스가 이 세션 하나를 끊었다.
	LocalRequest,

	// --- 상대가 내린 결정 ---

	// 정상 종료. FIN 을 받았다 (recv 0).
	PeerClosed,

	// --- 전송 계층 ---

	// RST, 10054 등. 경로가 끊겼거나 상대 프로세스가 사라졌다.
	SocketError,

	// 접속 자체가 성립하지 않았다. 이 사유는 OnClientDisconnect 가 아니라
	// OnConnectFailed 로 간다 — 접속한 적이 없으면 종료도 없다.
	ConnectFailed,

	// --- 상대를 더 이상 믿을 수 없다 ---

	// 패킷 크기나 ID 가 규칙 밖이거나, 스트림이 어긋났다.
	ProtocolError,

	// established 전에 서비스 패킷이 왔다.
	UnexpectedPacket,

	// --- 시간 ---

	// 클라이언트 : 서버의 하트비트가 끊겼다. (ClientWatchdogThread)
	IdleTimeout,

	// 서버 : 클라이언트가 하트비트에 답하지 않았다.
	HeartbeatTimeout,

	// 서버 : 상대가 살아는 있으나 유예 시간 내내 읽지 않았다.
	// HeartbeatTimeout 과 구분하는 이유는 HeartbeatConfig 주석 참고.
	StalledPeer,

	// --- 인증 ---
	//
	// 넷으로 나눠 둔 것은 서비스가 할 일이 각각 다르기 때문이다. 하나로
	// 묶으면 "재시도해도 되는가" 를 서비스가 판단할 수 없다.

	AuthRejectedServerFull,        // 다시 시도할 만하다
	AuthRejectedProtocolMismatch,  // 다시 시도해도 같다. 버전을 맞춰야 한다
	AuthRejectedInvalidState,      // 엔진 쪽 순서 문제
	AuthRejectedOther,

	// --- 자원 ---

	// 패킷 풀 고갈 등. 부하를 줄이거나 설정을 키워야 한다.
	ResourceExhausted,
};

// 로그와 서비스 쪽 표시에 쓴다. 항상 유효한 문자열을 돌려준다.
inline const char* ToString(DisconnectReason reason)
{
	switch (reason)
	{
	case DisconnectReason::Unknown:                      return "unknown";
	case DisconnectReason::LocalShutdown:                return "local shutdown";
	case DisconnectReason::LocalRequest:                 return "local request";
	case DisconnectReason::PeerClosed:                   return "peer closed";
	case DisconnectReason::SocketError:                  return "socket error";
	case DisconnectReason::ConnectFailed:                return "connect failed";
	case DisconnectReason::ProtocolError:                return "protocol error";
	case DisconnectReason::UnexpectedPacket:             return "unexpected packet";
	case DisconnectReason::IdleTimeout:                  return "idle timeout";
	case DisconnectReason::HeartbeatTimeout:             return "heartbeat timeout";
	case DisconnectReason::StalledPeer:                  return "stalled peer";
	case DisconnectReason::AuthRejectedServerFull:       return "auth rejected : server full";
	case DisconnectReason::AuthRejectedProtocolMismatch: return "auth rejected : protocol mismatch";
	case DisconnectReason::AuthRejectedInvalidState:     return "auth rejected : invalid state";
	case DisconnectReason::AuthRejectedOther:            return "auth rejected";
	case DisconnectReason::ResourceExhausted:            return "resource exhausted";
	}

	return "unrecognised";
}

// 다시 붙어 볼 가치가 있는가.
//
// 서비스가 재접속 정책을 짤 때 거의 항상 필요한 판단이라 엔진이 답을 준다.
// 물론 서비스가 사유를 직접 보고 다르게 정해도 된다.
inline bool IsRetryableDisconnect(DisconnectReason reason)
{
	switch (reason)
	{
		// 우리가 내린 결정이다. 서비스가 다시 붙고 싶으면 스스로 붙는다.
	case DisconnectReason::LocalShutdown:
	case DisconnectReason::LocalRequest:
		return false;

		// 다시 붙어도 같은 답이 온다.
	case DisconnectReason::AuthRejectedProtocolMismatch:
		return false;

		// 나머지는 일시적일 수 있다. 다만 간격을 두는 것은 서비스 몫이다 —
		// 만석인 서버에 즉시 다시 붙으면 거절만 반복한다.
	default:
		return true;
	}
}
