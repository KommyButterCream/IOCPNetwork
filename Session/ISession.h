#pragma once

#include <WinSock2.h>
#include <stdint.h>

#include "SessionDefs.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

enum class ClientSessionState
{
	NONE,
	CONNECT_READY,
	CONNECTING,
	CONNECTED,
	AUTH_PENDING,
	ESTABLISHED,
	CONNECT_ABORTED,
	DISCONNECTED,
};

enum class ServerSessionState
{
	NONE,
	CONNECT_READY,
	CONNECTED,
	AUTH_PENDING,
	ESTABLISHED,
	HEARTBEAT_TIMEOUT,
	DISCONNECTED,
};

enum class AcceptSessionState
{
	NONE,
	ACCEPT_READY,
	ACCEPT_WAIT,
	ACCEPT_COMPLETE,
	ACCEPT_ABORTED,
	DISCONNECTED,
};

// 서비스와 엔진 사이의 경계 타입.
//
// 패킷 핸들러(PacketHandlerFunc)와 On* 훅이 받는 것이 이 타입이고, 그래서
// 여기 올라온 것은 곧 "서비스가 세션에 대해 다형적으로 할 수 있는 일" 이다.
//
// 그 목록은 딱 하나다 — 세션 식별자 읽기.
//
// 예전에는 순수 가상 14개가 올라와 있었다. 전수 조사해 보니 ISession* 를
// 통해 실제로 불리는 것은 GetSessionID() 하나뿐이었다. 엔진에서는
// SessionManager::ReleaseClientSession 한 곳이고, 서비스도 같다.
// 나머지는 모두 구체 타입으로 캐스팅한 뒤에 호출되고 있었다.
//
// 그런데도 14개가 가상이었던 대가는 두 가지였다.
//   - virtual 은 "재정의될 수 있다" 는 선언인데 BaseSession 의 21개 중
//     6개만 참이었다. 어느 것이 확장점인지 선언만 보고 알 수 없어서
//     override 를 전수 확인해야 했다.
//   - 한 줄짜리 접근자가 인라인되지 못했다.
//
// 그래서 이 인터페이스는 경계에서 실제로 필요한 것만 남긴다. 나머지는
// BaseSession 의 비가상 멤버이며 대부분 헤더에서 인라인된다.
//
// IO 카운팅(IncrementIO / DecrementIO)이 여기 없는 것도 같은 이유의
// 연장이다. 발행/완료 짝으로만 움직여야 하는 엔진 내부 카운터를 서비스가
// 받는 타입에 노출하면, 핸들러가 한 번 잘못 부르는 것만으로 세션이 영구히
// 취소 대기에 묶이거나 사용 중인 세션이 풀로 반납된다.
class IOCP_ENGINE_API ISession
{
public:
	virtual ~ISession() = default;

	// 세션 식별자.
	//
	// 여전히 가상인 이유는 서비스가 ISession* 로 부르기 때문이다.
	// 데이터 멤버를 이 클래스로 올리면 비가상으로 만들 수 있지만, 그러면
	// "경계 계약" 이 "기반 클래스" 가 된다. 얻는 것은 초당 100만 번 남짓의
	// 값싼 호출(실측 잡당 비용의 0.1% 미만)이라 그 거래는 하지 않는다.
	virtual uint32_t GetSessionID() const = 0;
};
