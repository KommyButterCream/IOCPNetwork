#pragma once

#include <stdint.h>

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class ISession;

class IOCP_ENGINE_API ISessionEvent
{
public:
	virtual void OnDisconnectRequest(ISession* session) = 0;
	virtual ~ISessionEvent() {}
};

typedef void (*CloseSocketFunc)(uintptr_t);

class ClientSession;

// 세션이 반납될 때 서비스에 종료를 알리는 진입점.
//
// ISessionEvent 에 순수 가상을 하나 더 다는 대신 함수 포인터로 둔다.
// 저 인터페이스는 익스포트되어 있어서 메서드를 늘리면 밖에서 구현한
// 쪽이 전부 깨진다. CloseSocketFunc 가 같은 이유로 함수 포인터다.
typedef void (*SessionDisconnectNotifyFunc)(void* context, ClientSession* session);