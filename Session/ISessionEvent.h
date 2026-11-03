#pragma once

#include <stdint.h>


class ISession;

class ISessionEvent
{
public:
	virtual void OnDisconnectRequest(ISession* session) = 0;
	virtual ~ISessionEvent() {}
};

typedef void (*CloseSocketFunc)(uintptr_t);

class ClientSession;

// 세션이 풀로 반환될 때 사용하는 엔진 내부 알림 콜백입니다.
typedef void (*SessionDisconnectNotifyFunc)(void* context, ClientSession* session);