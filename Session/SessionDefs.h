#pragma once

#include <stdint.h>

enum class DisconnectReason : uint32_t;
class ClientSession;
using CloseSocketFunc = void (*)(uintptr_t);
using SessionDisconnectNotifyFunc = void (*)(void* context, ClientSession* session, DisconnectReason reason);

enum class IOWaitStatus { Completed, TimedOut, Failed };

struct IOWaitResult
{
	IOWaitStatus status;
	uint32_t sessionId = UINT32_MAX;
	uint32_t error = 0;
};

enum class IO_OPERATION
{
	INVALID = 0,
	ACCEPT,
	CONNECT,
	RECV,
	SEND
};

enum class SESSION_ROLE
{
	NONE,
	CLIENT,
	SERVER,
};


static constexpr uint32_t INVALID_SESSION_ID = UINT32_MAX;
