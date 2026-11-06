#pragma once

#include <stdint.h>

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
