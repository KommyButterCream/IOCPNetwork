#pragma once

#include <stdint.h>

using PACKET_ID_TYPE = uint16_t;

enum class PACKET_ID : PACKET_ID_TYPE
{
	NONE = 0,

	// System reserved
	SYSTEM_BEGIN = 1,
	CS_SYSTEM_AUTH_REQUEST = SYSTEM_BEGIN,
	SC_SYSTEM_AUTH_RESPONSE,
	CS_SYSTEM_HEARTBEAT_RESPONSE,
	SC_SYSTEM_HEARTBEAT_REQUEST,
	SYSTEM_END = 999,

	// Service packets start here
	SERVICE_BEGIN = 1000,

	MAX_PACKET_ID = 10'000
};

constexpr PACKET_ID_TYPE ToInteger(PACKET_ID packetId)
{
	return static_cast<PACKET_ID_TYPE>(packetId);
};

constexpr bool IsValidPacketId(PACKET_ID_TYPE id)
{
	return id > ToInteger(PACKET_ID::NONE) &&
		id < ToInteger(PACKET_ID::MAX_PACKET_ID);
}

constexpr bool IsSystemPacketId(PACKET_ID_TYPE id)
{
	return id >= ToInteger(PACKET_ID::SYSTEM_BEGIN) &&
		id <= ToInteger(PACKET_ID::SYSTEM_END);
}

constexpr bool IsServicePacketId(PACKET_ID_TYPE id)
{
	return id >= ToInteger(PACKET_ID::SERVICE_BEGIN) &&
		id < ToInteger(PACKET_ID::MAX_PACKET_ID);
}
