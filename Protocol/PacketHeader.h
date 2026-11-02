#pragma once

#include <stdint.h>

#include "PacketID.h"

#pragma pack(push, 1)

struct PACKET_HEADER
{
	uint16_t packetId = ToInteger(PACKET_ID::NONE);
	uint16_t packetSize = 0;

	PACKET_HEADER() = default;

	PACKET_HEADER(const uint16_t packetId, const uint16_t packetSize)
		: packetId(packetId)
		, packetSize(packetSize)
	{
	}

	PACKET_HEADER(const PACKET_ID packetId, const uint16_t packetSize)
		: packetId(ToInteger(packetId))
		, packetSize(packetSize)
	{
	}

	~PACKET_HEADER() = default;

	void Reset()
	{
		packetId = ToInteger(PACKET_ID::NONE);
		packetSize = 0;
	}
};

#pragma pack(pop)
