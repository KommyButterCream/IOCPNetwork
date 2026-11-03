#pragma once

#include <cstdint> // for uint16_t, uint32_t
#include <limits> // for std::numeric_limits

#include "PacketID.h"

// packetSize 필드로 표현 가능한 패킷 전체 길이의 상한입니다.
inline constexpr uint32_t PACKET_SIZE_LIMIT = (std::numeric_limits<uint16_t>::max)();

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
