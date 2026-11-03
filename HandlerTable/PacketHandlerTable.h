#pragma once

#include <stdint.h>

#include "../Job/HandlerContext.h" // for PacketHandlerFunc
#include "../Protocol/PacketID.h"


class PacketHandlerTable
{
private:
	PacketHandlerFunc m_handlerTable[ToInteger(PACKET_ID::MAX_PACKET_ID)] = {};

public:
	PacketHandlerTable() = default;
	virtual ~PacketHandlerTable() = default;

public:
	bool Register(uint16_t packetId, PacketHandlerFunc handler);

	PacketHandlerFunc GetHandler(uint16_t packetId) const;
};