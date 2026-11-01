#pragma once

#include <stdint.h>

#include "../Job/JobDefs.h" // for PacketHandlerFunc
#include "../Protocol/PacketID.h"

#ifdef BUILD_IOCP_ENGINE_DLL
#define IOCP_ENGINE_API __declspec(dllexport)
#else
#define IOCP_ENGINE_API __declspec(dllimport)
#endif

class IOCP_ENGINE_API PacketHandlerTable
{
private:
	PacketHandlerFunc m_handlerTable[ToPacketID(PACKET_ID::MAX_PACKET_ID)] = {};

public:
	PacketHandlerTable() = default;
	virtual ~PacketHandlerTable() = default;

public:
	bool Register(uint16_t packetId, PacketHandlerFunc handler);

	PacketHandlerFunc GetHandler(uint16_t packetId) const;
};