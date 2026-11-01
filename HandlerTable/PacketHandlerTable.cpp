#include "PacketHandlerTable.h"

bool PacketHandlerTable::Register(uint16_t packetId, PacketHandlerFunc handler)
{
	if (!IsServicePacketId(packetId))
		return false;

	if (!handler)
		return false;

	if (m_handlerTable[packetId] != nullptr)
		return false;

	m_handlerTable[packetId] = handler;

	return true;
}

PacketHandlerFunc PacketHandlerTable::GetHandler(uint16_t packetId) const
{
	constexpr uint16_t maxPacketId = ToPacketID(PACKET_ID::MAX_PACKET_ID);

	if (packetId >= maxPacketId)
		return nullptr;

	return m_handlerTable[packetId];
}