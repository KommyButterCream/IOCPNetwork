#include "PacketHandlerTable.h"

bool PacketHandlerTable::Register(uint16_t packetId, PacketHandlerFunc handler)
{
	if (!IsServicePacketId(packetId))
		return false;

	if (!handler)
		return false;

	// 같은 등록은 재시작 시 반복할 수 있지만 기존 핸들러를 덮어쓰지는 않는다.
	if (m_handlerTable[packetId] != nullptr)
		return m_handlerTable[packetId] == handler;

	m_handlerTable[packetId] = handler;

	return true;
}

PacketHandlerFunc PacketHandlerTable::GetHandler(uint16_t packetId) const
{
	constexpr uint16_t maxPacketId = ToInteger(PACKET_ID::MAX_PACKET_ID);

	if (packetId >= maxPacketId)
		return nullptr;

	return m_handlerTable[packetId];
}