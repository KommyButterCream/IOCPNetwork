#pragma once

#include <array> // for std::array
#include <cstdint> // for uint16_t

#include "../Job/HandlerContext.h" // for PacketHandlerFunc
#include "../Protocol/PacketID.h"

// 정지 상태에서 등록하고, 실행 중에는 패킷 ID로 읽기만 한다.
class PacketHandlerTable final
{
public:
	// 같은 등록은 허용하되, 기존 핸들러를 다른 함수로 덮어쓰지 않는다.
	[[nodiscard]] bool Register(uint16_t packetId, PacketHandlerFunc handler) noexcept
	{
		if (!IsServicePacketId(packetId) || !handler)
			return false;

		auto& registered = m_handlerTable[packetId];
		if (registered)
			return registered == handler;
		registered = handler;
		return true;
	}

	// 범위를 벗어났거나 등록되지 않은 ID는 nullptr을 반환한다.
	[[nodiscard]] PacketHandlerFunc GetHandler(uint16_t packetId) const noexcept
	{
		return packetId < m_handlerTable.size() ? m_handlerTable[packetId] : nullptr;
	}

private:
	std::array<PacketHandlerFunc, ToInteger(PACKET_ID::MAX_PACKET_ID)> m_handlerTable{};
};
