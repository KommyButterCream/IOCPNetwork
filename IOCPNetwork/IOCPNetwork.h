#pragma once

#include "Export.h"

#include "ConnectionId.h"
#include "NetworkOptions.h"
#include "NetworkTypes.h"
#include "Packet.h"
#include "PacketError.h"
#include "PacketResult.h"
#include "PacketTypes.h"
#include "PacketView.h"
#include "PacketWriter.h"
#include "SessionTask.h"

namespace IOCPNetwork
{
    // DLL이 소유하는 라이브러리 버전 문자열을 반환한다. 패킷 프로토콜 버전과는 별개다.
    [[nodiscard]] IOCPNETWORK_API const char* GetLibraryVersion() noexcept;
}
