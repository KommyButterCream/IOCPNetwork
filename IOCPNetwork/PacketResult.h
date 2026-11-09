#pragma once

#include "Packet.h"
#include "PacketError.h"
#include "PacketWriter.h"

namespace IOCPNetwork
{
    // 패킷 생성 또는 Writer 완료 결과를 전달한다.
    struct [[nodiscard]] PacketResult
    {
        Packet packet;

        PacketError error = PacketError::InvalidState;

        // 오류가 없고 실제 패킷 저장소를 소유할 때 성공이다.
        [[nodiscard]] explicit operator bool() const noexcept
        {
            return error == PacketError::None && static_cast<bool>(packet);
        }
    };

    // 작성용 저장소 생성 결과를 전달한다.
    struct [[nodiscard]] PacketWriterResult
    {
        PacketWriter writer;

        PacketError error = PacketError::InvalidState;

        // 오류가 없고 실제 작성용 저장소를 소유할 때 성공이다.
        [[nodiscard]] explicit operator bool() const noexcept
        {
            return error == PacketError::None && static_cast<bool>(writer);
        }
    };
}
