#pragma once

namespace IOCPNetwork
{
    // 패킷 생성 또는 작성 완료 과정의 실패 원인을 나타낸다.
    enum class PacketError
    {
        None,                   // 성공
        InvalidPacketId,        // 생성 경로에서 허용하지 않는 ID
        PacketTooLarge,         // 요청한 본문 크기·용량이 허용 상한 초과
        NoMemory,               // 패킷 저장소 확보 실패
        InvalidState,           // 빈 Writer에서 Finish() 호출 등
        InvalidPayloadSize      // Finish()에 전달한 작성 길이가 확보한 용량 초과
    };
}
