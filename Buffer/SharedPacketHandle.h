#pragma once
#include "PacketHandle.h"

#include <utility> // for std::exchange

// 복사본은 변경하지 않는 패킷 버퍼를 공유하며, 각 핸들은 참조 하나를 소유합니다.
// 서비스가 보관한 복사본은 해당 엔진을 종료하기 전에 반환합니다.
class SharedPacketHandle final
{
public:
    SharedPacketHandle() noexcept = default;
    IOCP_ENGINE_API ~SharedPacketHandle() noexcept;

    IOCP_ENGINE_API SharedPacketHandle(const SharedPacketHandle& other) noexcept;
    IOCP_ENGINE_API SharedPacketHandle& operator=(const SharedPacketHandle& other) noexcept;
    SharedPacketHandle(SharedPacketHandle&& other) noexcept
        : m_control(std::exchange(other.m_control, nullptr))
    {

    }
    IOCP_ENGINE_API SharedPacketHandle& operator=(SharedPacketHandle&& other) noexcept;

    // 엔진 내부 생성 함수입니다. 서비스는 CreateSharedPacket()을 사용합니다.
    [[nodiscard]] static SharedPacketHandle Create(TlsMemoryPool& pool, PacketHandle packet);

    [[nodiscard]] static std::size_t GetControlSize() noexcept;

    IOCP_ENGINE_API void Reset() noexcept;

    [[nodiscard]] explicit operator bool() const noexcept { return m_control != nullptr; }

    [[nodiscard]] IOCP_ENGINE_API std::size_t Size() const noexcept;

    [[nodiscard]] IOCP_ENGINE_API std::span<const std::byte> View() const noexcept;

private:
    struct Control;

    static void AddRef(Control* control) noexcept;
    static void Release(Control* control) noexcept;
    Control* m_control = nullptr;
};
