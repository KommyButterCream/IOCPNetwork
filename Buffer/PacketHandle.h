#pragma once
#include "../EngineApi.h"
#include <cstddef> // for std::byte, std::size_t
#include <span>    // for std::span
#include <utility> // for std::exchange
class TlsMemoryPool;

// 이동 전용 버퍼 소유권입니다. 서비스에 보관한 핸들은 엔진 종료 전에 반환합니다.
// 원본 풀은 모든 핸들보다 오래 살아 있어야 하며, 엔진 파괴·재시작 이후까지 핸들을 보관하지 않습니다.
class PacketHandle final
{
public:
    PacketHandle() noexcept = default;
    ~PacketHandle() noexcept { Reset(); }

    PacketHandle(const PacketHandle&) = delete;
    PacketHandle& operator=(const PacketHandle&) = delete;

    PacketHandle(PacketHandle&& other) noexcept
        : m_pool(std::exchange(other.m_pool, nullptr))
        , m_data(std::exchange(other.m_data, nullptr))
        ,  m_size(std::exchange(other.m_size, 0))
    {

    }

    PacketHandle& operator=(PacketHandle&& other) noexcept
    {
        if (this != &other)
        {
            Reset();

            m_pool = std::exchange(other.m_pool, nullptr);
            m_data = std::exchange(other.m_data, nullptr);
            m_size = std::exchange(other.m_size, 0);
        }

        return *this;
    }

    // 엔진 내부에서 버퍼 소유권을 인수합니다. 서비스는 CreatePacket()을 사용합니다.
    [[nodiscard]] static PacketHandle Take(TlsMemoryPool& pool, void* data, std::size_t size) noexcept;

    IOCP_ENGINE_API void Reset() noexcept;

    [[nodiscard]] explicit operator bool() const noexcept
    {
        return m_data != nullptr; 
    }

    [[nodiscard]] std::size_t Size() const noexcept
    { 
        return m_size; 
    }

    [[nodiscard]] std::span<const std::byte> View() const noexcept
    {
        return m_data ? std::span<const std::byte>{m_data, m_size} : std::span<const std::byte>{};
    }

    [[nodiscard]] std::span<std::byte> MutableView() noexcept
    {
        return m_data ? std::span<std::byte>{m_data, m_size} : std::span<std::byte>{};
    }

private:
    TlsMemoryPool* m_pool = nullptr;
    std::byte* m_data = nullptr;
    std::size_t m_size = 0;
};
