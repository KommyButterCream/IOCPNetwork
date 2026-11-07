#pragma once

#include "../Job/HandlerContext.h"
#include "../Protocol/PacketTraits.h"
#include <array> // for std::array
#include <bit> // for std::bit_cast
#include <cstring> // for std::memcpy
#include <functional> // for std::function, std::invoke
#include <span> // for std::span, std::as_bytes
#include <type_traits> // for std::is_void_v, std::is_same_v, std::remove_cvref_t
#include <utility> // for std::move

// 등록 시에만 콜백을 소유합니다. 실행 중에는 테이블이 불변이며 Job은 이 객체를 빌려 호출합니다.
class PacketHandler final
{
    using Callback = std::function<bool(ISession*, const char*, uint32_t, const HandlerContext&)>;

    template<typename Owner, typename Argument, typename Result>
    struct MemberBinding
    {
        Owner* owner;
        Result (Owner::*method)(ISession&, Argument);
        bool operator==(const MemberBinding&) const = default;

        bool operator()(ISession* session, const char* data, uint32_t size, const HandlerContext&) const
        {
            if (!session || !data)
                return false;
            if constexpr (std::is_same_v<Argument, std::span<const std::byte>>)
                return Invoke(*session, std::as_bytes(std::span(data, size)));
            else
            {
                using Packet = std::remove_cvref_t<Argument>;
                if (size != sizeof(Packet))
                    return false;
                // 바이트 수신 버퍼의 정렬·객체 수명에 의존하지 않는 작은 고정 패킷 복사입니다.
                std::array<std::byte, sizeof(Packet)> bytes;
                std::memcpy(bytes.data(), data, bytes.size());
                const auto packet = std::bit_cast<Packet>(bytes);
                if (packet.header.packetId != PacketTraits<Packet>::id || packet.header.packetSize != size)
                    return false;
                return Invoke(*session, packet);
            }
        }

        bool Invoke(ISession& session, Argument argument) const
        {
            if constexpr (std::is_void_v<Result>)
            {
                std::invoke(method, owner, session, argument);
                return true;
            }
            else
                return std::invoke(method, owner, session, argument);
        }
    };

public:
    PacketHandler() = default;
    PacketHandler(PacketHandlerFunc function)
    {
        if (function)
            *this = Make(function);
    }

    template<FixedServicePacket Packet, typename Owner, typename Result>
        requires (std::is_same_v<Result, bool> || std::is_void_v<Result>)
    static PacketHandler Bind(Owner* owner, Result (Owner::*method)(ISession&, const Packet&))
    {
        return owner && method ? Make(MemberBinding<Owner, const Packet&, Result>{owner, method}) : PacketHandler{};
    }

    template<typename Owner, typename Result>
        requires (std::is_same_v<Result, bool> || std::is_void_v<Result>)
    static PacketHandler Bind(Owner* owner, Result (Owner::*method)(ISession&, std::span<const std::byte>))
    {
        return owner && method ? Make(MemberBinding<Owner, std::span<const std::byte>, Result>{owner, method}) : PacketHandler{};
    }

    explicit operator bool() const noexcept { return static_cast<bool>(m_callback); }
    bool operator==(const PacketHandler& other) const
    {
        return m_same && m_same == other.m_same && m_same(m_callback, other.m_callback);
    }
    bool operator()(ISession* session, const char* data, uint32_t size, const HandlerContext& context) const
    {
        return m_callback(session, data, size, context);
    }

private:
    template<typename Binding> static PacketHandler Make(Binding binding)
    {
        PacketHandler handler;
        handler.m_callback = std::move(binding);
        handler.m_same = [](const Callback& left, const Callback& right)
        {
            const auto* a = left.target<Binding>();
            const auto* b = right.target<Binding>();
            return a && b && *a == *b;
        };
        return handler;
    }

    Callback m_callback;
    bool (*m_same)(const Callback&, const Callback&) = nullptr;
};
