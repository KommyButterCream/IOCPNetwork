#pragma once

#include "Export.h"

#include <memory> // for std::unique_ptr

namespace IOCPNetwork
{
    namespace Detail
    {
        class IOCPBaseImpl;
    }

    // 서버·클라이언트의 공통 공개 API와 Impl 소유권을 제공한다.
    // 실제 객체는 IOCPServer 또는 IOCPClient로 생성한다.
    class IOCPBase
    {
    public:
        // 기반 포인터를 통해서도 파생 공개 객체를 올바르게 파괴한다.
        IOCPNETWORK_API virtual ~IOCPBase();

        IOCPBase(const IOCPBase&) = delete;
        IOCPBase& operator=(const IOCPBase&) = delete;

        IOCPBase(IOCPBase&&) = delete;
        IOCPBase& operator=(IOCPBase&&) = delete;

    protected:
        // 파생 공개 객체가 만든 실제 Impl의 단독 소유권을 인수한다.
        explicit IOCPBase(std::unique_ptr<Detail::IOCPBaseImpl> impl) noexcept;

        // DLL 내부의 서버·클라이언트 구현에서 소유 중인 Impl을 빌린다.
        // 소유권은 IOCPBase에 있으며 반환한 참조를 별도로 보관하지 않는다.
        [[nodiscard]] Detail::IOCPBaseImpl& GetImpl() noexcept;

    private:
        // 실제 서버 또는 클라이언트 Impl 하나를 소유한다.
        std::unique_ptr<Detail::IOCPBaseImpl> m_impl;
    };
}
