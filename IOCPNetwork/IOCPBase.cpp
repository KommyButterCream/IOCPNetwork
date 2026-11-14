#include "IOCPBase.h"

#include "Detail/IOCPBaseImpl.h"

#include <exception> // for std::terminate
#include <utility>   // for std::move

namespace IOCPNetwork
{
    IOCPBase::IOCPBase(std::unique_ptr<Detail::IOCPBaseImpl> impl) noexcept
        : m_impl(std::move(impl))
    {
        // 공개 엔진 객체는 항상 대응하는 내부 구현을 소유해야 한다.
        if (!m_impl)
        {
            std::terminate();
        }
    }

    IOCPBase::~IOCPBase() = default;

    Detail::IOCPBaseImpl& IOCPBase::GetImpl() noexcept
    {
        return *m_impl;
    }
}
