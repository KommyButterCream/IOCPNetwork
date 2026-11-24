#include "IOCPClient.h"

#include "Detail/IOCPClientImpl.h"

#include <memory> // for std::make_unique

namespace IOCPNetwork
{
    IOCPClient::IOCPClient()
        : IOCPBase(std::make_unique<Detail::IOCPClientImpl>())
    {
    }

    IOCPClient::~IOCPClient() = default;

    StartResult IOCPClient::Start(
        const ClientOptions& options) noexcept
    {
        auto& impl = static_cast<Detail::IOCPClientImpl&>(GetImpl());

        return impl.Start(options);
    }

    StopResult IOCPClient::Stop() noexcept
    {
        auto& impl = static_cast<Detail::IOCPClientImpl&>(GetImpl());

        return impl.Stop();
    }
}
