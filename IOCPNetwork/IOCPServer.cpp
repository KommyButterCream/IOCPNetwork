#include "IOCPServer.h"

#include "Detail/IOCPServerImpl.h"

#include <memory> // for std::make_unique

namespace IOCPNetwork
{
    IOCPServer::IOCPServer()
        : IOCPBase(std::make_unique<Detail::IOCPServerImpl>())
    {
    }

    IOCPServer::~IOCPServer() = default;

    StartResult IOCPServer::Start(
        const ServerOptions& options) noexcept
    {
        auto& impl = static_cast<Detail::IOCPServerImpl&>(GetImpl());

        return impl.Start(options);
    }

    StopResult IOCPServer::Stop() noexcept
    {
        auto& impl = static_cast<Detail::IOCPServerImpl&>(GetImpl());

        return impl.Stop();
    }
}
