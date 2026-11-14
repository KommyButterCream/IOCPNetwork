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
        const ServerOptions& options,
        std::size_t maxTotalSegmentBytes) noexcept
    {
        auto& impl = static_cast<Detail::IOCPServerImpl&>(GetImpl());

        return impl.Start(options, maxTotalSegmentBytes);
    }

    StopResult IOCPServer::Stop() noexcept
    {
        auto& impl = static_cast<Detail::IOCPServerImpl&>(GetImpl());

        return impl.Stop();
    }
}
