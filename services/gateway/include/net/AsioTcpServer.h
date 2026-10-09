#pragma once

#include <memory>

#include <boost/asio/io_context.hpp>

#include "net/ConnectionFeature.h"
#include "net/ConnectionHandler.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"

namespace service::gateway
{
// The supplied context and all management operations belong to the
// constructor thread. Run it on that thread only and keep it alive
// until after destruction. Each optional worker owns a separate
// io_context. Construction starts listening; the default application
// callback is echo.
class AsioTcpServer final : public ITcpServer
{
public:
    AsioTcpServer(
        boost::asio::io_context&,
        const InetAddress&,
        std::size_t maxConnections,
        std::chrono::milliseconds idleTimeout =
            std::chrono::seconds(60),
        std::size_t workerCount = 0,
        ConnectionHandlerFactory handlerFactory = {},
        ConnectionFeatureFactory featureFactory = {}
    );
    ~AsioTcpServer() noexcept override;

    AsioTcpServer(const AsioTcpServer&) = delete;
    AsioTcpServer& operator=(const AsioTcpServer&) = delete;

    void stop(std::chrono::milliseconds grace = kDefaultStopGrace)
        override;
    bool stopped() const noexcept override;
    std::size_t connectionCount() const noexcept override;
    std::size_t maxConnections() const noexcept override;
    std::size_t workerCount() const noexcept override;

private:
    struct Impl;
    std::shared_ptr<Impl> m_impl;
};
} // namespace service::gateway
