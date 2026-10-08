#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>

#include "net/ConnectionHandler.h"
#include "net/InetAddress.h"

namespace service::gateway
{
// WebSocket binary messages carry the same PacketCodec byte stream as
// TCP. Construct, run and stop on the io_context owner thread. Origin
// is an exact allowlist entry; empty or different Origin is rejected
// before upgrade.
class WebSocketServer final
{
public:
    WebSocketServer(
        boost::asio::io_context& context,
        const InetAddress& address,
        std::string allowedOrigin,
        ConnectionHandlerFactory handlerFactory,
        std::size_t maxConnections = 1000
    );
    ~WebSocketServer() noexcept;

    WebSocketServer(const WebSocketServer&) = delete;
    WebSocketServer& operator=(const WebSocketServer&) = delete;

    void stop() noexcept;
    std::size_t connectionCount() const noexcept;

private:
    struct Impl;
    std::shared_ptr<Impl> m_impl;
};
} // namespace service::gateway