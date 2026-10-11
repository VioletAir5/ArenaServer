# services/gateway/include/net/WebSocketServer.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；50行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Serves WebSocket connections over the gateway transport.
 */
#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>

#include "net/ConnectionFeature.h"
#include "net/ConnectionHandler.h"
#include "net/InetAddress.h"

namespace service::gateway
{
// WebSocket binary messages carry the same PacketCodec byte stream as
// TCP. Construct, run and stop on the io_context owner thread. Origin
// is an exact allowlist entry; empty or different Origin is rejected
// before upgrade.
/** @brief Serves WebSocket connections over the gateway transport. */
class WebSocketServer final
{
public:
    WebSocketServer(
        boost::asio::io_context& context,
        const InetAddress& address,
        std::string allowedOrigin,
        ConnectionHandlerFactory handlerFactory,
        std::size_t maxConnections = 1000,
        ConnectionFeatureFactory featureFactory = {}
    );
    /** @brief Releases owned resources. */
    ~WebSocketServer() noexcept;

    /** @brief Copy construction is disabled. */
    WebSocketServer(const WebSocketServer&) = delete;
    WebSocketServer& operator=(const WebSocketServer&) = delete;

    void stop() noexcept;
    /** @brief Returns the number of active connections. */
    std::size_t connectionCount() const noexcept;

private:
    struct Impl;
    /// Stores the impl.
    std::shared_ptr<Impl> m_impl;
};
} // namespace service::gateway
~~~
