/** @file
 * @brief Serves WebSocket connections over the gateway transport.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>

#include "net/AdmissionController.h"
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
        ConnectionFeatureFactory featureFactory = {},
        std::shared_ptr<AdmissionController> admission = {}
    );
    /** @brief Releases owned resources. */
    ~WebSocketServer() noexcept;

    /** @brief Copy construction is disabled. */
    WebSocketServer(const WebSocketServer&) = delete;
    WebSocketServer& operator=(const WebSocketServer&) = delete;

    /** @brief Stops acceptance and drains output within the grace
     * period. Nonpositive grace aborts immediately and can escalate a
     * prior drain. Positive grace is not extended by repeated calls.
     * Destruction aborts.
     */
    void stop(
        std::chrono::milliseconds grace =
            std::chrono::milliseconds::zero()
    ) noexcept;
    /** @brief Returns the number of active connections. */
    std::size_t connectionCount() const noexcept;

private:
    struct Impl;
    /// Stores the impl.
    std::shared_ptr<Impl> m_impl;
};
} // namespace service::gateway
