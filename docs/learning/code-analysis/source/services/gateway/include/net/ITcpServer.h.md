# services/gateway/include/net/ITcpServer.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；44行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Defines the lifecycle interface for TCP server implementations.
 */
#pragma once

#include <chrono>
#include <cstddef>

namespace service::gateway
{
// Management operations and destruction belong to the server's owning
// execution context. The caller keeps that context alive until after
// destruction. No automatic cross-thread dispatch is promised.
// Backend-specific construction/startup is outside this interface.
/** @brief Defines the lifecycle interface for TCP server implementations. */
class ITcpServer
{
public:
    static constexpr std::chrono::milliseconds kDefaultStopGrace{
        std::chrono::seconds(5)};

    /** @brief Constructs the i tcp server. */
    virtual ~ITcpServer() noexcept = default;

    // Stop accepting, reject new application sends, drain queued output
    // and send FIN. Force-close remaining connections at the deadline.
    // With active connections, observe completion via stopped().
    // Repeated requests are ignored.
    // Negative grace throws std::invalid_argument before changing state.
    // This does not stop the caller's externally owned execution context.
    virtual void stop(
        std::chrono::milliseconds grace = kDefaultStopGrace) = 0;

    // True only after all connections are reclaimed and the backend's
    // owned I/O workers are cleaned up and joined.
    virtual bool stopped() const noexcept = 0;
    // Includes accepted sockets awaiting registration, until their
    // owner acknowledges cleanup. Zero does not by itself mean stopped.
    virtual std::size_t connectionCount() const noexcept = 0;
    virtual std::size_t maxConnections() const noexcept = 0;
    // Configured additional I/O workers; zero uses the external context.
    virtual std::size_t workerCount() const noexcept = 0;
};
} // namespace service::gateway
~~~
