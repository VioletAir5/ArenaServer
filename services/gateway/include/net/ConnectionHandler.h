/** @file
 * @brief Assembles per-connection protocol callbacks and lifecycle
 * hooks.
 */
#pragma once

#include <concepts>
#include <functional>
#include <utility>

#include "net/ConnectionChannel.h"

namespace service::gateway
{
using ConnectionMessageCallback = std::function<
    void(ConnectionChannel&, std::span<const std::byte>)>;

/** @brief Owns one protocol's input and lifecycle callbacks.
 *
 * All callbacks run on the connection owner. onClosed runs exactly
 * once, after active callbacks unwind, including when construction
 * never starts.
 */
struct ConnectionHandler
{
    ConnectionHandler() = default;
    /** @brief Creates a byte handler without optional lifecycle
     * hooks. */
    template <class F>
        requires std::invocable<
            F&,
            ConnectionChannel&,
            std::span<const std::byte>>
    ConnectionHandler(F&& callback)
        : onMessage(std::forward<F>(callback))
    {
    }
    /// Consumes borrowed input bytes during one owner-thread
    /// callback.
    ConnectionMessageCallback onMessage;
    /// Activates protocol deadlines and authentication lifecycle.
    std::function<void(ConnectionChannel&)> onStarted;
    /// Stops request admission, then invokes drained on the owner
    /// after every pending reply or deadline has completed. An absent
    /// hook drains at once. The callback is idempotent and does not
    /// retain the host or transport.
    std::function<void(ConnectionChannel&, std::function<void()>)>
        onDraining;
    /// Cancels protocol work and releases authentication/session
    /// bindings.
    std::function<void()> onClosed;
};

// Copied once per worker and invoked once on the connection owner
// before input delivery. Each call returns fresh protocol state.
// Shared captures need their own synchronization.
using ConnectionHandlerFactory = std::function<
    ConnectionHandler(ConnectionContext::Ptr)>;

/** @brief Retains context and validates the factory's protocol
 * callbacks.
 * @param[in] factory Produces fresh per-connection protocol state;
 * empty echoes.
 * @param[in] context Immutable admission metadata retained by the
 * handler.
 */
ConnectionHandler makeConnectionHandler(
    const ConnectionHandlerFactory& factory,
    ConnectionContext::Ptr context
);
} // namespace service::gateway
