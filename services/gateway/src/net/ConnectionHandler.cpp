/** @file
 * @brief Assembles per-connection protocol callbacks and lifecycle
 * hooks.
 */
#include "net/ConnectionHandler.h"

#include <stdexcept>
#include <utility>

namespace service::gateway
{
ConnectionHandler makeConnectionHandler(
    const ConnectionHandlerFactory& factory,
    ConnectionContext::Ptr context
)
{
    if (!context)
        throw std::invalid_argument("missing connection context");
    auto handler = factory
                       ? factory(context)
                       : ConnectionHandler(
                             [](ConnectionChannel& channel,
                                auto bytes) { channel.send(bytes); }
                         );
    if (!handler.onMessage)
        throw std::invalid_argument("empty connection handler");
    ConnectionHandler wrapped;
    wrapped.onMessage = [context,
                         callback = std::move(handler.onMessage)](
                            ConnectionChannel& channel,
                            std::span<const std::byte> bytes
                        ) { callback(channel, bytes); };
    wrapped.onStarted =
        [context, callback = std::move(handler.onStarted)](
            ConnectionChannel& channel
        )
    {
        if (callback)
            callback(channel);
    };
    if (handler.onDraining)
        wrapped.onDraining =
            [context, callback = std::move(handler.onDraining)](
                ConnectionChannel& channel,
                std::function<void()> drained
            ) { callback(channel, std::move(drained)); };
    wrapped.onClosed =
        [context, callback = std::move(handler.onClosed)]
    {
        if (callback)
            callback();
    };
    return wrapped;
}
} // namespace service::gateway
