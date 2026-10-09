#include "net/ConnectionHandler.h"

#include <stdexcept>
#include <utility>

namespace service::gateway
{
ConnectionMessageCallback makeConnectionHandler(
    const ConnectionHandlerFactory& factory,
    ConnectionContext::Ptr context
)
{
    if (!context)
    {
        throw std::invalid_argument("missing connection context");
    }
    auto handler = factory
                       ? factory(context)
                       : ConnectionMessageCallback(
                             [](IConnection& connection, auto bytes)
                             { connection.send(bytes); }
                         );
    if (!handler)
    {
        throw std::invalid_argument("empty connection handler");
    }
    return
        [context = std::move(context), handler = std::move(handler)](
            IConnection& connection,
            std::span<const std::byte> bytes
        ) { handler(connection, bytes); };
}
} // namespace service::gateway
