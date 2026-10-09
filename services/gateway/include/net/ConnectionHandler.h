#pragma once

#include <functional>

#include "net/ConnectionContext.h"
#include "net/IConnection.h"

namespace service::gateway
{
using ConnectionMessageCallback = std::function<
    void(IConnection&, std::span<const std::byte>)>;

// Copied once per worker; invoked once per connection on its owner
// before input delivery (before HTTP Upgrade for WS). Return fresh
// callback state. The immutable context may be retained; session
// access stays on its owner. Captures shared across worker copies
// need their own synchronization.
using ConnectionHandlerFactory = std::function<
    ConnectionMessageCallback(ConnectionContext::Ptr)>;

// Installs a lifetime wrapper that retains context even if the
// factory ignores it. An empty factory selects raw echo; an empty
// result is invalid.
ConnectionMessageCallback makeConnectionHandler(
    const ConnectionHandlerFactory& factory,
    ConnectionContext::Ptr context
);
} // namespace service::gateway
