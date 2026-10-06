#pragma once
#include <functional>

#include "net/IConnection.h"

namespace service::gateway
{
using ConnectionMessageCallback = std::function<
    void(IConnection&, std::span<const std::byte>)>;
// Copied once per worker, invoked once per connection on that owner.
// Return a fresh callback with per-connection state; captures shared
// across worker copies need their own synchronization. Callback state
// is released with the connection on its owner. Empty factory selects
// legacy raw echo.
using ConnectionHandlerFactory = std::function<
    ConnectionMessageCallback()>;
} // namespace service::gateway
