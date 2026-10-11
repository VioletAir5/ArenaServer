# services/gateway/include/net/ConnectionHandler.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；31行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Defines packet handling and connection callback interfaces.
 */
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
~~~
