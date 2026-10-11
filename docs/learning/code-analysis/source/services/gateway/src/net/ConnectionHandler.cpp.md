# services/gateway/src/net/ConnectionHandler.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；36行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Defines packet handling and connection callback interfaces.
 */
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
~~~
