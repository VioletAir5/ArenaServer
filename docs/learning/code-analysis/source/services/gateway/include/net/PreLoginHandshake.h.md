# services/gateway/include/net/PreLoginHandshake.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；30行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Handles the pre-login ping exchange.
 */
#pragma once

#include "net/MessageRouter.h"

namespace service::gateway
{
// Place after EnvelopeDecodeHandler. Handles Ping and forwards other
// messages so later login or routing handlers can receive them.
/** @brief Represents ping handler. */
class PingHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override;
};

// Shared immutable defaults: public Ping and an explicit unavailable
// Login.
std::shared_ptr<const MessageRegistry> defaultMessageRegistry();

// A fresh decoder -> MsgId router per connection. Authentication
// policy lives in each registered route. Custom registries must be
// sealed before use.
ConnectionMessageCallback makePreLoginHandshakeHandler(
    ConnectionContext::Ptr,
    std::shared_ptr<const MessageRegistry> registry = {}
);
} // namespace service::gateway
~~~
