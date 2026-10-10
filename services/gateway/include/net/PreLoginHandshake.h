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
