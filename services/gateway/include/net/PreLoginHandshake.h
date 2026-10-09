#pragma once

#include "net/PacketPipeline.h"

namespace service::gateway
{
// Place after EnvelopeDecodeHandler. Handles Ping and forwards other
// messages so later login or routing handlers can receive them.
class PingHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override;
};

// A fresh decoder -> Ping chain per connection. Other messages are
// rejected at the tail; authentication and login routing are planned.
ConnectionMessageCallback makePreLoginHandshakeHandler();
} // namespace service::gateway
