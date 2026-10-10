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

// A fresh decoder -> authentication gate -> Ping chain per connection.
// Login authentication and business routing are still planned.
ConnectionMessageCallback makePreLoginHandshakeHandler(ConnectionContext::Ptr);
} // namespace service::gateway
