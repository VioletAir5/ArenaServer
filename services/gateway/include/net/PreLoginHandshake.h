#pragma once

#include "net/ConnectionHandler.h"

namespace service::gateway
{
// One handler per connection. A valid Envelope Ping receives Pong
// before login is considered; authentication and login routing are
// future stages.
ConnectionMessageCallback makePreLoginHandshakeHandler();
} // namespace service::gateway