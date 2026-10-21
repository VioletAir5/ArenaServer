/** @file
 * @brief Configures the public pre-login routes for the Arena
 * protocol.
 */
#pragma once

#include "net/ArenaProtocol.h"

namespace service::gateway
{
std::shared_ptr<const MessageRegistry> defaultMessageRegistry();

/** @brief Creates an Arena protocol with public Ping and configured
 * Login routes. */
ConnectionHandler makePreLoginHandshakeHandler(
    ConnectionContext::Ptr,
    std::shared_ptr<const MessageRegistry> registry = {},
    PacketPolicyFactory policies = {}
);
} // namespace service::gateway
