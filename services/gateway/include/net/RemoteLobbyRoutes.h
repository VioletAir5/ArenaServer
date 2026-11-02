/** @file
 * @brief Builds transport-only Gateway routes backed by remote Lobby
 * authority.
 */
#pragma once

#include <memory>

#include "net/LobbyClient.h"
#include "net/MessageRegistry.h"

namespace service::gateway
{
/** @brief Installs local PING and every existing remote business
 * request route. */
std::shared_ptr<const MessageRegistry>
    makeRemoteLobbyMessageRegistry(std::shared_ptr<LobbyClient>);
} // namespace service::gateway
