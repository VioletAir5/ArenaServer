/** @file
 * @brief Builds authenticated login, presence, roster and matching
 * routes.
 */
#pragma once

#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "auth/LoginAuthenticator.h"

namespace service::gateway
{
class MatchmakingService;
class RoomService;

/** @brief Builds sealed routes sharing one authentication and online
 * authority.
 * @param[in] passwords Password identity verifier shared across
 * workers.
 * @param[in] tokens Exact-profile JWT verifier and rotating token
 * issuer.
 * @param[in] registry Shared presence and bounded roster authority.
 * @param[in] matchmaking Optional management-owner matching service.
 * @param[in] rooms Optional fixed-owner room input and snapshot
 * service.
 */
std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>,
    std::shared_ptr<const service::auth::JwtTokens>,
    std::shared_ptr<OnlineRegistry>,
    std::shared_ptr<MatchmakingService> matchmaking = {},
    std::shared_ptr<RoomService> rooms = {}
);
} // namespace service::gateway
