/** @file
 * @brief Builds authenticated login, recovery, presence and roster
 * routes.
 */
#pragma once

#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "auth/LoginAuthenticator.h"

namespace service::gateway
{
/** @brief Builds sealed routes sharing one authentication and online
 * authority.
 * @param[in] passwords Password identity verifier shared across
 * workers.
 * @param[in] tokens Exact-profile JWT verifier and rotating token
 * issuer.
 * @param[in] registry Shared presence and bounded roster authority.
 */
std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>,
    std::shared_ptr<const service::auth::JwtTokens>,
    std::shared_ptr<OnlineRegistry>
);
} // namespace service::gateway
