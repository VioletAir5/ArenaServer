/** @file
 * @brief Builds authenticated login, lobby, matching and managed
 * room lifecycle routes.
 */
#pragma once

#include <chrono>
#include <expected>
#include <functional>
#include <string>

#include "net/MessageRegistry.h"

#include "auth/LoginAuthenticator.h"
#include "gateway_test/RoomHandler.h"
#include "lobby/MatchmakingService.h"
#include "lobby/OnlineRegistry.h"
#include "lobby/RoomService.h"

namespace service::gateway
{

using PasswordLoginResult = std::
    expected<arena::domain::PlayerId, service::auth::AuthError>;
using PasswordLoginCompletion = std::function<
    void(PasswordLoginResult)>;
using PasswordLoginCancellation = std::function<void()>;
/** @brief Admits password verification away from the connection
 * owner. A rejected admission must not invoke completion. Accepted
 * completion may run on any thread; its cancellation handle must also
 * be thread-safe.
 */
using AsyncPasswordLogin = std::function<std::expected<
    PasswordLoginCancellation,
    service::auth::AuthError>(
    std::string,
    std::string,
    std::chrono::steady_clock::time_point,
    std::string,
    PasswordLoginCompletion
)>;

/** @brief Builds sealed routes sharing one authentication and online
 * authority.
 * @param[in] passwords Password identity verifier shared across
 * workers.
 * @param[in] tokens Exact-profile JWT verifier and rotating token
 * issuer.
 * @param[in] registry Shared presence and bounded roster authority.
 * @param[in] matchmaking Optional management-owner matching service.
 * @param[in] rooms Optional fixed-owner room input, readiness,
 * recovery and immutable result service.
 * @param[in] asyncPasswords Optional asynchronous password verifier
 * with a bounded admission/cancellation contract; otherwise passwords
 * is synchronous.
 * @param[in] settlementLookup Optional durable owner-fenced result
 * lookup.
 */
std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>,
    std::shared_ptr<const service::auth::JwtTokens>,
    std::shared_ptr<service::lobby::OnlineRegistry>,
    std::shared_ptr<service::lobby::MatchmakingService> matchmaking =
        {},
    std::shared_ptr<service::lobby::RoomService> rooms = {},
    AsyncPasswordLogin asyncPasswords = {},
    DurableSettlementLookup settlementLookup = {}
);
} // namespace service::gateway
