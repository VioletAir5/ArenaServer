/** @file
 * @brief Registers authenticated matchmaking commands before route
 * publication.
 */
#pragma once

#include <memory>

#include "lobby/MatchmakingService.h"
#include "lobby/OnlineRegistry.h"

namespace service::gateway
{
class MessageRegistry;

/** @brief Adds join, cancel and status routes to an unsealed
 * registry.
 * @param[in] routes Mutable route table that the assembly seals
 * afterward.
 * @param[in] sessions Shared authority used to capture the caller's
 * logical session.
 * @param[in] matchmaking Bounded management-owner command endpoint.
 */
void registerMatchRoutes(
    MessageRegistry& routes,
    std::shared_ptr<service::lobby::OnlineRegistry> sessions,
    std::shared_ptr<service::lobby::MatchmakingService> matchmaking
);
} // namespace service::gateway
