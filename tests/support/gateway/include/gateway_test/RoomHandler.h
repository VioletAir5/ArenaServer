/** @file
 * @brief Registers authenticated room controls, recovery and results.
 */
#pragma once

#include <memory>

#include "net/MessageRegistry.h"

#include "lobby/OnlineRegistry.h"
#include "lobby/RoomService.h"
#include "runtime/OwnerScheduler.h"

namespace service::gateway
{
class MessageRegistry;

/** @brief Queries a committed result for one authenticated canonical
 * player. */
using DurableSettlementLookup = std::function<
    std::expected<std::function<void()>, ErrorCode>(
        arena::domain::PlayerId,
        std::uint64_t,
        arena::runtime::OwnerClock::time_point,
        std::string,
        service::lobby::RoomService::SettlementCompletion
    )>;

/** @brief Adds room commands before publishing the immutable route
 * table.
 * @param[in] routes Unsealed application routes.
 * @param[in] sessions Authority used to capture current logical
 * identity/room.
 * @param[in] rooms Bounded fixed-owner mailbox for input, readiness,
 * current/recovered lifecycle state and retained settlement queries.
 */
void registerRoomRoutes(
    MessageRegistry& routes,
    std::shared_ptr<service::lobby::OnlineRegistry> sessions,
    std::shared_ptr<service::lobby::RoomService> rooms,
    DurableSettlementLookup lookup = {}
);
} // namespace service::gateway
