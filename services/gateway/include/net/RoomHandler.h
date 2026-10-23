/** @file
 * @brief Registers authenticated room input and snapshot routes.
 */
#pragma once

#include <memory>

namespace service::gateway
{
class MessageRegistry;
class OnlineRegistry;
class RoomService;

/** @brief Adds room commands before publishing the immutable route
 * table.
 * @param[in] routes Unsealed application routes.
 * @param[in] sessions Authority used to capture current logical
 * identity/room.
 * @param[in] rooms Bounded fixed-owner mailbox, started by the
 * application.
 */
void registerRoomRoutes(
    MessageRegistry& routes,
    std::shared_ptr<OnlineRegistry> sessions,
    std::shared_ptr<RoomService> rooms
);
} // namespace service::gateway
