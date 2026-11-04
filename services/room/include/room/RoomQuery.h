/** @file
 * @brief Authorizes immutable room queries without advancing
 * simulation.
 */
#pragma once
#include "room/RoomRuntime.h"
namespace service::room
{
/** @brief Checks Snapshot, Status or Settlement against one committed
 * image. The caller supplies a validated image and current trusted
 * authorization values. No clock, actor, I/O, mutation or allocation
 * is needed; time is explicit. Context shape, current transport,
 * membership, result retention and phase rules are shared by the
 * worker command path and the Lobby committed read path.
 */
std::expected<void, RuntimeError> authorizeRoomQuery(
    const RuntimeImage& image,
    const RuntimeCommand& command,
    const RuntimeCutoff& context
);
} // namespace service::room
