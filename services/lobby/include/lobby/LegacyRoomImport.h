/** @file
 * @brief Transfers legacy room checkpoint values to a real worker
 * boundary.
 */
#pragma once

#include <expected>
#include <optional>
#include <vector>

#include "lobby/RoomService.h"
#include "room/RoomRuntime.h"

namespace service::lobby
{
enum class LegacyRoomImportError
{
    Invalid,
    Unsupported,
    Capacity
};
struct LegacyRoomImage
{
    service::room::RuntimeImage image;
    /// Present only when an actual legacy aggregate supplied a
    /// rolling log.
    std::optional<service::room::ReplayLog> replay;
    /// Obsolete callback-admission counters retained without
    /// assigning worker jobs.
    RoomMetrics unattributed;
};
struct LegacyRoomImport
{
    std::vector<service::room::RuntimeImage> images;
    /// Retired and unknown historical events, never guessed into live
    /// rooms.
    RoomMetrics unattributed;
    std::uint64_t incarnationHighWater{0};
};
/** @brief Splits value checkpoints without constructing or validating
 * a kernel. Real worker Stage owns RoomRuntime/RoomActor semantic
 * validation. Linux boot fencing is a prerequisite supplied by the
 * authority; clock conversion remains overflow checked. No global
 * registry, callbacks or retry jobs are copied.
 */
std::expected<LegacyRoomImport, LegacyRoomImportError>
splitLegacyRooms(const RoomCheckpoint&);
/** @brief Retains the aggregate's actual rolling replay as opaque
 * input values. */
std::expected<LegacyRoomImage, LegacyRoomImportError>
importLegacyRoom(const RoomAggregate&);
} // namespace service::lobby