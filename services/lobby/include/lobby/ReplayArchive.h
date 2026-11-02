/** @file
 * @brief Assembles validated durable segments into complete offline
 * replay archives.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <vector>

#include "lobby/DurableStateStore.h"
#include "room/ReplayFile.h"

namespace service::lobby
{
enum class ReplayArchiveErrorCode
{
    Invalid,
    Desync,
    Capacity
};
struct ReplayArchiveError
{
    ReplayArchiveErrorCode code;
    /// Constant diagnostic text; no metadata, credentials or input
    /// payload is exposed.
    std::string reason;
    std::optional<std::uint64_t> roomId, incarnation, tick;
};
/** @brief Owns one complete actor history and its already verified
 * terminal state. */
struct ReplayArchive
{
    service::room::ReplayLog log;
    service::room::Snapshot finalSnapshot;
    /// Exact bytes produced by the existing version-one replay
    /// writer.
    std::uint64_t fileBytes;
};
/** @brief Reassembles only canonical replay keys without reading
 * files or clocks.
 *
 * All segments must begin at tick zero and join through identical
 * complete checkpoints/configuration. Inputs and ticks retain their
 * original order. Limits bound aggregate output bytes/ticks as well
 * as each actor policy; codec limits separately bound each segment.
 * maxRecords bounds the complete source map and must be between one
 * and the durable store's supported maximum of 1,000,000 keys.
 */
std::expected<std::vector<ReplayArchive>, ReplayArchiveError>
assembleReplayArchives(
    const DurableStateStore::Snapshot&,
    service::room::ReplayFileLimits limits = {},
    std::size_t maxRecords = 65536
);
} // namespace service::lobby
