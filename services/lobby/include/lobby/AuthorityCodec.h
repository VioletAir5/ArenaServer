/** @file
 * @brief Encodes bounded typed Lobby checkpoints and deterministic
 * replay segments.
 */
#pragma once

#include <cstddef>
#include <expected>
#include <string>
#include <string_view>

#include "lobby/OnlineRegistry.h"
#include "lobby/RoomService.h"
#include "match/MatchQueue.h"
#include "room/RoomActor.h"

namespace service::lobby
{
/// Maximum wire bytes for one checkpoint, tick or replay segment.
inline constexpr std::size_t authorityCodecMaxBytes = 4 * 1024 * 1024;
/// Maximum aggregate collection elements and nested messages per
/// value.
inline constexpr std::size_t authorityCodecMaxItems = 65536;

enum class CodecError
{
    Invalid,
    UnsupportedVersion,
    Capacity
};

/** @brief Owns all three business checkpoint roots without runtime
 * bindings. */
struct CoreCheckpoint
{
    /// Complete retained sessions and authoritative room indexes.
    RegistryCheckpoint registry;
    /// Complete tickets, search cursor, policy and metrics.
    service::match::QueueCheckpoint queue;
    /// Complete room kernel snapshots and managed lifecycle facts.
    RoomCheckpoint rooms;
    /// True after all remaining legacy rooms have moved to worker
    /// ownership.
    bool distributedRooms{false};
};

/** @brief Encodes schema version one; this function never reads
 * clocks or IDs. */
std::expected<std::string, CodecError>
encodeCoreCheckpoint(const CoreCheckpoint&);
/** @brief Rejects unknown durable fields and malformed or oversized
 * state. Business invariants are additionally checked by the existing
 * restore methods. Clock values require the supported Linux signed
 * 64-bit nanosecond ABI; the outer authority owns boot identity and
 * any cross-boot clock mapping.
 */
std::expected<CoreCheckpoint, CodecError>
    decodeCoreCheckpoint(std::string_view);
/** @brief Preserves the canonical attempts and roster, including
 * rejected inputs. */
std::expected<std::string, CodecError>
encodeTickRecord(const service::room::TickRecord&);
/** @brief Decodes one versioned canonical tick without applying
 * simulation state. */
std::expected<service::room::TickRecord, CodecError>
    decodeTickRecord(std::string_view);
/** @brief Encodes one bounded self-contained replay segment. */
std::expected<std::string, CodecError>
encodeReplayLog(const service::room::ReplayLog&);
/** @brief Preserves config, initial checkpoint and every ordered
 * cutoff in a segment. */
std::expected<service::room::ReplayLog, CodecError>
    decodeReplayLog(std::string_view);
} // namespace service::lobby
