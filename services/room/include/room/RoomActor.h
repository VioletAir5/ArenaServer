/** @file
 * @brief Defines an owner-affine deterministic room and replay
 * journal.
 */
#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arena/domain/Identity.h"

namespace service::room
{
using PlayerId = arena::domain::PlayerId;
using SessionId = arena::domain::SessionId;
using RoomId = arena::domain::RoomId;

/** @brief Bounds simulation work and retained replay history. */
struct ActorConfig
{
    /// Maximum synchronized logical members, including Detached
    /// players.
    std::size_t maxMembers{64};
    /// Maximum input attempts in one logical tick.
    std::size_t maxInputsPerTick{256};
    /// Number of ticks retained with a rolling checkpoint.
    std::size_t historyTicks{200};
    /// Inclusive integer coordinate limit; movement clamps at either
    /// boundary.
    std::int32_t positionLimit{1000000};
};

/** @brief Identifies one roster membership across leave/rejoin and
 * recovery. */
struct Member
{
    /// Stable authenticated player identity.
    PlayerId playerId;
    /// Logical login identity retained during authorized recovery.
    SessionId sessionId;
    /// Nonzero room-membership generation from the authoritative
    /// registry.
    std::uint64_t epoch;
    bool operator==(const Member&) const = default;
};

/** @brief Owns a bounded integer control, applied only at a tick
 * boundary. */
struct Input
{
    /// Exact logical membership addressed by this attempt.
    Member member;
    /// Contiguous sequence beginning at one for each membership
    /// generation.
    std::uint64_t sequence;
    /// One integer grid step per applied control, in [-1, 1].
    std::int32_t dx;
    /// Vertical control with the same range as dx.
    std::int32_t dy;
    bool operator==(const Input&) const = default;
};

enum class InputStatus
{
    Applied,
    Duplicate,
    Gap,
    Stale,
    Conflict,
    NotMember,
    Invalid
};

/** @brief Captures every player field that can affect a future input.
 */
struct PlayerState
{
    /// Full membership fence used to preserve or reset this state.
    Member member;
    /// Deterministic reference-state coordinates; no floating-point
    /// integration.
    std::int32_t x{0}, y{0};
    /// Last applied sequence and its controls for exact retry
    /// detection.
    std::uint64_t sequence{0};
    /// Controls associated with the last applied sequence.
    std::int32_t lastDx{0}, lastDy{0};
    bool operator==(const PlayerState&) const = default;
};

/** @brief Copies owner state in canonical ascending PlayerId order.
 */
struct Snapshot
{
    /// Authoritative roster room represented by this actor.
    RoomId roomId;
    /// Process-unique actor incarnation; input from an old actor
    /// cannot target a replacement.
    std::uint64_t incarnation;
    /// Logical 50 ms step index, independent of callback wall-time
    /// delay.
    std::uint64_t tick{0};
    /// Version-one FNV-1a64 over explicit little-endian integer
    /// state.
    std::uint64_t checksum{0};
    /// Complete canonical member state, sorted by PlayerId.
    std::vector<PlayerState> players;
    bool operator==(const Snapshot&) const = default;
};

/** @brief Stores the full tick cutoff, including empty ticks and
 * rejected attempts. */
struct TickRecord
{
    /// Contiguous logical step reproduced by this record.
    std::uint64_t tick;
    /// Full authoritative roster sampled for this tick, sorted by
    /// PlayerId.
    std::vector<Member> members;
    /// Canonically sorted attempts; arrival order and timestamps are
    /// excluded.
    std::vector<Input> inputs;
    /// Expected state hash after applying this full cutoff.
    std::uint64_t checksum;
};

/** @brief Keeps replay self-contained after bounded history rolls
 * over. */
struct ReplayLog
{
    /// Immutable simulation and retained-history limits.
    ActorConfig config;
    /// State immediately before the oldest retained record.
    Snapshot checkpoint;
    /// Canonical consecutive records, including empty-input ticks.
    std::vector<TickRecord> ticks;
};

struct TickResult
{
    /// Owning copy of the newly committed state.
    Snapshot snapshot;
    /// One outcome per input, returned in the caller's original
    /// order.
    std::vector<InputStatus> results;
    /// Owning canonical cutoff prepared before the tick commits,
    /// ready to archive.
    TickRecord record;
};
enum class ReplayError
{
    InvalidLog,
    ChecksumMismatch
};
/** @brief Locates the first invalid record or state divergence in
 * absolute ticks. */
struct ReplayFailure
{
    /// Invalid structure/configuration is distinct from a valid
    /// record that desyncs.
    ReplayError code;
    /// Affected absolute tick; checkpoint/configuration failures use
    /// checkpoint.tick.
    std::uint64_t tick;
    /// Stored and computed hashes are present only for
    /// ChecksumMismatch.
    std::optional<std::uint64_t> expectedChecksum;
    std::optional<std::uint64_t> actualChecksum;
    /// Short English diagnostic; it does not contain input payloads
    /// or credentials.
    std::string reason;
};
struct ReplayResult
{
    /// State after the final validated replay record.
    Snapshot snapshot;
    /// One checksum for every replayed tick, including empty ticks.
    std::vector<std::uint64_t> checksums;
};

/** @brief Owns room simulation state and a deterministic integer
 * reference simulation. All state access and destruction require its
 * construction thread. The gateway mailbox supplies roster/input
 * values; no network or wall clock enters this core.
 */
class RoomActor final
{
public:
    static constexpr std::uint64_t checksumVersion = 1;
    /** @brief Creates an empty tick-zero room with explicit
     * incarnation identity. */
    RoomActor(RoomId, std::uint64_t incarnation, ActorConfig = {});
    ~RoomActor() noexcept;
    RoomActor(const RoomActor&) = delete;
    RoomActor& operator=(const RoomActor&) = delete;
    /** @brief Reconciles membership, applies canonical input and
     * commits one 50 ms step. Duplicate player/sequence controls that
     * conflict within this tick are all rejected. Identical
     * duplicates apply at most once. Sequence gaps can be retried.
     * Invalid/oversized rosters or batches throw before changing
     * state or journal.
     */
    TickResult step(std::vector<Member>, std::vector<Input>);
    /** @brief Copies the last committed tick; polling never advances
     * simulation. */
    Snapshot snapshot() const;
    /** @brief Copies a bounded replay log with its matching rolling
     * checkpoint. */
    ReplayLog replayLog() const;
    /** @brief Replays a self-contained log and checks every recorded
     * checksum. */
    static std::expected<ReplayResult, ReplayFailure>
    replay(const ReplayLog&);
    /** @brief Computes the documented canonical state hash without
     * raw-struct hashing. */
    static std::uint64_t
    checksum(const Snapshot&, const ActorConfig&);

private:
    struct State;
    /// Room identity, simulation and journal are accessed only on the
    /// fixed owner.
    std::unique_ptr<State> m_state;
};
} // namespace service::room
