/** @file
 * @brief Defines the value boundary executed by a single room worker
 * owner.
 */
#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <vector>

#include "arena/domain/Battle.h"
#include "arena/domain/Room.h"
#include "room/RoomActor.h"

namespace service::room
{
/** @brief Freezes actor bounds and managed-match timing for one room
 * lifetime. */
struct RuntimePolicy
{
    /// Actor membership, input, history and position limits.
    ActorConfig actor;
    /// Maximum ordered commands evaluated in one cutoff.
    std::size_t maxCommands{256};
    /// Number of Playing ticks required for normal completion.
    std::uint64_t battleDurationTicks{100};
    /// Maximum cutoffs spent waiting for the fixed cohort to become
    /// ready.
    std::uint64_t readyTimeoutTicks{600};
    /// Monotonic retention duration after a successful settlement
    /// acknowledgment.
    std::int64_t resultRetentionNanos{60'000'000'000};
};

/** @brief Supplies an exact current authorization without a registry
 * loan. */
struct RuntimeAuthorization
{
    /// Player/session/membership generation, distinct from the
    /// transport generation.
    Member member;
    /// Current authenticated session generation issued by Lobby.
    std::uint64_t sessionEpoch{0};
    /// Current durable transport handle; this is not an OS
    /// descriptor.
    std::uint64_t connectionHandle{0};
    /// Current online presence; detached members remain in the cutoff
    /// roster.
    bool online{true};
    bool operator==(const RuntimeAuthorization&) const = default;
};
/** @brief Retains the immutable two-player cohort and its canonical
 * initial rating. */
struct RuntimeAllocation
{
    /// Durable match identity shared by result and settlement
    /// records.
    std::uint64_t matchId{0};
    /// Fixed cohort, ordered identically to the rating and presence
    /// arrays.
    std::array<Member, 2> members;
    /// Storage-authoritative ratings captured when the match was
    /// allocated.
    std::array<std::uint32_t, 2> initialMmr{1500, 1500};
    bool operator==(const RuntimeAllocation&) const = default;
};
/** @brief Separates simulation completion from externally
 * acknowledged settlement. */
enum class RuntimePhase : std::uint8_t
{
    Forming,
    Ready,
    Playing,
    Settling,
    Finished
};
/** @brief Records the immutable reason that froze a managed match. */
enum class RuntimeReason : std::uint8_t
{
    Completed,
    MemberUnavailable,
    ReadyTimeout
};
/** @brief Freezes the outcome before persistence; public release
 * still requires ACK. */
struct RuntimeSettlement
{
    /// Stable result identity used for idempotent settlement
    /// delivery.
    std::uint64_t resultId;
    /// Match owning this outcome.
    std::uint64_t matchId;
    /// Public room identity; the enclosing image retains its
    /// incarnation.
    RoomId roomId;
    /// Physical actor tick at which simulation became immutable.
    std::uint64_t finalTick;
    /// Actor checksum at finalTick, unchanged by persistence retries.
    std::uint64_t finalChecksum;
    /// Natural completion or the exact fixed-cohort abort reason.
    RuntimeReason reason;
    /// Winning player identity, or zero for a draw.
    std::uint64_t winnerPlayerId{0};
    /// Frozen participant states, including acknowledged input
    /// sequence fences.
    std::vector<PlayerState> players;
    /// Captured creation time in Unix milliseconds.
    std::uint64_t createdUnixMs{0};
    /// Captured Playing start time in Unix milliseconds.
    std::uint64_t startedUnixMs{0};
    /// Frozen simulation end time, independent of the later database
    /// ACK.
    std::uint64_t finishedUnixMs{0};
    /// First physical actor tick in Playing.
    std::uint64_t startedTick{0};
    /// Number of Playing ticks, excluding readiness and retention
    /// cutoffs.
    std::uint64_t playedTicks{0};
    bool operator==(const RuntimeSettlement&) const = default;
};
/** @brief Owns the complete managed-match state carried across worker
 * handoffs. */
struct RuntimeManaged
{
    /// Immutable match identity, participants and initial ratings.
    RuntimeAllocation allocation;
    /// Value checkpoint of the room lifecycle state machine.
    service::state::Snapshot<arena::domain::Room::State> room;
    /// Value checkpoint of the battle state machine.
    service::state::Snapshot<arena::domain::Battle::State> battle;
    /// Managed lifecycle; Finished is visible only after trusted
    /// persistence ACK.
    RuntimePhase phase{RuntimePhase::Forming};
    /// Readiness flags aligned with the fixed allocation.
    std::array<bool, 2> ready{false, false};
    /// Presence at the last cutoff; grace-retained members may be
    /// offline.
    std::array<bool, 2> online{true, true};
    /// Room creation time in captured Unix milliseconds.
    std::uint64_t createdUnixMs{0};
    /// Playing start time in captured Unix milliseconds.
    std::uint64_t startedUnixMs{0};
    /// Cutoffs consumed while waiting for readiness.
    std::uint64_t waitedTicks{0};
    /// Physical actor tick at the start of Playing.
    std::uint64_t startedTick{0};
    /// Frozen final physical actor tick, or zero before completion.
    std::uint64_t finishedTick{0};
    /// Playing-only tick count used by the duration contract.
    std::uint64_t playedTicks{0};
    /// Internally frozen at Settling; publicly exposed only at
    /// Finished.
    std::optional<RuntimeSettlement> result{};
    /// Same-boot monotonic result-retention deadline after successful
    /// ACK.
    std::optional<std::int64_t> expiresAtNanos{};
    /// Permanent or exhausted persistence failure; retries cannot
    /// invent success.
    bool persistenceBlocked{false};
    bool operator==(const RuntimeManaged&) const;
};
/** @brief Retains room-local cumulative counters without owning a
 * metrics backend. */
struct RuntimeCounters
{
    /// Physical actor steps accounted for by this image.
    std::uint64_t ticks{0};
    /// Successfully applied player inputs.
    std::uint64_t applied{0};
    /// Rejected player inputs.
    std::uint64_t rejected{0};
    /// Recognized duplicate input attempts.
    std::uint64_t duplicates{0};
    /// Managed matches that entered Playing.
    std::uint64_t battlesStarted{0};
    /// Matches that reached their configured tick budget.
    std::uint64_t battlesCompleted{0};
    /// Matches frozen by a cohort or readiness failure.
    std::uint64_t battlesAborted{0};
    /// Frozen outcomes requiring external persistence.
    std::uint64_t settlementIntents{0};
    /// Positively acknowledged external settlements.
    std::uint64_t settlementsCommitted{0};
    /// Failed settlement acknowledgments recorded by the kernel.
    std::uint64_t settlementsFailed{0};
    bool operator==(const RuntimeCounters&) const = default;
};
/** @brief Contains every committed room value; it owns no runtime
 * binding. */
struct RuntimeImage
{
    /// Immutable policy required to restore this exact room lifetime.
    RuntimePolicy policy;
    /// Physical simulation state; its tick may stop while logical
    /// cutoffs continue.
    Snapshot actor;
    /// Fixed-cohort lifecycle, absent for a manual room.
    std::optional<RuntimeManaged> managed;
    /// Room-local counters with explicit legacy-history coverage
    /// below.
    RuntimeCounters counters;
    /// Logical cutoff high-water, including frozen and
    /// retained-result cutoffs.
    std::uint64_t logicalCutoff{0};
    /// First physical actor tick covered by the room-local counters.
    std::uint64_t knownSinceTick{0};
    /// False means earlier counters remain in an external legacy
    /// ledger.
    bool completeCounterHistory{true};
};
/** @brief Classifies validation, authorization and admission failures
 * at this value boundary. */
enum class RuntimeError : std::uint8_t
{
    Invalid,
    Capacity,
    Unauthenticated,
    BusinessFailure,
    Unavailable,
    InvalidImage
};
/** @brief Identifies player commands and the trusted persistence
 * acknowledgment. */
enum class RuntimeCommandKind : std::uint8_t
{
    Snapshot,
    Input,
    Ready,
    Status,
    Settlement,
    SettlementAck
};
/** @brief Owns one ordered command without retaining a socket,
 * callback or registry loan. */
struct RuntimeCommand
{
    /// Operation interpreted against the supplied cutoff and
    /// immutable base.
    RuntimeCommandKind kind{RuntimeCommandKind::Snapshot};
    /// Trusted current caller tuple; internal SettlementAck has no
    /// player caller.
    std::optional<RuntimeAuthorization> caller;
    /// Expected room lifetime for mutating player commands.
    std::uint64_t incarnation{0};
    /// Per-member input sequence, independent of owner and logical
    /// tick.
    std::uint64_t sequence{0};
    /// Exact managed match for settlement queries or acknowledgments.
    std::uint64_t matchId{0};
    /// Horizontal input component.
    std::int32_t dx{0};
    /// Vertical input component.
    std::int32_t dy{0};
    /// Desired player readiness for a Ready command.
    bool ready{false};
    /// Trusted external persistence outcome for SettlementAck.
    bool committed{false};
};
/** @brief Correlates a command outcome by its position in the ordered
 * cutoff. */
struct RuntimeCommandResult
{
    /// Classified command rejection, absent on success.
    std::optional<RuntimeError> error;
    /// Input application or duplicate result for an Input command.
    std::optional<InputStatus> input;
};
/** @brief Freezes ordered commands, membership and both clocks once
 * at the owner boundary. */
struct RuntimeCutoff
{
    /// Exactly one room's current members, including grace-retained
    /// Detached.
    std::vector<RuntimeAuthorization> roster;
    /// Current tuples only for relevant callers, including callers
    /// outside room.
    std::vector<RuntimeAuthorization> currentAuthorizations;
    /// Ordered commands ending at this cutoff.
    std::vector<RuntimeCommand> commands;
    /// Captured same-boot monotonic time in nanoseconds.
    std::int64_t nowNanos{0};
    /// Captured Unix milliseconds for immutable lifecycle timestamps.
    std::uint64_t wallMillis{0};
};
/** @brief Returns an isolated image and effects that remain
 * unpublished until WAL ACK. */
struct RuntimeProposal
{
    /// Complete proposed successor image, without mutable owner
    /// bindings.
    RuntimeImage image;
    /// One outcome per ordered command, preserving positional
    /// correlation.
    std::vector<RuntimeCommandResult> results;
    /// Actual physical tick delta, absent when simulation remains
    /// frozen.
    std::optional<TickRecord> replay;
    /// Releases the fixed allocation only in the canonical
    /// publication CAS.
    bool releaseMembership{false};
    /// Retires an empty manual room or an expired retained managed
    /// result.
    bool retire{false};
};
/** @brief Computes an isolated one-cutoff candidate on its caller's
 * owner. No registry, callback, clock read, RPC or file I/O enters
 * this boundary. The caller publishes the returned
 * image/replies/effects only after WAL ACK.
 */
class RoomRuntime final
{
public:
    /** @brief Creates the deterministic initial image without
     * installing an owner.
     * @param[in] room Public room identity.
     * @param[in] incarnation Unique lifetime for this room identity.
     * @param[in] policy Frozen simulation bounds and timing policy.
     * @param[in] allocation Optional immutable managed-match cohort.
     * @param[in] createdWallMillis Captured creation time in Unix
     * milliseconds.
     */
    static std::expected<RuntimeImage, RuntimeError> create(
        RoomId room,
        std::uint64_t incarnation,
        RuntimePolicy policy,
        std::optional<RuntimeAllocation> allocation = {},
        std::uint64_t createdWallMillis = 0
    );
    /** @brief Checks the complete image and lifecycle invariants
     * without advancing it.
     * @param[in] image Detached image to validate before installation
     * or computation.
     */
    static std::expected<void, RuntimeError>
    validate(const RuntimeImage& image);
    /** @brief Evaluates one ordered cutoff against an unchanged base
     * image.
     * @param[in] image Last authoritative image; never mutated by
     * this call.
     * @param[in] cutoff Trusted membership, commands and captured
     * clocks.
     * @return Detached candidate and effects that the caller
     * publishes only after WAL ACK.
     */
    static std::expected<RuntimeProposal, RuntimeError>
    step(const RuntimeImage& image, const RuntimeCutoff& cutoff);
};
} // namespace service::room