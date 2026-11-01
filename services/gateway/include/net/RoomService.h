/** @file
 * @brief Routes bounded room mailboxes to a fixed management owner at
 * 20 Hz.
 */
#pragma once

#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "arena/domain/Battle.h"
#include "arena/domain/Room.h"
#include "room/ReplayRecorder.h"
#include "room/RoomActor.h"
#include "runtime/OwnerScheduler.h"

namespace service::gateway
{
struct RoomSettlement;
struct RoomServiceConfig
{
    /// Bounds active actors and retained finished managed results
    /// together.
    std::size_t maxRooms{128};
    /// Total commands retained between tick cutoffs.
    std::size_t maxPending{1024};
    /// Per-room mailbox bound, including snapshot requests.
    std::size_t maxPendingPerRoom{256};
    /// Maximum logical catch-up ticks before rebasing the next wall
    /// deadline.
    std::size_t maxCatchUp{4};
    /// Immutable simulation and journal limits shared by hosted
    /// actors.
    service::room::ActorConfig actor;
    /// Managed battles end after this many successful Playing ticks.
    std::uint64_t battleDurationTicks{100};
    /// Bounds managed rooms whose participants never finish readying.
    std::uint64_t readyTimeoutTicks{600};
    /// Finished results remain queryable by the same logical session
    /// for at most one hour.
    std::chrono::milliseconds resultRetention{60000};
    /// Explicit owner cutoffs replace automatic timer scheduling.
    bool externalTicks{false};
    /// An external durable owner acknowledges frozen settlement
    /// intents. Mutually exclusive with persistSettlement.
    bool externalSettlement{false};
    /// Optional nonblocking allocation of a strictly increasing actor
    /// ID. Restoring or cloning never invokes this binding.
    std::function<std::uint64_t()> incarnationSource;
    /// Optional full-session archive sink; disk I/O runs off the
    /// owner.
    std::shared_ptr<service::room::ReplayRecorder> replays;
    /// Nonblocking admission of one immutable terminal intent. A true
    /// return transfers retry/spool ownership; completion true means
    /// acknowledged PG commit, while completion false is terminal and
    /// must not reset its budget. False/throw means no ownership and
    /// permits bounded owner admission retry.
    std::function<bool(
        const ManagedRoom&,
        const RoomSettlement&,
        std::function<void(bool)>
    )>
        persistSettlement;
    /// Maximum attempts to hand a result to the durable adapter, not
    /// RPC retries.
    std::size_t persistenceAdmissionAttempts{8};
    /// Exponential admission backoff, independently bounded from
    /// simulation.
    std::chrono::milliseconds persistenceRetryBase{100},
        persistenceRetryMax{5000};
    /// Wall time is frozen once per lifecycle fact and never enters
    /// checksum.
    std::function<std::chrono::system_clock::time_point()> wallNow{
        std::chrono::system_clock::now
    };
    /// Injectable monotonic clock; only the management owner calls
    /// it.
    std::function<arena::runtime::OwnerClock::time_point()> now{
        arena::runtime::OwnerClock::now
    };
};
struct RoomMetrics
{
    /// Current actors; zero after retirement.
    std::size_t actors{0};
    /// Successful actor steps and typed input outcome counts.
    std::uint64_t ticks{0}, applied{0}, rejected{0}, duplicates{0};
    /// Overdue wall slots skipped after bounded catch-up; logical
    /// tick IDs never skip.
    std::uint64_t skippedWallSlots{0};
    /// Owner-hosted matches and finished results retained for
    /// recovery.
    std::size_t managedMatches{0}, retainedResults{0};
    /// Exactly-once lifecycle counters, including abort/timeout
    /// results.
    std::uint64_t battlesStarted{0}, battlesCompleted{0},
        battlesAborted{0};
    /// Durable admission and acknowledgement counters; memory mode
    /// leaves zero.
    std::uint64_t persistenceSubmitted{0}, persistenceRejected{0},
        persistenceFailed{0}, persistenceCommitted{0};
    /// Bounded owner-held terminal intents; blocked means operator
    /// action needed.
    std::size_t pendingSettlements{0}, blockedSettlements{0};
};
struct RoomInputReply
{
    /// Owning state after the input cutoff was committed.
    service::room::Snapshot snapshot;
    /// Per-attempt outcome, independent of successful RPC delivery.
    service::room::InputStatus status;
};

/** @brief Projects the managed match lifecycle without changing
 * replay state. */
enum class RoomPhase
{
    Forming,
    Ready,
    Playing,
    Settling,
    Finished
};
enum class SettlementReason
{
    Completed,
    MemberUnavailable,
    ReadyTimeout
};
struct RoomReadyMember
{
    /// Immutable allocation identity, including the membership
    /// generation.
    service::room::Member member;
    /// Owner-applied readiness retained across reconnects.
    bool ready{false};
    /// Current logical presence; Detached remains eligible during
    /// grace.
    Presence presence{Presence::Online};
};
/** @brief Immutable terminal intent; visible after durable
 * acknowledgement when configured. */
struct RoomSettlement
{
    /// One result per match; resultId equals matchId for this
    /// in-memory stub.
    std::uint64_t resultId, matchId;
    /// Server-allocated room whose final state is frozen.
    arena::domain::RoomId roomId;
    /// Absolute final kernel tick and its unchanged version-one hash.
    std::uint64_t finalTick, finalChecksum;
    /// Normal duration completion or a bounded lifecycle abort.
    SettlementReason reason;
    /// Zero denotes the deterministic draw stub; no rating/reward is
    /// applied.
    std::uint64_t winnerPlayerId{0};
    /// Final reference-state positions and sequence fences for both
    /// participants.
    std::vector<service::room::PlayerState> players;
    /// Frozen lifecycle timestamps; zero started time means no
    /// Playing phase.
    std::uint64_t createdUnixMs{0}, startedUnixMs{0},
        finishedUnixMs{0};
    /// Simulation lifecycle facts are excluded from its deterministic
    /// hash.
    std::uint64_t startedTick{0}, playedTicks{0};
};
/** @brief Owns recovery state, including sequence fences and any
 * final result. */
struct RoomStatus
{
    /// Latest kernel state, frozen after settlement begins.
    service::room::Snapshot snapshot;
    /// Server assignment identity, also used for idempotent result
    /// queries.
    std::uint64_t matchId;
    /// Management-owner lifecycle projection, excluded from replay
    /// hashes.
    RoomPhase phase;
    /// Fixed cohort with current readiness and presence values.
    std::vector<RoomReadyMember> members;
    /// First/last Playing state boundaries; playedTicks counts actual
    /// steps.
    std::uint64_t startedTick, finishedTick, playedTicks,
        durationTicks;
    /// Memory mode exposes the frozen intent from Settling; durable
    /// mode publishes it only after commit acknowledgement and
    /// bounded retention.
    std::optional<RoomSettlement> settlement;
};

/** @brief Copies all immutable value policies independently of
 * runtime bindings. */
struct RoomPolicy
{
    /// Complete mailbox, actor and catch-up admission limits.
    std::size_t maxRooms, maxPending, maxPendingPerRoom, maxCatchUp;
    service::room::ActorConfig actor;
    /// Lifecycle and retained-result bounds.
    std::uint64_t battleDurationTicks, readyTimeoutTicks;
    std::chrono::milliseconds resultRetention;
    /// Settlement admission and retry bounds remain unchanged on
    /// recovery.
    std::size_t persistenceAdmissionAttempts;
    std::chrono::milliseconds persistenceRetryBase,
        persistenceRetryMax;
    /// Binding kinds and driving modes are part of the policy
    /// contract.
    bool externalTicks, externalSettlement, callbackSettlement,
        archives;
    bool operator==(const RoomPolicy&) const noexcept;
};

/** @brief Stores one complete managed lifecycle without callback
 * ownership. */
struct ManagedMatchCheckpoint
{
    /// Historical allocation remains valid even after members expire.
    ManagedRoom allocation;
    /// Domain states and exact transition revisions.
    service::state::Snapshot<arena::domain::Room::State> room;
    service::state::Snapshot<arena::domain::Battle::State> battle;
    /// Owner projections sampled at the same cutoff as the actor.
    RoomPhase phase;
    std::array<bool, 2> ready;
    std::array<Presence, 2> presence;
    /// Frozen wall facts and logical lifecycle counters.
    std::uint64_t createdUnixMs, startedUnixMs;
    std::uint64_t waitedTicks, startedTick, finishedTick, playedTicks;
    /// Immutable intent and monotonic retention/retry deadlines.
    std::optional<RoomSettlement> result;
    std::optional<arena::runtime::OwnerClock::time_point> expires,
        retryAt;
    /// Exhausted or transferred admission budgets cannot reopen on
    /// recovery.
    std::size_t admissionAttempts;
    bool persistenceAccepted, persistenceBlocked;
    /// Zero is pending, one committed, two terminal failure. A
    /// pending callback-owned attempt is not checkpointable.
    int persistenceOutcome;
};

/** @brief Owns a quiescent service cut without schedulers or archive
 * handles. Actor replay journals restart at these snapshots; an upper
 * journal owns any full-session ordered log across recovery.
 */
struct RoomCheckpoint
{
    RoomPolicy policy;
    /// Canonically room-sorted committed actor and lifecycle values.
    std::vector<service::room::Snapshot> actors;
    std::vector<ManagedMatchCheckpoint> matches;
    /// Cumulative counters and derived gauges from the same cutoff.
    RoomMetrics metrics;
    /// Original next logical deadline, mapped by the durable owner if
    /// needed.
    arena::runtime::OwnerClock::time_point deadline;
    /// Lifecycle/admission state, without timer or executor
    /// ownership.
    bool running, started, accepting;
    /// Includes retired incarnations, preventing later allocation
    /// collisions.
    std::uint64_t incarnationHighWater;
};

enum class RoomRecoveryError
{
    Busy,
    Invalid
};

/** @brief Applies room commands only on a fixed owner with a 50 ms
 * tick. Construct/start/stop/destroy on the management owner.
 * Input/snapshot admission is thread-safe and bounded. An input
 * acknowledgement is sent after its tick.
 */
class RoomService final
{
public:
    using SnapshotResult = std::
        expected<service::room::Snapshot, ErrorCode>;
    using InputResult = std::expected<RoomInputReply, ErrorCode>;
    using StatusResult = std::expected<RoomStatus, ErrorCode>;
    using SettlementResult = std::expected<RoomSettlement, ErrorCode>;
    using StatusCompletion = std::function<void(StatusResult)>;
    using SettlementCompletion = std::function<
        void(SettlementResult)>;
    using SnapshotCompletion = std::function<void(SnapshotResult)>;
    using InputCompletion = std::function<void(InputResult)>;
    static constexpr std::chrono::milliseconds tickInterval{50};
    /** @brief Installs the synchronized roster authority and resource
     * limits. */
    explicit RoomService(
        std::shared_ptr<OnlineRegistry>, RoomServiceConfig = {}
    );
    ~RoomService() noexcept;
    RoomService(const RoomService&) = delete;
    RoomService& operator=(const RoomService&) = delete;
    /** @brief Starts the absolute-deadline 20 Hz owner timer. */
    void start(arena::runtime::OwnerScheduler);
    /** @brief Copies a quiescent owner cut without clocks, pruning or
     * I/O. Pending commands, active archives and unfinished callback
     * loans return Busy.
     */
    std::expected<RoomCheckpoint, RoomRecoveryError>
    checkpoint() const;
    /** @brief Validates and builds an isolated service on its
     * construction owner. Runtime bindings must reproduce the
     * complete policy. No IDs or clocks are consumed. External-tick
     * clones can advance immediately; timer-mode clones call start
     * once to attach a scheduler to the preserved deadline.
     */
    static std::
        expected<std::unique_ptr<RoomService>, RoomRecoveryError>
        restore(
            std::shared_ptr<OnlineRegistry>,
            const RoomCheckpoint&,
            RoomServiceConfig
        );
    /** @brief Applies exactly one explicit logical cutoff; false
     * requires the caller to discard the candidate and any captured
     * replies/effects.
     */
    bool advance();
    /** @brief Completes one externally owned durable intent. False
     * acknowledgement blocks the existing budget permanently.
     * Duplicate facts are idempotent.
     */
    bool acknowledgeSettlement(std::uint64_t matchId, bool committed);
    /** @brief Closes mailboxes, cancels ticking, clears actors and
     * settles pending once. */
    void stop() noexcept;
    /** @brief Atomically opens a server-managed match on the
     * management owner. False leaves no local actor or lifecycle
     * entry; the caller rolls back the registry allocation. Managed
     * rooms progress without client polling.
     */
    bool openMatch(const ManagedRoom&);
    /** @brief Sets readiness at a cutoff, fenced against old
     * actor/membership state. */
    bool ready(
        SessionBinding,
        arena::domain::SessionId,
        arena::domain::RoomId,
        std::uint64_t incarnation,
        std::uint64_t membershipEpoch,
        bool ready,
        StatusCompletion
    );
    /** @brief Recovers the current managed match or the latest
     * retained result. The exact current transport must authenticate
     * the same logical session.
     */
    bool status(
        SessionBinding, arena::domain::SessionId, StatusCompletion
    );
    /** @brief Queries one immutable result after registry membership
     * is released. */
    bool settlement(
        SessionBinding,
        arena::domain::SessionId,
        std::uint64_t matchId,
        SettlementCompletion
    );
    /** @brief Queries or lazily initializes the caller's existing
     * roster room. False means the mailbox rejected admission and
     * completion will not be called.
     */
    bool snapshot(
        SessionBinding,
        arena::domain::SessionId,
        arena::domain::RoomId,
        SnapshotCompletion
    );
    /** @brief Submits a control fenced by transport, actor
     * incarnation and membership epoch. */
    bool input(
        SessionBinding,
        arena::domain::SessionId,
        arena::domain::RoomId,
        std::uint64_t incarnation,
        std::uint64_t membershipEpoch,
        std::uint64_t sequence,
        std::int32_t dx,
        std::int32_t dy,
        InputCompletion
    );
    /** @brief Returns owner-consistent cumulative metrics and active
     * actor count. */
    RoomMetrics metrics() const;
    /** @brief Returns the immutable coordinate bound used by checksum
     * version one. */
    std::int32_t positionLimit() const noexcept;
    /** @brief Exports the owner's bounded journal for diagnosis or
     * deterministic replay. */
    std::optional<service::room::ReplayLog>
        replayLog(arena::domain::RoomId) const;

private:
    struct State;
    /// Shared mailbox state; only the management owner acquires
    /// mutable actors.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
