/** @file
 * @brief Routes bounded room mailboxes to a fixed management owner at
 * 20 Hz.
 */
#pragma once

#include "net/ConnectionFeature.h"
#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "room/ReplayRecorder.h"
#include "room/RoomActor.h"

namespace service::gateway
{
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
    /// Optional full-session archive sink; disk I/O runs off the
    /// owner.
    std::shared_ptr<service::room::ReplayRecorder> replays;
    /// Injectable monotonic clock; only the management owner calls
    /// it.
    std::function<ConnectionClock::time_point()> now{
        ConnectionClock::now
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
/** @brief Immutable in-memory result; no economy or durable storage
 * side effects. */
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
    /// Immutable result, present from Settling until bounded
    /// retention expires.
    std::optional<RoomSettlement> settlement;
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
    void start(ConnectionScheduler);
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
