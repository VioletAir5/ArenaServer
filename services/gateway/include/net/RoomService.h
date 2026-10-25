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
    /// Bounds lazily initialized active room actors.
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
};
struct RoomInputReply
{
    /// Owning state after the input cutoff was committed.
    service::room::Snapshot snapshot;
    /// Per-attempt outcome, independent of successful RPC delivery.
    service::room::InputStatus status;
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
