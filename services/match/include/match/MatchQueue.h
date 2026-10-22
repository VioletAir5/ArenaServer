/** @file
 * @brief Owns bounded, deterministic 1v1 matchmaking on one executor.
 */
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>

#include "arena/domain/Ticket.h"

namespace service::match
{
using Clock = std::chrono::steady_clock;
using PlayerId = arena::domain::PlayerId;
using SessionId = arena::domain::SessionId;
using TicketId = arena::domain::TicketId;
using TicketState = arena::domain::Ticket::State;

enum class Availability
{
    Ready,
    Paused,
    Gone
};
enum class QueueError
{
    InvalidRequest,
    Conflict,
    Capacity,
    NotFound,
    AlreadyAssigned
};

/** @brief Configures one process-local queue; durations use monotonic
 * time. */
struct QueueConfig
{
    /// Initial inclusive MMR difference accepted by each participant.
    std::uint32_t initialWindow{50};
    /// MMR points added after each complete expansion interval.
    std::uint32_t expansionStep{25};
    /// Positive interval between window expansions.
    std::chrono::milliseconds expansionInterval{1000};
    /// Inclusive ceiling for each participant's window.
    std::uint32_t maximumWindow{500};
    /// Exclusive queue deadline, never extended by retries or
    /// recovery.
    std::chrono::milliseconds timeout{30000};
    /// Minimum retry history after cancellation, timeout or
    /// assignment release.
    std::chrono::milliseconds retention{60000};
    /// Total retained records, including pinned assignments and retry
    /// history.
    std::size_t maxTickets{4096};
    /// Maximum waiting players; history consumes a separate total
    /// limit.
    std::size_t maxQueued{1024};
    /// Candidate comparisons allowed per tick; a rotating cursor
    /// prevents starvation.
    std::size_t comparisonsPerTick{8192};
};

/** @brief Copies ticket state without exposing mutable domain
 * entities. */
struct TicketSnapshot
{
    /// Authenticated participant owning this admission.
    PlayerId playerId;
    /// Logical session authorized to query or cancel the ticket.
    SessionId sessionId;
    /// Process-unique identity, independent of the client join key.
    TicketId ticketId;
    /// Client-generated nonzero operation key, scoped to the
    /// authenticated player.
    std::uint64_t joinKey;
    /// Copied domain lifecycle state; Reserved is never exposed
    /// mid-pair.
    TicketState state;
    /// Server rating captured once at initial admission.
    std::uint32_t mmr;
    /// Current inclusive difference, frozen together with terminal
    /// wait.
    std::uint32_t window;
    /// Elapsed wait from admission to observation or terminal
    /// transition.
    std::uint64_t waitedMs;
    /// Remaining queue lifetime, zero outside Queued.
    std::uint64_t remainingMs;
    /// Shared nonzero pair identity after assignment; zero before
    /// assignment.
    std::uint64_t matchId{0};
    /// Assigned peer identity, absent before pairing.
    std::optional<PlayerId> opponent;
    /// Reciprocal peer ticket, retained with assignment history.
    std::optional<TicketId> opponentTicket;
    /// Queued or assigned and not explicitly released by the
    /// allocation owner.
    bool active{true};
};
using QueueResult = std::expected<TicketSnapshot, QueueError>;

/** @brief Stores cumulative counts and fixed histograms without
 * sample growth. */
struct QueueMetrics
{
    /// Histogram upper bounds; the final bin also contains overflow.
    static constexpr std::array<std::uint64_t, 8> waitBoundsMs{
        100, 500, 1000, 5000, 10000, 30000, 60000, UINT64_MAX
    };
    static constexpr std::array<std::uint64_t, 8> gapBounds{
        0, 25, 50, 100, 200, 300, 500, UINT64_MAX
    };
    /// Saturating cumulative ticket lifecycle event counts.
    std::uint64_t joined{0}, replayed{0}, cancelled{0}, expired{0};
    /// Saturating cumulative assignment, failure and release counts.
    std::uint64_t matchedPairs{0}, rejected{0}, releasedPairs{0};
    /// Physical current gauges, reset when the owning service stops.
    std::size_t queued{0}, assignedPlayers{0}, retained{0};
    /// One observation per assigned player, measured from initial
    /// admission.
    std::array<std::uint64_t, 8> matchedWait{};
    /// One observation per pair; absolute difference of server-owned
    /// MMRs.
    std::array<std::uint64_t, 8> mmrGap{};
    /// Cancellation and expiry wait observations, separate from
    /// successful matches.
    std::array<std::uint64_t, 8> abandonedWait{};
    /// Cumulative sums for mean successful player wait and mean pair
    /// rating gap.
    std::uint64_t totalMatchedWaitMs{0}, totalMmrGap{0};
};

/** @brief Serializes ticket identity, lifecycle, pairing and
 * accounting.
 *
 * Construct, call and destroy on one owner thread. Availability is
 * sampled on that owner and must not reenter this queue. No room
 * allocation is performed.
 */
class MatchQueue final
{
public:
    using Eligibility = std::function<
        Availability(PlayerId, SessionId)>;
    /** @brief Validates limits and captures the current owner thread.
     * @param[in] eligibility Reports current logical session
     * eligibility.
     */
    explicit MatchQueue(
        QueueConfig = {}, Eligibility eligibility = {}
    );
    ~MatchQueue();
    MatchQueue(const MatchQueue&) = delete;
    MatchQueue& operator=(const MatchQueue&) = delete;
    /** @brief Replays a key or returns the player's single active
     * ticket.
     * @param[in] mmr Trusted server rating in [0, 10000].
     * @param[in] now Nondecreasing monotonic time used by every queue
     * operation.
     */
    QueueResult join(
        PlayerId,
        SessionId,
        std::uint64_t joinKey,
        std::uint32_t mmr,
        Clock::time_point now
    );
    /** @brief Recovers an existing operation or active ticket before
     * rating lookup. Returns empty only when a new admission is
     * needed. This owner-only lookup lets gateway retries preserve
     * the original server rating during outages.
     */
    std::optional<TicketSnapshot> replay(
        PlayerId,
        SessionId,
        std::uint64_t joinKey,
        Clock::time_point now
    );
    /** @brief Cancels only the caller's waiting ticket; terminal
     * retries are safe. */
    QueueResult
    cancel(PlayerId, SessionId, TicketId, Clock::time_point now);
    /** @brief Looks up only a ticket belonging to the caller's
     * logical session. */
    QueueResult
    status(PlayerId, SessionId, TicketId, Clock::time_point now);
    /** @brief Expires/reclaims records and pairs eligible players
     * within a bounded budget. */
    void advance(Clock::time_point now);
    /** @brief Releases both assignment fences after the allocation
     * owner finishes. Retry is harmless; assigned history remains for
     * the configured retention.
     */
    bool release(std::uint64_t matchId, Clock::time_point now);
    /** @brief Returns owner-consistent cumulative metrics and
     * physical gauges. */
    QueueMetrics metrics() const;

private:
    struct State;
    /// Owner-affine implementation containing domain Tickets and all
    /// indexes.
    std::unique_ptr<State> m_state;
};
} // namespace service::match
