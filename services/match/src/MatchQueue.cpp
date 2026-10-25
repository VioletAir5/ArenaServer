/** @file
 * @brief Implements owner-affine ticket admission, retry history and
 * bounded pairing.
 */
#include "match/MatchQueue.h"

#include <algorithm>
#include <atomic>
#include <exception>
#include <limits>
#include <map>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace service::match
{
namespace
{
using DomainTicket = arena::domain::Ticket;

// Queue instances in this process must not reuse ticket or pair
// identities.
std::atomic<std::uint64_t> nextTicketId{1};

/** @brief Allocates a process-wide identity without wrapping into
 * reused IDs. */
std::optional<std::uint64_t> allocateTicketId() noexcept
{
    auto current = nextTicketId.load(std::memory_order_relaxed);
    while (current)
    {
        const auto next = current == UINT64_MAX ? 0 : current + 1;
        if (nextTicketId.compare_exchange_weak(
                current,
                next,
                std::memory_order_relaxed
            ))
            return current;
    }
    return std::nullopt;
}

/** @brief Keeps cumulative metrics monotonic even after counter
 * saturation. */
void increment(
    std::uint64_t& value, std::uint64_t amount = 1
) noexcept
{
    value += std::min(amount, UINT64_MAX - value);
}

/** @brief Records one observation in inclusive, fixed-size histogram
 * bins. */
template <std::size_t Size>
void observe(
    std::array<std::uint64_t, Size>& bins,
    const std::array<std::uint64_t, Size>& bounds,
    std::uint64_t value
) noexcept
{
    const auto bin = std::lower_bound(
        bounds.begin(),
        bounds.end(),
        value
    );
    increment(bins[static_cast<std::size_t>(bin - bounds.begin())]);
}

/** @brief Measures complete monotonic milliseconds without signed
 * overflow. */
std::uint64_t
elapsedMs(Clock::time_point later, Clock::time_point earlier) noexcept
{
    if (later <= earlier)
        return 0;
    // Unsigned subtraction also handles a clock epoch crossing zero
    // without overflowing a signed duration before its conversion to
    // milliseconds.
    using Unsigned = std::make_unsigned_t<Clock::duration::rep>;
    using Duration = std::chrono::duration<Unsigned, Clock::period>;
    const auto ticks = static_cast<Unsigned>(
                           later.time_since_epoch().count()
                       ) -
                       static_cast<Unsigned>(
                           earlier.time_since_epoch().count()
                       );
    return std::chrono::duration_cast<
               std::chrono::duration<std::uint64_t, std::milli>>(
               Duration{ticks}
    )
        .count();
}

/** @brief Computes an unsigned rating difference without underflow.
 */
std::uint32_t gap(std::uint32_t left, std::uint32_t right) noexcept
{
    return left >= right ? left - right : right - left;
}
} // namespace

struct MatchQueue::State
{
    struct Key
    {
        /// Stable player identity, independent of a transport
        /// connection.
        std::uint64_t player;
        /// Logical session prevents password replacement from
        /// replaying an old command.
        std::uint64_t session;
        /// Original join key; fresh keys never alias this retry
        /// history.
        std::uint64_t operation;
        bool operator==(const Key&) const = default;
    };

    struct KeyHash
    {
        /** @brief Hashes the entire logical-session idempotency
         * scope. */
        std::size_t operator()(const Key& key) const noexcept
        {
            auto hash = std::hash<std::uint64_t>{}(key.player);
            const auto mix = [&hash](std::uint64_t value)
            {
                hash ^= std::hash<std::uint64_t>{}(value) +
                        static_cast<std::size_t>(
                            0x9e3779b97f4a7c15ULL
                        ) +
                        (hash << 6) + (hash >> 2);
            };
            mix(key.session);
            mix(key.operation);
            return hash;
        }
    };

    struct Entry
    {
        /** @brief Creates the domain ticket on its mutation owner. */
        Entry(
            TicketId id,
            PlayerId player,
            SessionId session,
            std::uint64_t operation,
            std::uint32_t rating,
            Clock::time_point now
        )
            : ticket(id, player)
            , sessionId(session)
            , joinKey(operation)
            , mmr(rating)
            , admitted(now)
        {
        }

        /** @brief Reconstructs the immutable retry index key. */
        Key key() const noexcept
        {
            return {
                ticket.playerId().value(),
                sessionId.value(),
                joinKey
            };
        }

        /** @brief Reads a lifecycle state only after the queue owner
         * check. */
        TicketState state() const noexcept
        {
            return ticket.snapshot()->state;
        }

        /** @brief Applies an internally validated lifecycle event
         * exactly once. */
        void apply(DomainTicket::Event event) noexcept
        {
            const auto before = ticket.snapshot();
            if (!before || !ticket.apply(event, before->revision))
                std::terminate();
        }

        /// The actual domain state machine is never shared outside
        /// this owner.
        DomainTicket ticket;
        /// Logical session admitted by the authenticated gateway
        /// adapter.
        SessionId sessionId;
        /// Original client join operation, retained with terminal
        /// results.
        std::uint64_t joinKey;
        /// Trusted rating sampled once at admission.
        std::uint32_t mmr;
        /// Initial queue entry time; retries never reset it.
        Clock::time_point admitted;
        /// Frozen at assignment, cancellation or expiry; release does
        /// not change it.
        std::optional<Clock::time_point> finished;
        /// Assignment history starts ageing only after its explicit
        /// release.
        std::optional<Clock::time_point> reclaimFrom;
        /// Minimum ticket ID of the provisional pair, or zero before
        /// assignment.
        std::uint64_t matchId{0};
        /// Counterpart values remain available in terminal retry
        /// responses.
        std::optional<PlayerId> opponent;
        std::optional<TicketId> opponentTicket;
        /// Waiting or assigned occupancy fence for this player.
        bool active{true};
        /// Trusted downstream allocation, retained with released
        /// history.
        std::optional<arena::domain::RoomId> roomId;
    };

    struct Candidate
    {
        /// Non-owning pointer valid for one advance, which never
        /// erases candidates.
        Entry* entry;
        /// A failed final eligibility check excludes the entry for
        /// this tick.
        bool enabled{true};
    };

    /** @brief Captures immutable policy and the current mutation
     * owner. */
    explicit State(QueueConfig configured, Eligibility availability)
        : config(configured)
        , eligibility(std::move(availability))
    {
    }

    /** @brief Rejects foreign access before touching mutable queue
     * state. */
    void checkOwner() const
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "match queue accessed outside its owner"
            );
        if (inEligibility)
            throw std::logic_error(
                "match eligibility must not reenter its queue"
            );
    }

    /** @brief Enforces owner affinity and monotonic command ordering.
     */
    void enter(Clock::time_point now)
    {
        checkOwner();
        if (lastNow && now < *lastNow)
            throw std::invalid_argument(
                "match queue time must not go backwards"
            );
        lastNow = now;
    }

    /** @brief Widens a ticket's inclusive window in complete bounded
     * steps. */
    std::uint32_t
    window(const Entry& entry, Clock::time_point now) const noexcept
    {
        const auto steps = elapsedMs(now, entry.admitted) /
                           static_cast<std::uint64_t>(
                               config.expansionInterval.count()
                           );
        const auto headroom = config.maximumWindow -
                              config.initialWindow;
        if (!config.expansionStep || !headroom)
            return config.initialWindow;
        // Clamp before multiplying so an arbitrarily old ticket
        // cannot wrap.
        const auto needed = (static_cast<std::uint64_t>(headroom) +
                             config.expansionStep - 1) /
                            config.expansionStep;
        if (steps >= needed)
            return config.maximumWindow;
        return config.initialWindow + static_cast<std::uint32_t>(
                                          steps * config.expansionStep
                                      );
    }

    /** @brief Copies live timing or the frozen timing of a terminal
     * result. */
    TicketSnapshot
    snapshot(const Entry& entry, Clock::time_point now) const noexcept
    {
        const auto observed = entry.finished.value_or(now);
        const auto waited = elapsedMs(observed, entry.admitted);
        const auto timeout = static_cast<std::uint64_t>(
            config.timeout.count()
        );
        return {
            entry.ticket.playerId(),
            entry.sessionId,
            entry.ticket.id(),
            entry.joinKey,
            entry.state(),
            entry.mmr,
            window(entry, observed),
            waited,
            entry.finished || waited >= timeout ? 0
                                                : timeout - waited,
            entry.matchId,
            entry.opponent,
            entry.opponentTicket,
            entry.active,
            entry.roomId
        };
    }

    /** @brief Accounts for a rejected public command without mutating
     * tickets. */
    QueueResult reject(QueueError error) noexcept
    {
        increment(counts.rejected);
        return std::unexpected(error);
    }

    /** @brief Removes only the occupancy fence belonging to this
     * exact ticket. */
    void removeActive(const Entry& entry) noexcept
    {
        const auto found = active.find(
            entry.ticket.playerId().value()
        );
        if (found != active.end() &&
            found->second == entry.ticket.id().value())
            active.erase(found);
    }

    /** @brief Cancels or expires one waiting ticket and records its
     * actual wait. */
    void finish(
        Entry& entry, DomainTicket::Event event, Clock::time_point now
    ) noexcept
    {
        entry.apply(event);
        entry.finished = now;
        entry.reclaimFrom = now;
        entry.active = false;
        removeActive(entry);
        --counts.queued;
        if (event == DomainTicket::Event::DeadlineExpired)
            increment(counts.expired);
        else
            increment(counts.cancelled);
        observe(
            counts.abandonedWait,
            QueueMetrics::waitBoundsMs,
            elapsedMs(now, entry.admitted)
        );
    }

    /** @brief Expires a waiting ticket and reports whether history
     * can be erased. */
    bool maintain(Entry& entry, Clock::time_point now) noexcept
    {
        if (entry.state() == TicketState::Queued &&
            elapsedMs(now, entry.admitted) >=
                static_cast<std::uint64_t>(config.timeout.count()))
            finish(entry, DomainTicket::Event::DeadlineExpired, now);
        return !entry.active && entry.reclaimFrom &&
               elapsedMs(now, *entry.reclaimFrom) >=
                   static_cast<std::uint64_t>(
                       config.retention.count()
                   );
    }

    /** @brief Maintains only a requested record for ordinary command
     * paths. */
    void maintainOne(std::uint64_t id, Clock::time_point now) noexcept
    {
        const auto found = tickets.find(id);
        if (found != tickets.end() && maintain(*found->second, now))
        {
            keys.erase(found->second->key());
            tickets.erase(found);
        }
    }

    /** @brief Reclaims global history during upkeep or admission
     * pressure. */
    void maintainAll(Clock::time_point now) noexcept
    {
        for (auto it = tickets.begin(); it != tickets.end();)
        {
            if (maintain(*it->second, now))
            {
                keys.erase(it->second->key());
                it = tickets.erase(it);
            }
            else
                ++it;
        }
    }

    /** @brief Samples identity availability, failing closed on
     * callback failure. */
    Availability available(const Entry& entry) noexcept
    {
        if (!eligibility)
            return Availability::Ready;
        inEligibility = true;
        try
        {
            const auto result = eligibility(
                entry.ticket.playerId(),
                entry.sessionId
            );
            inEligibility = false;
            return result;
        }
        catch (...)
        {
            // An unavailable identity source cannot authorize an
            // assignment.
            inEligibility = false;
            return Availability::Paused;
        }
    }

    /** @brief Hides records belonging to another player or logical
     * session. */
    Entry*
    owned(PlayerId player, SessionId session, TicketId id) noexcept
    {
        const auto found = tickets.find(id.value());
        if (found == tickets.end() ||
            found->second->ticket.playerId() != player ||
            found->second->sessionId != session)
            return nullptr;
        return found->second.get();
    }

    /** @brief Finds the next ordered candidate after a resumable scan
     * cursor. */
    static auto
    after(std::vector<Candidate>& candidates, std::uint64_t id)
    {
        return std::upper_bound(
            candidates.begin(),
            candidates.end(),
            id,
            [](std::uint64_t value, const Candidate& candidate)
            { return value < candidate.entry->ticket.id().value(); }
        );
    }

    /** @brief Resolves a saved candidate identity in this tick's
     * ready snapshot. */
    static Candidate*
    find(std::vector<Candidate>& candidates, std::uint64_t id)
    {
        const auto found = std::lower_bound(
            candidates.begin(),
            candidates.end(),
            id,
            [](const Candidate& candidate, std::uint64_t value)
            { return candidate.entry->ticket.id().value() < value; }
        );
        return found != candidates.end() &&
                       found->entry->ticket.id().value() == id
                   ? &*found
                   : nullptr;
    }

    /** @brief Excludes entries already assigned or rejected earlier
     * in this tick. */
    static bool ready(const Candidate& candidate) noexcept
    {
        return candidate.enabled &&
               candidate.entry->state() == TicketState::Queued;
    }

    /** @brief Rotates through oldest-first candidates without
     * restarting a prefix. */
    Candidate* nextAnchor(std::vector<Candidate>& candidates) noexcept
    {
        const auto start = after(candidates, anchorAfter);
        const auto first = std::find_if(
            start,
            candidates.end(),
            ready
        );
        if (first != candidates.end())
            return &*first;
        const auto wrapped = std::find_if(
            candidates.begin(),
            start,
            ready
        );
        return wrapped == start ? nullptr : &*wrapped;
    }

    /** @brief Discards an invalid or completed nearest-neighbour
     * search. */
    void clearSearch() noexcept
    {
        searchAnchor.reset();
        bestCandidate.reset();
        candidateAfter = 0;
    }

    /** @brief Rechecks identity availability immediately before pair
     * commitment. */
    bool recheck(Candidate& candidate, Clock::time_point now) noexcept
    {
        if (!ready(candidate))
            return false;
        const auto availability = available(*candidate.entry);
        if (availability == Availability::Ready)
            return true;
        candidate.enabled = false;
        if (availability == Availability::Gone)
            finish(
                *candidate.entry,
                DomainTicket::Event::Cancel,
                now
            );
        return false;
    }

    /** @brief Commits both domain lifecycles after preparing the pair
     * index. */
    void assign(Entry& left, Entry& right, Clock::time_point now)
    {
        const auto leftId = left.ticket.id().value();
        const auto rightId = right.ticket.id().value();
        const auto matchId = std::min(leftId, rightId);
        // Allocate the only new index node before either state
        // machine changes. Everything after this insertion is
        // nonthrowing on the owner thread.
        const auto inserted = assignments.emplace(
            matchId,
            std::pair{leftId, rightId}
        );
        if (!inserted.second)
            throw std::logic_error(
                "duplicate match assignment identity"
            );
        left.apply(DomainTicket::Event::Reserve);
        right.apply(DomainTicket::Event::Reserve);
        left.apply(DomainTicket::Event::Assign);
        right.apply(DomainTicket::Event::Assign);
        left.matchId = right.matchId = matchId;
        left.opponent = right.ticket.playerId();
        right.opponent = left.ticket.playerId();
        left.opponentTicket = right.ticket.id();
        right.opponentTicket = left.ticket.id();
        left.finished = right.finished = now;
        counts.queued -= 2;
        counts.assignedPlayers += 2;
        increment(counts.matchedPairs);
        for (const auto* entry : {&left, &right})
        {
            const auto waited = elapsedMs(now, entry->admitted);
            observe(
                counts.matchedWait,
                QueueMetrics::waitBoundsMs,
                waited
            );
            increment(counts.totalMatchedWaitMs, waited);
        }
        const auto difference = gap(left.mmr, right.mmr);
        observe(counts.mmrGap, QueueMetrics::gapBounds, difference);
        increment(counts.totalMmrGap, difference);
    }

    /** @brief Searches complete candidate sets across ticks within a
     * hard comparison budget. */
    std::vector<PairAssignment> pair(Clock::time_point now)
    {
        std::vector<PairAssignment> assigned;
        // Reserve notification storage before committing any pair.
        // Later index allocation failure must not lose earlier
        // committed pairs.
        assigned.reserve(counts.queued / 2);
        std::vector<Candidate> candidates;
        candidates.reserve(counts.queued);
        for (const auto& [id, entry] : tickets)
        {
            if (entry->state() != TicketState::Queued)
                continue;
            const auto availability = available(*entry);
            if (availability == Availability::Gone)
                finish(*entry, DomainTicket::Event::Cancel, now);
            else if (availability == Availability::Ready)
                candidates.push_back({entry.get()});
        }
        if (candidates.size() < 2)
        {
            clearSearch();
            return assigned;
        }

        std::size_t comparisons = 0;
        std::size_t completed = 0;
        std::size_t remainingReady = candidates.size();
        while (remainingReady >= 2 &&
               comparisons < config.comparisonsPerTick &&
               completed < candidates.size())
        {
            auto* anchor = searchAnchor
                               ? find(candidates, *searchAnchor)
                               : nullptr;
            if (!anchor || !ready(*anchor))
            {
                clearSearch();
                anchor = nextAnchor(candidates);
                if (!anchor)
                    break;
                searchAnchor = anchor->entry->ticket.id().value();
                searchTime = now;
                searchCeiling = candidates.back()
                                    .entry->ticket.id()
                                    .value();
            }

            auto candidate = after(candidates, candidateAfter);
            for (; candidate != candidates.end() &&
                   candidate->entry->ticket.id().value() <=
                       searchCeiling;
                 ++candidate)
            {
                const auto id = candidate->entry->ticket.id().value();
                if (id == *searchAnchor || !ready(*candidate))
                {
                    candidateAfter = id;
                    continue;
                }
                if (comparisons == config.comparisonsPerTick)
                    break;
                ++comparisons;
                candidateAfter = id;
                const auto difference = gap(
                    anchor->entry->mmr,
                    candidate->entry->mmr
                );
                if (difference <=
                        window(*anchor->entry, searchTime) &&
                    difference <=
                        window(*candidate->entry, searchTime) &&
                    (!bestCandidate || difference < bestGap ||
                     (difference == bestGap && id < *bestCandidate)))
                {
                    bestCandidate = id;
                    bestGap = difference;
                }
            }
            if (candidate != candidates.end() &&
                candidate->entry->ticket.id().value() <=
                    searchCeiling)
                break;

            auto* opponent = bestCandidate
                                 ? find(candidates, *bestCandidate)
                                 : nullptr;
            // Eligibility is a best-effort cross-service fence, not a
            // transaction with the presence registry. Assigned pairs
            // remain provisional.
            if (opponent)
            {
                const auto anchorReady = recheck(*anchor, now);
                if (!anchorReady)
                    --remainingReady;
                else
                {
                    const auto opponentReady = recheck(
                        *opponent,
                        now
                    );
                    if (!opponentReady)
                        --remainingReady;
                    else
                    {
                        try
                        {
                            assign(
                                *anchor->entry,
                                *opponent->entry,
                                now
                            );
                        }
                        catch (const std::bad_alloc&)
                        {
                            clearSearch();
                            return assigned;
                        }
                        // Capacity was reserved and these owning
                        // snapshots contain only nothrow-copyable
                        // value fields.
                        static_assert(
                            std::is_nothrow_move_constructible_v<
                                PairAssignment>
                        );
                        assigned.push_back(
                            {snapshot(*anchor->entry, now),
                             snapshot(*opponent->entry, now)}
                        );
                        remainingReady -= 2;
                    }
                }
            }
            anchorAfter = *searchAnchor;
            clearSearch();
            ++completed;
        }
        return assigned;
    }

    /// Immutable thread identity is checked before reading owner-only
    /// fields.
    const std::thread::id owner{std::this_thread::get_id()};
    /// Immutable policy bounds every retained record and candidate
    /// comparison pass.
    QueueConfig config;
    /// Owner-invoked identity sampler; it cannot call back into this
    /// queue.
    Eligibility eligibility;
    /// Greatest command timestamp accepted by this queue.
    std::optional<Clock::time_point> lastNow;
    /// Guards callback reentry without introducing cross-thread
    /// mutable access.
    bool inEligibility{false};
    /// Ordered IDs also preserve admission order because command time
    /// is monotonic.
    std::map<std::uint64_t, std::unique_ptr<Entry>> tickets;
    /// One original command key per retained ticket; no retry alias
    /// allocation.
    std::unordered_map<Key, std::uint64_t, KeyHash> keys;
    /// At most one queued or assigned ticket per player across
    /// logical sessions.
    std::unordered_map<std::uint64_t, std::uint64_t> active;
    /// Pins both records until the trusted allocation owner releases
    /// the pair.
    std::map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t>>
        assignments;
    /// Fixed-size cumulative observations and live occupancy gauges.
    QueueMetrics counts;
    /// Continue incomplete nearest-neighbour searches instead of
    /// rescanning a prefix.
    std::optional<std::uint64_t> searchAnchor;
    /// Nearest compatible candidate encountered in the current
    /// complete pass.
    std::optional<std::uint64_t> bestCandidate;
    std::uint32_t bestGap{0};
    /// Last completed anchor; wraparound guarantees later tickets
    /// receive a turn.
    std::uint64_t anchorAfter{0};
    /// Last scanned identity and initial ceiling exclude unbounded
    /// new arrivals.
    std::uint64_t candidateAfter{0};
    std::uint64_t searchCeiling{0};
    /// A saved pass uses one window timestamp; later windows only
    /// become wider.
    Clock::time_point searchTime{};
};

MatchQueue::MatchQueue(QueueConfig config, Eligibility eligibility)
{
    if (!config.maxTickets || !config.maxQueued ||
        config.maxQueued > config.maxTickets ||
        !config.comparisonsPerTick ||
        config.initialWindow > config.maximumWindow ||
        config.expansionInterval.count() <= 0 ||
        config.timeout.count() <= 0 || config.retention.count() <= 0)
        throw std::invalid_argument(
            "invalid match queue configuration"
        );
    m_state = std::make_unique<State>(config, std::move(eligibility));
}

MatchQueue::~MatchQueue() = default;

/** @brief Recovers original admission values before consulting a live
 * rating source. */
std::optional<TicketSnapshot> MatchQueue::replay(
    PlayerId player,
    SessionId session,
    std::uint64_t joinKey,
    Clock::time_point now
)
{
    auto& state = *m_state;
    state.enter(now);
    if (!joinKey)
        return std::nullopt;

    if (const auto active = state.active.find(player.value());
        active != state.active.end())
        state.maintainOne(active->second, now);
    const State::Key key{player.value(), session.value(), joinKey};
    if (const auto previous = state.keys.find(key);
        previous != state.keys.end())
    {
        state.maintainOne(previous->second, now);
        const auto retained = state.keys.find(key);
        if (retained != state.keys.end())
        {
            increment(state.counts.replayed);
            return state.snapshot(
                *state.tickets.at(retained->second),
                now
            );
        }
    }
    if (const auto active = state.active.find(player.value());
        active != state.active.end())
    {
        const auto& entry = *state.tickets.at(active->second);
        if (entry.sessionId == session)
            return state.snapshot(entry, now);
    }
    return std::nullopt;
}

QueueResult MatchQueue::join(
    PlayerId player,
    SessionId session,
    std::uint64_t joinKey,
    std::uint32_t mmr,
    Clock::time_point now
)
{
    auto& state = *m_state;
    state.enter(now);
    if (!joinKey || mmr > 10000)
        return state.reject(QueueError::InvalidRequest);

    if (const auto active = state.active.find(player.value());
        active != state.active.end())
        state.maintainOne(active->second, now);
    const State::Key key{player.value(), session.value(), joinKey};
    if (const auto previous = state.keys.find(key);
        previous != state.keys.end())
    {
        state.maintainOne(previous->second, now);
        const auto retained = state.keys.find(key);
        if (retained != state.keys.end())
        {
            const auto& entry = *state.tickets.at(retained->second);
            if (entry.mmr != mmr)
                return state.reject(QueueError::Conflict);
            increment(state.counts.replayed);
            return state.snapshot(entry, now);
        }
    }
    if (const auto active = state.active.find(player.value());
        active != state.active.end())
    {
        const auto& entry = *state.tickets.at(active->second);
        if (entry.sessionId != session)
            return state.reject(QueueError::Conflict);
        return state.snapshot(entry, now);
    }
    if (state.tickets.size() >= state.config.maxTickets ||
        state.counts.queued >= state.config.maxQueued)
        state.maintainAll(now);
    if (state.tickets.size() >= state.config.maxTickets ||
        state.counts.queued >= state.config.maxQueued)
        return state.reject(QueueError::Capacity);

    const auto id = allocateTicketId();
    if (!id)
        return state.reject(QueueError::Capacity);
    auto entry = std::make_unique<State::Entry>(
        TicketId{*id},
        player,
        session,
        joinKey,
        mmr,
        now
    );
    const auto inserted = state.tickets.emplace(
        *id,
        std::move(entry)
    );
    try
    {
        state.keys.emplace(key, *id);
        try
        {
            state.active.emplace(player.value(), *id);
        }
        catch (...)
        {
            state.keys.erase(key);
            throw;
        }
    }
    catch (...)
    {
        state.tickets.erase(inserted.first);
        throw;
    }
    increment(state.counts.joined);
    ++state.counts.queued;
    return state.snapshot(*inserted.first->second, now);
}

QueueResult MatchQueue::cancel(
    PlayerId player,
    SessionId session,
    TicketId id,
    Clock::time_point now
)
{
    auto& state = *m_state;
    state.enter(now);
    if (!state.owned(player, session, id))
        return state.reject(QueueError::NotFound);
    state.maintainOne(id.value(), now);
    auto* entry = state.owned(player, session, id);
    if (!entry)
        return state.reject(QueueError::NotFound);
    if (entry->state() == TicketState::Assigned)
        return state.reject(QueueError::AlreadyAssigned);
    if (entry->state() == TicketState::Queued)
        state.finish(*entry, DomainTicket::Event::Cancel, now);
    return state.snapshot(*entry, now);
}

QueueResult MatchQueue::status(
    PlayerId player,
    SessionId session,
    TicketId id,
    Clock::time_point now
)
{
    auto& state = *m_state;
    state.enter(now);
    if (!state.owned(player, session, id))
        return state.reject(QueueError::NotFound);
    state.maintainOne(id.value(), now);
    const auto* entry = state.owned(player, session, id);
    return entry ? QueueResult{state.snapshot(*entry, now)}
                 : state.reject(QueueError::NotFound);
}

std::vector<PairAssignment> MatchQueue::advance(Clock::time_point now)
{
    auto& state = *m_state;
    state.enter(now);
    state.maintainAll(now);
    return state.pair(now);
}

bool MatchQueue::release(
    std::uint64_t matchId,
    Clock::time_point now,
    std::optional<arena::domain::RoomId> room
)
{
    auto& state = *m_state;
    state.enter(now);
    const auto assignment = state.assignments.find(matchId);
    if (assignment == state.assignments.end())
        return false;
    auto& left = *state.tickets.at(assignment->second.first);
    auto& right = *state.tickets.at(assignment->second.second);
    left.roomId = right.roomId = room;
    left.active = right.active = false;
    left.reclaimFrom = right.reclaimFrom = now;
    state.removeActive(left);
    state.removeActive(right);
    state.assignments.erase(assignment);
    state.counts.assignedPlayers -= 2;
    increment(state.counts.releasedPairs);
    return true;
}

QueueMetrics MatchQueue::metrics() const
{
    m_state->checkOwner();
    auto result = m_state->counts;
    result.retained = m_state->tickets.size();
    return result;
}
} // namespace service::match
