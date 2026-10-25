#include "match/MatchQueue.h"

#include <algorithm>
#include <map>
#include <numeric>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace service::match;
using namespace std::chrono_literals;
namespace
{
const Clock::time_point origin{};
PlayerId player(std::uint64_t id)
{
    return PlayerId{id};
}
SessionId session(std::uint64_t id)
{
    return SessionId{id};
}
QueueConfig config()
{
    QueueConfig c;
    c.initialWindow = 50;
    c.expansionStep = 100;
    c.expansionInterval = 100ms;
    c.maximumWindow = 300;
    c.timeout = 1000ms;
    c.retention = 1000ms;
    return c;
}

TEST(MatchQueueTest, RejectsInvalidConfigurationAndRequests)
{
    auto c = config();
    c.expansionInterval = 0ms;
    EXPECT_THROW((MatchQueue{c}), std::invalid_argument);
    c = config();
    c.timeout = 0ms;
    EXPECT_THROW((MatchQueue{c}), std::invalid_argument);
    c = config();
    c.maxQueued = 0;
    EXPECT_THROW((MatchQueue{c}), std::invalid_argument);
    c = config();
    c.maximumWindow = 49;
    EXPECT_THROW((MatchQueue{c}), std::invalid_argument);
    MatchQueue queue(config());
    auto key = queue.join(player(1), session(1), 0, 1000, origin);
    ASSERT_FALSE(key);
    EXPECT_EQ(key.error(), QueueError::InvalidRequest);
    auto rating = queue.join(player(1), session(1), 1, 10001, origin);
    ASSERT_FALSE(rating);
    EXPECT_EQ(rating.error(), QueueError::InvalidRequest);
    EXPECT_EQ(queue.metrics().joined, 0U);
}

TEST(
    MatchQueueTest,
    JoinIsIdempotentWithoutExtendingDeadlineOrAddingAliases
)
{
    MatchQueue queue(config());
    auto first = queue.join(player(1), session(1), 10, 1000, origin);
    ASSERT_TRUE(first);
    auto retry = queue.join(
        player(1),
        session(1),
        10,
        1000,
        origin + 100ms
    );
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->ticketId, first->ticketId);
    EXPECT_EQ(retry->remainingMs, 900U);
    auto otherKey = queue.join(
        player(1),
        session(1),
        20,
        1000,
        origin + 200ms
    );
    ASSERT_TRUE(otherKey);
    EXPECT_EQ(otherKey->ticketId, first->ticketId);
    auto mismatch = queue.join(
        player(1),
        session(1),
        10,
        1001,
        origin + 200ms
    );
    ASSERT_FALSE(mismatch);
    EXPECT_EQ(mismatch.error(), QueueError::Conflict);
    ASSERT_TRUE(queue.cancel(
        player(1),
        session(1),
        first->ticketId,
        origin + 200ms
    ));
    auto cancelledReplay = queue.join(
        player(1),
        session(1),
        10,
        1000,
        origin + 300ms
    );
    ASSERT_TRUE(cancelledReplay);
    EXPECT_EQ(cancelledReplay->state, TicketState::Cancelled);
    auto newTicket = queue.join(
        player(1),
        session(1),
        20,
        1000,
        origin + 300ms
    );
    ASSERT_TRUE(newTicket);
    EXPECT_NE(newTicket->ticketId, first->ticketId);
    EXPECT_EQ(queue.metrics().joined, 2U);
}

TEST(MatchQueueTest, ExpandsAtCompleteIntervalsAndCapsWindow)
{
    MatchQueue queue(config());
    auto first = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(first);
    auto before = queue.status(
        player(1),
        session(1),
        first->ticketId,
        origin + 99ms
    );
    ASSERT_TRUE(before);
    EXPECT_EQ(before->window, 50U);
    auto boundary = queue.status(
        player(1),
        session(1),
        first->ticketId,
        origin + 100ms
    );
    ASSERT_TRUE(boundary);
    EXPECT_EQ(boundary->window, 150U);
    auto capped = queue.status(
        player(1),
        session(1),
        first->ticketId,
        origin + 900ms
    );
    ASSERT_TRUE(capped);
    EXPECT_EQ(capped->window, 300U);
}

TEST(
    MatchQueueTest,
    RequiresBothWindowsAndOnlyAdvanceCreatesAssignments
)
{
    MatchQueue queue(config());
    auto older = queue.join(player(1), session(1), 1, 1000, origin);
    auto younger = queue.join(
        player(2),
        session(2),
        1,
        1200,
        origin + 200ms
    );
    ASSERT_TRUE(older);
    ASSERT_TRUE(younger);
    queue.advance(origin + 200ms);
    auto pending = queue.status(
        player(1),
        session(1),
        older->ticketId,
        origin + 200ms
    );
    ASSERT_TRUE(pending);
    EXPECT_EQ(pending->state, TicketState::Queued);
    auto mature = queue.status(
        player(2),
        session(2),
        younger->ticketId,
        origin + 400ms
    );
    ASSERT_TRUE(mature);
    EXPECT_EQ(mature->state, TicketState::Queued);
    queue.advance(origin + 400ms);
    auto a = queue.status(
        player(1),
        session(1),
        older->ticketId,
        origin + 400ms
    );
    auto b = queue.status(
        player(2),
        session(2),
        younger->ticketId,
        origin + 400ms
    );
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(a->state, TicketState::Assigned);
    EXPECT_EQ(b->state, TicketState::Assigned);
    EXPECT_NE(a->matchId, 0U);
    EXPECT_EQ(a->matchId, b->matchId);
    EXPECT_EQ(a->opponent, player(2));
    EXPECT_EQ(b->opponent, player(1));
    EXPECT_EQ(a->opponentTicket, b->ticketId);
    EXPECT_EQ(b->opponentTicket, a->ticketId);
    auto m = queue.metrics();
    EXPECT_EQ(m.matchedPairs, 1U);
    EXPECT_EQ(m.assignedPlayers, 2U);
    EXPECT_EQ(m.totalMmrGap, 200U);
    EXPECT_EQ(m.totalMatchedWaitMs, 600U);
    EXPECT_EQ(
        std::accumulate(
            m.matchedWait.begin(),
            m.matchedWait.end(),
            0ULL
        ),
        2U
    );
    EXPECT_EQ(
        std::accumulate(m.mmrGap.begin(), m.mmrGap.end(), 0ULL),
        1U
    );
    queue.advance(origin + 500ms);
    EXPECT_EQ(queue.metrics().matchedPairs, 1U);
}

TEST(
    MatchQueueTest,
    DeadlineIsExclusiveAndRetriesCannotReviveExpiredTicket
)
{
    MatchQueue queue(config());
    auto first = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(first);
    auto before = queue.status(
        player(1),
        session(1),
        first->ticketId,
        origin + 999ms
    );
    ASSERT_TRUE(before);
    EXPECT_EQ(before->state, TicketState::Queued);
    auto expired = queue.join(
        player(1),
        session(1),
        1,
        1000,
        origin + 1000ms
    );
    ASSERT_TRUE(expired);
    EXPECT_EQ(expired->state, TicketState::Expired);
    EXPECT_EQ(expired->remainingMs, 0U);
    EXPECT_FALSE(expired->active);
    queue.advance(origin + 1000ms);
    EXPECT_EQ(queue.metrics().expired, 1U);
    EXPECT_EQ(queue.metrics().queued, 0U);
}

TEST(
    MatchQueueTest,
    CancelIsTerminalAndDoesNotTouchAnotherPlayersTicket
)
{
    MatchQueue queue(config());
    auto first = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(first);
    auto foreign = queue.cancel(
        player(2),
        session(2),
        first->ticketId,
        origin
    );
    ASSERT_FALSE(foreign);
    EXPECT_EQ(foreign.error(), QueueError::NotFound);
    ASSERT_TRUE(queue.cancel(
        player(1),
        session(1),
        first->ticketId,
        origin + 10ms
    ));
    auto retry = queue.cancel(
        player(1),
        session(1),
        first->ticketId,
        origin + 20ms
    );
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->state, TicketState::Cancelled);
    EXPECT_FALSE(retry->active);
    EXPECT_EQ(queue.metrics().cancelled, 1U);
    const auto metrics = queue.metrics();
    EXPECT_EQ(
        std::accumulate(
            metrics.abandonedWait.begin(),
            metrics.abandonedWait.end(),
            0ULL
        ),
        1U
    );
}

TEST(MatchQueueTest, AssignedPlayersStayPinnedUntilExplicitRelease)
{
    MatchQueue queue(config());
    auto a = queue.join(player(1), session(1), 1, 1000, origin);
    auto b = queue.join(player(2), session(2), 1, 1000, origin);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    queue.advance(origin);
    auto assigned = queue.status(
        player(1),
        session(1),
        a->ticketId,
        origin + 10000ms
    );
    ASSERT_TRUE(assigned);
    EXPECT_EQ(assigned->state, TicketState::Assigned);
    EXPECT_TRUE(assigned->active);
    auto cancelled = queue.cancel(
        player(1),
        session(1),
        a->ticketId,
        origin + 10000ms
    );
    ASSERT_FALSE(cancelled);
    EXPECT_EQ(cancelled.error(), QueueError::AlreadyAssigned);
    auto duplicate = queue.join(
        player(1),
        session(1),
        2,
        1000,
        origin + 10000ms
    );
    ASSERT_TRUE(duplicate);
    EXPECT_EQ(duplicate->ticketId, a->ticketId);
    EXPECT_TRUE(queue.release(assigned->matchId, origin + 10000ms));
    EXPECT_FALSE(queue.release(assigned->matchId, origin + 10000ms));
    EXPECT_EQ(queue.metrics().assignedPlayers, 0U);
    EXPECT_EQ(queue.metrics().releasedPairs, 1U);
    auto replay = queue.status(
        player(1),
        session(1),
        a->ticketId,
        origin + 10000ms
    );
    ASSERT_TRUE(replay);
    EXPECT_FALSE(replay->active);
    EXPECT_EQ(replay->state, TicketState::Assigned);
    auto next = queue.join(
        player(1),
        session(1),
        2,
        1000,
        origin + 10000ms
    );
    ASSERT_TRUE(next);
    EXPECT_NE(next->ticketId, a->ticketId);
}

TEST(
    MatchQueueTest,
    PausedSessionsAgeButCannotPairAndGoneSessionsCancel
)
{
    std::map<std::uint64_t, Availability> states{
        {1, Availability::Paused},
        {2, Availability::Ready}
    };
    MatchQueue queue(
        config(),
        [&](PlayerId p, SessionId) { return states.at(p.value()); }
    );
    auto a = queue.join(player(1), session(1), 1, 1000, origin);
    auto b = queue.join(player(2), session(2), 1, 1000, origin);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    queue.advance(origin + 500ms);
    EXPECT_EQ(queue.metrics().matchedPairs, 0U);
    states[1] = Availability::Ready;
    queue.advance(origin + 600ms);
    EXPECT_EQ(queue.metrics().matchedPairs, 1U);
    states[3] = Availability::Ready;
    auto c = queue.join(
        player(3),
        session(3),
        1,
        1000,
        origin + 600ms
    );
    ASSERT_TRUE(c);
    states[3] = Availability::Gone;
    queue.advance(origin + 700ms);
    auto cancelled = queue.status(
        player(3),
        session(3),
        c->ticketId,
        origin + 700ms
    );
    ASSERT_TRUE(cancelled);
    EXPECT_EQ(cancelled->state, TicketState::Cancelled);
}

TEST(
    MatchQueueTest,
    NewLogicalSessionCannotStealOldTicketAndMayRejoinAfterGoneSweep
)
{
    auto availability = Availability::Ready;
    MatchQueue queue(
        config(),
        [&](PlayerId, SessionId s)
        {
            return s == session(1) ? availability
                                   : Availability::Ready;
        }
    );
    auto old = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(old);
    auto conflict = queue
                        .join(player(1), session(2), 1, 1000, origin);
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), QueueError::Conflict);
    auto hidden = queue.status(
        player(1),
        session(2),
        old->ticketId,
        origin
    );
    ASSERT_FALSE(hidden);
    EXPECT_EQ(hidden.error(), QueueError::NotFound);
    availability = Availability::Gone;
    queue.advance(origin);
    auto fresh = queue.join(player(1), session(2), 1, 1000, origin);
    ASSERT_TRUE(fresh);
    EXPECT_NE(fresh->ticketId, old->ticketId);
}

TEST(MatchQueueTest, PausedDeadlineStillExpiresWithoutRecovery)
{
    MatchQueue queue(
        config(),
        [](PlayerId, SessionId) { return Availability::Paused; }
    );
    auto a = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(a);
    queue.advance(origin + 1000ms);
    auto end = queue.status(
        player(1),
        session(1),
        a->ticketId,
        origin + 1000ms
    );
    ASSERT_TRUE(end);
    EXPECT_EQ(end->state, TicketState::Expired);
}

TEST(
    MatchQueueTest,
    CapacityIncludesHistoryAndReclaimsAtRetentionBoundary
)
{
    auto c = config();
    c.maxQueued = 1;
    c.maxTickets = 2;
    MatchQueue queue(c);
    auto first = queue.join(player(1), session(1), 1, 1000, origin);
    ASSERT_TRUE(first);
    auto full = queue.join(player(2), session(2), 1, 1000, origin);
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error(), QueueError::Capacity);
    ASSERT_TRUE(
        queue.cancel(player(1), session(1), first->ticketId, origin)
    );
    auto second = queue.join(player(2), session(2), 1, 1000, origin);
    ASSERT_TRUE(second);
    ASSERT_TRUE(
        queue.cancel(player(2), session(2), second->ticketId, origin)
    );
    auto historyFull = queue.join(
        player(3),
        session(3),
        1,
        1000,
        origin + 999ms
    );
    ASSERT_FALSE(historyFull);
    EXPECT_EQ(historyFull.error(), QueueError::Capacity);
    auto reclaimed = queue.join(
        player(3),
        session(3),
        1,
        1000,
        origin + 1000ms
    );
    ASSERT_TRUE(reclaimed);
    EXPECT_EQ(queue.metrics().retained, 1U);
}

TEST(
    MatchQueueTest,
    SmallComparisonBudgetEventuallyReachesCompatibleTail
)
{
    auto c = config();
    c.initialWindow = 0;
    c.maximumWindow = 0;
    c.expansionStep = 0;
    c.comparisonsPerTick = 1;
    MatchQueue queue(c);
    for (std::uint64_t i = 1; i <= 8; ++i)
        ASSERT_TRUE(queue.join(
            player(i),
            session(i),
            1,
            static_cast<std::uint32_t>(i * 1000),
            origin
        ));
    auto tail = queue.join(player(9), session(9), 1, 8000, origin);
    ASSERT_TRUE(tail);
    for (int i = 0; i < 100 && queue.metrics().matchedPairs == 0; ++i)
        queue.advance(origin);
    EXPECT_EQ(queue.metrics().matchedPairs, 1U);
    auto paired = queue.status(
        player(9),
        session(9),
        tail->ticketId,
        origin
    );
    ASSERT_TRUE(paired);
    EXPECT_EQ(paired->opponent, player(8));
}

TEST(
    MatchQueueTest,
    RejectsForeignMutationAndBackwardTimeWithoutCorruptingOwner
)
{
    MatchQueue queue(config());
    auto first = queue.join(
        player(1),
        session(1),
        1,
        1000,
        origin + 10ms
    );
    ASSERT_TRUE(first);
    std::thread foreign(
        [&]
        {
            EXPECT_THROW(
                queue.advance(origin + 20ms),
                std::logic_error
            );
            EXPECT_THROW(queue.metrics(), std::logic_error);
        }
    );
    foreign.join();
    EXPECT_THROW(queue.advance(origin), std::invalid_argument);
    auto current = queue.status(
        player(1),
        session(1),
        first->ticketId,
        origin + 10ms
    );
    ASSERT_TRUE(current);
    EXPECT_EQ(current->state, TicketState::Queued);
}

TEST(
    MatchQueueTest,
    OldestAnchorChoosesNearestRatingAndTicketIdentityBreaksTies
)
{
    auto c = config();
    c.initialWindow = c.maximumWindow = 300;
    MatchQueue queue(c);
    auto anchor = queue.join(player(1), session(1), 1, 1000, origin);
    auto farther = queue.join(player(2), session(2), 1, 1250, origin);
    auto tiedEarlier = queue.join(
        player(3),
        session(3),
        1,
        1050,
        origin
    );
    auto tiedLater = queue
                         .join(player(4), session(4), 1, 950, origin);
    ASSERT_TRUE(anchor);
    ASSERT_TRUE(farther);
    ASSERT_TRUE(tiedEarlier);
    ASSERT_TRUE(tiedLater);
    queue.advance(origin);
    auto paired = queue.status(
        player(1),
        session(1),
        anchor->ticketId,
        origin
    );
    ASSERT_TRUE(paired);
    EXPECT_EQ(paired->state, TicketState::Assigned);
    EXPECT_EQ(paired->opponent, player(3));
    EXPECT_EQ(paired->opponentTicket, tiedEarlier->ticketId);
}

TEST(
    MatchQueueTest,
    BudgetedScanDropsRemovedSavedBestAndEventuallyFindsAnotherCandidate
)
{
    auto c = config();
    c.initialWindow = c.maximumWindow = 300;
    c.comparisonsPerTick = 1;
    std::map<std::uint64_t, Availability> states{
        {1, Availability::Ready},
        {2, Availability::Ready},
        {3, Availability::Ready},
        {4, Availability::Ready}
    };
    MatchQueue queue(
        c,
        [&](PlayerId p, SessionId) { return states.at(p.value()); }
    );
    auto anchor = queue.join(player(1), session(1), 1, 1000, origin);
    auto savedBest = queue.join(
        player(2),
        session(2),
        1,
        1001,
        origin
    );
    ASSERT_TRUE(anchor);
    ASSERT_TRUE(savedBest);
    ASSERT_TRUE(queue.join(player(3), session(3), 1, 1050, origin));
    ASSERT_TRUE(queue.join(player(4), session(4), 1, 4000, origin));
    queue.advance(origin); // The closest candidate is saved, not
                           // committed before a full scan.
    EXPECT_EQ(queue.metrics().matchedPairs, 0U);
    states[2] = Availability::Gone;
    queue.advance(
        origin
    ); // Deletes the waiting saved candidate from eligibility.
    for (int i = 0; i < 30 && queue.metrics().matchedPairs == 0; ++i)
        queue.advance(origin);
    auto result = queue.status(
        player(1),
        session(1),
        anchor->ticketId,
        origin
    );
    ASSERT_TRUE(result);
    EXPECT_EQ(result->state, TicketState::Assigned);
    EXPECT_EQ(result->opponent, player(3));
    auto cancelled = queue.status(
        player(2),
        session(2),
        savedBest->ticketId,
        origin
    );
    ASSERT_TRUE(cancelled);
    EXPECT_EQ(cancelled->state, TicketState::Cancelled);
}

TEST(
    MatchQueuePropertyTest,
    SeededMixedCommandsPreservePairAndAccountingInvariants
)
{
    for (std::uint32_t seed = 0; seed < 32; ++seed)
    {
        SCOPED_TRACE(testing::Message() << "seed=" << seed);
        auto c = config();
        c.timeout = 500ms;
        c.retention = 100ms;
        c.maxTickets = 128;
        c.maxQueued = 32;
        std::map<std::uint64_t, Availability> states;
        std::map<std::uint64_t, std::uint64_t> logical;
        for (std::uint64_t i = 1; i <= 32; ++i)
        {
            states[i] = Availability::Ready;
            logical[i] = i;
        }
        MatchQueue queue(
            c,
            [&](PlayerId p, SessionId s)
            {
                return logical.at(p.value()) == s.value()
                           ? states.at(p.value())
                           : Availability::Gone;
            }
        );
        std::mt19937 random(0xA2E1U + seed);
        struct Saved
        {
            std::uint64_t player, session, key;
            TicketId ticket;
            TicketState last;
            bool active;
        };
        std::vector<Saved> history;
        auto now = origin;
        std::uint64_t nextKey = 1;
        for (int step = 0; step < 1000; ++step)
        {
            SCOPED_TRACE(testing::Message() << "step=" << step);
            now += std::chrono::milliseconds(random() % 4);
            const auto p = 1U + random() % 32;
            const auto command = random() % 9;
            if (command <= 2)
            {
                auto key = nextKey++;
                if (command == 2 && !history.empty())
                {
                    const auto&
                        retry = history[random() % history.size()];
                    if (retry.player == p &&
                        retry.session == logical[p])
                        key = retry.key;
                }
                auto joined = queue.join(
                    player(p),
                    session(logical[p]),
                    key,
                    1000 + (p % 4) * 50,
                    now
                );
                if (joined &&
                    std::none_of(
                        history.begin(),
                        history.end(),
                        [&](const Saved& s)
                        { return s.ticket == joined->ticketId; }
                    ))
                    history.push_back(
                        {p,
                         logical[p],
                         joined->joinKey,
                         joined->ticketId,
                         joined->state,
                         joined->active}
                    );
            }
            else if (command == 3 && !history.empty())
            {
                const auto&
                    saved = history[random() % history.size()];
                queue.cancel(
                    player(saved.player),
                    session(saved.session),
                    saved.ticket,
                    now
                );
            }
            else if (command == 4)
                states[p] = Availability::Paused;
            else if (command == 5)
                states[p] = Availability::Ready;
            else if (command == 6)
            {
                logical[p] += 32;
                states[p] = Availability::Ready;
            }
            else if (command == 7)
                states[p] = Availability::Gone;
            queue.advance(now);
            std::map<std::uint64_t, std::vector<TicketSnapshot>>
                pairs;
            std::map<std::uint64_t, int> active;
            std::size_t waiting = 0, assigned = 0, visible = 0;
            for (auto& saved : history)
            {
                auto current = queue.status(
                    player(saved.player),
                    session(saved.session),
                    saved.ticket,
                    now
                );
                if (!current)
                {
                    EXPECT_EQ(current.error(), QueueError::NotFound);
                    continue;
                }
                ++visible;
                if (saved.last == TicketState::Cancelled ||
                    saved.last == TicketState::Expired ||
                    saved.last == TicketState::Assigned)
                {
                    EXPECT_EQ(current->state, saved.last);
                }
                if (!saved.active)
                {
                    EXPECT_FALSE(current->active);
                }
                saved.last = current->state;
                saved.active = current->active;
                if (!current->active)
                    continue;
                EXPECT_LE(++active[saved.player], 1);
                if (current->state == TicketState::Queued)
                    ++waiting;
                if (current->state == TicketState::Assigned)
                {
                    ++assigned;
                    pairs[current->matchId].push_back(*current);
                }
            }
            for (const auto& [match, members] : pairs)
            {
                ASSERT_NE(match, 0U);
                ASSERT_EQ(members.size(), 2U);
                EXPECT_NE(members[0].playerId, members[1].playerId);
                EXPECT_EQ(members[0].opponent, members[1].playerId);
                EXPECT_EQ(
                    members[1].opponentTicket,
                    members[0].ticketId
                );
                const auto
                    gap = std::max(members[0].mmr, members[1].mmr) -
                          std::min(members[0].mmr, members[1].mmr);
                EXPECT_LE(gap, members[0].window);
                EXPECT_LE(gap, members[1].window);
            }
            auto metrics = queue.metrics();
            EXPECT_EQ(metrics.queued, waiting);
            EXPECT_EQ(metrics.assignedPlayers, assigned);
            EXPECT_EQ(metrics.retained, visible);
            EXPECT_LE(metrics.retained, c.maxTickets);
            EXPECT_EQ(metrics.joined, history.size());
            EXPECT_EQ(
                metrics.matchedPairs * 2,
                std::accumulate(
                    metrics.matchedWait.begin(),
                    metrics.matchedWait.end(),
                    0ULL
                )
            );
            EXPECT_EQ(
                metrics.matchedPairs,
                std::accumulate(
                    metrics.mmrGap.begin(),
                    metrics.mmrGap.end(),
                    0ULL
                )
            );
            if (!pairs.empty() && random() % 4 == 0)
                queue.release(pairs.begin()->first, now);
        }
        now += 1000ms;
        queue.advance(now);
        for (const auto& saved : history)
        {
            auto current = queue.status(
                player(saved.player),
                session(saved.session),
                saved.ticket,
                now
            );
            if (current && current->matchId && current->active)
                queue.release(current->matchId, now);
        }
        queue.advance(now + 1000ms);
        EXPECT_EQ(queue.metrics().retained, 0U);
    }
}

TEST(
    MatchQueueTest, AdvanceExportsEveryNewPairOnceAndNoHistoricalPairs
)
{
    MatchQueue queue(config());
    for (std::uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(
            queue.join(player(id), session(id), 1, 1000, origin)
        );
    const auto pairs = queue.advance(origin);
    ASSERT_EQ(pairs.size(), 2U);
    std::vector<std::uint64_t> players;
    for (const auto& pair : pairs)
    {
        EXPECT_EQ(pair.first.matchId, pair.second.matchId);
        EXPECT_NE(pair.first.playerId, pair.second.playerId);
        EXPECT_EQ(pair.first.state, TicketState::Assigned);
        EXPECT_EQ(pair.second.opponent, pair.first.playerId);
        EXPECT_TRUE(pair.first.active);
        EXPECT_FALSE(pair.first.roomId);
        players.push_back(pair.first.playerId.value());
        players.push_back(pair.second.playerId.value());
    }
    std::sort(players.begin(), players.end());
    EXPECT_EQ(players, (std::vector<std::uint64_t>{1, 2, 3, 4}));
    EXPECT_TRUE(queue.advance(origin + 1ms).empty());
    EXPECT_EQ(queue.metrics().matchedPairs, 2U);
}

TEST(
    MatchQueueTest,
    AllocatorRoomIsRetainedInBothReleasedTicketHistories
)
{
    MatchQueue queue(config());
    const auto a = queue
                       .join(player(1), session(1), 10, 1000, origin);
    const auto b = queue
                       .join(player(2), session(2), 20, 1000, origin);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    const auto pairs = queue.advance(origin);
    ASSERT_EQ(pairs.size(), 1U);
    const arena::domain::RoomId room{
        (UINT64_C(1) << 63) | pairs[0].first.matchId
    };
    ASSERT_TRUE(
        queue.release(pairs[0].first.matchId, origin + 1ms, room)
    );
    EXPECT_FALSE(queue.release(
        pairs[0].first.matchId,
        origin + 2ms,
        arena::domain::RoomId{99}
    ));
    for (const auto& saved : {a.value(), b.value()})
    {
        const auto status = queue.status(
            saved.playerId,
            saved.sessionId,
            saved.ticketId,
            origin + 3ms
        );
        ASSERT_TRUE(status);
        EXPECT_EQ(status->roomId, room);
        EXPECT_FALSE(status->active);
        EXPECT_EQ(status->state, TicketState::Assigned);
        const auto retry = queue.replay(
            saved.playerId,
            saved.sessionId,
            saved.joinKey,
            origin + 3ms
        );
        ASSERT_TRUE(retry);
        EXPECT_EQ(retry->roomId, room);
    }
    EXPECT_EQ(queue.metrics().assignedPlayers, 0U);
    EXPECT_EQ(queue.metrics().releasedPairs, 1U);
    EXPECT_TRUE(queue.advance(origin + 1001ms).empty());
    EXPECT_EQ(queue.metrics().retained, 0U);
}

} // namespace
