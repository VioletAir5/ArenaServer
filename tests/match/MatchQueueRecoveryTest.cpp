#include <array>
#include <cstdint>
#include <memory>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "match/MatchQueue.h"

namespace
{
using namespace service::match;
using namespace std::chrono_literals;
const Clock::time_point origin{};

struct IdSource
{
    std::uint64_t next{1000}, calls{0};
    std::optional<std::uint64_t> take()
    {
        ++calls;
        return next++;
    }
};
QueueConfig policy(IdSource& ids)
{
    QueueConfig result;
    result.expansionStep = 25;
    result.expansionInterval = 25ms;
    result.timeout = 1000ms;
    result.retention = 500ms;
    result.maxTickets = 256;
    result.maxQueued = 128;
    result.comparisonsPerTick = 3;
    result.ticketIdSource = [&ids] { return ids.take(); };
    return result;
}
std::uint32_t rating(std::uint64_t player)
{
    return 1000 + static_cast<std::uint32_t>((player * 71) % 500);
}
struct Action
{
    unsigned kind;
    std::uint64_t player, key, index;
    Clock::time_point now;
};
struct Observation
{
    std::optional<QueueResult> response;
    std::optional<TicketSnapshot> replay;
    std::vector<TicketSnapshot> paired;
    bool released{false};
    bool operator==(const Observation&) const = default;
};
Observation execute(MatchQueue& queue, const Action& action)
{
    Observation result;
    const PlayerId player{action.player};
    const SessionId session{action.player + 100};
    switch (action.kind)
    {
    case 0:
    case 1:
    case 2:
        result.response = queue.join(
            player,
            session,
            action.key,
            rating(action.player),
            action.now
        );
        break;
    case 3:
    case 4:
    {
        const auto image = queue.checkpoint();
        if (image.entries.empty())
            result.response = queue.join(
                player,
                session,
                action.key,
                rating(action.player),
                action.now
            );
        else
        {
            const auto& entry =
                image.entries[action.index % image.entries.size()];
            result.response = action.kind == 3 ? queue.cancel(
                                                     entry.playerId,
                                                     entry.sessionId,
                                                     entry.ticketId,
                                                     action.now
                                                 )
                                               : queue.status(
                                                     entry.playerId,
                                                     entry.sessionId,
                                                     entry.ticketId,
                                                     action.now
                                                 );
        }
        break;
    }
    case 5:
    case 8:
        for (const auto& pair : queue.advance(action.now))
        {
            result.paired.push_back(pair.first);
            result.paired.push_back(pair.second);
        }
        break;
    case 6:
    {
        const auto image = queue.checkpoint();
        const auto id =
            image.entries.empty()
                ? action.key
                : image.entries[action.index % image.entries.size()]
                      .matchId;
        result.released = queue.release(
            id,
            action.now,
            arena::domain::RoomId{700}
        );
        break;
    }
    case 7:
        result.replay = queue.replay(
            player,
            session,
            action.key,
            action.now
        );
        break;
    default:
        result.response = queue.join(
            player,
            session,
            0,
            10001,
            action.now
        );
        break;
    }
    return result;
}

TEST(
    MatchQueueRecoveryTest,
    OneHundredRandomPrefixesRetainAllFutureObservations
)
{
    std::mt19937_64 random(0x33adab1e);
    for (unsigned round = 0; round != 100; ++round)
    {
        SCOPED_TRACE(round);
        IdSource liveIds, recoveredIds;
        std::array<Availability, 17> livePresence{},
            recoveredPresence{};
        livePresence.fill(Availability::Ready);
        std::uint64_t liveEligibility = 0, recoveredEligibility = 0;
        auto liveConfig = policy(liveIds);
        MatchQueue live(
            liveConfig,
            [&](auto player, auto)
            {
                ++liveEligibility;
                return livePresence[player.value()];
            }
        );
        auto now = origin;
        for (unsigned step = 0; step != 60; ++step)
        {
            now += std::chrono::milliseconds(random() % 31);
            const Action action{
                static_cast<unsigned>(random() % 10),
                1 + random() % 16,
                1 + random() % 8,
                random(),
                now
            };
            if (action.kind == 8)
                livePresence[action.player] = static_cast<
                    Availability>(random() % 3);
            execute(live, action);
        }
        const auto before = live.checkpoint();
        recoveredIds.next = liveIds.next;
        recoveredPresence = livePresence;
        auto recovered = MatchQueue::restore(
            before,
            policy(recoveredIds),
            [&](auto player, auto)
            {
                ++recoveredEligibility;
                return recoveredPresence[player.value()];
            }
        );
        ASSERT_TRUE(recovered);
        EXPECT_EQ(recoveredIds.calls, 0U);
        EXPECT_EQ(recoveredEligibility, 0U);
        EXPECT_EQ((*recovered)->checkpoint(), before);
        for (unsigned step = 0; step != 80; ++step)
        {
            SCOPED_TRACE(step);
            now += std::chrono::milliseconds(random() % 31);
            const Action action{
                static_cast<unsigned>(random() % 10),
                1 + random() % 16,
                1 + random() % 8,
                random(),
                now
            };
            if (action.kind == 8)
            {
                const auto presence = static_cast<Availability>(
                    random() % 3
                );
                livePresence[action.player] = recoveredPresence
                    [action.player] = presence;
            }
            EXPECT_EQ(
                execute(live, action),
                execute(**recovered, action)
            );
            EXPECT_EQ(live.checkpoint(), (*recovered)->checkpoint());
            EXPECT_EQ(liveIds.next, recoveredIds.next);
        }
    }
}

TEST(
    MatchQueueRecoveryTest,
    RetainsReleasedHistoryAfterOnlyOnePeerWasReclaimed
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    auto first = queue.join(
        PlayerId{1},
        SessionId{11},
        1,
        1000,
        origin
    );
    auto second = queue.join(
        PlayerId{2},
        SessionId{12},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    const auto pairs = queue.advance(origin);
    ASSERT_EQ(pairs.size(), 1U);
    ASSERT_TRUE(queue.release(
        pairs[0].first.matchId,
        origin + 1ms,
        arena::domain::RoomId{900}
    ));
    EXPECT_FALSE(queue.status(
        PlayerId{1},
        SessionId{11},
        first->ticketId,
        origin + 502ms
    ));
    const auto image = queue.checkpoint();
    ASSERT_EQ(image.entries.size(), 1U);
    EXPECT_EQ(
        image.entries[0].lifecycle.state,
        TicketState::Assigned
    );
    EXPECT_FALSE(image.entries[0].active);
    auto restored = MatchQueue::restore(image, config);
    ASSERT_TRUE(restored);
    EXPECT_EQ((*restored)->checkpoint(), image);
    auto replay = (*restored)->status(
        PlayerId{2},
        SessionId{12},
        second->ticketId,
        origin + 502ms
    );
    EXPECT_FALSE(replay);
    EXPECT_EQ(replay.error(), QueueError::NotFound);
}

TEST(
    MatchQueueRecoveryTest,
    DoesNotRenewQueueDeadlineOrTerminalRetention
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    auto first = queue.join(
        PlayerId{1},
        SessionId{11},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(queue.status(
        PlayerId{1},
        SessionId{11},
        first->ticketId,
        origin + 250ms
    ));
    auto restored = MatchQueue::restore(queue.checkpoint(), config);
    ASSERT_TRUE(restored);
    auto waiting = (*restored)->status(
        PlayerId{1},
        SessionId{11},
        first->ticketId,
        origin + 999ms
    );
    ASSERT_TRUE(waiting);
    EXPECT_EQ(waiting->remainingMs, 1U);
    auto expired = (*restored)->status(
        PlayerId{1},
        SessionId{11},
        first->ticketId,
        origin + 1000ms
    );
    ASSERT_TRUE(expired);
    EXPECT_EQ(expired->state, TicketState::Expired);
    EXPECT_EQ(expired->waitedMs, 1000U);
    const auto terminal = (*restored)->checkpoint();
    auto again = MatchQueue::restore(terminal, config);
    ASSERT_TRUE(again);
    EXPECT_FALSE((*again)->status(
        PlayerId{1},
        SessionId{11},
        first->ticketId,
        origin + 1500ms
    ));
}

TEST(
    MatchQueueRecoveryTest,
    RejectsCorruptIdentityLifecycleTimeIndexesAndAccounting
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    ASSERT_TRUE(
        queue.join(PlayerId{2}, SessionId{12}, 1, 1000, origin)
    );
    ASSERT_EQ(queue.advance(origin + 1ms).size(), 1U);
    const auto valid = queue.checkpoint();
    ASSERT_TRUE(MatchQueue::restore(valid, config));
    const auto rejected = [&](auto corrupt)
    {
        auto image = valid;
        corrupt(image);
        auto restored = MatchQueue::restore(image, config);
        ASSERT_FALSE(restored);
        EXPECT_EQ(
            restored.error(),
            QueueRecoveryError::InvalidCheckpoint
        );
    };
    rejected([](auto& image) { ++image.version; });
    rejected(
        [](auto& image)
        { image.entries[1].ticketId = image.entries[0].ticketId; }
    );
    rejected([](auto& image) { image.entries[0].joinKey = 0; });
    rejected([](auto& image)
             { image.entries[0].lifecycle.revision = 1; });
    rejected(
        [](auto& image)
        {
            image.entries[0]
                .lifecycle.state = static_cast<TicketState>(99);
        }
    );
    rejected(
        [](auto& image)
        { image.entries[0].lifecycle.state = TicketState::Reserved; }
    );
    rejected([](auto& image)
             { image.entries[0].finished = origin - 1ms; });
    rejected([](auto& image) { image.entries[0].active = false; });
    rejected([](auto& image)
             { image.entries[0].opponentTicket = TicketId{99999}; });
    rejected(
        [](auto& image)
        { image.entries[0].opponent = image.entries[0].playerId; }
    );
    rejected(
        [](auto& image)
        { image.entries[0].roomId = arena::domain::RoomId{900}; }
    );
    rejected([](auto& image) { image.metrics.assignedPlayers = 0; });
    rejected([](auto& image) { image.metrics.mmrGap[0] = 0; });
    rejected(
        [](auto& image)
        { image.lastTicketId = image.entries[0].ticketId.value(); }
    );
    rejected([](auto& image) { image.lastDurableId = 0; });
    rejected([](auto& image)
             { image.search.anchor = image.lastTicketId + 1; });
    rejected(
        [](auto& image)
        {
            image.entries.pop_back();
            --image.metrics.retained;
        }
    );
    rejected([](auto& image) { image.metrics.totalMmrGap = 1; });
    rejected([](auto& image)
             { image.metrics.totalMatchedWaitMs = 201; });
    rejected(
        [](auto& image)
        {
            image.metrics.mmrGap[0] = 0;
            image.metrics.mmrGap[7] = 1;
        }
    );
    auto mismatched = config;
    ++mismatched.expansionStep;
    auto restored = MatchQueue::restore(valid, mismatched);
    ASSERT_FALSE(restored);
    EXPECT_EQ(restored.error(), QueueRecoveryError::PolicyMismatch);
}

TEST(
    MatchQueueRecoveryTest,
    AcceptsReachableReservationCycleAndPreservesItsRevision
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    auto image = queue.checkpoint();
    image.entries[0].lifecycle.revision = 2;
    auto restored = MatchQueue::restore(image, config);
    ASSERT_TRUE(restored);
    EXPECT_EQ((*restored)->checkpoint(), image);
    ASSERT_TRUE((*restored)->cancel(
        PlayerId{1},
        SessionId{11},
        image.entries[0].ticketId,
        origin
    ));
    EXPECT_EQ(
        (*restored)->checkpoint().entries[0].lifecycle.revision,
        3U
    );
    image.entries[0].lifecycle.revision = 1;
    EXPECT_FALSE(MatchQueue::restore(image, config));
}

TEST(
    MatchQueueRecoveryTest,
    PreservesPartialSearchWhoseAnchorWasCancelled
)
{
    IdSource ids;
    auto config = policy(ids);
    config.comparisonsPerTick = 1;
    MatchQueue queue(config);
    for (std::uint64_t player = 1; player != 6; ++player)
        ASSERT_TRUE(queue.join(
            PlayerId{player},
            SessionId{player + 10},
            1,
            1000,
            origin
        ));
    queue.advance(origin + 1ms);
    const auto partial = queue.checkpoint();
    ASSERT_TRUE(partial.search.anchor);
    const auto anchor = partial.entries[0];
    ASSERT_TRUE(queue.cancel(
        anchor.playerId,
        anchor.sessionId,
        anchor.ticketId,
        origin + 2ms
    ));
    const auto image = queue.checkpoint();
    ASSERT_TRUE(image.search.anchor);
    auto restored = MatchQueue::restore(image, config);
    ASSERT_TRUE(restored);
    EXPECT_EQ((*restored)->checkpoint(), image);
    const Action advance{5, 1, 1, 0, origin + 3ms};
    EXPECT_EQ(execute(queue, advance), execute(**restored, advance));
    EXPECT_EQ(queue.checkpoint(), (*restored)->checkpoint());
}

TEST(
    MatchQueueRecoveryTest,
    DoesNotAllocateIdsOrCallEligibilityDuringCloning
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    const auto calls = ids.calls;
    const auto image = queue.checkpoint();
    auto injected = config;
    injected.ticketIdSource = []() -> std::optional<std::uint64_t>
    { throw std::runtime_error("recovery must not allocate"); };
    bool called = false;
    auto restored = MatchQueue::restore(
        image,
        injected,
        [&](auto, auto)
        {
            called = true;
            throw std::runtime_error(
                "recovery must not sample eligibility"
            );
            return Availability::Ready;
        }
    );
    ASSERT_TRUE(restored);
    EXPECT_EQ(ids.calls, calls);
    EXPECT_FALSE(called);
    EXPECT_EQ((*restored)->checkpoint(), image);
}

TEST(
    MatchQueueRecoveryTest,
    DefersGlobalIdHighWaterAdvanceUntilARealAdmission
)
{
    MatchQueue queue;
    const auto first = queue.join(
        PlayerId{1},
        SessionId{11},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(first);
    auto image = queue.checkpoint();
    image.lastTicketId += 10000;
    auto restored = MatchQueue::restore(image, QueueConfig{});
    ASSERT_TRUE(restored);
    MatchQueue unrelated;
    const auto second = unrelated.join(
        PlayerId{2},
        SessionId{12},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(second);
    EXPECT_EQ(second->ticketId.value(), first->ticketId.value() + 1);
    const auto resumed = (*restored)->join(
        PlayerId{3},
        SessionId{13},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(resumed);
    EXPECT_GT(resumed->ticketId.value(), image.lastTicketId);
}

TEST(
    MatchQueueRecoveryTest,
    ChecksCheckpointOwnerAndRehydratesOnANewThread
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    const auto image = queue.checkpoint();
    bool foreignRejected = false, restoredThere = false;
    std::thread other(
        [&]
        {
            try
            {
                queue.checkpoint();
            }
            catch (const std::logic_error&)
            {
                foreignRejected = true;
            }
            auto restored = MatchQueue::restore(image, config);
            restoredThere = restored &&
                            (*restored)->checkpoint() == image;
        }
    );
    other.join();
    EXPECT_TRUE(foreignRejected);
    EXPECT_TRUE(restoredThere);
    EXPECT_EQ(queue.checkpoint(), image);
}
TEST(
    MatchQueueRecoveryTest,
    RejectsQueuedWithoutBudgetForWholeAssignment
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    auto image = queue.checkpoint();
    // Queued is reachable after any even number of reservation
    // cycles, but another Reserve+Assign must both fit before this
    // queue is usable.
    image.entries[0].lifecycle.revision = UINT64_MAX - 1;
    const auto restored = MatchQueue::restore(image, config);
    ASSERT_FALSE(restored);
    EXPECT_EQ(
        restored.error(),
        QueueRecoveryError::InvalidCheckpoint
    );
}

TEST(
    MatchQueueRecoveryTest,
    ForgedSavedCandidateCannotBypassGapOrSavedWindow
)
{
    IdSource ids;
    auto config = policy(ids);
    config.comparisonsPerTick = 1;
    MatchQueue queue(config);
    ASSERT_TRUE(queue.join(PlayerId{1}, SessionId{11}, 1, 0, origin));
    ASSERT_TRUE(
        queue.join(PlayerId{2}, SessionId{12}, 1, 10000, origin)
    );
    ASSERT_TRUE(
        queue.join(PlayerId{3}, SessionId{13}, 1, 10000, origin)
    );
    ASSERT_TRUE(queue.advance(origin + 1ms).empty());
    const auto partial = queue.checkpoint();
    ASSERT_TRUE(partial.search.anchor);
    ASSERT_FALSE(partial.search.bestCandidate);
    ASSERT_LT(partial.search.candidateAfter, partial.search.ceiling);
    for (unsigned scenario = 0; scenario != 3; ++scenario)
    {
        SCOPED_TRACE(scenario);
        auto forged = partial;
        forged.search.bestCandidate = forged.entries[1]
                                          .ticketId.value();
        forged.search.candidateAfter = forged.search.ceiling;
        forged.search.bestGap = scenario == 0 ? 0 : 10000;
        if (scenario == 2)
        {
            // This gap fits maximumWindow, but not the original
            // search-time window. Recovery must not validate it
            // against a later wider one.
            forged.entries[1].mmr = 400;
            forged.search.bestGap = 400;
        }
        auto restored = MatchQueue::restore(forged, config);
        if (!restored)
        {
            EXPECT_EQ(
                restored.error(),
                QueueRecoveryError::InvalidCheckpoint
            );
            continue;
        }
        // Recovery may instead discard an unsafe cached search.
        // Either path must stop these players from being assigned to
        // each other.
        for (const auto& pair : (*restored)->advance(origin + 2ms))
        {
            EXPECT_NE(pair.first.playerId, PlayerId{1});
            EXPECT_NE(pair.second.playerId, PlayerId{1});
        }
        auto first = (*restored)->status(
            PlayerId{1},
            SessionId{11},
            forged.entries[0].ticketId,
            origin + 2ms
        );
        ASSERT_TRUE(first);
        EXPECT_EQ(first->state, TicketState::Queued);
    }
}
TEST(
    MatchQueueRecoveryTest,
    ReleasedHistoryCannotReferToAnUnallocatedFuturePeer
)
{
    IdSource ids;
    auto config = policy(ids);
    MatchQueue queue(config);
    const auto first = queue.join(
        PlayerId{1},
        SessionId{11},
        1,
        1000,
        origin
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(
        queue.join(PlayerId{2}, SessionId{12}, 1, 1000, origin)
    );
    const auto pairs = queue.advance(origin);
    ASSERT_EQ(pairs.size(), 1U);
    ASSERT_TRUE(queue.release(pairs[0].first.matchId, origin + 1ms));
    ASSERT_FALSE(queue.status(
        PlayerId{2},
        SessionId{12},
        pairs[0].second.ticketId,
        origin + 502ms
    ));
    auto image = queue.checkpoint();
    ASSERT_EQ(image.entries.size(), 1U);
    ASSERT_TRUE(MatchQueue::restore(image, config));
    image.entries[0].opponentTicket = TicketId{
        image.lastTicketId + 1
    };
    auto restored = MatchQueue::restore(image, config);
    ASSERT_FALSE(restored);
    EXPECT_EQ(
        restored.error(),
        QueueRecoveryError::InvalidCheckpoint
    );
}

TEST(
    MatchQueueRecoveryTest,
    RejectsImpossibleSumsAndRetainsSaturatedValidBounds
)
{
    MatchQueue empty;
    const auto emptyImage = empty.checkpoint();
    for (unsigned which = 0; which != 2; ++which)
    {
        auto invalid = emptyImage;
        if (which)
            invalid.metrics.totalMmrGap = 1;
        else
            invalid.metrics.totalMatchedWaitMs = 1;
        const auto restored = MatchQueue::restore(
            invalid,
            QueueConfig{}
        );
        ASSERT_FALSE(restored);
        EXPECT_EQ(
            restored.error(),
            QueueRecoveryError::InvalidCheckpoint
        );
    }
    MatchQueue queue;
    ASSERT_TRUE(
        queue.join(PlayerId{1}, SessionId{11}, 1, 1000, origin)
    );
    ASSERT_TRUE(
        queue.join(PlayerId{2}, SessionId{12}, 1, 1000, origin)
    );
    ASSERT_EQ(queue.advance(origin + 1ms).size(), 1U);
    auto saturated = queue.checkpoint();
    // Model a long history whose positive wait sum saturated while
    // individual sample counters and zero-gap samples remained
    // representable.
    const auto pairs = (UINT64_MAX - 1) / 2;
    saturated.lastTicketId = UINT64_MAX;
    saturated.metrics.joined = UINT64_MAX;
    saturated.metrics.matchedPairs = pairs;
    saturated.metrics.releasedPairs = pairs - 1;
    saturated.metrics.matchedWait.fill(0);
    saturated.metrics.matchedWait[0] = UINT64_MAX - 1;
    saturated.metrics.mmrGap.fill(0);
    saturated.metrics.mmrGap[0] = pairs;
    saturated.metrics.totalMatchedWaitMs = UINT64_MAX;
    saturated.metrics.totalMmrGap = 0;
    auto restored = MatchQueue::restore(saturated, QueueConfig{});
    ASSERT_TRUE(restored);
    EXPECT_EQ((*restored)->checkpoint(), saturated);
    saturated.metrics.totalMmrGap = UINT64_MAX;
    EXPECT_FALSE(MatchQueue::restore(saturated, QueueConfig{}));
}
} // namespace
