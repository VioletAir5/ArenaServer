#include "room/RoomActor.h"

#include <algorithm>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>
#include <tuple>

#include <gtest/gtest.h>

using namespace service::room;
namespace
{
Member member(
    std::uint64_t player,
    std::uint64_t epoch = 1,
    std::uint64_t session = 0
)
{
    return {
        PlayerId{player},
        SessionId{session ? session : player + 100},
        epoch
    };
}

// Independent format oracle: fixed-width bytes, never raw object
// representation or a production hash/serialization helper.
std::uint64_t
referenceChecksum(const Snapshot& snapshot, const ActorConfig& config)
{
    std::uint64_t value = 14695981039346656037ULL;
    const auto append = [&](std::uint64_t number, unsigned bytes)
    {
        for (unsigned i = 0; i < bytes; ++i)
        {
            value ^= number & 0xffU;
            value *= 1099511628211ULL;
            number >>= 8;
        }
    };
    append(1, 8);
    append(snapshot.roomId.value(), 8);
    append(snapshot.incarnation, 8);
    append(snapshot.tick, 8);
    append(static_cast<std::uint32_t>(config.positionLimit), 4);
    append(snapshot.players.size(), 8);
    for (const auto& player : snapshot.players)
    {
        append(player.member.playerId.value(), 8);
        append(player.member.sessionId.value(), 8);
        append(player.member.epoch, 8);
        append(static_cast<std::uint32_t>(player.x), 4);
        append(static_cast<std::uint32_t>(player.y), 4);
        append(player.sequence, 8);
        append(static_cast<std::uint32_t>(player.lastDx), 4);
        append(static_cast<std::uint32_t>(player.lastDy), 4);
    }
    return value;
}

struct ReferenceRoom
{
    std::map<std::uint64_t, PlayerState> players;
    Snapshot step(
        const std::vector<Member>& roster,
        std::vector<Input> attempts,
        const ActorConfig& config,
        std::uint64_t tick
    )
    {
        std::map<std::uint64_t, PlayerState> next;
        for (const auto& m : roster)
        {
            const auto old = players.find(m.playerId.value());
            next.emplace(
                m.playerId.value(),
                old != players.end() && old->second.member == m
                    ? old->second
                    : PlayerState{m}
            );
        }
        players = std::move(next);
        std::sort(
            attempts.begin(),
            attempts.end(),
            [](const Input& a, const Input& b)
            {
                return std::tuple{
                           a.member.playerId.value(),
                           a.sequence,
                           a.dx,
                           a.dy
                       } < std::tuple{
                               b.member.playerId.value(),
                               b.sequence,
                               b.dx,
                               b.dy
                           };
            }
        );
        for (std::size_t begin = 0; begin < attempts.size();)
        {
            std::size_t end = begin + 1;
            while (end < attempts.size() &&
                   attempts[end].member == attempts[begin].member &&
                   attempts[end].sequence == attempts[begin].sequence)
                ++end;
            const auto& input = attempts[begin];
            const bool conflicts = std::any_of(
                attempts.begin() + begin,
                attempts.begin() + end,
                [&](const auto& other)
                {
                    return other.dx != input.dx ||
                           other.dy != input.dy;
                }
            );
            const auto found = players.find(
                input.member.playerId.value()
            );
            if (!conflicts && found != players.end() &&
                found->second.member == input.member &&
                input.sequence != 0 && input.dx >= -1 &&
                input.dx <= 1 && input.dy >= -1 && input.dy <= 1)
            {
                auto& state = found->second;
                if (state.sequence != UINT64_MAX &&
                    input.sequence == state.sequence + 1)
                {
                    state.x = std::clamp(
                        state.x + input.dx,
                        -config.positionLimit,
                        config.positionLimit
                    );
                    state.y = std::clamp(
                        state.y + input.dy,
                        -config.positionLimit,
                        config.positionLimit
                    );
                    state.sequence = input.sequence;
                    state.lastDx = input.dx;
                    state.lastDy = input.dy;
                }
            }
            begin = end;
        }
        Snapshot result{RoomId{7}, 9, tick, 0, {}};
        for (const auto& [id, state] : players)
        {
            (void)id;
            result.players.push_back(state);
        }
        result.checksum = referenceChecksum(result, config);
        return result;
    }
};

TEST(
    RoomActorTest,
    CanonicalChecksumMatchesIndependentGoldenIncludingNegativeCoordinates
)
{
    ActorConfig config;
    config.positionLimit = 100;
    Snapshot snapshot{
        RoomId{7},
        9,
        3,
        0,
        {{member(1, 21, 11), -1, 2, 5, -1, 1},
         {member(2, 22, 12), 3, -4, 6, 1, -1}}
    };
    constexpr auto golden = 13475540215897792617ULL;
    EXPECT_EQ(referenceChecksum(snapshot, config), golden);
    EXPECT_EQ(RoomActor::checksum(snapshot, config), golden);
    auto changed = snapshot;
    changed.players[0].member.epoch++;
    EXPECT_NE(referenceChecksum(changed, config), golden);
    changed = snapshot;
    changed.players[0].lastDx = 1;
    EXPECT_NE(referenceChecksum(changed, config), golden);
}

TEST(
    RoomActorTest,
    ValidatesConfigurationAndBatchesBeforeChangingStateOrJournal
)
{
    auto config = ActorConfig{};
    config.maxMembers = 0;
    EXPECT_THROW(
        (RoomActor(RoomId{7}, 9, config)),
        std::invalid_argument
    );
    config = {};
    config.historyTicks = 0;
    EXPECT_THROW(
        (RoomActor(RoomId{7}, 9, config)),
        std::invalid_argument
    );
    config = {};
    config.positionLimit = 0;
    EXPECT_THROW(
        (RoomActor(RoomId{7}, 9, config)),
        std::invalid_argument
    );
    EXPECT_THROW((RoomActor(RoomId{7}, 0)), std::invalid_argument);
    config = {};
    config.maxMembers = 1;
    config.maxInputsPerTick = 1;
    RoomActor actor(RoomId{7}, 9, config);
    const auto before = actor.snapshot();
    EXPECT_THROW(
        actor.step({member(1), member(2)}, {}),
        std::invalid_argument
    );
    EXPECT_THROW(
        actor.step(
            {member(1)},
            {{member(1), 1, 0, 1}, {member(1), 2, 1, 0}}
        ),
        std::invalid_argument
    );
    EXPECT_THROW(
        actor.step({member(1), member(1)}, {}),
        std::invalid_argument
    );
    EXPECT_EQ(actor.snapshot(), before);
    EXPECT_TRUE(actor.replayLog().ticks.empty());
}

TEST(
    RoomActorTest,
    RosterAndInputPermutationPreserveStateAndCanonicalJournal
)
{
    RoomActor first(RoomId{7}, 9), second(RoomId{7}, 9);
    const std::vector<Input> forward{
        {member(1), 1, -1, 0},
        {member(1), 2, 0, 1},
        {member(2), 1, 1, 1}
    };
    auto reverse = forward;
    std::reverse(reverse.begin(), reverse.end());
    auto a = first.step({member(1), member(2)}, forward);
    auto b = second.step({member(2), member(1)}, reverse);
    EXPECT_EQ(a.snapshot, b.snapshot);
    EXPECT_EQ(
        first.replayLog().ticks[0].inputs,
        second.replayLog().ticks[0].inputs
    );
    EXPECT_EQ(
        first.replayLog().ticks[0].members,
        second.replayLog().ticks[0].members
    );
    EXPECT_EQ(a.snapshot.checksum, referenceChecksum(a.snapshot, {}));
}

TEST(
    RoomActorTest,
    IdenticalDuplicatesApplyOnceAndSequenceSortAllowsReversedContiguousInputs
)
{
    RoomActor actor(RoomId{7}, 9);
    const auto result = actor.step(
        {member(1)},
        {{member(1), 2, 0, 1},
         {member(1), 1, 1, 0},
         {member(1), 1, 1, 0}}
    );
    ASSERT_EQ(result.snapshot.players.size(), 1U);
    EXPECT_EQ(result.snapshot.players[0].sequence, 2U);
    EXPECT_EQ(result.snapshot.players[0].x, 1);
    EXPECT_EQ(result.snapshot.players[0].y, 1);
    EXPECT_EQ(
        std::count(
            result.results.begin(),
            result.results.end(),
            InputStatus::Applied
        ),
        2
    );
    EXPECT_EQ(
        std::count(
            result.results.begin(),
            result.results.end(),
            InputStatus::Duplicate
        ),
        1
    );
    EXPECT_EQ(
        result.results,
        (std::vector{
            InputStatus::Applied,
            InputStatus::Applied,
            InputStatus::Duplicate
        })
    );
}

TEST(
    RoomActorTest,
    ConflictingControlsInOneBatchRejectAllOccurrencesBeforeSequenceAdvances
)
{
    RoomActor actor(RoomId{7}, 9);
    const auto conflict = actor.step(
        {member(1)},
        {{member(1), 1, 1, 0},
         {member(1), 1, 0, 1},
         {member(1), 1, 1, 0}}
    );
    EXPECT_EQ(
        conflict.results,
        std::vector(3, InputStatus::Conflict)
    );
    EXPECT_EQ(conflict.snapshot.players[0].sequence, 0U);
    const auto retry = actor.step(
        {member(1)},
        {{member(1), 1, 1, 0}}
    );
    EXPECT_EQ(retry.results[0], InputStatus::Applied);
}

TEST(
    RoomActorTest,
    GapRetryDuplicateStaleAndDifferentLastControlsAreDistinct
)
{
    RoomActor actor(RoomId{7}, 9);
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 2, 1, 0}}).results[0],
        InputStatus::Gap
    );
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 1, 1, 0}}).results[0],
        InputStatus::Applied
    );
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 1, 1, 0}}).results[0],
        InputStatus::Duplicate
    );
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 1, 0, 1}}).results[0],
        InputStatus::Conflict
    );
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 2, 0, 1}}).results[0],
        InputStatus::Applied
    );
    EXPECT_EQ(
        actor.step({member(1)}, {{member(1), 1, 1, 0}}).results[0],
        InputStatus::Stale
    );
}

TEST(
    RoomActorTest,
    MembershipGenerationResetsControlsButSameMemberSurvivesPollingAndEmptyTicks
)
{
    RoomActor actor(RoomId{7}, 9);
    auto moved = actor.step({member(1)}, {{member(1), 1, 1, 0}})
                     .snapshot;
    EXPECT_EQ(actor.snapshot(), moved);
    auto unchanged = actor.step({member(1)}, {}).snapshot;
    EXPECT_EQ(unchanged.players, moved.players);
    EXPECT_EQ(unchanged.tick, moved.tick + 1);
    auto replaced = actor.step(
        {member(1, 2)},
        {{member(1), 2, 1, 0}}
    );
    EXPECT_EQ(replaced.results[0], InputStatus::NotMember);
    EXPECT_EQ(replaced.snapshot.players[0].x, 0);
    EXPECT_EQ(replaced.snapshot.players[0].sequence, 0U);
    actor.step({}, {});
    auto rejoined = actor.step(
        {member(1, 3)},
        {{member(1, 3), 1, -1, 0}}
    );
    EXPECT_EQ(rejoined.results[0], InputStatus::Applied);
    EXPECT_EQ(rejoined.snapshot.players[0].x, -1);
}

TEST(
    RoomActorTest,
    PositionClampsAtBothBoundariesWithoutIntegerOverflow
)
{
    ActorConfig config;
    config.positionLimit = 2;
    RoomActor actor(RoomId{7}, 9, config);
    for (std::uint64_t seq = 1; seq <= 5; ++seq)
        actor.step({member(1)}, {{member(1), seq, 1, -1}});
    EXPECT_EQ(actor.snapshot().players[0].x, 2);
    EXPECT_EQ(actor.snapshot().players[0].y, -2);
    for (std::uint64_t seq = 6; seq <= 10; ++seq)
        actor.step({member(1)}, {{member(1), seq, -1, 1}});
    EXPECT_EQ(actor.snapshot().players[0].x, -2);
    EXPECT_EQ(actor.snapshot().players[0].y, 2);
}

TEST(RoomActorTest, InvalidControlsAndForeignMembersCannotMoveAPlayer)
{
    RoomActor actor(RoomId{7}, 9);
    auto result = actor.step(
        {member(1)},
        {{member(1), 0, 1, 0},
         {member(1), 1, 2, 0},
         {member(2), 1, 1, 0}}
    );
    EXPECT_EQ(
        result.results,
        (std::vector{
            InputStatus::Invalid,
            InputStatus::Invalid,
            InputStatus::NotMember
        })
    );
    EXPECT_EQ(result.snapshot.players[0].sequence, 0U);
}

TEST(
    RoomActorTest,
    ConflictingGroupsPrecedeValidationWithoutCollapsingMembershipIdentities
)
{
    RoomActor actor(RoomId{7}, 9);
    auto conflicts = actor.step(
        {member(1)},
        {{member(1), 0, 2, 0},
         {member(1), 0, 3, 0},
         {member(2), 1, 1, 0},
         {member(2), 1, 0, 1}}
    );
    EXPECT_EQ(
        conflicts.results,
        std::vector(4, InputStatus::Conflict)
    );
    const auto current = member(1, 2, 200);
    auto separated = actor.step(
        {current},
        {{member(1, 1, 100), 1, 1, 0},
         {current, 1, 0, 1},
         {member(1, 1, 200), 1, -1, 0}}
    );
    EXPECT_EQ(
        separated.results,
        (std::vector{
            InputStatus::NotMember,
            InputStatus::Applied,
            InputStatus::NotMember
        })
    );
    EXPECT_EQ(separated.snapshot.players[0].sequence, 1U);
    EXPECT_EQ(separated.snapshot.players[0].x, 0);
    EXPECT_EQ(separated.snapshot.players[0].y, 1);
}

TEST(
    RoomActorTest,
    ReplayCoversEmptyAndInvalidOnlyTicksAndDetectsIntermediateHashTamper
)
{
    RoomActor actor(RoomId{7}, 9);
    const auto first = actor.step({member(1)}, {}).snapshot;
    const auto second = actor
                            .step({member(1)}, {{member(1), 0, 0, 0}})
                            .snapshot;
    const auto third = actor.step({}, {}).snapshot;
    auto log = actor.replayLog();
    auto replay = RoomActor::replay(log);
    ASSERT_TRUE(replay);
    const std::vector expected{
        referenceChecksum(first, {}),
        referenceChecksum(second, {}),
        referenceChecksum(third, {})
    };
    EXPECT_EQ(replay->checksums, expected);
    log.ticks[1].checksum ^= 1;
    auto tampered = RoomActor::replay(log);
    ASSERT_FALSE(tampered);
    EXPECT_EQ(tampered.error(), ReplayError::ChecksumMismatch);
}

TEST(
    RoomActorTest,
    ReplayHandlesMaximumSequenceWithoutWrapAndMaximumPositionWithoutOverflow
)
{
    ActorConfig config;
    config.positionLimit = INT32_MAX;
    Snapshot checkpoint{
        RoomId{7},
        9,
        0,
        0,
        {{member(1), INT32_MAX, -INT32_MAX, UINT64_MAX, 1, -1}}
    };
    checkpoint.checksum = referenceChecksum(checkpoint, config);
    auto expected = checkpoint;
    expected.tick = 1;
    expected.checksum = referenceChecksum(expected, config);
    ReplayLog maximumSequence{
        config,
        checkpoint,
        {{1,
          {member(1)},
          {{member(1), 0, 0, 0}, {member(1), UINT64_MAX, 1, -1}},
          expected.checksum}}
    };
    auto unchanged = RoomActor::replay(maximumSequence);
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(unchanged->snapshot, expected);
    checkpoint.players[0].sequence = 1;
    checkpoint.checksum = referenceChecksum(checkpoint, config);
    expected = checkpoint;
    expected.tick = 1;
    expected.players[0].sequence = 2;
    expected.checksum = referenceChecksum(expected, config);
    ReplayLog maximumPosition{
        config,
        checkpoint,
        {{1, {member(1)}, {{member(1), 2, 1, -1}}, expected.checksum}}
    };
    auto clamped = RoomActor::replay(maximumPosition);
    ASSERT_TRUE(clamped);
    EXPECT_EQ(clamped->snapshot, expected);
}

TEST(
    RoomActorTest,
    RollingCheckpointRetainsExactlyBoundedTicksAndDetectsTampering
)
{
    ActorConfig config;
    config.historyTicks = 3;
    RoomActor actor(RoomId{7}, 9, config);
    for (std::uint64_t seq = 1; seq <= 10; ++seq)
        actor.step({member(1)}, {{member(1), seq, 1, 0}});
    const auto log = actor.replayLog();
    ASSERT_EQ(log.ticks.size(), 3U);
    EXPECT_EQ(log.checkpoint.tick, 7U);
    EXPECT_EQ(
        log.checkpoint.checksum,
        referenceChecksum(log.checkpoint, config)
    );
    auto replay = RoomActor::replay(log);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, actor.snapshot());
    ASSERT_EQ(replay->checksums.size(), 3U);
    for (std::size_t index = 0; index < 3; ++index)
        EXPECT_EQ(
            replay->checksums[index],
            log.ticks[index].checksum
        );
    auto tampered = log;
    tampered.ticks.back().inputs[0].dx = -1;
    EXPECT_FALSE(RoomActor::replay(tampered));
    tampered = log;
    tampered.checkpoint.players[0].sequence--;
    EXPECT_FALSE(RoomActor::replay(tampered));
    tampered = log;
    tampered.ticks.erase(tampered.ticks.begin());
    EXPECT_FALSE(RoomActor::replay(tampered));
    tampered = log;
    tampered.ticks[1].tick = tampered.ticks[0].tick;
    EXPECT_FALSE(RoomActor::replay(tampered));
}

TEST(RoomActorTest, ReplayRejectsInvalidCheckpointAndTickOverflow)
{
    RoomActor actor(RoomId{7}, 9);
    auto log = actor.replayLog();
    log.checkpoint.tick = UINT64_MAX;
    log.checkpoint.checksum = referenceChecksum(
        log.checkpoint,
        log.config
    );
    log.ticks.push_back({0, {}, {}, 0});
    EXPECT_FALSE(RoomActor::replay(log));
    log = actor.replayLog();
    log.checkpoint.incarnation = 0;
    log.checkpoint.checksum = referenceChecksum(
        log.checkpoint,
        log.config
    );
    EXPECT_FALSE(RoomActor::replay(log));
    log = actor.replayLog();
    log.checkpoint.players = {{member(1), 1, 0, 0, 0, 0}};
    log.checkpoint.checksum = referenceChecksum(
        log.checkpoint,
        log.config
    );
    EXPECT_FALSE(RoomActor::replay(log));
}

TEST(RoomActorTest, ForeignOwnerAccessRejectsBeforeMutation)
{
    RoomActor actor(RoomId{7}, 9);
    std::thread foreign(
        [&]
        {
            EXPECT_THROW(actor.snapshot(), std::logic_error);
            EXPECT_THROW(actor.replayLog(), std::logic_error);
            EXPECT_THROW(actor.step({}, {}), std::logic_error);
        }
    );
    foreign.join();
    EXPECT_EQ(actor.snapshot().tick, 0U);
}

TEST(
    RoomActorPropertyTest,
    FixedSeedLogsMatchIndependentModelAcrossRosterAndArrivalPermutations
)
{
    for (std::uint32_t seed = 0; seed < 32; ++seed)
    {
        SCOPED_TRACE(testing::Message() << "seed=" << seed);
        ActorConfig config;
        config.historyTicks = 7;
        config.positionLimit = 4;
        RoomActor actor(RoomId{7}, 9, config),
            permuted(RoomId{7}, 9, config);
        ReferenceRoom oracle;
        std::mt19937 random(0x2000U + seed);
        std::map<std::uint64_t, std::uint64_t> epochs{
            {1, 1},
            {2, 1},
            {3, 1},
            {4, 1}
        };
        for (std::uint64_t tick = 1; tick <= 1000; ++tick)
        {
            SCOPED_TRACE(testing::Message() << "tick=" << tick);
            std::vector<Member> roster;
            for (std::uint64_t p = 1; p <= 4; ++p)
            {
                if (random() % 31 == 0)
                    ++epochs[p];
                if (random() % 9 != 0)
                    roster.push_back(member(p, epochs[p]));
            }
            std::vector<Input> inputs;
            for (const auto& m : roster)
            {
                const auto old = oracle.players.find(
                    m.playerId.value()
                );
                const auto sequence = old != oracle.players.end() &&
                                              old->second.member == m
                                          ? old->second.sequence
                                          : 0;
                const auto next = sequence + 1;
                const auto dx = static_cast<std::int32_t>(
                                    random() % 3
                                ) -
                                1;
                const auto dy = static_cast<std::int32_t>(
                                    random() % 3
                                ) -
                                1;
                inputs.push_back({m, next, dx, dy});
                if (random() % 3 == 0)
                    inputs.push_back(inputs.back());
                if (random() % 5 == 0)
                    inputs.push_back({m, next, dx == 1 ? -1 : 1, dy});
                if (random() % 3 == 0)
                    inputs.push_back({m, next + 1, 0, 1});
                if (random() % 7 == 0)
                    inputs.push_back({m, next + 3, 1, 0});
            }
            auto expected = oracle.step(roster, inputs, config, tick);
            auto result = actor.step(roster, inputs);
            std::shuffle(roster.begin(), roster.end(), random);
            std::shuffle(inputs.begin(), inputs.end(), random);
            auto reordered = permuted.step(roster, inputs);
            EXPECT_EQ(result.snapshot, expected);
            EXPECT_EQ(reordered.snapshot, expected);
            const auto log = actor.replayLog();
            EXPECT_LE(log.ticks.size(), config.historyTicks);
            if (tick % 37 == 0)
            {
                auto replay = RoomActor::replay(log);
                ASSERT_TRUE(replay);
                EXPECT_EQ(replay->snapshot, expected);
            }
        }
    }
}
} // namespace
