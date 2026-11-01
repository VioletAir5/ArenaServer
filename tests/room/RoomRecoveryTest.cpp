#include <future>
#include <random>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "room/RoomActor.h"

using namespace service::room;

namespace
{
std::vector<Member> roster()
{
    return {
        {PlayerId{1}, SessionId{101}, 7},
        {PlayerId{2}, SessionId{102}, 9}
    };
}

TEST(
    RoomRecoveryTest,
    NewOwnerContinuesOneHundredRandomMatchesAtExactTickAndChecksum
)
{
    std::mt19937 random(0x33c0ffeeU);
    ActorConfig config;
    config.historyTicks = 8;
    for (unsigned match = 0; match < 100; ++match)
    {
        RoomActor live(RoomId{1000 + match}, 2000 + match, config);
        const auto members = roster();
        std::uint64_t sequence[2]{};
        for (unsigned tick = 0; tick < 25; ++tick)
        {
            std::vector<Input> inputs;
            for (unsigned player = 0; player < 2; ++player)
                inputs.push_back(
                    {members[player],
                     ++sequence[player],
                     static_cast<int>(random() % 3) - 1,
                     static_cast<int>(random() % 3) - 1}
                );
            live.step(members, std::move(inputs));
        }
        const auto committed = live.snapshot();
        std::vector<std::vector<Input>> future;
        std::vector<TickResult> expected;
        for (unsigned tick = 0; tick < 40; ++tick)
        {
            std::vector<Input> inputs;
            for (unsigned player = 0; player < 2; ++player)
            {
                const auto next = ++sequence[player];
                inputs.push_back(
                    {members[player],
                     next,
                     static_cast<int>(random() % 3) - 1,
                     static_cast<int>(random() % 3) - 1}
                );
                if (random() % 5 == 0)
                    inputs.push_back(inputs.back());
                if (random() % 7 == 0)
                    inputs.push_back(
                        {members[player], next + 5, 1, 0}
                    );
            }
            future.push_back(inputs);
            expected.push_back(live.step(members, std::move(inputs)));
        }
        std::promise<std::vector<TickResult>> completed;
        auto result = completed.get_future();
        std::thread owner(
            [&, committed, future, config]
            {
                try
                {
                    auto restored = RoomActor::restore(
                        committed,
                        config
                    );
                    if (!restored ||
                        (*restored)->snapshot() != committed)
                        throw std::runtime_error(
                            "restore did not retain the committed "
                            "state"
                        );
                    std::vector<TickResult> actual;
                    for (const auto& inputs : future)
                        actual.push_back(
                            (*restored)->step(members, inputs)
                        );
                    const auto replayed = RoomActor::replay(
                        (*restored)->replayLog()
                    );
                    if (!replayed ||
                        replayed->snapshot != (*restored)->snapshot())
                        throw std::runtime_error(
                            "continued recovery log is not "
                            "self-contained"
                        );
                    completed.set_value(std::move(actual));
                }
                catch (...)
                {
                    completed.set_exception(std::current_exception());
                }
            }
        );
        owner.join();
        const auto actual = result.get();
        ASSERT_EQ(actual.size(), expected.size());
        for (std::size_t tick = 0; tick < actual.size(); ++tick)
        {
            EXPECT_EQ(actual[tick].snapshot, expected[tick].snapshot)
                << match << ":" << tick;
            EXPECT_EQ(actual[tick].results, expected[tick].results)
                << match << ":" << tick;
        }
    }
}

TEST(
    RoomRecoveryTest,
    DuplicateAcceptedInputAfterRecoveryCannotMoveTwice
)
{
    RoomActor live(RoomId{1}, 10);
    const auto members = roster();
    const Input input{members.front(), 1, 1, -1};
    live.step(members, {input});
    const auto committed = live.snapshot();
    auto restored = RoomActor::restore(committed);
    ASSERT_TRUE(restored);
    EXPECT_EQ((*restored)->replayLog().checkpoint, committed);
    EXPECT_TRUE((*restored)->replayLog().ticks.empty());
    const auto retried = (*restored)->step(members, {input});
    ASSERT_EQ(retried.results.size(), 1U);
    EXPECT_EQ(retried.results.front(), InputStatus::Duplicate);
    EXPECT_EQ(retried.snapshot.players, committed.players);
    EXPECT_EQ(retried.snapshot.tick, committed.tick + 1);
    EXPECT_EQ(retried.snapshot.incarnation, committed.incarnation);
}

TEST(
    RoomRecoveryTest,
    CorruptCheckpointOrDifferentSimulationPolicyFailsClosed
)
{
    RoomActor live(RoomId{1}, 10);
    live.step(roster(), {{roster().front(), 1, 1, 0}});
    const auto committed = live.snapshot();
    auto invalid = committed;
    ++invalid.checksum;
    auto restored = RoomActor::restore(invalid);
    ASSERT_FALSE(restored);
    EXPECT_EQ(restored.error().code, ReplayError::ChecksumMismatch);
    EXPECT_EQ(restored.error().tick, committed.tick);
    invalid = committed;
    invalid.players.front().lastDx = 2;
    restored = RoomActor::restore(invalid);
    ASSERT_FALSE(restored);
    EXPECT_EQ(restored.error().code, ReplayError::InvalidLog);
    invalid = committed;
    invalid.incarnation = 0;
    EXPECT_FALSE(RoomActor::restore(invalid));
    ActorConfig changed;
    --changed.positionLimit;
    EXPECT_FALSE(RoomActor::restore(committed, changed));
    EXPECT_EQ(live.snapshot(), committed);
}
} // namespace