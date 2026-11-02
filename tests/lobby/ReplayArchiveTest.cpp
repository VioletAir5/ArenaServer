#include "lobby/ReplayArchive.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "lobby/AuthorityCodec.h"

namespace
{
using namespace service::lobby;
using namespace service::room;
using namespace std::chrono_literals;

std::string key(const ReplayLog& log)
{
    return "replay/" + std::to_string(log.checkpoint.roomId.value()) +
           "/" + std::to_string(log.checkpoint.incarnation) + "/" +
           std::to_string(log.ticks.front().tick);
}
std::string encoded(const ReplayLog& log)
{
    auto result = encodeReplayLog(log);
    if (!result)
        throw std::runtime_error("fixture encoding failed");
    return std::move(*result);
}
class ReplayArchiveTest : public testing::Test
{
protected:
    boost::asio::io_context context;
    std::filesystem::path directory;
    ActorConfig config;
    Member alice{PlayerId{1}, SessionId{11}, 1},
        bob{PlayerId{2}, SessionId{12}, 1};
    std::vector<Member> members{alice, bob};
    ReplayLog first{{}, {RoomId{7}, 9}, {}},
        second{{}, {RoomId{7}, 9}, {}};
    Snapshot final{RoomId{7}, 9};
    std::vector<std::uint64_t> checksums;

    void SetUp() override
    {
        std::array<char, 64> pattern{};
        const std::string name = "/tmp/arena-replay-export-XXXXXX";
        std::copy(name.begin(), name.end(), pattern.begin());
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error("mkdtemp failed");
        directory = pattern.data();
        config.historyTicks = 8;
        RoomActor original(RoomId{7}, 9, config);
        checksums.push_back(original.step(members, {{alice, 1, 1, 0}})
                                .snapshot.checksum);
        checksums.push_back(
            original
                .step(members, {{alice, 1, 1, 0}, {alice, 1, -1, 0}})
                .snapshot.checksum
        );
        first = original.replayLog();
        auto recovered = RoomActor::restore(
            original.snapshot(),
            config
        );
        if (!recovered)
            throw std::runtime_error("fixture actor restore failed");
        checksums.push_back((*recovered)
                                ->step(members, {{alice, 2, 0, 1}})
                                .snapshot.checksum);
        checksums.push_back((*recovered)
                                ->step(members, {{bob, 1, 1, 1}})
                                .snapshot.checksum);
        second = (*recovered)->replayLog();
        final = (*recovered)->snapshot();
    }
    void TearDown() override
    {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    DurableStateStore::Snapshot image()
    {
        return {
            2,
            {{key(first), encoded(first)},
             {key(second), encoded(second)}}
        };
    }
    bool until(const std::function<bool()>& predicate)
    {
        const auto end = std::chrono::steady_clock::now() + 3s;
        while (std::chrono::steady_clock::now() < end)
        {
            context.restart();
            context.run_for(2ms);
            if (predicate())
                return true;
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    void append(DurableStateStore& store, const ReplayLog& log)
    {
        auto completion = std::make_shared<
            std::optional<DurableStateStore::Result>>();
        if (!store.commit(
                store.snapshot()->revision,
                {{key(log), encoded(log)}},
                [completion](auto result)
                { completion->emplace(std::move(result)); }
            ) ||
            !until([&] { return completion->has_value(); }) ||
            !**completion)
            throw std::runtime_error("real journal commit failed");
    }
    void stop(DurableStateStore& store)
    {
        store.stop();
        if (!until([&] { return store.stats().closed; }))
            throw std::runtime_error("real journal did not close");
    }
    void invalid(
        const DurableStateStore::Snapshot& snapshot,
        ReplayArchiveErrorCode expected
    )
    {
        auto result = assembleReplayArchives(snapshot);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, expected);
    }
};

TEST_F(
    ReplayArchiveTest,
    RealJournalRestartAndExistingReplayFilePreserveEveryChecksumAndAttempt
)
{
    DurableStateStore::Config journal;
    journal.directory = directory / "state";
    {
        DurableStateStore store(context.get_executor(), journal);
        append(store, first);
        stop(store);
    }
    {
        DurableStateStore reopened(context.get_executor(), journal);
        EXPECT_EQ(reopened.snapshot()->revision, 1u);
        append(reopened, second);
        stop(reopened);
    }
    {
        DurableStateStore reopened(context.get_executor(), journal);
        auto archives = assembleReplayArchives(*reopened.snapshot());
        ASSERT_TRUE(archives);
        ASSERT_EQ(archives->size(), 1u);
        const auto& archive = archives->front();
        EXPECT_EQ(archive.log.checkpoint, first.checkpoint);
        EXPECT_EQ(archive.finalSnapshot, final);
        ASSERT_EQ(archive.log.ticks.size(), 4u);
        EXPECT_EQ(archive.log.ticks[1].inputs, first.ticks[1].inputs);
        EXPECT_EQ(
            archive.log.ticks[2].inputs,
            second.ticks[0].inputs
        );
        auto replayed = RoomActor::replay(archive.log);
        ASSERT_TRUE(replayed);
        EXPECT_EQ(replayed->snapshot, final);
        EXPECT_EQ(replayed->checksums, checksums);
        const auto destination = directory / "complete.arena";
        ASSERT_TRUE(writeReplayFile(destination, archive.log));
        EXPECT_EQ(
            std::filesystem::file_size(destination),
            archive.fileBytes
        );
        auto loaded = readReplayFile(destination);
        ASSERT_TRUE(loaded);
        auto offline = RoomActor::replay(*loaded);
        ASSERT_TRUE(offline);
        EXPECT_EQ(offline->snapshot, final);
        EXPECT_EQ(offline->checksums, checksums);
        stop(reopened);
        EXPECT_TRUE(reopened.stats().writerStopped);
        EXPECT_EQ(reopened.stats().pending, 0u);
    }
}

TEST_F(
    ReplayArchiveTest,
    MissingPrefixGapAndOverlapCannotSilentlyDropOrderedInputs
)
{
    auto missing = image();
    missing.values.erase(key(first));
    invalid(missing, ReplayArchiveErrorCode::Invalid);
    auto afterThird = RoomActor::restore(
        first.ticks.size() ? RoomActor::replay(first)->snapshot
                           : first.checkpoint,
        config
    );
    ASSERT_TRUE(afterThird);
    (*afterThird)->step(members, {{alice, 2, 0, 1}});
    auto atThird = RoomActor::restore(
        (*afterThird)->snapshot(),
        config
    );
    ASSERT_TRUE(atThird);
    (*atThird)->step(members, {{bob, 1, 1, 1}});
    const auto gapLog = (*atThird)->replayLog();
    auto gap = image();
    gap.values.erase(key(second));
    gap.values.emplace(key(gapLog), encoded(gapLog));
    invalid(gap, ReplayArchiveErrorCode::Invalid);

    RoomActor one(RoomId{7}, 9, config);
    one.step(members, {{alice, 1, 1, 0}});
    auto overlapActor = RoomActor::restore(one.snapshot(), config);
    ASSERT_TRUE(overlapActor);
    (*overlapActor)->step(members, {});
    (*overlapActor)->step(members, {{alice, 2, 0, 1}});
    const auto overlapLog = (*overlapActor)->replayLog();
    auto overlap = image();
    overlap.values.erase(key(second));
    overlap.values.emplace(key(overlapLog), encoded(overlapLog));
    invalid(overlap, ReplayArchiveErrorCode::Invalid);
}

TEST_F(ReplayArchiveTest, AllCheckpointFieldsAndActorPolicyMustAgree)
{
    auto wrong = second;
    auto checkpoint = wrong.checkpoint;
    ++checkpoint.players.front().x;
    checkpoint.checksum = RoomActor::checksum(checkpoint, config);
    auto actor = RoomActor::restore(checkpoint, config);
    ASSERT_TRUE(actor);
    (*actor)->step(members, {{alice, 2, 0, 1}});
    const auto divergent = (*actor)->replayLog();
    ASSERT_TRUE(RoomActor::replay(divergent));
    auto discontinuity = image();
    discontinuity.values[key(second)] = encoded(divergent);
    invalid(discontinuity, ReplayArchiveErrorCode::Invalid);
    wrong = second;
    ++wrong.config.historyTicks;
    auto policy = image();
    policy.values[key(second)] = encoded(wrong);
    invalid(policy, ReplayArchiveErrorCode::Invalid);
}

TEST_F(
    ReplayArchiveTest,
    CanonicalKeysAndGlobalIncarnationOwnershipAreStrict
)
{
    auto malformed = image();
    const auto value = malformed.values.extract(key(first)).mapped();
    malformed.values.emplace("replay/07/9/1", value);
    invalid(malformed, ReplayArchiveErrorCode::Invalid);
    auto mismatch = image();
    mismatch.values.erase(key(first));
    mismatch.values.emplace("replay/7/9/2", encoded(first));
    invalid(mismatch, ReplayArchiveErrorCode::Invalid);
    RoomActor other(RoomId{8}, 9, config);
    other.step(members, {});
    const auto collision = other.replayLog();
    auto reused = image();
    reused.values.emplace(key(collision), encoded(collision));
    invalid(reused, ReplayArchiveErrorCode::Invalid);
}

TEST_F(
    ReplayArchiveTest,
    CorruptBytesAndAValidlyEncodedWrongChecksumFailClosed
)
{
    auto corrupted = image();
    corrupted.values[key(second)] = "not-a-valid-segment";
    invalid(corrupted, ReplayArchiveErrorCode::Invalid);
    auto wrong = second;
    wrong.ticks.front().checksum ^= 1;
    auto desync = image();
    desync.values[key(second)] = encoded(wrong);
    auto result = assembleReplayArchives(desync);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReplayArchiveErrorCode::Desync);
    EXPECT_EQ(result.error().tick, second.ticks.front().tick);
}

TEST_F(
    ReplayArchiveTest,
    AggregateTicksBytesPoliciesAndCodecPayloadsAreBounded
)
{
    const auto snapshot = image();
    auto normal = assembleReplayArchives(snapshot);
    ASSERT_TRUE(normal);
    ReplayFileLimits limits;
    limits.maxTicks = 3;
    auto ticks = assembleReplayArchives(snapshot, limits);
    ASSERT_FALSE(ticks);
    EXPECT_EQ(ticks.error().code, ReplayArchiveErrorCode::Capacity);
    limits = {};
    limits.maxFileBytes = normal->front().fileBytes - 1;
    EXPECT_FALSE(assembleReplayArchives(snapshot, limits));
    limits.maxFileBytes = normal->front().fileBytes;
    EXPECT_TRUE(assembleReplayArchives(snapshot, limits));
    limits = {};
    limits.maxMembers = 1;
    EXPECT_FALSE(assembleReplayArchives(snapshot, limits));
    auto oversized = snapshot;
    oversized.values[key(second)].assign(
        authorityCodecMaxBytes + 1,
        'x'
    );
    invalid(oversized, ReplayArchiveErrorCode::Capacity);
}

TEST_F(
    ReplayArchiveTest,
    NumericOrderingPreservesTicksBeyondSingleDigitKeys
)
{
    DurableStateStore::Snapshot snapshot;
    Snapshot current{RoomId{7}, 9};
    {
        RoomActor initial(RoomId{7}, 9, config);
        current = initial.snapshot();
    }
    for (std::uint64_t tick = 1; tick <= 12; ++tick)
    {
        auto actor = RoomActor::restore(current, config);
        ASSERT_TRUE(actor);
        current = (*actor)
                      ->step(members, {{alice, tick, 1, 0}})
                      .snapshot;
        const auto segment = (*actor)->replayLog();
        snapshot.values.emplace(key(segment), encoded(segment));
    }
    auto assembled = assembleReplayArchives(snapshot);
    ASSERT_TRUE(assembled);
    ASSERT_EQ(assembled->size(), 1u);
    EXPECT_EQ(assembled->front().finalSnapshot, current);
    ASSERT_EQ(assembled->front().log.ticks.size(), 12u);
    for (std::size_t index = 0; index < 12; ++index)
        EXPECT_EQ(
            assembled->front().log.ticks[index].tick,
            index + 1
        );
}

TEST_F(
    ReplayArchiveTest,
    DifferentIncarnationsProduceSeparateCompleteArchives
)
{
    auto snapshot = image();
    RoomActor replacement(RoomId{7}, 10, config);
    replacement.step(members, {});
    const auto replacementLog = replacement.replayLog();
    snapshot.values.emplace(
        key(replacementLog),
        encoded(replacementLog)
    );
    auto archives = assembleReplayArchives(snapshot);
    ASSERT_TRUE(archives);
    ASSERT_EQ(archives->size(), 2u);
    EXPECT_EQ(archives->at(0).log.checkpoint.incarnation, 9u);
    EXPECT_EQ(archives->at(1).log.checkpoint.incarnation, 10u);
    EXPECT_EQ(archives->at(1).finalSnapshot, replacement.snapshot());
}
TEST(
    ReplayArchiveBoundsTest,
    CallerKeyBudgetIncludesNonReplayRecordsWithoutWeakeningCodecLimits
)
{
    DurableStateStore::Snapshot values;
    for (std::size_t index = 0; index <= authorityCodecMaxItems;
         ++index)
        values.values.emplace(
            "metadata/" + std::to_string(index),
            ""
        );
    auto defaults = assembleReplayArchives(values);
    ASSERT_FALSE(defaults);
    EXPECT_EQ(
        defaults.error().code,
        ReplayArchiveErrorCode::Capacity
    );
    auto admitted = assembleReplayArchives(
        values,
        {},
        values.values.size()
    );
    ASSERT_TRUE(admitted);
    EXPECT_TRUE(admitted->empty());
    for (const std::size_t maximum :
         {std::size_t{0}, std::size_t{1000001}})
    {
        auto invalid = assembleReplayArchives(
            DurableStateStore::Snapshot{},
            {},
            maximum
        );
        ASSERT_FALSE(invalid);
        EXPECT_EQ(
            invalid.error().code,
            ReplayArchiveErrorCode::Invalid
        );
    }
}

} // namespace
