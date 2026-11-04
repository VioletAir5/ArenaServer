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
#include "lobby_state.pb.h"
#include "room/RoomRuntime.h"
#include "room/RoomWire.h"

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
    AtomicJournal::Snapshot image()
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
    void append(AtomicJournal& store, const ReplayLog& log)
    {
        auto completion = std::make_shared<
            std::optional<AtomicJournal::Result>>();
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
    void stop(AtomicJournal& store)
    {
        store.stop();
        if (!until([&] { return store.stats().closed; }))
            throw std::runtime_error("real journal did not close");
    }
    void invalid(
        const AtomicJournal::Snapshot& snapshot,
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
    AtomicJournal::Config journal;
    journal.directory = directory / "state";
    {
        AtomicJournal store(context.get_executor(), journal);
        append(store, first);
        stop(store);
    }
    {
        AtomicJournal reopened(context.get_executor(), journal);
        EXPECT_EQ(reopened.snapshot()->revision, 1u);
        append(reopened, second);
        stop(reopened);
    }
    {
        AtomicJournal reopened(context.get_executor(), journal);
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
    AtomicJournal::Snapshot snapshot;
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
    AtomicJournal::Snapshot values;
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
            AtomicJournal::Snapshot{},
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

namespace roomWire = service::room::wire;
class DirectoryReplayArchiveTest : public ReplayArchiveTest
{
protected:
    std::optional<RuntimeImage> initial, boundary, current;
    RuntimeAuthorization authorization(std::uint64_t player)
    {
        return {
            player == 1 ? alice : bob,
            player + 20,
            player + 1000,
            true
        };
    }
    RuntimeCommand control(
        std::uint64_t player, std::uint64_t sequence, int dx, int dy
    )
    {
        RuntimeCommand value;
        value.kind = RuntimeCommandKind::Input;
        value.caller = authorization(player);
        value.incarnation = 9;
        value.sequence = sequence;
        value.dx = dx;
        value.dy = dy;
        return value;
    }
    TickRecord step(std::vector<RuntimeCommand> commands)
    {
        const RuntimeCutoff cutoff{
            {authorization(1), authorization(2)},
            {authorization(1), authorization(2)},
            std::move(commands),
            static_cast<std::int64_t>(
                (current->logicalCutoff + 1) * 50'000'000
            ),
            100 + current->logicalCutoff * 50
        };
        auto proposal = RoomRuntime::step(*current, cutoff);
        if (!proposal || !proposal->replay)
            throw std::runtime_error(
                "real archive runtime cutoff rejected"
            );
        current = std::move(proposal->image);
        checksums.push_back(current->actor.checksum);
        return std::move(*proposal->replay);
    }
    void SetUp() override
    {
        ReplayArchiveTest::SetUp();
        checksums.clear();
        RuntimePolicy policy;
        policy.actor = config;
        auto created = RoomRuntime::create(RoomId{7}, 9, policy);
        if (!created)
            throw std::runtime_error(
                "real archive runtime creation rejected"
            );
        initial = *created;
        current = *created;
        first = {config, current->actor, {}};
        first.ticks.push_back(step({control(1, 1, 1, 0)}));
        first.ticks.push_back(
            step({control(1, 1, 1, 0), control(1, 1, -1, 0)})
        );
        boundary = *current;
        // Real restart reconstruction precedes the next physical
        // segment.
        const auto bytes = roomWire::encodeImage(*current);
        if (!bytes)
            throw std::runtime_error(
                "real archive image encoding rejected"
            );
        const auto restored = roomWire::decodeImage(*bytes);
        if (!restored || !RoomRuntime::validate(*restored))
            throw std::runtime_error(
                "real archive image restore rejected"
            );
        current = *restored;
        second = {config, current->actor, {}};
        second.ticks.push_back(step({control(1, 2, 0, 1)}));
        second.ticks.push_back(step({control(2, 1, 1, 1)}));
        final = current->actor;
    }
    std::string directoryKey(const ReplayLog& log)
    {
        return "room-directory/" + key(log);
    }
    std::string wrapper(const ReplayLog& log)
    {
        const auto payload = roomWire::encodeReplayLog(log);
        if (!payload)
            throw std::runtime_error(
                "worker replay log encoding rejected"
            );
        arena::lobby_state::DirectoryReplay value;
        value.set_version(2);
        value.set_room_id(log.checkpoint.roomId.value());
        value.set_incarnation(log.checkpoint.incarnation);
        value.set_first_tick(log.ticks.front().tick);
        value.set_last_tick(log.ticks.back().tick);
        value.set_payload(*payload);
        return value.SerializeAsString();
    }
    std::string metadata()
    {
        arena::lobby_state::DirectoryMeta value;
        value.set_version(2);
        value.set_boot_id("same-boot");
        return value.SerializeAsString();
    }
    std::string route(const RuntimeImage& image, bool retired = false)
    {
        const auto checkpoint = roomWire::encodeImage(image);
        if (!checkpoint)
            throw std::runtime_error(
                "worker archive anchor encoding rejected"
            );
        arena::lobby_state::DirectoryRoom value;
        value.set_version(2);
        value.set_room_id(image.actor.roomId.value());
        value.set_incarnation(image.actor.incarnation);
        value.set_owner("room-a");
        value.set_owner_epoch(1);
        value.set_next_sequence(7);
        value.set_admitted_tick(image.logicalCutoff);
        value.set_lifecycle(retired ? 2 : 0);
        auto* imageValue = value.mutable_image();
        imageValue->set_tick(image.actor.tick);
        imageValue->set_checksum(image.actor.checksum);
        imageValue->set_cursor(6);
        imageValue->set_checkpoint(*checkpoint);
        imageValue->set_logical_tick(image.logicalCutoff);
        return value.SerializeAsString();
    }
    AtomicJournal::Snapshot directoryImage()
    {
        return {
            2,
            {{"room-directory/meta", metadata()},
             {"room-directory/room/7/9", route(*current, true)},
             {directoryKey(first), wrapper(first)},
             {directoryKey(second), wrapper(second)}}
        };
    }
    void write(
        AtomicJournal& store,
        std::vector<AtomicJournal::Mutation> mutations
    )
    {
        auto completion = std::make_shared<
            std::optional<AtomicJournal::Result>>();
        if (!store.commit(
                store.snapshot()->revision,
                std::move(mutations),
                [completion](auto result)
                { *completion = std::move(result); }
            ) ||
            !until([&] { return completion->has_value(); }) ||
            !**completion)
            throw std::runtime_error(
                "directory archive journal commit failed"
            );
    }
};

TEST_F(
    DirectoryReplayArchiveTest,
    RealRuntimeSegmentsRestartRetireAndExportEveryChecksum
)
{
    AtomicJournal::Config journal;
    journal.directory = directory / "state";
    {
        AtomicJournal store(context.get_executor(), journal);
        write(
            store,
            {{"room-directory/meta", metadata()},
             {"room-directory/room/7/9", route(*boundary)},
             {directoryKey(first), wrapper(first)}}
        );
        stop(store);
    }
    {
        AtomicJournal store(context.get_executor(), journal);
        ASSERT_TRUE(inspectReplayArchives(*store.snapshot()));
        write(
            store,
            {{"room-directory/room/7/9", route(*current, true)},
             {directoryKey(second), wrapper(second)}}
        );
        stop(store);
    }
    {
        AtomicJournal store(context.get_executor(), journal);
        const auto inspected = inspectReplayArchives(
            *store.snapshot()
        );
        ASSERT_TRUE(inspected);
        ASSERT_EQ(inspected->size(), 1u);
        EXPECT_EQ(inspected->front().lastTick, final.tick);
        EXPECT_EQ(inspected->front().checksum, final.checksum);
        const auto archives = assembleReplayArchives(
            *store.snapshot()
        );
        ASSERT_TRUE(archives);
        ASSERT_EQ(archives->size(), 1u);
        EXPECT_EQ(archives->front().finalSnapshot, final);
        EXPECT_EQ(
            archives->front().log.ticks[1].inputs,
            first.ticks[1].inputs
        );
        const auto path = directory / "directory.arena";
        ASSERT_TRUE(writeReplayFile(path, archives->front().log));
        const auto decoded = readReplayFile(path);
        ASSERT_TRUE(decoded);
        const auto replayed = RoomActor::replay(*decoded);
        ASSERT_TRUE(replayed);
        EXPECT_EQ(replayed->snapshot, final);
        EXPECT_EQ(replayed->checksums, checksums);
        stop(store);
        EXPECT_TRUE(store.stats().writerStopped);
        EXPECT_EQ(store.stats().pending, 0u);
    }
}

TEST_F(
    DirectoryReplayArchiveTest,
    LegacyHistoryContinuesExactlyOnceIntoDirectoryHistory
)
{
    auto snapshot = directoryImage();
    snapshot.values.erase(directoryKey(first));
    snapshot.values.emplace(key(first), encoded(first));
    ASSERT_TRUE(inspectReplayArchives(snapshot));
    const auto complete = assembleReplayArchives(snapshot);
    ASSERT_TRUE(complete);
    EXPECT_EQ(complete->front().finalSnapshot, final);
    EXPECT_EQ(complete->front().log.ticks.size(), 4u);
    auto atBoundary = snapshot;
    atBoundary.values.erase(directoryKey(second));
    atBoundary.values["room-directory/room/7/9"] = route(*boundary);
    EXPECT_TRUE(inspectReplayArchives(atBoundary));
    EXPECT_TRUE(assembleReplayArchives(atBoundary));
    auto overlap = snapshot;
    overlap.values.emplace(directoryKey(first), wrapper(first));
    invalid(overlap, ReplayArchiveErrorCode::Invalid);
    auto missing = snapshot;
    missing.values.erase(key(first));
    invalid(missing, ReplayArchiveErrorCode::Invalid);
    auto backwards = directoryImage();
    backwards.values.erase(directoryKey(second));
    backwards.values.emplace(key(second), encoded(second));
    invalid(backwards, ReplayArchiveErrorCode::Invalid);
}

TEST_F(
    DirectoryReplayArchiveTest,
    OnlineInspectionDoesNotEvaluatePhysicsButOfflineReportsDesyncTick
)
{
    auto snapshot = directoryImage();
    auto corrupt = second;
    corrupt.ticks.front().checksum ^= 1;
    snapshot.values[directoryKey(second)] = wrapper(corrupt);
    const auto inspection = inspectReplayArchives(snapshot);
    ASSERT_TRUE(inspection);
    EXPECT_EQ(inspection->front().lastTick, final.tick);
    const auto offline = assembleReplayArchives(snapshot);
    ASSERT_FALSE(offline);
    EXPECT_EQ(offline.error().code, ReplayArchiveErrorCode::Desync);
    EXPECT_EQ(offline.error().tick, second.ticks.front().tick);

    snapshot = directoryImage();
    auto wrong = *current;
    ++wrong.actor.players.front().x;
    // Metadata still links; full worker-state identity is checked
    // offline.
    snapshot.values["room-directory/room/7/9"] = route(wrong, true);
    EXPECT_TRUE(inspectReplayArchives(snapshot));
    const auto wrongTail = assembleReplayArchives(snapshot);
    ASSERT_FALSE(wrongTail);
    EXPECT_EQ(
        wrongTail.error().code,
        ReplayArchiveErrorCode::Invalid
    );
}

TEST_F(
    DirectoryReplayArchiveTest,
    CanonicalWrappersAnchorsAndTerminalMetadataAreRequired
)
{
    const auto original = directoryImage();
    auto wrong = original;
    wrong.values.erase("room-directory/room/7/9");
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    wrong = original;
    wrong.values.erase("room-directory/meta");
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    wrong = original;
    wrong.values.emplace("room-directory/unknown", "opaque");
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    wrong = original;
    const auto payload = wrong.values.extract(directoryKey(first))
                             .mapped();
    wrong.values.emplace("room-directory/replay/07/9/1", payload);
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    arena::lobby_state::DirectoryReplay record;
    ASSERT_TRUE(record.ParseFromString(
        original.values.at(directoryKey(second))
    ));
    record.set_last_tick(record.last_tick() + 1);
    wrong = original;
    wrong.values[directoryKey(second)] = record.SerializeAsString();
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    ASSERT_TRUE(record.ParseFromString(
        original.values.at(directoryKey(second))
    ));
    record.GetReflection()->MutableUnknownFields(&record)->AddVarint(
        99,
        1
    );
    wrong.values[directoryKey(second)] = record.SerializeAsString();
    invalid(wrong, ReplayArchiveErrorCode::Invalid);
    auto image = *current;
    image.actor.checksum ^= 1;
    wrong = original;
    wrong.values["room-directory/room/7/9"] = route(image, true);
    EXPECT_FALSE(inspectReplayArchives(wrong));
    ReplayFileLimits small;
    small.maxTicks = 3;
    EXPECT_FALSE(inspectReplayArchives(original, small));
    EXPECT_FALSE(assembleReplayArchives(original, small));
}

TEST_F(
    DirectoryReplayArchiveTest,
    TickZeroExportsEmptyHistoryAndNonzeroTailCannotLoseHistory
)
{
    AtomicJournal::Snapshot zero{
        1,
        {{"room-directory/meta", metadata()},
         {"room-directory/room/7/9", route(*initial, true)}}
    };
    const auto inspected = inspectReplayArchives(zero);
    ASSERT_TRUE(inspected);
    ASSERT_EQ(inspected->size(), 1u);
    EXPECT_EQ(inspected->front().firstTick, 0u);
    EXPECT_EQ(inspected->front().lastTick, 0u);
    const auto archives = assembleReplayArchives(zero);
    ASSERT_TRUE(archives);
    ASSERT_EQ(archives->size(), 1u);
    EXPECT_TRUE(archives->front().log.ticks.empty());
    const auto path = directory / "empty.arena";
    ASSERT_TRUE(writeReplayFile(path, archives->front().log));
    const auto file = readReplayFile(path);
    ASSERT_TRUE(file);
    EXPECT_TRUE(RoomActor::replay(*file));
    zero.values["room-directory/room/7/9"] = route(*current, true);
    EXPECT_FALSE(inspectReplayArchives(zero));
    invalid(zero, ReplayArchiveErrorCode::Invalid);
}
} // namespace
