#include "lobby/LegacyRoomImport.h"

#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>

#include <gtest/gtest.h>

namespace
{
using namespace service::lobby;
using namespace std::chrono_literals;
namespace runtime = service::room;
using Clock = arena::runtime::OwnerClock;
using arena::domain::PlayerId;

struct World
{
    Clock::time_point now{};
    bool forbidden{false};
    std::uint64_t epoch{0}, incarnation{0};
    RoomServiceConfig config;
    std::shared_ptr<OnlineRegistry> registry;
    std::unique_ptr<RoomService> rooms;
    SessionGrant first, second;
    ManagedRoom allocation;

    explicit World(std::uint64_t duration = 20)
        : first(makeGrant(1))
        , second(makeGrant(2))
        , allocation(makeAllocation())
    {
        config.externalTicks = true;
        config.externalSettlement = true;
        config.battleDurationTicks = duration;
        config.now = [this]
        {
            check();
            return now;
        };
        config.wallNow = [this]
        {
            check();
            return std::chrono::system_clock::time_point{
                       1700000000s
                   } +
                   now.time_since_epoch();
        };
        config.incarnationSource = [this]
        {
            check();
            return ++incarnation;
        };
        rooms = std::make_unique<RoomService>(registry, config);
        rooms->start({});
        if (!rooms->openMatch(allocation))
            throw std::logic_error("legacy fixture allocation");
    }
    void check() const
    {
        if (forbidden)
            throw std::logic_error("legacy runtime binding invoked");
    }
    SessionGrant makeGrant(std::uint64_t player)
    {
        if (!registry)
        {
            OnlineRegistryConfig cfg;
            cfg.now = [this]
            {
                check();
                return now;
            };
            registry = std::make_shared<OnlineRegistry>(cfg);
        }
        auto grant = registry->installVerified(
            {"fixture", {PlayerId{player}, ++epoch, 1700003600}},
            player + 100,
            {},
            [] { return std::int64_t{1700000000}; },
            [] {}
        );
        if (!grant)
            throw std::logic_error("legacy fixture login");
        return std::move(*grant);
    }
    ManagedRoom makeAllocation()
    {
        auto value = registry->allocateMatch(
            7,
            {{{first.snapshot.playerId,
               first.snapshot.sessionId,
               1400},
              {second.snapshot.playerId,
               second.snapshot.sessionId,
               1800}}}
        );
        if (!value)
            throw std::logic_error("legacy fixture match");
        return *value;
    }
    void tick()
    {
        now += 50ms;
        if (!rooms->advance())
            throw std::logic_error("legacy fixture cutoff");
    }
    RoomAggregate aggregate()
    {
        auto value = rooms->exportRoom(allocation.roomId);
        if (!value)
            throw std::logic_error("legacy aggregate export");
        return *value;
    }
    RoomCheckpoint checkpoint()
    {
        auto value = rooms->checkpoint();
        if (!value)
            throw std::logic_error("legacy checkpoint export");
        return *value;
    }
    void play()
    {
        const auto actor = aggregate().actor;
        for (std::size_t i = 0; i != 2; ++i)
        {
            const auto& grant = i ? second : first;
            if (!rooms->ready(
                    grant.lease->binding,
                    grant.snapshot.sessionId,
                    allocation.roomId,
                    actor.incarnation,
                    allocation.members[i].roomEpoch,
                    true,
                    [](auto) {}
                ))
                throw std::logic_error("legacy fixture ready");
        }
        tick();
        tick();
    }
    void input()
    {
        const auto actor = aggregate().actor;
        if (!rooms->input(
                first.lease->binding,
                first.snapshot.sessionId,
                allocation.roomId,
                actor.incarnation,
                allocation.members[0].roomEpoch,
                1,
                1,
                -1,
                [](auto) {}
            ))
            throw std::logic_error("legacy fixture input");
        tick();
    }
};
auto counterValues(const RoomMetrics& c)
{
    return std::tie(
        c.ticks,
        c.applied,
        c.rejected,
        c.duplicates,
        c.skippedWallSlots,
        c.battlesStarted,
        c.battlesCompleted,
        c.battlesAborted,
        c.persistenceSubmitted,
        c.persistenceRejected,
        c.persistenceFailed,
        c.persistenceCommitted
    );
}
void clearGauges(RoomMetrics& c)
{
    c.actors = c.managedMatches = c.retainedResults = 0;
    c.pendingSettlements = c.blockedSettlements = 0;
}
runtime::RuntimeCutoff cutoff(const World& w)
{
    runtime::RuntimeCutoff cut;
    for (std::size_t i = 0; i != 2; ++i)
    {
        const auto& grant = i ? w.second : w.first;
        const auto& member = w.allocation.members[i];
        cut.roster.push_back(
            {{member.playerId, member.sessionId, member.roomEpoch},
             grant.snapshot.epoch,
             grant.snapshot.connectionId,
             true}
        );
    }
    cut.currentAuthorizations = cut.roster;
    cut.nowNanos = w.now.time_since_epoch().count() + 50'000'000;
    cut.wallMillis = 1700000000200;
    return cut;
}

TEST(
    LegacyRoomImportTest,
    PlayingValuesCrossTheBoundaryExactlyWithoutBindings
)
{
    World world;
    world.play();
    world.input();
    const auto saved = world.checkpoint();
    world.forbidden = true;
    const auto imported = splitLegacyRooms(saved);
    ASSERT_TRUE(imported);
    ASSERT_EQ(imported->images.size(), 1u);
    const auto& image = imported->images[0];
    EXPECT_EQ(image.actor, saved.actors[0]);
    EXPECT_EQ(image.logicalCutoff, image.actor.tick);
    EXPECT_EQ(
        image.policy.actor.maxInputsPerTick,
        saved.policy.actor.maxInputsPerTick
    );
    EXPECT_EQ(
        image.policy.maxCommands,
        saved.policy.maxPendingPerRoom
    );
    EXPECT_EQ(
        image.policy.resultRetentionNanos,
        saved.policy.resultRetention.count() * 1'000'000
    );
    ASSERT_TRUE(image.managed);
    EXPECT_EQ(
        image.managed->allocation.matchId,
        saved.matches[0].allocation.matchId
    );
    EXPECT_EQ(image.managed->allocation.initialMmr[0], 1400u);
    EXPECT_EQ(image.managed->allocation.initialMmr[1], 1800u);
    EXPECT_EQ(image.managed->room.state, saved.matches[0].room.state);
    EXPECT_EQ(
        image.managed->room.revision,
        saved.matches[0].room.revision
    );
    EXPECT_EQ(
        image.managed->battle.revision,
        saved.matches[0].battle.revision
    );
    EXPECT_EQ(image.managed->ready, saved.matches[0].ready);
    EXPECT_EQ(
        image.managed->playedTicks,
        saved.matches[0].playedTicks
    );
    EXPECT_EQ(image.actor.players[0].sequence, 1u);
    EXPECT_EQ(image.actor.players[0].lastDy, -1);
    EXPECT_EQ(
        image.knownSinceTick,
        saved.roomCounters[0].knownSinceTick
    );
    EXPECT_EQ(
        image.completeCounterHistory,
        saved.roomCounters[0].completeHistory
    );
    EXPECT_EQ(
        imported->incarnationHighWater,
        saved.incarnationHighWater
    );
    // This is the real worker validation boundary, outside the
    // converter.
    EXPECT_TRUE(runtime::RoomRuntime::validate(image));
    world.forbidden = false;
}

TEST(
    LegacyRoomImportTest,
    FinishedSettlementExpiryAndFrozenKernelRemainExact
)
{
    World world(2);
    world.play();
    world.input();
    ASSERT_EQ(world.aggregate().managed->phase, RoomPhase::Settling);
    ASSERT_TRUE(world.rooms->acknowledgeSettlement(7, true));
    const auto saved = world.checkpoint();
    const auto result = splitLegacyRooms(saved);
    ASSERT_TRUE(result);
    const auto& image = result->images[0];
    EXPECT_TRUE(runtime::RoomRuntime::validate(image));
    const auto& expected = *saved.matches[0].result;
    const auto& actual = *image.managed->result;
    EXPECT_EQ(actual.resultId, expected.resultId);
    EXPECT_EQ(actual.players, expected.players);
    EXPECT_EQ(actual.finalChecksum, expected.finalChecksum);
    EXPECT_EQ(actual.finalTick, expected.finalTick);
    EXPECT_EQ(actual.createdUnixMs, expected.createdUnixMs);
    EXPECT_EQ(actual.startedUnixMs, expected.startedUnixMs);
    EXPECT_EQ(actual.finishedUnixMs, expected.finishedUnixMs);
    EXPECT_EQ(actual.startedTick, expected.startedTick);
    EXPECT_EQ(actual.playedTicks, expected.playedTicks);
    EXPECT_EQ(
        *image.managed->expiresAtNanos,
        saved.matches[0].expires->time_since_epoch().count()
    );
}

TEST(
    LegacyRoomImportTest,
    VersionZeroKeepsUnknownGlobalHistoryOutsideLiveRooms
)
{
    for (const bool terminal : {false, true})
    {
        World world(terminal ? 2 : 20);
        world.play();
        world.input();
        auto saved = world.checkpoint();
        saved.counterVersion = 0;
        saved.roomCounters.clear();
        saved.unattributedCounters = {};
        const auto result = splitLegacyRooms(saved);
        ASSERT_TRUE(result);
        EXPECT_EQ(
            counterValues(result->unattributed),
            counterValues(saved.metrics)
        );
        auto image = result->images[0];
        EXPECT_FALSE(image.completeCounterHistory);
        EXPECT_EQ(image.knownSinceTick, saved.actors[0].tick);
        EXPECT_EQ(image.counters.ticks, 0u);
        EXPECT_EQ(image.counters.battlesStarted, 0u);
        EXPECT_EQ(image.counters.settlementIntents, 0u);
        ASSERT_TRUE(runtime::RoomRuntime::validate(image));
        auto cut = cutoff(world);
        if (terminal)
        {
            runtime::RuntimeCommand command;
            command.kind = runtime::RuntimeCommandKind::SettlementAck;
            command.matchId = 7;
            command.committed = true;
            cut.commands.push_back(command);
        }
        const auto stepped = runtime::RoomRuntime::step(image, cut);
        ASSERT_TRUE(stepped);
        EXPECT_EQ(stepped->image.counters.battlesStarted, 0u);
        EXPECT_FALSE(stepped->image.completeCounterHistory);
        if (terminal)
        {
            EXPECT_TRUE(stepped->releaseMembership);
            EXPECT_EQ(
                stepped->image.counters.settlementsCommitted,
                1u
            );
            EXPECT_EQ(stepped->image.counters.settlementIntents, 0u);
            EXPECT_EQ(stepped->image.counters.ticks, 0u);
        }
        else
            EXPECT_EQ(stepped->image.counters.ticks, 1u);
    }
}

TEST(
    LegacyRoomImportTest,
    ExplicitPartialCoverageDoesNotBecomeInventedHistory
)
{
    World world;
    world.play();
    world.input();
    auto saved = world.checkpoint();
    saved.roomCounters[0].completeHistory = false;
    saved.roomCounters[0].knownSinceTick = saved.actors[0].tick;
    saved.roomCounters[0].counters = {};
    saved.unattributedCounters = saved.metrics;
    clearGauges(saved.unattributedCounters);
    const auto imported = splitLegacyRooms(saved);
    ASSERT_TRUE(imported);
    EXPECT_EQ(
        counterValues(imported->unattributed),
        counterValues(saved.metrics)
    );
    EXPECT_EQ(
        imported->images[0].knownSinceTick,
        saved.actors[0].tick
    );
    EXPECT_TRUE(runtime::RoomRuntime::validate(imported->images[0]));
    auto contradictory = saved;
    contradictory.roomCounters[0].counters.ticks = 1;
    EXPECT_FALSE(splitLegacyRooms(contradictory));
}

TEST(
    LegacyRoomImportTest,
    CallbackTimerArchiveAndPendingJobsAreUnsupported
)
{
    World world(2);
    world.play();
    world.input();
    const auto saved = world.checkpoint();
    for (int kind = 0; kind != 4; ++kind)
    {
        auto bad = saved;
        if (kind == 0)
            bad.policy.callbackSettlement = true;
        if (kind == 1)
            bad.policy.externalTicks = false;
        if (kind == 2)
            bad.policy.archives = true;
        if (kind == 3)
            bad.policy.externalSettlement = false;
        auto imported = splitLegacyRooms(bad);
        ASSERT_FALSE(imported);
        EXPECT_EQ(
            imported.error(),
            LegacyRoomImportError::Unsupported
        );
    }
    auto job = saved;
    job.matches[0].retryAt = world.now + 1ms;
    EXPECT_EQ(
        splitLegacyRooms(job).error(),
        LegacyRoomImportError::Unsupported
    );
    job = saved;
    job.matches[0].persistenceOutcome = 1;
    EXPECT_EQ(
        splitLegacyRooms(job).error(),
        LegacyRoomImportError::Unsupported
    );
}

TEST(
    LegacyRoomImportTest,
    InvalidClocksDuplicateIdentitiesAndTotalsFailClosed
)
{
    World world(2);
    world.play();
    world.input();
    ASSERT_TRUE(world.rooms->acknowledgeSettlement(7, true));
    const auto saved = world.checkpoint();
    auto bad = saved;
    bad.matches[0].expires = Clock::time_point{Clock::duration{-1}};
    EXPECT_EQ(
        splitLegacyRooms(bad).error(),
        LegacyRoomImportError::Invalid
    );
    bad = saved;
    bad.policy.resultRetention = std::chrono::milliseconds{
        std::numeric_limits<std::int64_t>::max()
    };
    EXPECT_EQ(
        splitLegacyRooms(bad).error(),
        LegacyRoomImportError::Invalid
    );
    bad = saved;
    ++bad.metrics.applied;
    EXPECT_EQ(
        splitLegacyRooms(bad).error(),
        LegacyRoomImportError::Invalid
    );
    bad = saved;
    bad.actors.push_back(bad.actors[0]);
    bad.metrics.actors = bad.actors.size();
    EXPECT_FALSE(splitLegacyRooms(bad));
    bad = saved;
    bad.counterVersion = 2;
    EXPECT_EQ(
        splitLegacyRooms(bad).error(),
        LegacyRoomImportError::Unsupported
    );
}

TEST(
    LegacyRoomImportTest,
    AggregateCarriesOnlyActualReplayAndWorkerChecksSemantics
)
{
    World world;
    world.play();
    world.input();
    auto aggregate = world.aggregate();
    const auto imported = importLegacyRoom(aggregate);
    ASSERT_TRUE(imported);
    ASSERT_TRUE(imported->replay);
    EXPECT_EQ(imported->image.actor, aggregate.actor);
    EXPECT_EQ(
        imported->replay->checkpoint,
        aggregate.replay.checkpoint
    );
    ASSERT_EQ(
        imported->replay->ticks.size(),
        aggregate.replay.ticks.size()
    );
    for (std::size_t i = 0; i != aggregate.replay.ticks.size(); ++i)
    {
        EXPECT_EQ(
            imported->replay->ticks[i].tick,
            aggregate.replay.ticks[i].tick
        );
        EXPECT_EQ(
            imported->replay->ticks[i].inputs,
            aggregate.replay.ticks[i].inputs
        );
        EXPECT_EQ(
            imported->replay->ticks[i].members,
            aggregate.replay.ticks[i].members
        );
        EXPECT_EQ(
            imported->replay->ticks[i].checksum,
            aggregate.replay.ticks[i].checksum
        );
    }
    const auto replay = runtime::RoomActor::replay(*imported->replay);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, imported->image.actor);
    ++aggregate.actor.checksum;
    ++aggregate.replay.ticks.back().checksum;
    const auto forwarded = importLegacyRoom(aggregate);
    ASSERT_TRUE(forwarded);
    EXPECT_EQ(
        forwarded->image.actor.checksum,
        aggregate.actor.checksum
    );
    EXPECT_FALSE(runtime::RoomRuntime::validate(forwarded->image));
    EXPECT_FALSE(runtime::RoomActor::replay(*forwarded->replay));
}
TEST(
    LegacyRoomImportTest,
    SplitsManualAndManagedActorsWithoutMovingGlobalHistory
)
{
    World world;
    world.play();
    auto third = world.makeGrant(3);
    const arena::domain::RoomId manual{5};
    ASSERT_TRUE(
        world.registry->joinRoom(third.lease->binding, manual)
    );
    ASSERT_TRUE(world.rooms->snapshot(
        third.lease->binding,
        third.snapshot.sessionId,
        manual,
        [](auto) {}
    ));
    world.tick();
    auto saved = world.checkpoint();
    ASSERT_EQ(saved.actors.size(), 2u);
    saved.metrics.ticks += 3;
    saved.metrics.applied += 2;
    saved.metrics.skippedWallSlots = 11;
    saved.unattributedCounters.ticks += 3;
    saved.unattributedCounters.applied += 2;
    saved.unattributedCounters.skippedWallSlots = 11;
    const auto result = splitLegacyRooms(saved);
    ASSERT_TRUE(result);
    ASSERT_EQ(result->images.size(), 2u);
    EXPECT_EQ(result->images[0].actor, saved.actors[0]);
    EXPECT_FALSE(result->images[0].managed);
    EXPECT_EQ(result->images[1].actor, saved.actors[1]);
    EXPECT_TRUE(result->images[1].managed);
    EXPECT_EQ(result->unattributed.ticks, 3u);
    EXPECT_EQ(result->unattributed.applied, 2u);
    EXPECT_EQ(result->unattributed.skippedWallSlots, 11u);
    for (const auto& image : result->images)
        EXPECT_TRUE(runtime::RoomRuntime::validate(image));
}
} // namespace