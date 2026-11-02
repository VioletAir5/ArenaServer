#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <tuple>

#include <gtest/gtest.h>

#include "lobby/AuthorityCodec.h"
#include "lobby/RoomService.h"

namespace
{
using namespace std::chrono_literals;
using namespace service::lobby;
using arena::domain::PlayerId;
using arena::domain::RoomId;
using Clock = arena::runtime::OwnerClock;

auto counters(const RoomMetrics& c)
{
    return std::tie(
        c.ticks,
        c.applied,
        c.rejected,
        c.duplicates,
        c.battlesStarted,
        c.battlesCompleted,
        c.battlesAborted,
        c.persistenceSubmitted,
        c.persistenceRejected,
        c.persistenceFailed,
        c.persistenceCommitted
    );
}
struct World
{
    Clock::time_point now{};
    bool forbidden{false};
    std::uint64_t epoch{0};
    RoomServiceConfig config;
    std::shared_ptr<OnlineRegistry> registry;
    std::unique_ptr<RoomService> rooms;

    World()
    {
        OnlineRegistryConfig online;
        online.now = [this]
        {
            if (forbidden)
                throw std::logic_error("clock");
            return now;
        };
        registry = std::make_shared<OnlineRegistry>(online);
        config.externalTicks = true;
        config.externalSettlement = true;
        config.battleDurationTicks = 1000;
        config.now = online.now;
        config.wallNow = [this]
        {
            if (forbidden)
                throw std::logic_error("wall");
            return std::chrono::system_clock::time_point{
                       1700000000s
                   } +
                   now.time_since_epoch();
        };
        config.incarnationSource = [this]
        {
            if (forbidden)
                throw std::logic_error("ID");
            return ++incarnation;
        };
        rooms = make();
    }
    std::uint64_t incarnation{0};
    std::unique_ptr<RoomService> make()
    {
        auto value = std::make_unique<RoomService>(registry, config);
        value->start({});
        return value;
    }
    SessionGrant login(std::uint64_t id)
    {
        auto grant = registry->installVerified(
            {"trusted-candidate",
             {PlayerId{id}, ++epoch, 1700003600}},
            id + 100,
            {},
            [] { return std::int64_t{1700000000}; },
            [] {}
        );
        if (!grant)
            throw std::logic_error("login");
        return std::move(*grant);
    }
    ManagedRoom allocate(
        const SessionGrant& a,
        const SessionGrant& b,
        std::uint64_t match
    )
    {
        auto value = registry->allocateMatch(
            match,
            {{{a.snapshot.playerId, a.snapshot.sessionId, 1400},
              {b.snapshot.playerId, b.snapshot.sessionId, 1500}}}
        );
        if (!value || !rooms->openMatch(*value))
            throw std::logic_error("allocate");
        return *value;
    }
    void tick(RoomService& service)
    {
        now += 50ms;
        if (!service.advance())
            throw std::logic_error("advance");
    }
    RoomAggregate cut(RoomService& service, RoomId room)
    {
        auto value = service.exportRoom(room);
        if (!value)
            throw std::logic_error("export");
        return *value;
    }
    void ready(
        const ManagedRoom& room,
        const SessionGrant& a,
        const SessionGrant& b
    )
    {
        const auto actor = cut(*rooms, room.roomId).actor;
        for (const auto* grant : {&a, &b})
        {
            const auto index = room.members[0].playerId ==
                                       grant->snapshot.playerId
                                   ? 0
                                   : 1;
            if (!rooms->ready(
                    grant->lease->binding,
                    grant->snapshot.sessionId,
                    room.roomId,
                    actor.incarnation,
                    room.members[index].roomEpoch,
                    true,
                    [](auto) {}
                ))
                throw std::logic_error("ready");
        }
        tick(*rooms);
        tick(*rooms);
    }
    void input(
        RoomService& service,
        const ManagedRoom& room,
        const SessionGrant& grant,
        std::uint64_t sequence
    )
    {
        const auto actor = cut(service, room.roomId).actor;
        const auto index = room.members[0].playerId ==
                                   grant.snapshot.playerId
                               ? 0
                               : 1;
        if (!service.input(
                grant.lease->binding,
                grant.snapshot.sessionId,
                room.roomId,
                actor.incarnation,
                room.members[index].roomEpoch,
                sequence,
                1,
                -1,
                [](auto) {}
            ))
            throw std::logic_error("input");
        tick(service);
    }
};

TEST(
    RoomAggregate,
    ExactRoundtripPreservesKernelLifecycleReplayAndKnownCounters
)
{
    World world;
    auto a = world.login(1), b = world.login(2);
    auto room = world.allocate(a, b, 1);
    world.ready(room, a, b);
    world.input(*world.rooms, room, a, 1);
    world.input(*world.rooms, room, a, 1);
    auto original = world.cut(*world.rooms, room.roomId);
    ASSERT_EQ(original.contribution.counters.applied, 1u);
    ASSERT_EQ(original.contribution.counters.duplicates, 1u);
    auto target = world.make();
    const auto directory = world.registry->checkpoint();
    world.forbidden = true;
    ASSERT_EQ(target->importRoom(original), RoomImport::Imported);
    auto actual = world.cut(*target, room.roomId);
    EXPECT_EQ(actual.actor, original.actor);
    EXPECT_EQ(actual.managed, original.managed);
    EXPECT_EQ(
        counters(actual.contribution.counters),
        counters(original.contribution.counters)
    );
    EXPECT_EQ(actual.replay.checkpoint, original.replay.checkpoint);
    EXPECT_EQ(
        actual.replay.ticks.size(),
        original.replay.ticks.size()
    );
    const auto totals = target->metrics();
    EXPECT_EQ(
        target->importRoom(original),
        RoomImport::AlreadyPresent
    );
    EXPECT_EQ(counters(target->metrics()), counters(totals));
    EXPECT_EQ(
        world.registry->checkpoint().entries.size(),
        directory.entries.size()
    );
    EXPECT_EQ(
        world.registry->checkpoint().rooms.size(),
        directory.rooms.size()
    );
    world.forbidden = false;
    world.input(*world.rooms, room, a, 2);
    world.input(*target, room, a, 2);
    EXPECT_EQ(
        world.cut(*target, room.roomId).actor,
        world.cut(*world.rooms, room.roomId).actor
    );
    const auto advanced = target->metrics();
    EXPECT_EQ(
        target->importRoom(original).error(),
        RoomAggregateError::Conflict
    );
    EXPECT_EQ(counters(target->metrics()), counters(advanced));
}

TEST(RoomAggregate, InvalidImagesAreAtomicAndDoNotDisturbAnotherRoom)
{
    World world;
    auto a = world.login(1), b = world.login(2), c = world.login(3),
         d = world.login(4);
    auto first = world.allocate(a, b, 1),
         second = world.allocate(c, d, 2);
    world.ready(first, a, b);
    auto image = world.cut(*world.rooms, first.roomId);
    auto other = world.cut(*world.rooms, second.roomId);
    auto target = world.make();
    ASSERT_TRUE(target->importRoom(other));
    const auto before = target->metrics();
    for (int corruption = 0; corruption < 7; ++corruption)
    {
        auto bad = image;
        if (corruption == 0)
            ++bad.actor.checksum;
        if (corruption == 1)
            ++bad.contribution.counters.ticks;
        if (corruption == 2)
            bad.managed->allocation.matchId = 99;
        if (corruption == 3)
            bad.replay.config.positionLimit++;
        if (corruption == 4)
            bad.managed->room.revision = UINT64_MAX;
        if (corruption == 5)
            bad.contribution.counters.applied = UINT64_MAX;
        if (corruption == 6)
            bad.contribution.counters.battlesStarted = 0;
        EXPECT_FALSE(target->importRoom(bad));
        EXPECT_EQ(counters(target->metrics()), counters(before));
        EXPECT_EQ(
            world.cut(*target, second.roomId).actor,
            other.actor
        );
        EXPECT_EQ(
            target->exportRoom(first.roomId).error(),
            RoomAggregateError::NotFound
        );
    }
    ASSERT_EQ(target->importRoom(image), RoomImport::Imported);
    auto conflict = image;
    conflict.contribution.counters.rejected++;
    EXPECT_EQ(
        target->importRoom(conflict).error(),
        RoomAggregateError::Conflict
    );
    EXPECT_EQ(world.cut(*target, second.roomId).actor, other.actor);
}

TEST(
    RoomAggregate,
    LegacyTotalsStayUnattributedAndCoverageSurvivesBatchCodec
)
{
    World world;
    auto a = world.login(1), b = world.login(2);
    auto room = world.allocate(a, b, 1);
    world.ready(room, a, b);
    world.input(*world.rooms, room, a, 1);
    auto legacy = world.rooms->checkpoint().value();
    legacy.counterVersion = 0;
    legacy.roomCounters.clear();
    legacy.unattributedCounters = {};
    auto recovered = RoomService::restore(
        world.registry,
        legacy,
        world.config
    );
    ASSERT_TRUE(recovered);
    auto image = world.cut(**recovered, room.roomId);
    EXPECT_FALSE(image.contribution.completeHistory);
    EXPECT_EQ(image.contribution.knownSinceTick, image.actor.tick);
    EXPECT_EQ(image.contribution.counters.ticks, 0u);
    EXPECT_EQ(
        counters((*recovered)->metrics()),
        counters(legacy.metrics)
    );
    auto upgraded = (*recovered)->checkpoint().value();
    EXPECT_EQ(
        counters(upgraded.unattributedCounters),
        counters(legacy.metrics)
    );
    service::match::MatchQueue queue;
    CoreCheckpoint core{
        world.registry->checkpoint(),
        queue.checkpoint(),
        upgraded
    };
    auto encoded = encodeCoreCheckpoint(core);
    ASSERT_TRUE(encoded);
    auto decoded = decodeCoreCheckpoint(*encoded);
    ASSERT_TRUE(decoded);
    auto again = RoomService::restore(
        world.registry,
        decoded->rooms,
        world.config
    );
    ASSERT_TRUE(again);
    auto target = world.make();
    ASSERT_TRUE(target->importRoom(world.cut(**again, room.roomId)));
    EXPECT_EQ(target->metrics().ticks, 0u);
    EXPECT_EQ(target->metrics().applied, 0u);
    EXPECT_EQ(world.cut(*target, room.roomId).actor, image.actor);
    world.input(*target, room, a, 2);
    auto continued = world.cut(*target, room.roomId);
    EXPECT_FALSE(continued.contribution.completeHistory);
    EXPECT_EQ(
        continued.contribution.knownSinceTick,
        image.actor.tick
    );
    EXPECT_EQ(continued.contribution.counters.ticks, 1u);
    auto cut = target->checkpoint().value();
    EXPECT_TRUE(
        RoomService::restore(world.registry, cut, world.config)
    );
}

TEST(
    RoomAggregate,
    PendingMailboxIsNotExportedAndSettlingBudgetIsPreserved
)
{
    World world;
    world.config.battleDurationTicks = 1;
    world.rooms = world.make();
    auto a = world.login(1), b = world.login(2);
    auto room = world.allocate(a, b, 1);
    const auto initial = world.cut(*world.rooms, room.roomId);
    ASSERT_TRUE(world.rooms->snapshot(
        a.lease->binding,
        a.snapshot.sessionId,
        room.roomId,
        [](auto) {}
    ));
    EXPECT_EQ(
        world.rooms->exportRoom(room.roomId).error(),
        RoomAggregateError::Busy
    );
    world.tick(*world.rooms);
    world.ready(room, a, b);
    auto settled = world.cut(*world.rooms, room.roomId);
    ASSERT_EQ(settled.managed->phase, RoomPhase::Settling);
    ASSERT_TRUE(settled.managed->persistenceAccepted);
    auto target = world.make();
    ASSERT_TRUE(target->importRoom(settled));
    EXPECT_EQ(
        world.cut(*target, room.roomId).managed,
        settled.managed
    );
    ASSERT_TRUE(target->acknowledgeSettlement(1, false));
    auto blocked = world.cut(*target, room.roomId);
    EXPECT_TRUE(blocked.managed->persistenceBlocked);
    EXPECT_EQ(blocked.contribution.counters.persistenceFailed, 1u);
    EXPECT_TRUE(target->acknowledgeSettlement(1, false));
    EXPECT_EQ(
        world.cut(*target, room.roomId)
            .contribution.counters.persistenceFailed,
        1u
    );
    EXPECT_NE(initial.actor.tick, blocked.actor.tick);
}

TEST(
    RoomAggregate,
    FinishedCohortOutlivesCurrentMemberAndRetiredTotalsStayUnattributed
)
{
    World world;
    world.config.battleDurationTicks = 1;
    world.rooms = world.make();
    auto a = world.login(1), b = world.login(2);
    auto room = world.allocate(a, b, 1);
    world.ready(room, a, b);
    ASSERT_TRUE(world.rooms->acknowledgeSettlement(1, true));
    world.registry->detach(a.lease->binding);
    world.now += 31s;
    world.registry->sweep();
    auto historical = world.cut(*world.rooms, room.roomId);
    ASSERT_EQ(historical.managed->phase, RoomPhase::Finished);
    EXPECT_EQ(historical.actor.players.size(), 2u);
    EXPECT_EQ(world.registry->checkpoint().entries.size(), 1u);
    auto target = world.make();
    ASSERT_EQ(target->importRoom(historical), RoomImport::Imported);
    EXPECT_EQ(
        world.cut(*target, room.roomId).managed,
        historical.managed
    );
    EXPECT_EQ(target->metrics().persistenceCommitted, 1u);
    EXPECT_EQ(
        target->importRoom(historical),
        RoomImport::AlreadyPresent
    );
    EXPECT_EQ(target->metrics().persistenceCommitted, 1u);
    world.now += 31s;
    world.tick(*target);
    auto retired = target->checkpoint().value();
    EXPECT_TRUE(retired.actors.empty());
    EXPECT_TRUE(retired.roomCounters.empty());
    EXPECT_EQ(retired.unattributedCounters.persistenceCommitted, 1u);
    EXPECT_EQ(retired.unattributedCounters.battlesCompleted, 1u);
    EXPECT_TRUE(
        RoomService::restore(world.registry, retired, world.config)
    );
    auto corrupt = retired;
    ++corrupt.unattributedCounters.persistenceCommitted;
    EXPECT_FALSE(
        RoomService::restore(world.registry, corrupt, world.config)
    );
}
} // namespace
