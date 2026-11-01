#include <unistd.h>

#include <array>
#include <filesystem>
#include <limits>
#include <optional>
#include <random>
#include <stdexcept>
#include <thread>
#include <tuple>

#include <gtest/gtest.h>

#include "net/RoomService.h"

using namespace service::gateway;
using namespace std::chrono_literals;
namespace sim = service::room;
namespace
{
using Clock = arena::runtime::OwnerClock;
struct RecoveryWorld
{
    Clock::time_point now{};
    bool clocksForbidden{false};
    unsigned clockCalls{0};
    std::uint64_t epoch{0};
    OnlineRegistryConfig registryConfig;
    RoomServiceConfig config;
    std::shared_ptr<OnlineRegistry> online;
    std::unique_ptr<RoomService> rooms;
    explicit RecoveryWorld(RoomServiceConfig supplied = {})
        : config(std::move(supplied))
    {
        registryConfig.grace = 100ms;
        registryConfig.now = [this] { return monotonic(); };
        config.externalTicks = true;
        config.now = [this] { return monotonic(); };
        config.wallNow = [this]
        {
            if (clocksForbidden)
                throw std::logic_error("unexpected wall clock read");
            ++clockCalls;
            return std::chrono::system_clock::time_point{
                       1700000000s
                   } +
                   now.time_since_epoch();
        };
        online = std::make_shared<OnlineRegistry>(registryConfig);
        rooms = std::make_unique<RoomService>(online, config);
        rooms->start({});
    }
    Clock::time_point monotonic()
    {
        if (clocksForbidden)
            throw std::logic_error("unexpected monotonic clock read");
        ++clockCalls;
        return now;
    }
    SessionGrant login(std::uint64_t player)
    {
        auto result = online->installVerified(
            {"room-test-auth-candidate",
             {arena::domain::PlayerId{player}, ++epoch, 1700003600}},
            player + 100,
            {},
            [] { return std::int64_t{1700000000}; },
            [] {}
        );
        if (!result)
            throw std::logic_error("login failed");
        return std::move(*result);
    }
    ManagedRoom allocate(
        const SessionGrant& a,
        const SessionGrant& b,
        std::uint64_t match = 1
    )
    {
        auto result = online->allocateMatch(
            match,
            {{{a.snapshot.playerId, a.snapshot.sessionId},
              {b.snapshot.playerId, b.snapshot.sessionId}}}
        );
        if (!result || !rooms->openMatch(*result))
            throw std::logic_error("allocation failed");
        return *result;
    }
    RoomCheckpoint cut(RoomService& service)
    {
        auto result = service.checkpoint();
        if (!result)
            throw std::logic_error("checkpoint failed");
        return std::move(*result);
    }
    RoomCheckpoint cut()
    {
        return cut(*rooms);
    }
    std::shared_ptr<OnlineRegistry> cloneRegistry()
    {
        auto result = OnlineRegistry::restore(
            online->checkpoint(),
            registryConfig,
            [] { return std::int64_t{1700000000}; },
            [](SessionBinding) { return [] {}; }
        );
        if (!result)
            throw std::logic_error("registry restore failed");
        return std::move(*result);
    }
    std::unique_ptr<RoomService> clone(
        const std::shared_ptr<OnlineRegistry>& registry,
        const RoomCheckpoint& checkpoint
    )
    {
        auto result = RoomService::restore(
            registry,
            checkpoint,
            config
        );
        if (!result)
            throw std::logic_error("room restore failed");
        return std::move(*result);
    }
    void ready(
        RoomService& service,
        const ManagedRoom& allocation,
        const SessionGrant& grant,
        std::uint64_t incarnation,
        bool value
    )
    {
        const auto index = allocation.members[0].playerId ==
                                   grant.snapshot.playerId
                               ? 0
                               : 1;
        EXPECT_TRUE(service.ready(
            grant.lease->binding,
            grant.snapshot.sessionId,
            allocation.roomId,
            incarnation,
            allocation.members[index].roomEpoch,
            value,
            [](auto) {}
        ));
    }
    void bothReady(
        const ManagedRoom& allocation,
        const SessionGrant& a,
        const SessionGrant& b
    )
    {
        const auto incarnation = cut().actors[0].incarnation;
        ready(*rooms, allocation, a, incarnation, true);
        ready(*rooms, allocation, b, incarnation, true);
    }
    void advance()
    {
        now += 50ms;
        ASSERT_TRUE(rooms->advance());
    }
};

void sameCut(const RoomCheckpoint& a, const RoomCheckpoint& b)
{
    EXPECT_TRUE(a.policy == b.policy);
    EXPECT_EQ(a.actors, b.actors);
    EXPECT_EQ(a.deadline, b.deadline);
    EXPECT_EQ(a.running, b.running);
    EXPECT_EQ(a.started, b.started);
    EXPECT_EQ(a.accepting, b.accepting);
    EXPECT_EQ(a.incarnationHighWater, b.incarnationHighWater);
    const auto metrics = [](const RoomMetrics& m)
    {
        return std::tie(
            m.actors,
            m.ticks,
            m.applied,
            m.rejected,
            m.duplicates,
            m.skippedWallSlots,
            m.managedMatches,
            m.retainedResults,
            m.battlesStarted,
            m.battlesCompleted,
            m.battlesAborted,
            m.persistenceSubmitted,
            m.persistenceRejected,
            m.persistenceFailed,
            m.persistenceCommitted,
            m.pendingSettlements,
            m.blockedSettlements
        );
    };
    EXPECT_EQ(metrics(a.metrics), metrics(b.metrics));
    ASSERT_EQ(a.matches.size(), b.matches.size());
    for (std::size_t i = 0; i != a.matches.size(); ++i)
    {
        const auto& x = a.matches[i];
        const auto& y = b.matches[i];
        EXPECT_EQ(x.allocation.matchId, y.allocation.matchId);
        EXPECT_EQ(x.allocation.roomId, y.allocation.roomId);
        EXPECT_EQ(x.allocation.members, y.allocation.members);
        EXPECT_EQ(x.room, y.room);
        EXPECT_EQ(x.battle, y.battle);
        EXPECT_EQ(x.phase, y.phase);
        EXPECT_EQ(x.ready, y.ready);
        EXPECT_EQ(x.presence, y.presence);
        EXPECT_EQ(
            std::tie(
                x.createdUnixMs,
                x.startedUnixMs,
                x.waitedTicks,
                x.startedTick,
                x.finishedTick,
                x.playedTicks,
                x.expires,
                x.retryAt,
                x.admissionAttempts,
                x.persistenceAccepted,
                x.persistenceBlocked,
                x.persistenceOutcome
            ),
            std::tie(
                y.createdUnixMs,
                y.startedUnixMs,
                y.waitedTicks,
                y.startedTick,
                y.finishedTick,
                y.playedTicks,
                y.expires,
                y.retryAt,
                y.admissionAttempts,
                y.persistenceAccepted,
                y.persistenceBlocked,
                y.persistenceOutcome
            )
        );
        ASSERT_EQ(x.result.has_value(), y.result.has_value());
        if (x.result)
        {
            const auto& p = *x.result;
            const auto& q = *y.result;
            EXPECT_EQ(
                std::tie(
                    p.resultId,
                    p.matchId,
                    p.roomId,
                    p.finalTick,
                    p.finalChecksum,
                    p.reason,
                    p.winnerPlayerId,
                    p.players,
                    p.createdUnixMs,
                    p.startedUnixMs,
                    p.finishedUnixMs,
                    p.startedTick,
                    p.playedTicks
                ),
                std::tie(
                    q.resultId,
                    q.matchId,
                    q.roomId,
                    q.finalTick,
                    q.finalChecksum,
                    q.reason,
                    q.winnerPlayerId,
                    q.players,
                    q.createdUnixMs,
                    q.startedUnixMs,
                    q.finishedUnixMs,
                    q.startedTick,
                    q.playedTicks
                )
            );
        }
    }
}

TEST(
    RoomServiceRecovery,
    OneHundredRandomContinuationsKeepCompleteCutExact
)
{
    std::mt19937 random(0x771923);
    std::uint64_t applied = 0;
    for (int trial = 0; trial != 100; ++trial)
    {
        SCOPED_TRACE(trial);
        RoomServiceConfig config;
        config.externalSettlement = true;
        config.battleDurationTicks = 8 + random() % 18;
        config.readyTimeoutTicks = 30;
        config.actor.positionLimit = 2 + random() % 10;
        RecoveryWorld world(config);
        auto alice = world.login(1), bob = world.login(2);
        const auto allocation = world.allocate(alice, bob);
        const auto incarnation = world.cut().actors[0].incarnation;
        for (unsigned prelude = random() % 8; prelude; --prelude)
        {
            world.ready(
                *world.rooms,
                allocation,
                alice,
                incarnation,
                random() % 2
            );
            world.ready(
                *world.rooms,
                allocation,
                bob,
                incarnation,
                random() % 2
            );
            world.advance();
        }
        auto clonedRegistry = world.cloneRegistry();
        auto clone = world.clone(clonedRegistry, world.cut());
        sameCut(world.cut(), world.cut(*clone));
        for (int tick = 0; tick != 45; ++tick)
        {
            SCOPED_TRACE(tick);
            const auto before = world.cut();
            for (const auto* grant : {&alice, &bob})
            {
                const bool ready = tick > 5 || random() % 2;
                world.ready(
                    *world.rooms,
                    allocation,
                    *grant,
                    incarnation,
                    ready
                );
                world.ready(
                    *clone,
                    allocation,
                    *grant,
                    incarnation,
                    ready
                );
            }
            std::array<std::vector<int>, 2> results;
            for (std::size_t player = 0; player != 2; ++player)
            {
                const auto& grant = player ? bob : alice;
                std::uint64_t
                    sequence = before.actors[0].players.empty()
                                   ? 1
                                   : before.actors[0]
                                             .players[player]
                                             .sequence +
                                         1;
                if (random() % 5 == 0 && sequence > 1)
                    --sequence;
                if (random() % 7 == 0)
                    sequence += 2;
                const auto dx = static_cast<std::int32_t>(
                                    random() % 3
                                ) -
                                1;
                const auto dy = static_cast<std::int32_t>(
                                    random() % 3
                                ) -
                                1;
                for (std::size_t side = 0; side != 2; ++side)
                {
                    auto& service = side ? *clone : *world.rooms;
                    ASSERT_TRUE(service.input(
                        grant.lease->binding,
                        grant.snapshot.sessionId,
                        allocation.roomId,
                        incarnation,
                        allocation.members[player].roomEpoch,
                        sequence,
                        dx,
                        dy,
                        [&, side](auto result)
                        {
                            results[side].push_back(
                                result
                                    ? static_cast<int>(result->status)
                                    : -1000 - static_cast<int>(
                                                  result.error()
                                              )
                            );
                        }
                    ));
                }
            }
            world.now += 50ms;
            ASSERT_TRUE(world.rooms->advance());
            ASSERT_TRUE(clone->advance());
            EXPECT_EQ(results[0], results[1]);
            sameCut(world.cut(), world.cut(*clone));
            if (tick == 35 &&
                world.cut().matches[0].phase == RoomPhase::Settling)
            {
                ASSERT_TRUE(world.rooms->acknowledgeSettlement(
                    allocation.matchId,
                    true
                ));
                ASSERT_TRUE(clone->acknowledgeSettlement(
                    allocation.matchId,
                    true
                ));
                sameCut(world.cut(), world.cut(*clone));
            }
        }
        applied += world.rooms->metrics().applied;
    }
    EXPECT_GT(applied, 100u);
}

TEST(
    RoomServiceRecovery,
    CheckpointAndRestoreReadNoClockOrIdentifierSource
)
{
    unsigned identifiers = 0;
    RoomServiceConfig config;
    config.externalSettlement = true;
    config.incarnationSource = [&] { return 1000 + ++identifiers; };
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    world.allocate(a, b);
    world.advance();
    const auto beforeClocks = world.clockCalls;
    world.clocksForbidden = true;
    auto registry = world.cloneRegistry();
    const auto saved = world.cut();
    auto clone = world.clone(registry, saved);
    sameCut(saved, world.cut(*clone));
    EXPECT_EQ(world.clockCalls, beforeClocks);
    EXPECT_EQ(identifiers, 1u);
    EXPECT_EQ(
        clone->replayLog(saved.actors[0].roomId)->checkpoint,
        saved.actors[0]
    );
    EXPECT_TRUE(
        clone->replayLog(saved.actors[0].roomId)->ticks.empty()
    );
    world.clocksForbidden = false;
}

TEST(
    RoomServiceRecovery,
    TimerRestoreAttachesPreservedDeadlineWithoutReadingClock
)
{
    RecoveryWorld world;
    auto grant = world.login(1);
    const arena::domain::RoomId id{7};
    ASSERT_TRUE(world.online->joinRoom(grant.lease->binding, id));
    struct TimerHarness
    {
        Clock::time_point deadline{};
        std::function<void()> action;
        unsigned schedules{0};
        arena::runtime::OwnerScheduler scheduler()
        {
            return {
                [](auto) {},
                [this](auto at, auto callback)
                {
                    ++schedules;
                    deadline = at;
                    action = std::move(callback);
                    return arena::runtime::OwnerTimer([] {});
                }
            };
        }
        void fire()
        {
            auto callback = action;
            callback();
        }
    } originalTimers, restoredTimers;
    auto config = world.config;
    config.externalTicks = false;
    RoomService original(world.online, config);
    original.start(originalTimers.scheduler());
    ASSERT_TRUE(original.snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        id,
        [](auto) {}
    ));
    world.now += 50ms;
    originalTimers.fire();
    auto saved = world.cut(original);
    auto registry = world.cloneRegistry();
    auto restored = RoomService::restore(registry, saved, config);
    ASSERT_TRUE(restored);
    const auto clockCalls = world.clockCalls;
    world.clocksForbidden = true;
    EXPECT_NO_THROW((*restored)->start(restoredTimers.scheduler()));
    EXPECT_EQ(restoredTimers.schedules, 1u);
    EXPECT_EQ(restoredTimers.deadline, saved.deadline);
    EXPECT_EQ(world.clockCalls, clockCalls);
    world.clocksForbidden = false;
    world.now += 50ms;
    originalTimers.fire();
    restoredTimers.fire();
    sameCut(world.cut(original), world.cut(**restored));
    EXPECT_FALSE(original.advance());
    EXPECT_FALSE((*restored)->advance());
}

TEST(
    RoomServiceRecovery,
    PlainCutCanRebindActorsAndStateMachinesToANewOwner
)
{
    RoomServiceConfig config;
    config.externalSettlement = true;
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    world.allocate(a, b);
    world.advance();
    const auto saved = world.cut();
    const auto registrySaved = world.online->checkpoint();
    auto bindings = world.config;
    auto registryConfig = world.registryConfig;
    std::optional<RoomCheckpoint> result;
    std::exception_ptr failure;
    std::thread owner(
        [&]
        {
            try
            {
                registryConfig.now = []
                { return Clock::time_point{} + 100ms; };
                bindings.now = registryConfig.now;
                bindings.wallNow = []
                {
                    return std::chrono::system_clock::time_point{
                               1700000000s
                           } +
                           100ms;
                };
                auto registry = OnlineRegistry::restore(
                    registrySaved,
                    registryConfig,
                    [] { return std::int64_t{1700000000}; },
                    [](SessionBinding) { return [] {}; }
                );
                if (!registry)
                    throw std::logic_error(
                        "foreign registry restore failed"
                    );
                auto restored = RoomService::restore(
                    *registry,
                    saved,
                    bindings
                );
                if (!restored || !(*restored)->advance())
                    throw std::logic_error(
                        "foreign room restore failed"
                    );
                auto cut = (*restored)->checkpoint();
                if (!cut)
                    throw std::logic_error(
                        "foreign checkpoint failed"
                    );
                result.emplace(std::move(*cut));
            }
            catch (...)
            {
                failure = std::current_exception();
            }
        }
    );
    owner.join();
    ASSERT_FALSE(failure);
    ASSERT_TRUE(result);
    world.advance();
    sameCut(world.cut(), *result);
}

TEST(
    RoomServiceRecovery,
    PendingAndReentrantCutoffsAreBusyAndStrictAdvanceReportsFailure
)
{
    RecoveryWorld world;
    auto grant = world.login(1);
    const arena::domain::RoomId room{7};
    ASSERT_TRUE(world.online->joinRoom(grant.lease->binding, room));
    std::optional<RoomRecoveryError> reentrant;
    ASSERT_TRUE(world.rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        room,
        [&](auto result)
        {
            EXPECT_TRUE(result);
            const auto saved = world.rooms->checkpoint();
            if (!saved)
                reentrant = saved.error();
        }
    ));
    const auto pending = world.rooms->checkpoint();
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error(), RoomRecoveryError::Busy);
    world.advance();
    EXPECT_EQ(reentrant, RoomRecoveryError::Busy);
    EXPECT_TRUE(world.rooms->checkpoint());
    world.clocksForbidden = true;
    EXPECT_FALSE(world.rooms->advance());
    world.clocksForbidden = false;
    ASSERT_TRUE(world.rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        room,
        [](auto)
        {
            throw std::runtime_error(
                "candidate reply capture failed"
            );
        }
    ));
    EXPECT_FALSE(world.rooms->advance());
}

TEST(
    RoomServiceRecovery,
    CorruptStateIsRejectedWithoutChangingAuthority
)
{
    RoomServiceConfig config;
    config.externalSettlement = true;
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    const auto allocation = world.allocate(a, b);
    world.advance();
    const auto saved = world.cut();
    const auto reject =
        [&](const RoomCheckpoint& corrupt, RoomServiceConfig bindings)
    {
        auto result = RoomService::restore(
            world.online,
            corrupt,
            std::move(bindings)
        );
        EXPECT_FALSE(result);
        if (!result)
            EXPECT_EQ(result.error(), RoomRecoveryError::Invalid);
        const auto current = world.online->managedRoom(
            allocation.roomId
        );
        ASSERT_TRUE(current);
        EXPECT_EQ(current->members, allocation.members);
        EXPECT_EQ(
            world.online->snapshot(a.snapshot.playerId)->roomId,
            allocation.roomId
        );
        sameCut(saved, world.cut());
    };
    auto corrupt = saved;
    corrupt.matches[0].room.state = arena::domain::Room::State::Ready;
    reject(corrupt, world.config);
    corrupt = saved;
    corrupt.matches[0].room.revision = UINT64_MAX;
    reject(corrupt, world.config);
    corrupt = saved;
    ++corrupt.actors[0].checksum;
    reject(corrupt, world.config);
    corrupt = saved;
    ++corrupt.matches[0].allocation.members[0].roomEpoch;
    reject(corrupt, world.config);
    corrupt = saved;
    corrupt.actors.push_back(corrupt.actors[0]);
    reject(corrupt, world.config);
    corrupt = saved;
    corrupt.incarnationHighWater = 0;
    reject(corrupt, world.config);
    corrupt = saved;
    corrupt.matches[0].admissionAttempts = 1;
    reject(corrupt, world.config);
    corrupt = saved;
    ++corrupt.metrics.actors;
    reject(corrupt, world.config);
    for (int field = 0; field != 17; ++field)
    {
        auto changed = world.config;
        switch (field)
        {
        case 0:
            ++changed.maxRooms;
            break;
        case 1:
            ++changed.maxPending;
            break;
        case 2:
            --changed.maxPendingPerRoom;
            break;
        case 3:
            ++changed.maxCatchUp;
            break;
        case 4:
            ++changed.actor.maxMembers;
            break;
        case 5:
            ++changed.actor.maxInputsPerTick;
            break;
        case 6:
            ++changed.actor.historyTicks;
            break;
        case 7:
            ++changed.actor.positionLimit;
            break;
        case 8:
            ++changed.battleDurationTicks;
            break;
        case 9:
            ++changed.readyTimeoutTicks;
            break;
        case 10:
            changed.resultRetention += 1ms;
            break;
        case 11:
            ++changed.persistenceAdmissionAttempts;
            break;
        case 12:
            changed.persistenceRetryBase += 1ms;
            break;
        case 13:
            changed.persistenceRetryMax += 1ms;
            break;
        case 14:
            changed.externalTicks = false;
            break;
        case 15:
            changed.externalSettlement = false;
            break;
        case 16:
            changed.persistSettlement =
                [](const auto&, const auto&, auto) { return true; };
            break;
        }
        reject(saved, std::move(changed));
    }
    corrupt = saved;
    corrupt.policy.archives = true;
    reject(corrupt, world.config);
}

TEST(
    RoomServiceRecovery,
    ManualRosterMayLagOneCutoffAndExpiredManagedCohortSurvives
)
{
    RecoveryWorld manual;
    auto a = manual.login(1), b = manual.login(2);
    const arena::domain::RoomId id{7};
    ASSERT_TRUE(manual.online->joinRoom(a.lease->binding, id));
    ASSERT_TRUE(manual.rooms->snapshot(
        a.lease->binding,
        a.snapshot.sessionId,
        id,
        [](auto) {}
    ));
    manual.advance();
    ASSERT_TRUE(manual.online->joinRoom(b.lease->binding, id));
    ASSERT_TRUE(manual.online->leaveRoom(a.lease->binding));
    auto registry = manual.cloneRegistry();
    auto clone = manual.clone(registry, manual.cut());
    manual.now += 50ms;
    ASSERT_TRUE(manual.rooms->advance());
    ASSERT_TRUE(clone->advance());
    sameCut(manual.cut(), manual.cut(*clone));
    ASSERT_EQ(manual.cut().actors[0].players.size(), 1u);
    EXPECT_EQ(
        manual.cut().actors[0].players[0].member.playerId,
        b.snapshot.playerId
    );

    RoomServiceConfig config;
    config.externalSettlement = true;
    RecoveryWorld managed(config);
    auto c = managed.login(3), d = managed.login(4);
    const auto allocation = managed.allocate(c, d);
    c.lease->release();
    d.lease->release();
    managed.now += 101ms;
    EXPECT_EQ(managed.online->sweep(), 2u);
    auto expiredRegistry = managed.cloneRegistry();
    auto expiredClone = managed.clone(expiredRegistry, managed.cut());
    ASSERT_TRUE(managed.rooms->advance());
    ASSERT_TRUE(expiredClone->advance());
    sameCut(managed.cut(), managed.cut(*expiredClone));
    EXPECT_EQ(
        managed.cut().matches[0].result->reason,
        SettlementReason::MemberUnavailable
    );
    EXPECT_EQ(
        managed.cut().matches[0].allocation.members,
        allocation.members
    );
}

TEST(
    RoomServiceRecovery,
    ExternalSettlementAcknowledgementHasIsolatedAtomicReleaseAndFiniteBudget
)
{
    RoomServiceConfig config;
    config.externalSettlement = true;
    config.battleDurationTicks = 1;
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    const auto allocation = world.allocate(a, b);
    world.bothReady(allocation, a, b);
    world.advance();
    world.advance();
    const auto intent = world.cut();
    ASSERT_EQ(intent.matches[0].phase, RoomPhase::Settling);
    EXPECT_EQ(intent.matches[0].admissionAttempts, 1u);
    EXPECT_TRUE(intent.matches[0].persistenceAccepted);
    EXPECT_EQ(intent.metrics.persistenceSubmitted, 1u);
    EXPECT_EQ(intent.metrics.persistenceCommitted, 0u);
    auto blockedRegistry = world.cloneRegistry();
    auto blocked = world.clone(blockedRegistry, intent);
    ASSERT_TRUE(
        blocked->acknowledgeSettlement(allocation.matchId, false)
    );
    for (int i = 0; i != 20; ++i)
    {
        world.now += 50ms;
        ASSERT_TRUE(blocked->advance());
    }
    const auto blockedCut = world.cut(*blocked);
    EXPECT_EQ(blockedCut.matches[0].admissionAttempts, 1u);
    EXPECT_EQ(blockedCut.metrics.persistenceSubmitted, 1u);
    EXPECT_EQ(blockedCut.metrics.persistenceFailed, 1u);
    auto blockedAgain = world.clone(blockedRegistry, blockedCut);
    EXPECT_FALSE(
        blockedAgain->acknowledgeSettlement(allocation.matchId, true)
    );
    EXPECT_TRUE(
        blockedAgain->acknowledgeSettlement(allocation.matchId, false)
    );
    EXPECT_TRUE(blockedRegistry->managedRoom(allocation.roomId));
    EXPECT_TRUE(world.online->managedRoom(allocation.roomId));
    auto committedRegistry = world.cloneRegistry();
    auto committed = world.clone(committedRegistry, intent);
    ASSERT_TRUE(
        committed->acknowledgeSettlement(allocation.matchId, true)
    );
    EXPECT_FALSE(committedRegistry->managedRoom(allocation.roomId));
    EXPECT_FALSE(
        committedRegistry->snapshot(a.snapshot.playerId)->roomId
    );
    EXPECT_FALSE(
        committedRegistry->snapshot(b.snapshot.playerId)->roomId
    );
    EXPECT_EQ(
        world.cut(*committed).matches[0].phase,
        RoomPhase::Finished
    );
    EXPECT_EQ(committed->metrics().persistenceCommitted, 1u);
    EXPECT_EQ(committed->metrics().battlesCompleted, 1u);
    EXPECT_TRUE(
        committed->acknowledgeSettlement(allocation.matchId, true)
    );
    EXPECT_FALSE(
        committed->acknowledgeSettlement(allocation.matchId, false)
    );
    EXPECT_EQ(committed->metrics().persistenceCommitted, 1u);
    auto finishedAgain = world.clone(
        committedRegistry,
        world.cut(*committed)
    );
    sameCut(world.cut(*committed), world.cut(*finishedAgain));
    sameCut(intent, world.cut());
    EXPECT_EQ(
        world.online->snapshot(a.snapshot.playerId)->roomId,
        allocation.roomId
    );
}

TEST(
    RoomServiceRecovery,
    CallbackLoanIsBusyUntilItsRealOutcomeAndRejectedHandoffRetainsBudget
)
{
    std::function<void(bool)> completion;
    RoomServiceConfig config;
    config.battleDurationTicks = 1;
    config.persistSettlement =
        [&](const auto&, const auto&, auto callback)
    {
        completion = std::move(callback);
        return true;
    };
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    const auto allocation = world.allocate(a, b);
    world.bothReady(allocation, a, b);
    world.advance();
    world.advance();
    ASSERT_TRUE(completion);
    auto pending = world.rooms->checkpoint();
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error(), RoomRecoveryError::Busy);
    completion(true);
    completion = {};
    auto registry = world.cloneRegistry();
    auto cutWithOutcome = world.cut();
    auto unfinished = cutWithOutcome;
    unfinished.matches[0].persistenceOutcome = 0;
    auto rejectedLoan = RoomService::restore(
        registry,
        unfinished,
        world.config
    );
    ASSERT_FALSE(rejectedLoan);
    EXPECT_EQ(rejectedLoan.error(), RoomRecoveryError::Busy);
    auto clone = world.clone(registry, cutWithOutcome);
    world.now += 50ms;
    ASSERT_TRUE(clone->advance());
    EXPECT_EQ(clone->metrics().persistenceCommitted, 1u);
    EXPECT_FALSE(registry->managedRoom(allocation.roomId));
    EXPECT_TRUE(world.online->managedRoom(allocation.roomId));

    unsigned rejectedCalls = 0;
    RoomServiceConfig rejectedConfig;
    rejectedConfig.battleDurationTicks = 1;
    rejectedConfig.persistenceAdmissionAttempts = 3;
    rejectedConfig
        .persistenceRetryBase = rejectedConfig
                                    .persistenceRetryMax = 50ms;
    rejectedConfig.persistSettlement =
        [&](const auto&, const auto&, auto)
    {
        ++rejectedCalls;
        return false;
    };
    RecoveryWorld rejected(rejectedConfig);
    auto c = rejected.login(3), d = rejected.login(4);
    const auto second = rejected.allocate(c, d);
    rejected.bothReady(second, c, d);
    rejected.advance();
    rejected.advance();
    ASSERT_EQ(rejectedCalls, 1u);
    auto retryRegistry = rejected.cloneRegistry();
    auto retry = rejected.clone(retryRegistry, rejected.cut());
    for (int i = 0; i != 8; ++i)
    {
        rejected.now += 50ms;
        ASSERT_TRUE(rejected.rooms->advance());
        ASSERT_TRUE(retry->advance());
        sameCut(rejected.cut(), rejected.cut(*retry));
    }
    EXPECT_EQ(rejectedCalls, 5u);
    EXPECT_EQ(rejected.cut().matches[0].admissionAttempts, 3u);
    EXPECT_TRUE(rejected.cut().matches[0].persistenceBlocked);
    auto exhausted = rejected.clone(
        retryRegistry,
        rejected.cut(*retry)
    );
    ASSERT_TRUE(exhausted->advance());
    EXPECT_EQ(rejectedCalls, 5u);
}

TEST(
    RoomServiceRecovery,
    RestoredHighWaterRejectsStaleSourceAndRaisesDefaultOnlyOnNewAllocation
)
{
    std::uint64_t next = 1000;
    unsigned calls = 0;
    RoomServiceConfig config;
    config.externalSettlement = true;
    config.incarnationSource = [&]
    {
        ++calls;
        return ++next;
    };
    RecoveryWorld world(config);
    auto a = world.login(1), b = world.login(2);
    world.allocate(a, b);
    auto c = world.login(3), d = world.login(4);
    auto saved = world.cut();
    saved.incarnationHighWater = 10000;
    auto registry = world.cloneRegistry();
    auto clone = world.clone(registry, saved);
    EXPECT_EQ(calls, 1u);
    auto allocation = registry->allocateMatch(
        2,
        {{{c.snapshot.playerId, c.snapshot.sessionId},
          {d.snapshot.playerId, d.snapshot.sessionId}}}
    );
    ASSERT_TRUE(allocation);
    next = 9999;
    EXPECT_THROW(
        clone->openMatch(*allocation),
        std::invalid_argument
    );
    EXPECT_EQ(world.cut(*clone).incarnationHighWater, 10000u);
    next = 10000;
    ASSERT_TRUE(clone->openMatch(*allocation));
    EXPECT_EQ(world.cut(*clone).incarnationHighWater, 10001u);

    auto defaultBindings = world.config;
    defaultBindings.incarnationSource = {};
    auto defaultRegistry = world.cloneRegistry();
    auto restored = RoomService::restore(
        defaultRegistry,
        saved,
        defaultBindings
    );
    ASSERT_TRUE(restored);
    EXPECT_EQ(calls, 3u);
    auto second = defaultRegistry->allocateMatch(
        2,
        {{{c.snapshot.playerId, c.snapshot.sessionId},
          {d.snapshot.playerId, d.snapshot.sessionId}}}
    );
    ASSERT_TRUE(second);
    ASSERT_TRUE((*restored)->openMatch(*second));
    EXPECT_GT(
        world.cut(**restored).actors[1].incarnation,
        saved.incarnationHighWater
    );
}

TEST(
    RoomServiceRecovery,
    ActiveArchiveCannotBecomeAnApparentlyCompleteRecoveredArchive
)
{
    const auto base = std::filesystem::temp_directory_path() /
                      ("arena-room-recovery-" +
                       std::to_string(::getpid()));
    struct Cleanup
    {
        std::filesystem::path path;
        ~Cleanup()
        {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
    } cleanup{base};
    RoomServiceConfig config;
    config.externalSettlement = true;
    config.replays = std::make_shared<sim::ReplayRecorder>(
        sim::ReplayRecorderConfig{.directory = base}
    );
    {
        RecoveryWorld world(config);
        auto a = world.login(1), b = world.login(2);
        world.allocate(a, b);
        auto saved = world.rooms->checkpoint();
        ASSERT_FALSE(saved);
        EXPECT_EQ(saved.error(), RoomRecoveryError::Busy);
    }
    config.replays->stop();
}
} // namespace