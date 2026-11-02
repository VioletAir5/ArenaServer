#include <atomic>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "lobby/OnlineRegistry.h"

namespace
{
using namespace std::chrono_literals;
using arena::domain::PlayerId;
using arena::domain::RoomId;
using service::auth::AuthError;
using service::auth::IssuedToken;
using service::auth::JwtTokens;
using service::auth::TokenClaims;
using Clock = std::chrono::steady_clock;
using Effects = std::shared_ptr<
    std::vector<service::lobby::SessionBinding>>;

std::string describe(const service::lobby::RegistryCheckpoint& saved)
{
    std::ostringstream text;
    text << saved.epochWatermark << ':' << saved.sessionWatermark
         << ':' << saved.stopped;
    for (const auto& entry : saved.entries)
    {
        const auto& value = entry.snapshot;
        text << " P " << value.playerId.value() << ':'
             << value.sessionId.value() << ':' << value.connectionId
             << ':' << value.epoch << ':' << int(value.presence)
             << ':' << (value.roomId ? value.roomId->value() : 0)
             << ':' << value.roomEpoch << ':';
        if (value.graceDeadline)
            text << value.graceDeadline->time_since_epoch().count();
        else
            text << '-';
        text << ':' << entry.binding.playerId.value() << ':'
             << entry.binding.connectionId << ':'
             << entry.binding.epoch << ':' << entry.expiresAt << ':'
             << entry.leaseValid;
    }
    for (const auto& room : saved.rooms)
    {
        text << " R " << room.roomId.value();
        for (const auto player : room.players)
            text << ':' << player.value();
        if (room.managed)
        {
            text << " M " << room.managed->matchId << ':'
                 << room.managed->roomId.value();
            for (const auto& member : room.managed->members)
                text << ':' << member.playerId.value() << ':'
                     << member.sessionId.value() << ':'
                     << member.roomEpoch << ':' << member.initialMmr;
        }
    }
    return text.str();
}

class OnlineRegistryRecoveryTest : public testing::Test
{
protected:
    std::atomic<std::int64_t> wall{1000}, steady{0};
    std::atomic<bool> rejectClock{false};
    Effects liveEffects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    std::unique_ptr<JwtTokens> tokens;
    std::shared_ptr<service::lobby::OnlineRegistry> online;

    service::lobby::OnlineRegistryConfig configuration()
    {
        service::lobby::OnlineRegistryConfig config;
        config.grace = 100ms;
        config.now = [this]
        {
            if (rejectClock.load())
                throw std::runtime_error(
                    "unexpected monotonic clock read"
                );
            return Clock::time_point{} +
                   std::chrono::milliseconds(steady.load());
        };
        return config;
    }
    std::function<std::int64_t()> wallClock()
    {
        return [this]
        {
            if (rejectClock.load())
                throw std::runtime_error(
                    "unexpected authentication clock read"
                );
            return wall.load();
        };
    }
    static service::lobby::OnlineRegistry::KickFactory
    effectsFactory(Effects effects)
    {
        return [effects = std::move(effects)](
                   service::lobby::SessionBinding binding
               )
        {
            return [effects, binding]
            { effects->push_back(binding); };
        };
    }
    void SetUp() override
    {
        service::auth::JwtConfig config;
        config.key = "0123456789abcdef0123456789abcdef";
        config.now = wallClock();
        tokens = std::make_unique<JwtTokens>(std::move(config));
        online = std::make_shared<service::lobby::OnlineRegistry>(
            configuration()
        );
    }
    void TearDown() override
    {
        rejectClock = false;
        online->shutdown();
        online.reset();
    }
    service::lobby::SessionGrant install(
        const std::shared_ptr<service::lobby::OnlineRegistry>&
            registry,
        std::uint64_t player,
        std::uint64_t connection,
        std::optional<TokenClaims> resume = {},
        Effects effects = {}
    )
    {
        auto token = tokens->issue(
            PlayerId{player},
            registry->epochWatermark() + 1
        );
        if (!token)
            throw std::runtime_error("real signing failed");
        auto result = registry->installVerified(
            std::move(*token),
            connection,
            resume,
            wallClock(),
            effectsFactory(effects ? effects : liveEffects)(
                service::lobby::SessionBinding{
                    PlayerId{player},
                    connection,
                    registry->epochWatermark() + 1
                }
            )
        );
        if (!result)
            throw std::runtime_error("fixture install failed");
        return std::move(*result);
    }
    service::lobby::SessionGrant
    login(std::uint64_t player, std::uint64_t connection = 0)
    {
        return install(
            online,
            player,
            connection ? connection : 100 + player
        );
    }
    TokenClaims claims(const service::lobby::SessionGrant& grant)
    {
        auto verified = tokens->verify(grant.token);
        if (!verified)
            throw std::runtime_error(
                "real token verification failed"
            );
        return *verified;
    }
    std::shared_ptr<service::lobby::OnlineRegistry> clone(
        const service::lobby::RegistryCheckpoint& saved,
        Effects effects
    )
    {
        auto result = service::lobby::OnlineRegistry::restore(
            saved,
            configuration(),
            wallClock(),
            effectsFactory(effects)
        );
        if (!result)
            throw std::runtime_error(
                "fixture checkpoint restore failed"
            );
        return std::move(*result);
    }
    void same(
        const std::shared_ptr<service::lobby::OnlineRegistry>& left,
        const std::shared_ptr<service::lobby::OnlineRegistry>& right
    )
    {
        EXPECT_EQ(
            describe(left->checkpoint()),
            describe(right->checkpoint())
        );
    }
    void rejected(
        const service::lobby::RegistryCheckpoint& saved,
        service::lobby::RegistryRecoveryError expected
    )
    {
        auto effects = std::make_shared<
            std::vector<service::lobby::SessionBinding>>();
        const auto before = describe(online->checkpoint());
        const auto liveBefore = liveEffects->size();
        auto result = service::lobby::OnlineRegistry::restore(
            saved,
            configuration(),
            wallClock(),
            effectsFactory(effects)
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), expected);
        EXPECT_TRUE(effects->empty());
        EXPECT_EQ(liveEffects->size(), liveBefore);
        EXPECT_EQ(describe(online->checkpoint()), before);
    }
};

TEST_F(
    OnlineRegistryRecoveryTest,
    PreservesManualManagedAndDetachedStateWithIndependentLeases
)
{
    auto alice = login(1), bob = login(2), charlie = login(3),
         dana = login(4);
    ASSERT_TRUE(online->joinRoom(alice.lease->binding, RoomId{99}));
    ASSERT_TRUE(online->joinRoom(bob.lease->binding, RoomId{99}));
    auto allocation = online->allocateMatch(
        201,
        {{{PlayerId{3}, charlie.snapshot.sessionId, 1400},
          {PlayerId{4}, dana.snapshot.sessionId, 1600}}}
    );
    ASSERT_TRUE(allocation);
    bob.lease->release();
    dana.lease->release();
    steady = 42;
    const auto saved = online->checkpoint();
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(saved, effects);
    EXPECT_TRUE(effects->empty());
    EXPECT_EQ(describe(candidate->checkpoint()), describe(saved));
    EXPECT_EQ(candidate->activeCount(), 2u);
    auto manual = candidate->state(alice.lease->binding);
    ASSERT_TRUE(manual);
    EXPECT_EQ(manual->members.size(), 2u);
    auto managed = candidate->managedRoom(allocation->roomId);
    ASSERT_TRUE(managed);
    EXPECT_EQ(managed->members, allocation->members);
    candidate->detach(alice.lease->binding);
    EXPECT_TRUE(alice.lease->current());
    EXPECT_EQ(
        online->snapshot(PlayerId{1})->presence,
        service::lobby::Presence::Online
    );
    EXPECT_EQ(
        candidate->snapshot(PlayerId{1})->presence,
        service::lobby::Presence::Detached
    );
    EXPECT_EQ(
        candidate->snapshot(PlayerId{2})->graceDeadline,
        Clock::time_point{} + 100ms
    );
    EXPECT_EQ(describe(online->checkpoint()), describe(saved));
}

TEST_F(
    OnlineRegistryRecoveryTest,
    CheckpointAndRestoreDoNotReadClocksOrPruneExpiredEntries
)
{
    auto alice = login(1), bob = login(2);
    ASSERT_TRUE(online->joinRoom(alice.lease->binding, RoomId{99}));
    ASSERT_TRUE(online->joinRoom(bob.lease->binding, RoomId{99}));
    bob.lease->release();
    wall = 1300;
    steady = 100;
    rejectClock = true;
    const auto saved = online->checkpoint();
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(saved, effects);
    EXPECT_EQ(describe(candidate->checkpoint()), describe(saved));
    EXPECT_EQ(candidate->retainedCount(), 2u);
    EXPECT_TRUE(effects->empty());
    EXPECT_TRUE(liveEffects->empty());
    rejectClock = false;
    EXPECT_EQ(candidate->activeCount(), 0u);
    EXPECT_EQ(candidate->retainedCount(), 2u);
    EXPECT_EQ(candidate->sweep(), 2u);
    EXPECT_EQ(candidate->roomCount(), 0u);
    EXPECT_EQ(effects->size(), 1u);
    EXPECT_EQ(online->retainedCount(), 2u);
    EXPECT_TRUE(liveEffects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    DetachedGraceKeepsItsOriginalExclusiveCutoff
)
{
    auto alice = login(1);
    steady = 10;
    alice.lease->release();
    steady = 109;
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(online->checkpoint(), effects);
    auto detached = candidate->snapshot(PlayerId{1});
    ASSERT_TRUE(detached);
    EXPECT_EQ(detached->graceDeadline, Clock::time_point{} + 110ms);
    EXPECT_EQ(candidate->sweep(), 0u);
    steady = 110;
    EXPECT_EQ(candidate->sweep(), 1u);
    EXPECT_EQ(online->retainedCount(), 1u);
    EXPECT_EQ(online->sweep(), 1u);
    EXPECT_TRUE(effects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    CandidateReplacementCollectsEffectsWithoutRevokingLiveAuthority
)
{
    auto alice = login(1);
    const auto prior = claims(alice);
    ASSERT_TRUE(online->joinRoom(alice.lease->binding, RoomId{99}));
    const auto saved = online->checkpoint();
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(saved, effects);
    auto resumed = install(candidate, 1, 201, prior, effects);
    EXPECT_TRUE(resumed.resumed);
    EXPECT_EQ(resumed.snapshot.sessionId, alice.snapshot.sessionId);
    EXPECT_EQ(resumed.snapshot.roomId, RoomId{99});
    ASSERT_EQ(effects->size(), 1u);
    EXPECT_EQ(
        effects->front().connectionId,
        alice.lease->binding.connectionId
    );
    EXPECT_TRUE(alice.lease->current());
    EXPECT_TRUE(liveEffects->empty());
    EXPECT_EQ(describe(online->checkpoint()), describe(saved));
    auto rejectedResume = candidate->installVerified(
        tokens->issue(PlayerId{1}, candidate->epochWatermark() + 1)
            .value(),
        202,
        prior,
        wallClock(),
        [] {}
    );
    EXPECT_FALSE(rejectedResume);
    EXPECT_TRUE(resumed.lease->current());
    EXPECT_TRUE(alice.lease->current());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    ShutdownAndDestructionRemainIsolatedAcrossAuthorities
)
{
    auto alice = login(1);
    auto firstEffects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto secondEffects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto first = clone(online->checkpoint(), firstEffects);
    auto second = clone(online->checkpoint(), secondEffects);
    first->shutdown();
    first->shutdown();
    EXPECT_EQ(firstEffects->size(), 1u);
    EXPECT_TRUE(alice.lease->current());
    ASSERT_TRUE(second->state(alice.lease->binding));
    online->shutdown();
    EXPECT_FALSE(alice.lease->current());
    EXPECT_EQ(liveEffects->size(), 1u);
    ASSERT_TRUE(second->state(alice.lease->binding));
    second.reset();
    EXPECT_EQ(secondEffects->size(), 1u);
}

TEST_F(
    OnlineRegistryRecoveryTest,
    SubsequentOperationsPreserveIdentityAllocationAndObservableState
)
{
    auto alice = login(1), bob = login(2);
    auto prior = claims(bob);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(online->checkpoint(), effects);
    for (auto registry : {online, candidate})
        ASSERT_TRUE(
            registry->joinRoom(alice.lease->binding, RoomId{99})
        );
    same(online, candidate);
    for (auto registry : {online, candidate})
    {
        ASSERT_TRUE(registry->leaveRoom(alice.lease->binding));
        registry->detach(bob.lease->binding);
    }
    same(online, candidate);
    steady = 99;
    auto resumed = install(online, 2, 202, prior);
    auto copiedResume = install(candidate, 2, 202, prior, effects);
    const auto resumedClaims = claims(resumed);
    const auto copiedClaims = claims(copiedResume);
    EXPECT_EQ(resumedClaims.playerId, copiedClaims.playerId);
    EXPECT_EQ(resumedClaims.epoch, copiedClaims.epoch);
    EXPECT_EQ(resumedClaims.expiresAt, copiedClaims.expiresAt);
    same(online, candidate);
    auto replacement = install(online, 1, 201);
    auto copiedReplacement = install(candidate, 1, 201, {}, effects);
    EXPECT_EQ(
        replacement.snapshot.sessionId,
        copiedReplacement.snapshot.sessionId
    );
    auto charlie = install(online, 3, 203);
    auto copiedCharlie = install(candidate, 3, 203, {}, effects);
    EXPECT_EQ(
        charlie.snapshot.sessionId,
        copiedCharlie.snapshot.sessionId
    );
    same(online, candidate);
    const std::array<service::lobby::MatchParticipant, 2> cohort{
        {{PlayerId{1}, replacement.snapshot.sessionId, 1400},
         {PlayerId{3}, charlie.snapshot.sessionId, 1600}}
    };
    auto allocation = online->allocateMatch(301, cohort);
    auto copiedAllocation = candidate->allocateMatch(301, cohort);
    ASSERT_TRUE(allocation);
    ASSERT_TRUE(copiedAllocation);
    EXPECT_EQ(allocation->members, copiedAllocation->members);
    same(online, candidate);
    for (auto registry : {online, candidate})
    {
        registry->detach(charlie.lease->binding);
        ASSERT_TRUE(registry->finishMatch(
            allocation->roomId,
            allocation->matchId
        ));
    }
    same(online, candidate);
    steady = 199;
    EXPECT_EQ(online->sweep(), candidate->sweep());
    same(online, candidate);
}

TEST_F(
    OnlineRegistryRecoveryTest,
    HistoricalCohortSurvivesExpiredAndReplacedSessionsWithEmptyIndex
)
{
    auto alice = login(1), bob = login(2);
    auto allocation = online->allocateMatch(
        201,
        {{{PlayerId{1}, alice.snapshot.sessionId, 1400},
          {PlayerId{2}, bob.snapshot.sessionId, 1600}}}
    );
    ASSERT_TRUE(allocation);
    auto replacement = install(online, 1, 201);
    bob.lease->release();
    steady = 100;
    ASSERT_EQ(online->sweep(), 1u);
    const auto saved = online->checkpoint();
    ASSERT_EQ(saved.rooms.size(), 1u);
    EXPECT_TRUE(saved.rooms.front().players.empty());
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(saved, effects);
    auto historical = candidate->managedRoom(allocation->roomId);
    ASSERT_TRUE(historical);
    EXPECT_EQ(historical->members, allocation->members);
    for (auto registry : {online, candidate})
    {
        ASSERT_TRUE(
            registry->joinRoom(replacement.lease->binding, RoomId{99})
        );
        ASSERT_TRUE(registry->finishMatch(
            allocation->roomId,
            allocation->matchId
        ));
        EXPECT_EQ(
            registry->snapshot(PlayerId{1})->roomId,
            RoomId{99}
        );
    }
    same(online, candidate);
}

TEST_F(
    OnlineRegistryRecoveryTest,
    OldLeaseReleaseCannotDetachRestoredAuthority
)
{
    auto alice = login(1);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(online->checkpoint(), effects);
    alice.lease->release();
    EXPECT_FALSE(alice.lease->current());
    EXPECT_EQ(
        online->snapshot(PlayerId{1})->presence,
        service::lobby::Presence::Detached
    );
    auto current = candidate->state(alice.lease->binding);
    ASSERT_TRUE(current);
    EXPECT_EQ(
        current->session.presence,
        service::lobby::Presence::Online
    );
    online->shutdown();
    EXPECT_TRUE(candidate->state(alice.lease->binding));
    EXPECT_TRUE(effects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    ReleaseRevocationLinearizesWithTheRegistryMutation
)
{
    struct Gate
    {
        std::mutex mutex;
        std::condition_variable changed;
        bool armed{false}, entered{false}, opened{false};
        void open()
        {
            std::lock_guard lock(mutex);
            opened = true;
            changed.notify_all();
        }
    };
    auto gate = std::make_shared<Gate>();
    auto config = configuration();
    config.now = [gate]
    {
        std::unique_lock lock(gate->mutex);
        if (gate->armed)
        {
            gate->entered = true;
            gate->changed.notify_all();
            gate->changed.wait(lock, [&] { return gate->opened; });
        }
        return Clock::time_point{};
    };
    auto registry = std::make_shared<service::lobby::OnlineRegistry>(
        config
    );
    auto alice = install(registry, 1, 101);
    {
        std::lock_guard lock(gate->mutex);
        gate->armed = true;
    }
    std::future<std::size_t> holding;
    std::future<void> releasing;
    struct Unlock
    {
        std::shared_ptr<Gate> gate;
        ~Unlock()
        {
            gate->open();
        }
    } unlock{gate};
    holding = std::async(
        std::launch::async,
        [registry] { return registry->activeCount(); }
    );
    {
        std::unique_lock lock(gate->mutex);
        ASSERT_TRUE(gate->changed.wait_for(
            lock,
            2s,
            [&] { return gate->entered; }
        ));
    }
    auto started = std::make_shared<std::atomic<bool>>(false);
    releasing = std::async(
        std::launch::async,
        [lease = alice.lease, started]
        {
            started->store(true, std::memory_order_release);
            lease->release();
        }
    );
    const auto deadline = Clock::now() + 1s;
    while (!started->load(std::memory_order_acquire) &&
           Clock::now() < deadline)
        std::this_thread::yield();
    ASSERT_TRUE(started->load(std::memory_order_acquire));
    bool revokedWhileAuthorityLocked = false;
    while (Clock::now() < deadline)
    {
        if (!alice.lease->current())
        {
            revokedWhileAuthorityLocked = true;
            break;
        }
        std::this_thread::yield();
    }
    EXPECT_FALSE(revokedWhileAuthorityLocked);
    EXPECT_EQ(releasing.wait_for(0ms), std::future_status::timeout);
    gate->open();
    EXPECT_EQ(holding.get(), 1u);
    releasing.get();
    EXPECT_FALSE(alice.lease->current());
    const auto saved = registry->checkpoint();
    ASSERT_EQ(saved.entries.size(), 1u);
    EXPECT_EQ(
        saved.entries.front().snapshot.presence,
        service::lobby::Presence::Detached
    );
    EXPECT_EQ(saved.entries.front().snapshot.connectionId, 0u);
    EXPECT_FALSE(saved.entries.front().leaseValid);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(saved, effects);
    EXPECT_EQ(describe(candidate->checkpoint()), describe(saved));
    EXPECT_TRUE(effects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    RestoresZeroStoppedAndExhaustedHighWatermarksWithoutRandomInitialization
)
{
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    service::lobby::RegistryCheckpoint empty;
    auto candidate = clone(empty, effects);
    EXPECT_EQ(candidate->epochWatermark(), 0u);
    EXPECT_EQ(candidate->checkpoint().sessionWatermark, 0u);
    auto first = candidate
                     ->install(PlayerId{1}, 101, {}, *tokens, [] {});
    ASSERT_TRUE(first);
    EXPECT_EQ(first->snapshot.epoch, 1u);
    EXPECT_EQ(first->snapshot.sessionId.value(), 1u);
    EXPECT_TRUE(tokens->verify(first->token));
    service::lobby::RegistryCheckpoint exhausted;
    exhausted
        .epochWatermark = std::numeric_limits<std::uint64_t>::max();
    exhausted
        .sessionWatermark = std::numeric_limits<std::uint64_t>::max();
    auto full = clone(exhausted, effects);
    auto unavailable = full->install(
        PlayerId{1},
        101,
        {},
        *tokens,
        [] {}
    );
    ASSERT_FALSE(unavailable);
    EXPECT_EQ(unavailable.error(), AuthError::Unavailable);
    EXPECT_EQ(describe(full->checkpoint()), describe(exhausted));
    service::lobby::RegistryCheckpoint stopped;
    stopped.epochWatermark = 42;
    stopped.sessionWatermark = 43;
    stopped.stopped = true;
    auto retired = clone(stopped, effects);
    EXPECT_EQ(describe(retired->checkpoint()), describe(stopped));
    auto result = retired
                      ->install(PlayerId{1}, 101, {}, *tokens, [] {});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), AuthError::Unavailable);
}

TEST_F(
    OnlineRegistryRecoveryTest,
    RejectsCorruptEntriesAndBothDirectionsOfRoomIndexes
)
{
    auto alice = login(1), bob = login(2);
    ASSERT_TRUE(online->joinRoom(alice.lease->binding, RoomId{99}));
    ASSERT_TRUE(online->joinRoom(bob.lease->binding, RoomId{99}));
    bob.lease->release();
    const auto valid = online->checkpoint();
    using Error = service::lobby::RegistryRecoveryError;
    const auto check = [&](auto mutate, Error expected)
    {
        auto broken = valid;
        mutate(broken);
        rejected(broken, expected);
    };
    check(
        [](auto& s) { s.entries.push_back(s.entries.front()); },
        Error::Identity
    );
    check(
        [](auto& s)
        {
            s.entries[1].snapshot.sessionId = s.entries[0]
                                                  .snapshot.sessionId;
        },
        Error::Identity
    );
    check(
        [](auto& s)
        {
            s.entries[1].snapshot.epoch = s.entries[0].snapshot.epoch;
            s.entries[1].binding.epoch = s.entries[0].snapshot.epoch;
        },
        Error::Identity
    );
    check([](auto& s) { s.epochWatermark = 0; }, Error::Identity);
    check([](auto& s) { s.sessionWatermark = 0; }, Error::Identity);
    check(
        [](auto& s) { s.entries[0].binding.connectionId = 0; },
        Error::Entry
    );
    check(
        [](auto& s)
        { s.entries[0].binding.playerId = PlayerId{999}; },
        Error::Entry
    );
    check([](auto& s) { s.entries[0].expiresAt = 0; }, Error::Entry);
    check(
        [](auto& s)
        {
            s.entries[0]
                .snapshot
                .presence = static_cast<service::lobby::Presence>(99);
        },
        Error::Entry
    );
    check(
        [](auto& s) { s.entries[0].leaseValid = false; },
        Error::Entry
    );
    check(
        [](auto& s)
        {
            s.entries[0].snapshot.graceDeadline = Clock::time_point{};
        },
        Error::Entry
    );
    check(
        [](auto& s) { s.entries[1].snapshot.connectionId = 102; },
        Error::Entry
    );
    check(
        [](auto& s) { s.entries[1].snapshot.graceDeadline.reset(); },
        Error::Entry
    );
    check(
        [](auto& s) { s.entries[1].leaseValid = true; },
        Error::Entry
    );
    check(
        [](auto& s) { s.entries[0].snapshot.roomEpoch = 0; },
        Error::Entry
    );
    check([](auto& s) { s.rooms.clear(); }, Error::Room);
    check([](auto& s) { s.rooms[0].players.clear(); }, Error::Room);
    check(
        [](auto& s) { s.rooms[0].players.push_back(PlayerId{999}); },
        Error::Room
    );
    check(
        [](auto& s)
        { s.rooms[0].players.push_back(s.rooms[0].players.front()); },
        Error::Room
    );
    check(
        [](auto& s) { s.entries[0].snapshot.roomId.reset(); },
        Error::Room
    );
    check(
        [](auto& s) { s.rooms.push_back(s.rooms.front()); },
        Error::Room
    );
    check([](auto& s) { s.stopped = true; }, Error::Entry);
    EXPECT_TRUE(alice.lease->current());
}

TEST_F(OnlineRegistryRecoveryTest, RejectsCorruptManagedCohorts)
{
    auto alice = login(1), bob = login(2);
    ASSERT_TRUE(online->allocateMatch(
        201,
        {{{PlayerId{1}, alice.snapshot.sessionId, 1400},
          {PlayerId{2}, bob.snapshot.sessionId, 1600}}}
    ));
    const auto valid = online->checkpoint();
    const auto check = [&](auto mutate)
    {
        auto broken = valid;
        mutate(*broken.rooms[0].managed);
        rejected(
            broken,
            service::lobby::RegistryRecoveryError::Cohort
        );
    };
    check([](auto& s) { s.matchId = 0; });
    check([](auto& s) { s.roomId = RoomId{99}; });
    check([](auto& s) { std::swap(s.members[0], s.members[1]); });
    check([](auto& s)
          { s.members[0].sessionId = s.members[1].sessionId; });
    check([](auto& s) { s.members[0].initialMmr = 10001; });
    check(
        [](auto& s)
        {
            s.members[0].roomEpoch = std::numeric_limits<
                std::uint64_t>::max();
        }
    );
    check([](auto& s) { ++s.members[0].roomEpoch; });
}

TEST_F(
    OnlineRegistryRecoveryTest,
    ValidatesConfigurationCapacityAndCallbacksBeforePublishing
)
{
    auto alice = login(1), bob = login(2);
    ASSERT_TRUE(online->joinRoom(alice.lease->binding, RoomId{99}));
    ASSERT_TRUE(online->joinRoom(bob.lease->binding, RoomId{100}));
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    const auto saved = online->checkpoint();
    const auto check =
        [&](service::lobby::OnlineRegistryConfig config,
            auto clock,
            service::lobby::OnlineRegistry::KickFactory factory,
            service::lobby::RegistryRecoveryError expected)
    {
        auto result = service::lobby::OnlineRegistry::restore(
            saved,
            std::move(config),
            std::move(clock),
            std::move(factory)
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), expected);
        EXPECT_TRUE(effects->empty());
        EXPECT_TRUE(alice.lease->current());
        EXPECT_TRUE(bob.lease->current());
    };
    auto config = configuration();
    config.grace = 0ms;
    check(
        config,
        wallClock(),
        effectsFactory(effects),
        service::lobby::RegistryRecoveryError::Configuration
    );
    config = configuration();
    config.maxPlayers = 1;
    check(
        config,
        wallClock(),
        effectsFactory(effects),
        service::lobby::RegistryRecoveryError::Capacity
    );
    config = configuration();
    config.maxRooms = 1;
    check(
        config,
        wallClock(),
        effectsFactory(effects),
        service::lobby::RegistryRecoveryError::Capacity
    );
    check(
        configuration(),
        std::function<std::int64_t()>{},
        effectsFactory(effects),
        service::lobby::RegistryRecoveryError::Configuration
    );
    check(
        configuration(),
        wallClock(),
        {},
        service::lobby::RegistryRecoveryError::Callback
    );
    check(
        configuration(),
        wallClock(),
        [](auto) { return std::function<void()>{}; },
        service::lobby::RegistryRecoveryError::Callback
    );
    ASSERT_TRUE(online->joinRoom(bob.lease->binding, RoomId{99}));
    config = configuration();
    config.maxRoomMembers = 1;
    auto result = service::lobby::OnlineRegistry::restore(
        online->checkpoint(),
        config,
        wallClock(),
        effectsFactory(effects)
    );
    ASSERT_FALSE(result);
    EXPECT_EQ(
        result.error(),
        service::lobby::RegistryRecoveryError::Capacity
    );
}

TEST_F(
    OnlineRegistryRecoveryTest,
    FailedCallbackConstructionCannotEmitPreparedEffects
)
{
    auto alice = login(1), bob = login(2);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto calls = std::make_shared<unsigned>(0);
    const auto saved = online->checkpoint();
    auto result = service::lobby::OnlineRegistry::restore(
        saved,
        configuration(),
        wallClock(),
        [effects, calls](service::lobby::SessionBinding binding)
            -> std::function<void()>
        {
            if (++*calls == 2)
                throw std::runtime_error(
                    "factory cannot construct second callback"
                );
            return [effects, binding]
            { effects->push_back(binding); };
        }
    );
    ASSERT_FALSE(result);
    EXPECT_EQ(
        result.error(),
        service::lobby::RegistryRecoveryError::Callback
    );
    EXPECT_EQ(*calls, 2u);
    EXPECT_TRUE(effects->empty());
    EXPECT_TRUE(liveEffects->empty());
    EXPECT_TRUE(alice.lease->current());
    EXPECT_TRUE(bob.lease->current());
    EXPECT_EQ(describe(online->checkpoint()), describe(saved));
    result = service::lobby::OnlineRegistry::restore(
        saved,
        configuration(),
        wallClock(),
        [](service::lobby::SessionBinding) -> std::function<void()>
        { throw std::bad_alloc(); }
    );
    ASSERT_FALSE(result);
    EXPECT_EQ(
        result.error(),
        service::lobby::RegistryRecoveryError::Allocation
    );
    EXPECT_TRUE(liveEffects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    RestoredLeasesUseTheSuppliedAuthenticationClock
)
{
    auto alice = login(1);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidateWall = std::make_shared<std::atomic<std::int64_t>>(
        1000
    );
    auto result = service::lobby::OnlineRegistry::restore(
        online->checkpoint(),
        configuration(),
        [candidateWall] { return candidateWall->load(); },
        effectsFactory(effects)
    );
    ASSERT_TRUE(result);
    auto candidate = *result;
    ASSERT_TRUE(candidate->state(alice.lease->binding));
    *candidateWall = 1300;
    EXPECT_FALSE(candidate->state(alice.lease->binding));
    EXPECT_EQ(candidate->retainedCount(), 0u);
    EXPECT_TRUE(alice.lease->current());
    EXPECT_EQ(online->retainedCount(), 1u);
    EXPECT_EQ(effects->size(), 1u);
    EXPECT_TRUE(liveEffects->empty());
}

TEST_F(
    OnlineRegistryRecoveryTest,
    DetachedTransportTupleMayBeReusedByAnotherOnlinePlayer
)
{
    auto alice = login(1, 101);
    alice.lease->release();
    auto bob = login(2, 101);
    auto effects = std::make_shared<
        std::vector<service::lobby::SessionBinding>>();
    auto candidate = clone(online->checkpoint(), effects);
    same(online, candidate);
    EXPECT_TRUE(candidate->state(bob.lease->binding));
    EXPECT_EQ(
        candidate->snapshot(PlayerId{1})->presence,
        service::lobby::Presence::Detached
    );
}
} // namespace
