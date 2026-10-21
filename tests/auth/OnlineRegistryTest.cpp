#include "net/OnlineRegistry.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <limits>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using arena::domain::PlayerId;
using arena::domain::RoomId;
using service::auth::AuthError;
using service::auth::JwtConfig;
using service::auth::JwtTokens;
using service::auth::TokenClaims;

class OnlineRegistryPresenceTest : public ::testing::Test
{
protected:
    std::atomic<std::int64_t> steadyMilliseconds{0};
    std::atomic<std::int64_t> wallSeconds{1700000000};
    std::shared_ptr<OnlineRegistry> registry;
    std::unique_ptr<JwtTokens> tokens;

    OnlineRegistryConfig configuration()
    {
        OnlineRegistryConfig config;
        config.grace = 100ms;
        config.now = [this]
        {
            return std::chrono::steady_clock::time_point{} +
                   std::chrono::milliseconds(
                       steadyMilliseconds.load()
                   );
        };
        return config;
    }

    JwtConfig tokenConfiguration()
    {
        JwtConfig config;
        config.key = "0123456789abcdef0123456789abcdef";
        config.now = [this] { return wallSeconds.load(); };
        return config;
    }

    void SetUp() override
    {
        tokens = std::make_unique<JwtTokens>(tokenConfiguration());
        registry = std::make_shared<OnlineRegistry>(configuration());
    }

    void TearDown() override
    {
        registry->shutdown();
    }

    std::expected<SessionGrant, AuthError> login(
        std::uint64_t player = 1,
        std::uint64_t connection = 1,
        std::function<void()> kick = [] {}
    )
    {
        return registry->install(
            PlayerId{player},
            connection,
            {},
            *tokens,
            std::move(kick)
        );
    }

    TokenClaims claims(const SessionGrant& grant)
    {
        return tokens->verify(grant.token).value();
    }
};

TEST_F(OnlineRegistryPresenceTest, RejectsInvalidConfiguration)
{
    auto config = configuration();
    config.maxPlayers = 0;
    EXPECT_THROW(OnlineRegistry{config}, std::invalid_argument);
    config = configuration();
    config.maxRooms = 0;
    EXPECT_THROW(OnlineRegistry{config}, std::invalid_argument);
    config = configuration();
    config.maxRoomMembers = 0;
    EXPECT_THROW(OnlineRegistry{config}, std::invalid_argument);
    for (const auto grace :
         {0ms,
          -1ms,
          std::chrono::duration_cast<std::chrono::milliseconds>(1h) +
              1ms})
    {
        config = configuration();
        config.grace = grace;
        EXPECT_THROW(OnlineRegistry{config}, std::invalid_argument);
    }
    config = configuration();
    config.now = {};
    EXPECT_THROW(OnlineRegistry{config}, std::invalid_argument);
}

TEST_F(
    OnlineRegistryPresenceTest,
    JwtRecoveryPreservesSessionAndRoomButRotatesEpoch
)
{
    auto original = login();
    ASSERT_TRUE(original);
    ASSERT_TRUE(
        registry->joinRoom(original->lease->binding, RoomId{42})
    );
    const auto oldClaims = claims(*original);
    original->lease->release();
    steadyMilliseconds = 99;
    auto resumed = registry->install(
        PlayerId{1},
        2,
        oldClaims,
        *tokens,
        [] {}
    );
    ASSERT_TRUE(resumed);
    EXPECT_TRUE(resumed->resumed);
    EXPECT_EQ(
        resumed->snapshot.sessionId,
        original->snapshot.sessionId
    );
    EXPECT_EQ(resumed->snapshot.roomId, RoomId{42});
    EXPECT_EQ(resumed->snapshot.presence, Presence::Online);
    EXPECT_EQ(resumed->snapshot.connectionId, 2u);
    EXPECT_FALSE(resumed->snapshot.graceDeadline);
    EXPECT_GT(resumed->snapshot.epoch, original->snapshot.epoch);
    EXPECT_NE(resumed->token, original->token);
    EXPECT_FALSE(original->lease->current());
    original->lease->release();
    steadyMilliseconds = 200;
    EXPECT_EQ(registry->sweep(), 0u);
    ASSERT_EQ(registry->roomMembers(RoomId{42}).size(), 1u);
    EXPECT_TRUE(resumed->lease->current());
    EXPECT_EQ(registry->activeCount(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    OnlineTakeoverPreservesLogicalSessionAndKicksUnlocked
)
{
    bool kicked = false;
    auto first = login(
        1,
        1,
        [&]
        {
            kicked = true;
            EXPECT_EQ(registry->retainedCount(), 1u);
        }
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(registry->joinRoom(first->lease->binding, RoomId{7}));
    auto second = registry->install(
        PlayerId{1},
        2,
        claims(*first),
        *tokens,
        [] {}
    );
    ASSERT_TRUE(second);
    EXPECT_TRUE(kicked);
    EXPECT_EQ(second->snapshot.sessionId, first->snapshot.sessionId);
    EXPECT_EQ(second->snapshot.roomId, RoomId{7});
    EXPECT_FALSE(first->lease->current());
    first->lease->release();
    EXPECT_EQ(registry->activeCount(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest, DetachIsVisibleAndRetainsItsRoomSlot
)
{
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{7}));
    alice->lease->release();
    const auto aliceState = registry->snapshot(PlayerId{1});
    ASSERT_TRUE(aliceState);
    EXPECT_EQ(aliceState->presence, Presence::Detached);
    EXPECT_EQ(aliceState->connectionId, 0u);
    ASSERT_TRUE(aliceState->graceDeadline);
    EXPECT_EQ(
        *aliceState->graceDeadline,
        std::chrono::steady_clock::time_point{} + 100ms
    );
    const auto state = registry->state(bob->lease->binding);
    ASSERT_TRUE(state);
    ASSERT_EQ(state->members.size(), 2u);
    EXPECT_EQ(state->members[0].presence, Presence::Detached);
    EXPECT_EQ(state->members[1].presence, Presence::Online);
    EXPECT_EQ(registry->activeCount(), 1u);
    EXPECT_EQ(registry->retainedCount(), 2u);
    EXPECT_EQ(
        registry->state(alice->lease->binding).error(),
        OnlineError::NotCurrent
    );
}

TEST_F(OnlineRegistryPresenceTest, RepeatedDetachNeverExtendsGrace)
{
    auto grant = login();
    ASSERT_TRUE(grant);
    grant->lease->release();
    const auto deadline = registry->snapshot(PlayerId{1})
                              ->graceDeadline;
    steadyMilliseconds = 75;
    grant->lease->release();
    registry->detach(grant->lease->binding);
    EXPECT_EQ(
        registry->snapshot(PlayerId{1})->graceDeadline,
        deadline
    );
    steadyMilliseconds = 100;
    EXPECT_EQ(registry->retainedCount(), 1u);
    EXPECT_EQ(registry->sweep(), 1u);
    EXPECT_EQ(registry->retainedCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    ExactGraceDeadlineRejectsResumeBeforeMaintenance
)
{
    auto grant = login();
    ASSERT_TRUE(grant);
    const auto credential = claims(*grant);
    ASSERT_TRUE(registry->joinRoom(grant->lease->binding, RoomId{7}));
    grant->lease->release();
    steadyMilliseconds = 100;
    auto resumed = registry->install(
        PlayerId{1},
        2,
        credential,
        *tokens,
        [] {}
    );
    ASSERT_FALSE(resumed);
    EXPECT_EQ(resumed.error(), AuthError::Superseded);
    EXPECT_EQ(registry->retainedCount(), 0u);
    EXPECT_EQ(registry->roomCount(), 0u);
    EXPECT_TRUE(registry->roomMembers(RoomId{7}).empty());
}

TEST_F(
    OnlineRegistryPresenceTest,
    SweepReclaimsWithoutAnyNetworkQueryOrNewLogin
)
{
    auto grant = login();
    ASSERT_TRUE(grant);
    ASSERT_TRUE(registry->joinRoom(grant->lease->binding, RoomId{7}));
    grant->lease->release();
    steadyMilliseconds = 101;
    EXPECT_EQ(registry->retainedCount(), 1u);
    EXPECT_EQ(registry->roomCount(), 1u);
    EXPECT_EQ(registry->sweep(), 1u);
    EXPECT_EQ(registry->retainedCount(), 0u);
    EXPECT_EQ(registry->roomCount(), 0u);
    EXPECT_EQ(registry->sweep(), 0u);
    grant->lease->release();
    EXPECT_EQ(registry->retainedCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    ExpiredQueriesNeverExposeOrAuthorizeDanglingMembers
)
{
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{7}));
    alice->lease->release();
    steadyMilliseconds = 100;
    auto observed = registry->state(bob->lease->binding);
    ASSERT_TRUE(observed);
    ASSERT_EQ(observed->members.size(), 1u);
    EXPECT_EQ(observed->members[0].playerId, PlayerId{2});
    EXPECT_FALSE(registry->snapshot(PlayerId{1}));
    EXPECT_EQ(
        registry->joinRoom(alice->lease->binding, RoomId{8}).error(),
        OnlineError::NotCurrent
    );
    EXPECT_EQ(registry->retainedCount(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    PlayerQueriesDoNotSweepUnrelatedExpiredRooms
)
{
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{8}));
    bob->lease->release();
    steadyMilliseconds = 100;
    ASSERT_TRUE(registry->snapshot(PlayerId{1}));
    ASSERT_TRUE(registry->state(alice->lease->binding));
    EXPECT_EQ(registry->roomMembers(RoomId{7}).size(), 1u);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{9}));
    ASSERT_TRUE(registry->leaveRoom(alice->lease->binding));
    auto charlie = login(3, 3);
    ASSERT_TRUE(charlie);
    EXPECT_EQ(registry->retainedCount(), 3u);
    EXPECT_EQ(registry->roomCount(), 1u);
    EXPECT_EQ(registry->sweep(), 1u);
    EXPECT_EQ(registry->retainedCount(), 2u);
    EXPECT_EQ(registry->roomCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest, HotPathsNeverReadUnrelatedPlayerClocks
)
{
    std::size_t unrelatedReads = 0;
    auto config = tokenConfiguration();
    config.now = [&]
    {
        ++unrelatedReads;
        return wallSeconds.load();
    };
    const JwtTokens otherTokens(config);
    auto alice = login(1, 1);
    auto bob = registry
                   ->install(PlayerId{2}, 2, {}, otherTokens, [] {});
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{8}));
    unrelatedReads = 0;
    EXPECT_TRUE(registry->snapshot(PlayerId{1}));
    EXPECT_TRUE(registry->state(alice->lease->binding));
    EXPECT_EQ(registry->roomMembers(RoomId{7}).size(), 1u);
    EXPECT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{9}));
    EXPECT_TRUE(registry->leaveRoom(alice->lease->binding));
    EXPECT_TRUE(login(3, 3));
    EXPECT_EQ(unrelatedReads, 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    RelevantRosterExpiryReclaimsCapacityBeforeJoin
)
{
    auto config = configuration();
    config.maxRooms = 1;
    config.maxRoomMembers = 1;
    registry = std::make_shared<OnlineRegistry>(config);
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{7}));
    bob->lease->release();
    steadyMilliseconds = 100;
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    EXPECT_EQ(registry->retainedCount(), 1u);
    EXPECT_EQ(registry->roomCount(), 1u);
    ASSERT_EQ(registry->roomMembers(RoomId{7}).size(), 1u);
    EXPECT_EQ(
        registry->roomMembers(RoomId{7})[0].playerId,
        PlayerId{1}
    );
}

TEST_F(
    OnlineRegistryPresenceTest,
    RoomAndPlayerCapacityPressureMayRunFullReclamation
)
{
    auto config = configuration();
    config.maxPlayers = 2;
    config.maxRooms = 1;
    registry = std::make_shared<OnlineRegistry>(config);
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{7}));
    bob->lease->release();
    steadyMilliseconds = 100;
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{9}));
    EXPECT_EQ(registry->retainedCount(), 1u);
    EXPECT_TRUE(registry->roomMembers(RoomId{7}).empty());
    auto charlie = login(3, 3);
    ASSERT_TRUE(charlie);
    charlie->lease->release();
    steadyMilliseconds = 200;
    auto david = login(4, 4);
    ASSERT_TRUE(david);
    EXPECT_EQ(registry->retainedCount(), 2u);
    EXPECT_FALSE(registry->snapshot(PlayerId{3}));
}

TEST_F(
    OnlineRegistryPresenceTest,
    AdvancingWallClockDuringRosterPruneCannotReuseErasedEntry
)
{
    std::size_t reads = 0;
    bool advance = false;
    auto config = tokenConfiguration();
    config.now = [&]
    {
        const auto late = advance && ++reads > 2;
        return wallSeconds.load() + (late ? 300 : 0);
    };
    const JwtTokens movingClock(config);
    auto alice = registry->install(
        PlayerId{1},
        1,
        {},
        movingClock,
        [] {}
    );
    ASSERT_TRUE(alice);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    advance = true;
    EXPECT_EQ(
        registry->state(alice->lease->binding).error(),
        OnlineError::NotCurrent
    );
    EXPECT_EQ(registry->retainedCount(), 0u);
    EXPECT_EQ(registry->roomCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    PasswordReplacementCreatesFreshSessionAndRemovesOldRoom
)
{
    auto old = login();
    ASSERT_TRUE(old);
    ASSERT_TRUE(registry->joinRoom(old->lease->binding, RoomId{7}));
    auto replacement = login(1, 2);
    ASSERT_TRUE(replacement);
    EXPECT_FALSE(replacement->resumed);
    EXPECT_NE(
        replacement->snapshot.sessionId,
        old->snapshot.sessionId
    );
    EXPECT_FALSE(replacement->snapshot.roomId);
    EXPECT_EQ(registry->roomCount(), 0u);
    old->lease->release();
    EXPECT_TRUE(replacement->lease->current());
    EXPECT_EQ(registry->activeCount(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    FailedTokenIssuePreservesPreviousValidSessionAndRoom
)
{
    auto old = login();
    ASSERT_TRUE(old);
    ASSERT_TRUE(registry->joinRoom(old->lease->binding, RoomId{7}));
    auto config = tokenConfiguration();
    config.now = [] { return -1; };
    const JwtTokens unavailable(config);
    auto failed = registry->install(
        PlayerId{1},
        2,
        {},
        unavailable,
        [] {}
    );
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), AuthError::Unavailable);
    EXPECT_TRUE(old->lease->current());
    const auto state = registry->state(old->lease->binding);
    ASSERT_TRUE(state);
    EXPECT_EQ(state->session.sessionId, old->snapshot.sessionId);
    EXPECT_EQ(state->session.roomId, RoomId{7});
    EXPECT_EQ(state->members.size(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    WrongPlayerEpochAndExpiryCannotReplaceCurrentBinding
)
{
    auto grant = login();
    ASSERT_TRUE(grant);
    const auto good = claims(*grant);
    for (const auto bad :
         {TokenClaims{PlayerId{2}, good.epoch, good.expiresAt},
          TokenClaims{PlayerId{1}, good.epoch + 1, good.expiresAt},
          TokenClaims{PlayerId{1}, good.epoch, good.expiresAt + 1}})
    {
        EXPECT_EQ(
            registry->install(PlayerId{1}, 2, bad, *tokens, [] {})
                .error(),
            AuthError::Superseded
        );
        EXPECT_TRUE(grant->lease->current());
    }
    EXPECT_EQ(registry->activeCount(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    ExpiredJwtReclaimsOnlineRoomAndKicksOutsideMutex
)
{
    std::size_t kicks = 0;
    auto grant = login(
        1,
        1,
        [&]
        {
            ++kicks;
            EXPECT_EQ(registry->retainedCount(), 0u);
            EXPECT_EQ(registry->roomCount(), 0u);
        }
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(registry->joinRoom(grant->lease->binding, RoomId{7}));
    wallSeconds += 300;
    EXPECT_FALSE(grant->lease->current());
    EXPECT_EQ(registry->activeCount(), 0u);
    EXPECT_EQ(registry->sweep(), 1u);
    EXPECT_EQ(kicks, 1u);
    EXPECT_EQ(registry->retainedCount(), 0u);
    EXPECT_EQ(registry->roomCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    CapacityCountsDetachedPlayersAndRejectedMoveKeepsOldRoom
)
{
    auto config = configuration();
    config.maxPlayers = 2;
    config.maxRooms = 2;
    config.maxRoomMembers = 1;
    registry = std::make_shared<OnlineRegistry>(config);
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{8}));
    alice->lease->release();
    EXPECT_EQ(login(3, 3).error(), AuthError::Unavailable);
    auto resumed = registry->install(
        PlayerId{1},
        3,
        claims(*alice),
        *tokens,
        [] {}
    );
    ASSERT_TRUE(resumed);
    EXPECT_EQ(
        registry->joinRoom(resumed->lease->binding, RoomId{8})
            .error(),
        OnlineError::RoomFull
    );
    EXPECT_EQ(
        registry->state(resumed->lease->binding)->session.roomId,
        RoomId{7}
    );
    EXPECT_EQ(registry->roomMembers(RoomId{7}).size(), 1u);
    EXPECT_EQ(registry->roomMembers(RoomId{8}).size(), 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    RoomMovesAndLeavesPreserveOneAuthoritativeMemberIndex
)
{
    auto config = configuration();
    config.maxRooms = 1;
    registry = std::make_shared<OnlineRegistry>(config);
    auto alice = login(1, 1);
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    EXPECT_EQ(
        registry->joinRoom(bob->lease->binding, RoomId{8}).error(),
        OnlineError::Capacity
    );
    EXPECT_FALSE(
        registry->state(bob->lease->binding)->session.roomId
    );
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{8}));
    EXPECT_TRUE(registry->roomMembers(RoomId{7}).empty());
    EXPECT_EQ(registry->roomCount(), 1u);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{8}));
    EXPECT_EQ(registry->roomMembers(RoomId{8}).size(), 1u);
    ASSERT_TRUE(registry->leaveRoom(alice->lease->binding));
    ASSERT_TRUE(registry->leaveRoom(alice->lease->binding));
    EXPECT_EQ(registry->roomCount(), 0u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    StaleBindingsCannotReadMoveOrRemoveRecoveredMembers
)
{
    auto old = login();
    ASSERT_TRUE(old);
    ASSERT_TRUE(registry->joinRoom(old->lease->binding, RoomId{7}));
    auto resumed = registry->install(
        PlayerId{1},
        2,
        claims(*old),
        *tokens,
        [] {}
    );
    ASSERT_TRUE(resumed);
    EXPECT_EQ(
        registry->state(old->lease->binding).error(),
        OnlineError::NotCurrent
    );
    EXPECT_EQ(
        registry->joinRoom(old->lease->binding, RoomId{8}).error(),
        OnlineError::NotCurrent
    );
    EXPECT_EQ(
        registry->leaveRoom(old->lease->binding).error(),
        OnlineError::NotCurrent
    );
    old->lease->release();
    EXPECT_EQ(
        registry->state(resumed->lease->binding)->session.roomId,
        RoomId{7}
    );
}

TEST_F(
    OnlineRegistryPresenceTest,
    ResumeRacingExpiryCannotLeaveAHangingOrDetachedWinner
)
{
    for (int iteration = 0; iteration != 30; ++iteration)
    {
        steadyMilliseconds = 0;
        auto current = std::make_shared<OnlineRegistry>(
            configuration()
        );
        auto old = current
                       ->install(PlayerId{1}, 1, {}, *tokens, [] {});
        ASSERT_TRUE(old);
        ASSERT_TRUE(
            current->joinRoom(old->lease->binding, RoomId{7})
        );
        const auto credential = claims(*old);
        old->lease->release();
        steadyMilliseconds = 99;
        std::barrier start(2);
        std::expected<SessionGrant, AuthError>
            recovered = std::unexpected(AuthError::Unavailable);
        std::thread resume(
            [&]
            {
                start.arrive_and_wait();
                recovered = current->install(
                    PlayerId{1},
                    2,
                    credential,
                    *tokens,
                    [] {}
                );
            }
        );
        std::thread expire(
            [&]
            {
                start.arrive_and_wait();
                steadyMilliseconds = 100;
                current->sweep();
            }
        );
        resume.join();
        expire.join();
        old->lease->release();
        current->sweep();
        if (recovered)
        {
            EXPECT_EQ(
                recovered->snapshot.sessionId,
                old->snapshot.sessionId
            );
            EXPECT_EQ(recovered->snapshot.roomId, RoomId{7});
            EXPECT_TRUE(recovered->lease->current());
            EXPECT_EQ(current->activeCount(), 1u);
            EXPECT_EQ(current->retainedCount(), 1u);
            EXPECT_EQ(current->roomMembers(RoomId{7}).size(), 1u);
        }
        else
        {
            EXPECT_EQ(recovered.error(), AuthError::Superseded);
            EXPECT_EQ(current->retainedCount(), 0u);
            EXPECT_EQ(current->roomCount(), 0u);
        }
        current->shutdown();
        EXPECT_EQ(current->retainedCount(), 0u);
        EXPECT_EQ(current->roomCount(), 0u);
    }
}

TEST_F(
    OnlineRegistryPresenceTest,
    ShutdownClearsIndexesAndRejectsLateReleaseAndInstall
)
{
    std::size_t kicks = 0;
    auto alice = login(
        1,
        1,
        [&]
        {
            ++kicks;
            EXPECT_EQ(registry->retainedCount(), 0u);
            EXPECT_EQ(registry->roomCount(), 0u);
        }
    );
    auto bob = login(2, 2);
    ASSERT_TRUE(alice);
    ASSERT_TRUE(bob);
    ASSERT_TRUE(registry->joinRoom(alice->lease->binding, RoomId{7}));
    ASSERT_TRUE(registry->joinRoom(bob->lease->binding, RoomId{7}));
    bob->lease->release();
    registry->shutdown();
    EXPECT_EQ(kicks, 1u);
    EXPECT_FALSE(alice->lease->current());
    EXPECT_EQ(registry->retainedCount(), 0u);
    EXPECT_EQ(registry->roomCount(), 0u);
    alice->lease->release();
    bob->lease->release();
    EXPECT_EQ(registry->sweep(), 0u);
    EXPECT_EQ(login(3, 3).error(), AuthError::Unavailable);
    registry->shutdown();
    EXPECT_EQ(kicks, 1u);
}

TEST_F(
    OnlineRegistryPresenceTest,
    RetainedSnapshotsOwnValuesAndNoTransportLifetime
)
{
    auto grant = login();
    ASSERT_TRUE(grant);
    const auto copied = grant->snapshot;
    registry->shutdown();
    registry.reset();
    EXPECT_FALSE(grant->lease->current());
    EXPECT_NO_THROW(grant->lease->release());
    EXPECT_EQ(copied.playerId, PlayerId{1});
    EXPECT_EQ(copied.presence, Presence::Online);
    registry = std::make_shared<OnlineRegistry>(configuration());
}
} // namespace
} // namespace service::gateway
