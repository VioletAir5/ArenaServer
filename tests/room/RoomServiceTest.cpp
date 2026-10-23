#include "net/RoomService.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

#include "net/ConnectionSchedulers.h"
#include "net/EventLoop.h"

using namespace service::gateway;
using namespace std::chrono_literals;
namespace sim = service::room;
namespace wire = arena::protocol;
namespace
{
struct OwnerTimers
{
    struct Timer
    {
        ConnectionClock::time_point deadline;
        std::shared_ptr<bool> live;
        std::function<void()> action;
    };
    std::vector<Timer> timers;
    std::atomic<int> deferred{0};
    int schedules{0}, failAt{0};
    ConnectionScheduler scheduler()
    {
        return {
            [this](auto)
            {
                ++deferred;
                throw std::runtime_error(
                    "room input must await its tick"
                );
            },
            [this](auto when, auto action)
            {
                if (++schedules == failAt)
                    throw std::runtime_error("timer unavailable");
                auto live = std::make_shared<bool>(true);
                timers.push_back({when, live, std::move(action)});
                return ConnectionTimer([live] { *live = false; });
            }
        };
    }
    void due(ConnectionClock::time_point now)
    {
        for (int limit = 0; limit < 20; ++limit)
        {
            auto found = std::find_if(
                timers.begin(),
                timers.end(),
                [&](const auto& t)
                { return *t.live && t.deadline <= now; }
            );
            if (found == timers.end())
                return;
            auto saved = *found;
            *saved.live = false;
            saved.action();
        }
        FAIL() << "timer rearmed repeatedly in the past";
    }
    void readyAfterCancel(std::size_t index)
    {
        timers.at(index).action();
    }
};
service::auth::JwtTokens tokens()
{
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef";
    config.now = [] { return std::int64_t{1700000000}; };
    return service::auth::JwtTokens(std::move(config));
}
struct RoomServiceTest : testing::Test
{
    ConnectionClock::time_point now{};
    OwnerTimers timers;
    std::shared_ptr<OnlineRegistry>
        online = std::make_shared<OnlineRegistry>();
    service::auth::JwtTokens jwt = tokens();
    std::shared_ptr<RoomService> rooms;
    const arena::domain::RoomId roomId{7};
    SessionGrant
    login(std::uint64_t player = 42, std::uint64_t connection = 7)
    {
        auto result = online->install(
            arena::domain::PlayerId{player},
            connection,
            {},
            jwt,
            [] {}
        );
        if (!result)
            throw std::runtime_error("test login failed");
        return std::move(*result);
    }
    void join(
        const SessionGrant& grant,
        arena::domain::RoomId id = arena::domain::RoomId{7}
    )
    {
        ASSERT_TRUE(online->joinRoom(grant.lease->binding, id));
    }
    void start(RoomServiceConfig config = {})
    {
        config.now = [&] { return now; };
        rooms = std::make_shared<RoomService>(
            online,
            std::move(config)
        );
        rooms->start(timers.scheduler());
    }
    void at(std::chrono::milliseconds elapsed)
    {
        now = ConnectionClock::time_point{} + elapsed;
        timers.due(now);
    }
    sim::Snapshot initialize(const SessionGrant& grant)
    {
        std::optional<RoomService::SnapshotResult> result;
        EXPECT_TRUE(rooms->snapshot(
            grant.lease->binding,
            grant.snapshot.sessionId,
            roomId,
            [&](auto reply) { result.emplace(std::move(reply)); }
        ));
        at(50ms);
        if (!result || !*result)
            throw std::runtime_error("room initialization failed");
        return **result;
    }
};

TEST_F(
    RoomServiceTest,
    InvalidDependenciesLimitsAndForeignOwnerAccessReject
)
{
    EXPECT_THROW((RoomService({})), std::invalid_argument);
    RoomServiceConfig config;
    config.maxPending = 0;
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    config = {};
    config.maxCatchUp = 0;
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    start();
    EXPECT_THROW(rooms->start(timers.scheduler()), std::logic_error);
    std::thread foreign(
        [&]
        {
            EXPECT_THROW(rooms->metrics(), std::logic_error);
            EXPECT_THROW(rooms->replayLog(roomId), std::logic_error);
        }
    );
    foreign.join();
}

TEST_F(
    RoomServiceTest,
    CutoffAtFiftyMillisecondsDrainsOnceAndPollingNeverTicksEarly
)
{
    auto grant = login();
    join(grant);
    start();
    std::optional<RoomService::SnapshotResult> result;
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto reply) { result.emplace(std::move(reply)); }
    ));
    at(49ms);
    EXPECT_FALSE(result);
    EXPECT_EQ(rooms->metrics().ticks, 0U);
    at(50ms);
    ASSERT_TRUE(result);
    ASSERT_TRUE(*result);
    EXPECT_EQ((**result).tick, 1U);
    at(51ms);
    EXPECT_EQ(rooms->metrics().ticks, 1U);
    EXPECT_EQ(timers.deferred.load(), 0);
    ASSERT_EQ(
        timers.timers.back().deadline,
        ConnectionClock::time_point{} + 100ms
    );
}

TEST_F(
    RoomServiceTest,
    LongStallRunsAtMostFourLogicalTicksThenRebasesAbsoluteWallDeadline
)
{
    auto grant = login();
    join(grant);
    start();
    initialize(grant);
    at(1000ms);
    auto log = rooms->replayLog(roomId);
    ASSERT_TRUE(log);
    EXPECT_EQ(log->ticks.back().tick, 5U);
    EXPECT_EQ(rooms->metrics().ticks, 5U);
    EXPECT_EQ(rooms->metrics().skippedWallSlots, 15U);
    EXPECT_EQ(
        timers.timers.back().deadline,
        ConnectionClock::time_point{} + 1050ms
    );
    at(1049ms);
    EXPECT_EQ(rooms->metrics().ticks, 5U);
    at(1050ms);
    EXPECT_EQ(rooms->metrics().ticks, 6U);
}

TEST_F(
    RoomServiceTest,
    GlobalAndPerRoomMailboxLimitsRejectWithoutCallingCompletion
)
{
    auto alice = login();
    join(alice);
    auto bob = login(43, 8);
    join(bob, arena::domain::RoomId{8});
    RoomServiceConfig config;
    config.maxPending = 3;
    config.maxPendingPerRoom = 2;
    start(config);
    int accepted = 0, rejected = 0;
    const auto queue =
        [&](const SessionGrant& grant, arena::domain::RoomId id)
    {
        return rooms->snapshot(
            grant.lease->binding,
            grant.snapshot.sessionId,
            id,
            [&](auto result)
            {
                ++accepted;
                EXPECT_TRUE(result);
            }
        );
    };
    EXPECT_TRUE(queue(alice, roomId));
    EXPECT_TRUE(queue(alice, roomId));
    EXPECT_FALSE(rooms->snapshot(
        alice.lease->binding,
        alice.snapshot.sessionId,
        roomId,
        [&](auto) { ++rejected; }
    ));
    EXPECT_TRUE(queue(bob, arena::domain::RoomId{8}));
    EXPECT_FALSE(rooms->snapshot(
        bob.lease->binding,
        bob.snapshot.sessionId,
        arena::domain::RoomId{8},
        [&](auto) { ++rejected; }
    ));
    EXPECT_EQ(accepted, 0);
    at(50ms);
    EXPECT_EQ(accepted, 3);
    EXPECT_EQ(rejected, 0);
    EXPECT_EQ(timers.deferred.load(), 0);
}

TEST_F(
    RoomServiceTest,
    ThirtyTwoProducerSequenceOrderIsCanonicalAtOneTickCutoff
)
{
    auto grant = login();
    join(grant);
    start();
    const auto initial = initialize(grant);
    const auto epoch = initial.players.at(0).member.epoch;
    std::barrier gate(33);
    std::array<bool, 32> accepted{};
    std::array<std::optional<RoomService::InputResult>, 32> replies;
    std::vector<std::thread> producers;
    for (std::size_t index = 0; index < 32; ++index)
        producers.emplace_back(
            [&, index]
            {
                gate.arrive_and_wait();
                accepted[index] = rooms->input(
                    grant.lease->binding,
                    grant.snapshot.sessionId,
                    roomId,
                    initial.incarnation,
                    epoch,
                    32 - index,
                    1,
                    0,
                    [&, index](auto result)
                    { replies[index].emplace(std::move(result)); }
                );
            }
        );
    gate.arrive_and_wait();
    for (auto& producer : producers)
        producer.join();
    for (const auto& reply : replies)
        EXPECT_FALSE(reply);
    at(100ms);
    for (std::size_t index = 0; index < 32; ++index)
    {
        EXPECT_TRUE(accepted[index]);
        ASSERT_TRUE(replies[index]);
        ASSERT_TRUE(*replies[index]);
        EXPECT_EQ(
            (**replies[index]).status,
            sim::InputStatus::Applied
        );
        EXPECT_EQ(
            (**replies[index]).snapshot.players.at(0).sequence,
            32U
        );
        EXPECT_EQ((**replies[index]).snapshot.players.at(0).x, 32);
    }
    EXPECT_EQ(rooms->metrics().applied, 32U);
    EXPECT_EQ(timers.deferred.load(), 0);
    const auto replay = rooms->replayLog(roomId);
    ASSERT_TRUE(replay);
    auto restored = sim::RoomActor::replay(*replay);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->snapshot, (**replies[0]).snapshot);
}

TEST_F(
    RoomServiceTest,
    ConflictingConcurrentControlsAllRejectWithinSameCutoff
)
{
    auto grant = login();
    join(grant);
    start();
    const auto initial = initialize(grant);
    std::barrier gate(33);
    std::atomic<int> callbacks{0};
    std::vector<std::thread> producers;
    for (int index = 0; index < 32; ++index)
        producers.emplace_back(
            [&, index]
            {
                gate.arrive_and_wait();
                EXPECT_TRUE(rooms->input(
                    grant.lease->binding,
                    grant.snapshot.sessionId,
                    roomId,
                    initial.incarnation,
                    initial.players[0].member.epoch,
                    1,
                    index % 2,
                    (index + 1) % 2,
                    [&](auto result)
                    {
                        ++callbacks;
                        ASSERT_TRUE(result);
                        EXPECT_EQ(
                            result->status,
                            sim::InputStatus::Conflict
                        );
                        EXPECT_EQ(
                            result->snapshot.players[0].sequence,
                            0U
                        );
                    }
                ));
            }
        );
    gate.arrive_and_wait();
    for (auto& producer : producers)
        producer.join();
    at(100ms);
    EXPECT_EQ(callbacks.load(), 32);
    EXPECT_EQ(rooms->metrics().applied, 0U);
}

TEST_F(
    RoomServiceTest,
    ProducersRacingStopReceiveExactlyOneCallbackOnlyWhenAccepted
)
{
    auto grant = login();
    join(grant);
    start();
    std::atomic<int> callbacks{0}, accepted{0};
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto result)
        {
            ++callbacks;
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    ++accepted;
    std::array<bool, 32> admitted{};
    std::array<std::atomic<int>, 32> perCommand{};
    std::barrier gate(33);
    std::vector<std::thread> producers;
    for (std::size_t index = 0; index < 32; ++index)
        producers.emplace_back(
            [&, index]
            {
                gate.arrive_and_wait();
                admitted[index] = rooms->snapshot(
                    grant.lease->binding,
                    grant.snapshot.sessionId,
                    roomId,
                    [&, index](auto result)
                    {
                        ++callbacks;
                        ++perCommand[index];
                        ASSERT_FALSE(result);
                        EXPECT_EQ(
                            result.error(),
                            wire::ERR_UNAVAILABLE
                        );
                    }
                );
                if (admitted[index])
                    ++accepted;
            }
        );
    gate.arrive_and_wait();
    rooms->stop();
    for (auto& producer : producers)
        producer.join();
    EXPECT_EQ(callbacks.load(), accepted.load());
    for (std::size_t index = 0; index < 32; ++index)
        EXPECT_EQ(perCommand[index].load(), admitted[index] ? 1 : 0);
    const auto completed = callbacks.load();
    timers.readyAfterCancel(0);
    EXPECT_EQ(callbacks.load(), completed);
    EXPECT_EQ(timers.timers.size(), 1U);
    EXPECT_EQ(rooms->metrics().actors, 0U);
}

TEST_F(
    RoomServiceTest,
    TimerRearmFailureClearsActorsAndRejectsFurtherAdmission
)
{
    auto grant = login();
    join(grant);
    start();
    timers.failAt = 2;
    int callbacks = 0;
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto result)
        {
            ++callbacks;
            EXPECT_TRUE(result);
        }
    ));
    at(50ms);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_FALSE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto) { ++callbacks; }
    ));
    timers.readyAfterCancel(0);
    EXPECT_EQ(callbacks, 1);
}

TEST_F(
    RoomServiceTest,
    JwtRecoveryKeepsMembershipAndSequenceButFencesDeferredOldBinding
)
{
    auto grant = login();
    join(grant);
    start();
    const auto initial = initialize(grant);
    const auto epoch = initial.players[0].member.epoch;
    std::optional<RoomService::InputResult> first;
    ASSERT_TRUE(rooms->input(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        initial.incarnation,
        epoch,
        1,
        1,
        0,
        [&](auto result) { first.emplace(std::move(result)); }
    ));
    at(100ms);
    ASSERT_TRUE(first);
    ASSERT_TRUE(*first);
    std::optional<RoomService::InputResult> stale;
    ASSERT_TRUE(rooms->input(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        initial.incarnation,
        epoch,
        2,
        1,
        0,
        [&](auto result) { stale.emplace(std::move(result)); }
    ));
    grant.lease->release();
    const auto claims = jwt.verify(grant.token);
    ASSERT_TRUE(claims);
    auto recovered = online->install(
        arena::domain::PlayerId{42},
        8,
        *claims,
        jwt,
        [] {}
    );
    ASSERT_TRUE(recovered);
    EXPECT_EQ(
        recovered->snapshot.sessionId,
        grant.snapshot.sessionId
    );
    std::optional<RoomService::InputResult> current;
    ASSERT_TRUE(rooms->input(
        recovered->lease->binding,
        recovered->snapshot.sessionId,
        roomId,
        initial.incarnation,
        epoch,
        2,
        0,
        1,
        [&](auto result) { current.emplace(std::move(result)); }
    ));
    at(150ms);
    ASSERT_TRUE(stale);
    ASSERT_FALSE(*stale);
    EXPECT_EQ(stale->error(), wire::ERR_UNAUTHENTICATED);
    ASSERT_TRUE(current);
    ASSERT_TRUE(*current);
    EXPECT_EQ((**current).status, sim::InputStatus::Applied);
    EXPECT_EQ((**current).snapshot.players[0].sequence, 2U);
    EXPECT_EQ((**current).snapshot.players[0].member.epoch, epoch);
}

TEST_F(
    RoomServiceTest,
    MembershipLeaveRejoinAbaRejectsOldEpochAndResetsPlayerState
)
{
    auto grant = login();
    join(grant);
    start();
    const auto initial = initialize(grant);
    ASSERT_TRUE(online->leaveRoom(grant.lease->binding));
    join(grant);
    std::optional<RoomService::InputResult> stale;
    ASSERT_TRUE(rooms->input(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        initial.incarnation,
        initial.players[0].member.epoch,
        1,
        1,
        0,
        [&](auto result) { stale.emplace(std::move(result)); }
    ));
    std::optional<RoomService::SnapshotResult> state;
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto result) { state.emplace(std::move(result)); }
    ));
    at(100ms);
    ASSERT_TRUE(stale);
    ASSERT_FALSE(*stale);
    EXPECT_EQ(stale->error(), wire::ERR_BUSINESS_FAILURE);
    ASSERT_TRUE(state);
    ASSERT_TRUE(*state);
    EXPECT_GT(
        (**state).players[0].member.epoch,
        initial.players[0].member.epoch
    );
    EXPECT_EQ((**state).players[0].sequence, 0U);
}

TEST_F(
    RoomServiceTest,
    EmptyRoomRetiresActorAndOldIncarnationCannotTargetItsReplacement
)
{
    auto grant = login();
    join(grant);
    start();
    const auto initial = initialize(grant);
    ASSERT_TRUE(online->leaveRoom(grant.lease->binding));
    at(100ms);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    join(grant);
    std::optional<RoomService::SnapshotResult> replacement;
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto result) { replacement.emplace(std::move(result)); }
    ));
    at(150ms);
    ASSERT_TRUE(replacement);
    ASSERT_TRUE(*replacement);
    EXPECT_NE((**replacement).incarnation, initial.incarnation);
    std::optional<RoomService::InputResult> old;
    ASSERT_TRUE(rooms->input(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        initial.incarnation,
        (**replacement).players[0].member.epoch,
        1,
        1,
        0,
        [&](auto result) { old.emplace(std::move(result)); }
    ));
    at(200ms);
    ASSERT_TRUE(old);
    ASSERT_FALSE(*old);
    EXPECT_EQ(old->error(), wire::ERR_BUSINESS_FAILURE);
}

TEST_F(
    RoomServiceTest,
    PendingCommandsAndAlreadyReadyTimerCannotRetainDestroyedService
)
{
    auto grant = login();
    join(grant);
    start();
    int callbacks = 0;
    ASSERT_TRUE(rooms->snapshot(
        grant.lease->binding,
        grant.snapshot.sessionId,
        roomId,
        [&](auto) { ++callbacks; }
    ));
    rooms.reset();
    EXPECT_EQ(callbacks, 1);
    timers.readyAfterCancel(0);
    EXPECT_EQ(callbacks, 1);
}

TEST_F(
    RoomServiceTest,
    ExpiredRosterKickReentryCannotRecreateActorAfterOwnerStop
)
{
    std::int64_t wall = 1700000000;
    service::auth::JwtConfig shortConfig;
    shortConfig.key = "0123456789abcdef0123456789abcdef";
    shortConfig.lifetime = 1s;
    shortConfig.now = [&] { return wall; };
    service::auth::JwtTokens shortTokens(std::move(shortConfig));
    int kicks = 0;
    auto alice = online->install(
        arena::domain::PlayerId{42},
        7,
        {},
        shortTokens,
        [&]
        {
            ++kicks;
            rooms->stop();
        }
    );
    ASSERT_TRUE(alice);
    join(*alice);
    auto bob = login(43, 8);
    join(bob);
    start();
    initialize(bob);
    ASSERT_EQ(rooms->metrics().actors, 1U);
    int callbacks = 0;
    ASSERT_TRUE(rooms->snapshot(
        bob.lease->binding,
        bob.snapshot.sessionId,
        roomId,
        [&](auto result)
        {
            ++callbacks;
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    ++wall;
    at(100ms);
    EXPECT_EQ(kicks, 1);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_EQ(rooms->metrics().ticks, 1U);
    EXPECT_FALSE(rooms->snapshot(
        bob.lease->binding,
        bob.snapshot.sessionId,
        roomId,
        [&](auto) { ++callbacks; }
    ));
    timers.readyAfterCancel(1);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(timers.timers.size(), 2U);
}

TEST(
    RoomServiceTimerTest,
    RealAsioTwentyHertzTimerAdvancesWithoutTrafficAndStopsNaturally
)
{
    boost::asio::io_context context;
    auto online = std::make_shared<OnlineRegistry>();
    auto jwt = tokens();
    auto grant = online->install(
        arena::domain::PlayerId{42},
        7,
        {},
        jwt,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(online->joinRoom(
        grant->lease->binding,
        arena::domain::RoomId{7}
    ));
    RoomService rooms(online);
    rooms.start(connectionScheduler(context.get_executor()));
    int replies = 0;
    ASSERT_TRUE(rooms.snapshot(
        grant->lease->binding,
        grant->snapshot.sessionId,
        arena::domain::RoomId{7},
        [&](auto result)
        {
            ++replies;
            EXPECT_TRUE(result);
        }
    ));
    boost::asio::steady_timer stop(context);
    stop.expires_after(275ms);
    stop.async_wait(
        [&](auto error)
        {
            ASSERT_FALSE(error);
            rooms.stop();
        }
    );
    context.run();
    EXPECT_EQ(replies, 1);
    EXPECT_GE(rooms.metrics().ticks, 3U);
    EXPECT_EQ(rooms.metrics().actors, 0U);
}

TEST(
    RoomServiceTimerTest,
    RealEpollTwentyHertzTimerAdvancesWithoutTraffic
)
{
    EventLoop loop;
    auto online = std::make_shared<OnlineRegistry>();
    auto jwt = tokens();
    auto grant = online->install(
        arena::domain::PlayerId{42},
        7,
        {},
        jwt,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(online->joinRoom(
        grant->lease->binding,
        arena::domain::RoomId{7}
    ));
    RoomService rooms(online);
    rooms.start(connectionScheduler(loop));
    int replies = 0;
    ASSERT_TRUE(rooms.snapshot(
        grant->lease->binding,
        grant->snapshot.sessionId,
        arena::domain::RoomId{7},
        [&](auto result)
        {
            ++replies;
            EXPECT_TRUE(result);
        }
    ));
    loop.runAfter(
        275ms,
        [&]
        {
            rooms.stop();
            loop.quit();
        }
    );
    loop.loop();
    EXPECT_EQ(replies, 1);
    EXPECT_GE(rooms.metrics().ticks, 3U);
    EXPECT_EQ(rooms.metrics().actors, 0U);
}
} // namespace
