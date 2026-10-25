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
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

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

/** @brief Exercises managed matches using the same deterministic
 * owner clock as presence. */
struct ManagedRoomServiceTest : RoomServiceTest
{
    ManagedRoomServiceTest()
    {
        OnlineRegistryConfig config;
        config.grace = 100ms;
        config.now = [this] { return now; };
        online = std::make_shared<OnlineRegistry>(std::move(config));
    }
    ManagedRoom allocate(
        const SessionGrant& alice,
        const SessionGrant& bob,
        std::uint64_t match = 1
    )
    {
        auto result = online->allocateMatch(
            match,
            {{{alice.snapshot.playerId, alice.snapshot.sessionId},
              {bob.snapshot.playerId, bob.snapshot.sessionId}}}
        );
        if (!result)
            throw std::runtime_error("test allocation failed");
        return *result;
    }
    RoomStatus
    query(const SessionGrant& grant, std::chrono::milliseconds cutoff)
    {
        std::optional<RoomService::StatusResult> result;
        EXPECT_TRUE(rooms->status(
            grant.lease->binding,
            grant.snapshot.sessionId,
            [&](auto value) { result.emplace(std::move(value)); }
        ));
        at(cutoff);
        if (!result || !*result)
            throw std::runtime_error("test status failed");
        return std::move(**result);
    }
    void markReady(
        const SessionGrant& grant,
        const ManagedRoom& allocation,
        std::uint64_t incarnation,
        bool value,
        RoomService::StatusCompletion complete
    )
    {
        const auto member = std::find_if(
            allocation.members.begin(),
            allocation.members.end(),
            [&](const auto& member)
            { return member.playerId == grant.snapshot.playerId; }
        );
        ASSERT_NE(member, allocation.members.end());
        ASSERT_TRUE(rooms->ready(
            grant.lease->binding,
            grant.snapshot.sessionId,
            allocation.roomId,
            incarnation,
            member->roomEpoch,
            value,
            std::move(complete)
        ));
    }
    void bothReady(
        const SessionGrant& alice,
        const SessionGrant& bob,
        const ManagedRoom& allocation,
        std::uint64_t incarnation
    )
    {
        markReady(
            alice,
            allocation,
            incarnation,
            true,
            [](auto result) { EXPECT_TRUE(result); }
        );
        markReady(
            bob,
            allocation,
            incarnation,
            true,
            [](auto result) { EXPECT_TRUE(result); }
        );
    }
};

/** @brief Runs an owner stop from inside synchronous logging, once
 * per target event. */
class RoomStopSink final : public spdlog::sinks::base_sink<std::mutex>
{
public:
    RoomStopSink(std::string_view trigger, std::function<void()> stop)
        : trigger(trigger)
        , stop(std::move(stop))
    {
    }
    bool fired{false};

private:
    void sink_it_(const spdlog::details::log_msg& message) override
    {
        const std::string_view payload(
            message.payload.data(),
            message.payload.size()
        );
        if (!fired && payload.find(trigger) != std::string_view::npos)
        {
            fired = true;
            stop();
        }
    }
    void flush_() override
    {
    }
    std::string trigger;
    std::function<void()> stop;
};
struct RestoreRoomLogger
{
    std::shared_ptr<spdlog::logger>
        previous = spdlog::default_logger();
    ~RestoreRoomLogger()
    {
        spdlog::set_default_logger(std::move(previous));
    }
};

struct ManagedRoomLogReentryTest
    : ManagedRoomServiceTest,
      testing::WithParamInterface<const char*>
{
};

TEST_P(
    ManagedRoomLogReentryTest,
    OwnerStopInsideLogRetiresWithoutAccessingDestroyedMatch
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.battleDurationTicks = 1;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    const auto first = query(alice, 50ms);
    RestoreRoomLogger restore;
    auto sink = std::make_shared<RoomStopSink>(
        GetParam(),
        [&] { rooms->stop(); }
    );
    auto logger = std::make_shared<spdlog::logger>(
        "room-reentrant-stop",
        sink
    );
    spdlog::set_default_logger(std::move(logger));
    int callbacks = 0, accepted = 0;
    for (const auto* grant : {&alice, &bob})
    {
        markReady(
            *grant,
            allocation,
            first.snapshot.incarnation,
            true,
            [&](auto value)
            {
                ++callbacks;
                if (!value)
                {
                    EXPECT_EQ(value.error(), wire::ERR_UNAVAILABLE);
                }
            }
        );
        ++accepted;
    }
    for (const auto cutoff : {100ms, 150ms, 200ms})
    {
        std::optional<RoomService::StatusResult> result;
        ASSERT_TRUE(rooms->status(
            alice.lease->binding,
            alice.snapshot.sessionId,
            [&](auto value)
            {
                ++callbacks;
                result.emplace(std::move(value));
            }
        ));
        ++accepted;
        at(cutoff);
        ASSERT_TRUE(result);
        if (sink->fired)
        {
            ASSERT_FALSE(*result);
            EXPECT_EQ(result->error(), wire::ERR_UNAVAILABLE);
            break;
        }
        EXPECT_TRUE(*result);
    }
    EXPECT_TRUE(sink->fired);
    EXPECT_EQ(callbacks, accepted);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_EQ(rooms->metrics().managedMatches, 0U);
    EXPECT_EQ(rooms->metrics().retainedResults, 0U);
    EXPECT_FALSE(online->managedRoom(allocation.roomId));
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    EXPECT_FALSE(rooms->status(
        alice.lease->binding,
        alice.snapshot.sessionId,
        [](auto) {}
    ));
    timers.readyAfterCancel(timers.timers.size() - 1);
    EXPECT_EQ(callbacks, accepted);
}
INSTANTIATE_TEST_SUITE_P(
    SynchronousSink,
    ManagedRoomLogReentryTest,
    testing::Values(
        "event=room_tick",
        "phase=Ready",
        "phase=Playing",
        "phase=Settling",
        "phase=Finished",
        "event=room_result"
    )
);

TEST_F(
    ManagedRoomServiceTest,
    OpenMatchReportsFailureWhenFormingLogStopsItsOwner
)
{
    auto alice = login();
    auto bob = login(43, 8);
    start();
    const auto allocation = allocate(alice, bob);
    RestoreRoomLogger restore;
    auto sink = std::make_shared<RoomStopSink>(
        "phase=Forming",
        [&] { rooms->stop(); }
    );
    spdlog::set_default_logger(
        std::make_shared<spdlog::logger>("room-open-stop", sink)
    );
    EXPECT_FALSE(rooms->openMatch(allocation));
    EXPECT_TRUE(sink->fired);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_EQ(rooms->metrics().managedMatches, 0U);
    EXPECT_FALSE(online->managedRoom(allocation.roomId));
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    timers.readyAfterCancel(0);
}

TEST_F(
    ManagedRoomServiceTest,
    ReadyGateExactDurationAndRetainedResultReuseKernelReplay
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.battleDurationTicks = 3;
    config.resultRetention = 500ms;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    auto forming = query(alice, 50ms);
    EXPECT_EQ(forming.phase, RoomPhase::Forming);
    EXPECT_EQ(forming.snapshot.players.size(), 2U);
    std::optional<RoomService::InputResult> early;
    ASSERT_TRUE(rooms->input(
        alice.lease->binding,
        alice.snapshot.sessionId,
        allocation.roomId,
        forming.snapshot.incarnation,
        allocation.members[0].roomEpoch,
        1,
        1,
        0,
        [&](auto value) { early.emplace(std::move(value)); }
    ));
    bothReady(alice, bob, allocation, forming.snapshot.incarnation);
    auto ready = query(alice, 100ms);
    EXPECT_EQ(ready.phase, RoomPhase::Ready);
    ASSERT_TRUE(early);
    ASSERT_FALSE(*early);
    EXPECT_EQ(early->error(), wire::ERR_BUSINESS_FAILURE);
    auto playing = query(alice, 150ms);
    EXPECT_EQ(playing.phase, RoomPhase::Playing);
    EXPECT_EQ(playing.playedTicks, 1U);
    EXPECT_EQ(playing.startedTick, 3U);
    std::optional<RoomService::InputResult> applied;
    ASSERT_TRUE(rooms->input(
        alice.lease->binding,
        alice.snapshot.sessionId,
        allocation.roomId,
        forming.snapshot.incarnation,
        allocation.members[0].roomEpoch,
        1,
        1,
        0,
        [&](auto value) { applied.emplace(std::move(value)); }
    ));
    at(200ms);
    ASSERT_TRUE(applied);
    ASSERT_TRUE(*applied);
    EXPECT_EQ((**applied).status, sim::InputStatus::Applied);
    auto settling = query(alice, 250ms);
    ASSERT_TRUE(settling.settlement);
    EXPECT_EQ(settling.phase, RoomPhase::Settling);
    EXPECT_EQ(settling.playedTicks, 3U);
    EXPECT_EQ(settling.finishedTick, 5U);
    EXPECT_EQ(
        settling.settlement->reason,
        SettlementReason::Completed
    );
    EXPECT_EQ(settling.settlement->winnerPlayerId, 0U);
    EXPECT_EQ(settling.settlement->resultId, allocation.matchId);
    auto finished = query(bob, 300ms);
    EXPECT_EQ(finished.phase, RoomPhase::Finished);
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->managedRoom(allocation.roomId));
    EXPECT_EQ(finished.snapshot, settling.snapshot);
    EXPECT_EQ(
        finished.settlement->finalChecksum,
        settling.snapshot.checksum
    );
    EXPECT_EQ(finished.snapshot.players[0].x, 1);
    EXPECT_EQ(finished.snapshot.players[0].sequence, 1U);
    for (const auto* grant : {&alice, &bob})
    {
        std::optional<RoomService::SettlementResult> result;
        ASSERT_TRUE(rooms->settlement(
            grant->lease->binding,
            grant->snapshot.sessionId,
            allocation.matchId,
            [&](auto value) { result.emplace(std::move(value)); }
        ));
        at(grant == &alice ? 350ms : 400ms);
        ASSERT_TRUE(result);
        ASSERT_TRUE(*result);
        EXPECT_EQ((**result).resultId, allocation.matchId);
        EXPECT_EQ(
            (**result).finalChecksum,
            settling.snapshot.checksum
        );
    }
    EXPECT_EQ(rooms->metrics().battlesStarted, 1U);
    EXPECT_EQ(rooms->metrics().battlesCompleted, 1U);
    EXPECT_EQ(rooms->metrics().retainedResults, 1U);
    const auto log = rooms->replayLog(allocation.roomId);
    ASSERT_TRUE(log);
    auto replay = sim::RoomActor::replay(*log);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, finished.snapshot);
    EXPECT_EQ(replay->checksums.size(), 5U);
    std::optional<RoomService::StatusResult> expired;
    ASSERT_TRUE(rooms->status(
        alice.lease->binding,
        alice.snapshot.sessionId,
        [&](auto value) { expired.emplace(std::move(value)); }
    ));
    at(800ms);
    ASSERT_TRUE(expired);
    ASSERT_FALSE(*expired);
    EXPECT_EQ(expired->error(), wire::ERR_BUSINESS_FAILURE);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_EQ(rooms->metrics().retainedResults, 0U);
}

TEST_F(
    ManagedRoomServiceTest,
    DisconnectGracePausesPresenceWithoutResetAndExpiryAbortsOnce
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.battleDurationTicks = 20;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    auto first = query(alice, 50ms);
    bothReady(alice, bob, allocation, first.snapshot.incarnation);
    at(100ms);
    at(150ms);
    bob.lease->release();
    auto detached = query(alice, 200ms);
    EXPECT_EQ(detached.phase, RoomPhase::Playing);
    EXPECT_EQ(detached.members[1].presence, Presence::Detached);
    EXPECT_EQ(detached.playedTicks, 2U);
    auto aborted = query(alice, 250ms);
    ASSERT_TRUE(aborted.settlement);
    EXPECT_EQ(aborted.phase, RoomPhase::Settling);
    EXPECT_EQ(
        aborted.settlement->reason,
        SettlementReason::MemberUnavailable
    );
    EXPECT_EQ(aborted.playedTicks, 2U);
    EXPECT_EQ(aborted.snapshot.players.size(), 2U);
    auto final = query(alice, 300ms);
    EXPECT_EQ(final.phase, RoomPhase::Finished);
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_EQ(online->roomCount(), 0U);
    EXPECT_EQ(rooms->metrics().battlesAborted, 1U);
    at(400ms);
    EXPECT_EQ(rooms->metrics().battlesAborted, 1U);
}

TEST_F(
    ManagedRoomServiceTest,
    NeverReadyTimesOutWithoutClientPollingAndStopReleasesManagedBindings
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.readyTimeoutTicks = 3;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    at(50ms);
    at(100ms);
    at(150ms);
    auto result = query(alice, 200ms);
    EXPECT_EQ(result.phase, RoomPhase::Finished);
    ASSERT_TRUE(result.settlement);
    EXPECT_EQ(
        result.settlement->reason,
        SettlementReason::ReadyTimeout
    );
    EXPECT_EQ(result.playedTicks, 0U);
    EXPECT_EQ(result.finishedTick, 3U);
    const auto second = allocate(alice, bob, 2);
    ASSERT_TRUE(rooms->openMatch(second));
    int callbacks = 0;
    ASSERT_TRUE(rooms->status(
        alice.lease->binding,
        alice.snapshot.sessionId,
        [&](auto value)
        {
            ++callbacks;
            EXPECT_FALSE(value);
            EXPECT_EQ(value.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    rooms->stop();
    EXPECT_EQ(callbacks, 1);
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    EXPECT_EQ(online->roomCount(), 0U);
    EXPECT_EQ(rooms->metrics().actors, 0U);
}

TEST_F(
    ManagedRoomServiceTest,
    JwtRecoveryPreservesReadySequenceAndRetainedResultButRejectsNewSession
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.battleDurationTicks = 4;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    auto first = query(alice, 50ms);
    bothReady(alice, bob, allocation, first.snapshot.incarnation);
    at(100ms);
    at(150ms);
    std::optional<RoomService::InputResult> stale;
    ASSERT_TRUE(rooms->input(
        alice.lease->binding,
        alice.snapshot.sessionId,
        allocation.roomId,
        first.snapshot.incarnation,
        allocation.members[0].roomEpoch,
        1,
        1,
        0,
        [&](auto value) { stale.emplace(std::move(value)); }
    ));
    alice.lease->release();
    auto claims = jwt.verify(alice.token);
    ASSERT_TRUE(claims);
    auto resumed = online->install(
        alice.snapshot.playerId,
        9,
        *claims,
        jwt,
        [] {}
    );
    ASSERT_TRUE(resumed);
    std::optional<RoomService::InputResult> current;
    ASSERT_TRUE(rooms->input(
        resumed->lease->binding,
        resumed->snapshot.sessionId,
        allocation.roomId,
        first.snapshot.incarnation,
        allocation.members[0].roomEpoch,
        1,
        1,
        0,
        [&](auto value) { current.emplace(std::move(value)); }
    ));
    auto state = query(*resumed, 200ms);
    ASSERT_TRUE(stale);
    EXPECT_FALSE(*stale);
    EXPECT_EQ(stale->error(), wire::ERR_UNAUTHENTICATED);
    ASSERT_TRUE(current);
    ASSERT_TRUE(*current);
    EXPECT_EQ(state.snapshot.incarnation, first.snapshot.incarnation);
    EXPECT_EQ(state.snapshot.players[0].sequence, 1U);
    EXPECT_TRUE(state.members[0].ready);
    at(250ms);
    at(300ms);
    auto final = query(*resumed, 350ms);
    EXPECT_EQ(final.phase, RoomPhase::Finished);
    ASSERT_TRUE(final.settlement);
    resumed->lease->release();
    claims = jwt.verify(resumed->token);
    ASSERT_TRUE(claims);
    auto after = online->install(
        alice.snapshot.playerId,
        10,
        *claims,
        jwt,
        [] {}
    );
    ASSERT_TRUE(after);
    auto recovered = query(*after, 400ms);
    EXPECT_EQ(recovered.snapshot, final.snapshot);
    EXPECT_EQ(recovered.settlement->resultId, allocation.matchId);
    auto replacement = login(42, 11);
    std::optional<RoomService::SettlementResult> denied;
    ASSERT_TRUE(rooms->settlement(
        replacement.lease->binding,
        replacement.snapshot.sessionId,
        allocation.matchId,
        [&](auto value) { denied.emplace(std::move(value)); }
    ));
    at(450ms);
    ASSERT_TRUE(denied);
    ASSERT_FALSE(*denied);
    EXPECT_EQ(denied->error(), wire::ERR_BUSINESS_FAILURE);
}

TEST_F(
    ManagedRoomServiceTest,
    ReadyFencesRejectOldActorAndEpochAndCannotWithdrawDuringPlay
)
{
    auto alice = login();
    auto bob = login(43, 8);
    start();
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    auto first = query(alice, 50ms);
    int rejected = 0;
    for (bool oldActor : {true, false})
        ASSERT_TRUE(rooms->ready(
            alice.lease->binding,
            alice.snapshot.sessionId,
            allocation.roomId,
            first.snapshot.incarnation + (oldActor ? 1 : 0),
            allocation.members[0].roomEpoch + (oldActor ? 0 : 1),
            true,
            [&](auto value)
            {
                ++rejected;
                ASSERT_FALSE(value);
                EXPECT_EQ(value.error(), wire::ERR_BUSINESS_FAILURE);
            }
        ));
    at(100ms);
    EXPECT_EQ(rejected, 2);
    bothReady(alice, bob, allocation, first.snapshot.incarnation);
    at(150ms);
    auto playing = query(alice, 200ms);
    EXPECT_EQ(playing.phase, RoomPhase::Playing);
    markReady(
        alice,
        allocation,
        first.snapshot.incarnation,
        false,
        [&](auto value)
        {
            ++rejected;
            ASSERT_FALSE(value);
            EXPECT_EQ(value.error(), wire::ERR_BUSINESS_FAILURE);
        }
    );
    at(250ms);
    EXPECT_EQ(rejected, 3);
    EXPECT_EQ(rooms->metrics().battlesStarted, 1U);
}

TEST_F(
    ManagedRoomServiceTest,
    OpenMatchOwnerCapacityAndFailureLeaveNoPartialLocalRoom
)
{
    auto alice = login();
    auto bob = login(43, 8);
    auto carl = login(44, 9);
    auto dave = login(45, 10);
    RoomServiceConfig config;
    config.maxRooms = 1;
    start(config);
    const auto first = allocate(alice, bob);
    const auto second = allocate(carl, dave, 2);
    std::thread foreign(
        [&]
        { EXPECT_THROW(rooms->openMatch(first), std::logic_error); }
    );
    foreign.join();
    ASSERT_TRUE(rooms->openMatch(first));
    EXPECT_FALSE(rooms->openMatch(first));
    EXPECT_FALSE(rooms->openMatch(second));
    EXPECT_EQ(rooms->metrics().actors, 1U);
    EXPECT_EQ(rooms->metrics().managedMatches, 1U);
    EXPECT_FALSE(rooms->replayLog(second.roomId));
    EXPECT_TRUE(online->finishMatch(second.roomId, second.matchId));
    EXPECT_FALSE(online->snapshot(carl.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(dave.snapshot.playerId)->roomId);
}

TEST_F(
    ManagedRoomServiceTest,
    ConcurrentReadyAndRecoveryQueriesShareBoundedCutoffAndStopSettlesOnce
)
{
    auto alice = login();
    auto bob = login(43, 8);
    RoomServiceConfig config;
    config.maxPending = 64;
    start(config);
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    auto first = query(alice, 50ms);
    std::barrier barrier(33);
    std::atomic<int> accepted{0}, callbacks{0};
    std::vector<std::thread> producers;
    for (int i = 0; i != 32; ++i)
        producers.emplace_back(
            [&, i]
            {
                barrier.arrive_and_wait();
                const auto& grant = i % 2 ? alice : bob;
                const auto epoch = allocation.members[i % 2 ? 0 : 1]
                                       .roomEpoch;
                if (rooms->ready(
                        grant.lease->binding,
                        grant.snapshot.sessionId,
                        allocation.roomId,
                        first.snapshot.incarnation,
                        epoch,
                        true,
                        [&](auto value)
                        {
                            EXPECT_TRUE(value);
                            ++callbacks;
                        }
                    ))
                    ++accepted;
            }
        );
    barrier.arrive_and_wait();
    for (auto& producer : producers)
        producer.join();
    EXPECT_EQ(accepted, 32);
    EXPECT_EQ(callbacks, 0);
    auto ready = query(alice, 100ms);
    EXPECT_EQ(ready.phase, RoomPhase::Ready);
    EXPECT_EQ(callbacks, 32);
    int stopped = 0;
    ASSERT_TRUE(rooms->status(
        alice.lease->binding,
        alice.snapshot.sessionId,
        [&](auto value)
        {
            ++stopped;
            EXPECT_FALSE(value);
            EXPECT_EQ(value.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    ASSERT_TRUE(rooms->settlement(
        alice.lease->binding,
        alice.snapshot.sessionId,
        1,
        [&](auto value)
        {
            ++stopped;
            EXPECT_FALSE(value);
            EXPECT_EQ(value.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    rooms->stop();
    EXPECT_EQ(stopped, 2);
    timers.readyAfterCancel(timers.timers.size() - 1);
    EXPECT_EQ(stopped, 2);
    EXPECT_FALSE(rooms->status(
        alice.lease->binding,
        alice.snapshot.sessionId,
        [](auto) {}
    ));
}

TEST_F(
    ManagedRoomServiceTest, ReadinessLostBeforeStartReturnsToForming
)
{
    auto alice = login();
    auto bob = login(43, 8);
    start();
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    const auto first = query(alice, 50ms);
    bothReady(alice, bob, allocation, first.snapshot.incarnation);
    EXPECT_EQ(query(alice, 100ms).phase, RoomPhase::Ready);
    markReady(
        alice,
        allocation,
        first.snapshot.incarnation,
        false,
        [](auto result) { EXPECT_TRUE(result); }
    );
    const auto forming = query(bob, 150ms);
    EXPECT_EQ(forming.phase, RoomPhase::Forming);
    EXPECT_EQ(forming.playedTicks, 0U);
    EXPECT_EQ(rooms->metrics().battlesStarted, 0U);
    markReady(
        alice,
        allocation,
        first.snapshot.incarnation,
        true,
        [](auto result) { EXPECT_TRUE(result); }
    );
    EXPECT_EQ(query(bob, 200ms).phase, RoomPhase::Ready);
    EXPECT_EQ(query(bob, 250ms).phase, RoomPhase::Playing);
    EXPECT_EQ(rooms->metrics().battlesStarted, 1U);
}

TEST_F(
    ManagedRoomServiceTest,
    ExpiryKickReentryStopsBeforeTouchingManagedLifecycle
)
{
    std::int64_t wall = 1700000000;
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef";
    config.lifetime = 1s;
    config.now = [&] { return wall; };
    service::auth::JwtTokens shortTokens(std::move(config));
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
    auto bob = login(43, 8);
    start();
    const auto allocation = allocate(*alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    query(bob, 50ms);
    int callbacks = 0;
    ASSERT_TRUE(rooms->status(
        bob.lease->binding,
        bob.snapshot.sessionId,
        [&](auto value)
        {
            ++callbacks;
            EXPECT_FALSE(value);
            EXPECT_EQ(value.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    ++wall;
    at(100ms);
    EXPECT_EQ(kicks, 1);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(rooms->metrics().actors, 0U);
    EXPECT_EQ(rooms->metrics().managedMatches, 0U);
    EXPECT_FALSE(online->managedRoom(allocation.roomId));
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    timers.readyAfterCancel(timers.timers.size() - 1);
    EXPECT_EQ(callbacks, 1);
}

TEST_F(
    ManagedRoomServiceTest,
    ExpiryBeforeFirstCutoffStillProducesCompleteReplayableResult
)
{
    auto alice = login();
    auto bob = login(43, 8);
    start();
    const auto allocation = allocate(alice, bob);
    ASSERT_TRUE(rooms->openMatch(allocation));
    bob.lease->release();
    at(100ms);
    const auto final = query(alice, 150ms);
    ASSERT_TRUE(final.settlement);
    EXPECT_EQ(final.phase, RoomPhase::Finished);
    EXPECT_EQ(
        final.settlement->reason,
        SettlementReason::MemberUnavailable
    );
    EXPECT_EQ(final.settlement->players.size(), 2U);
    EXPECT_EQ(final.playedTicks, 0U);
    const auto log = rooms->replayLog(allocation.roomId);
    ASSERT_TRUE(log);
    auto replay = sim::RoomActor::replay(*log);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, final.snapshot);
}

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
    config = {};
    config.battleDurationTicks = 0;
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    config = {};
    config.readyTimeoutTicks = 0;
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    config = {};
    config.resultRetention = 0ms;
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    config.resultRetention = std::chrono::milliseconds::max();
    EXPECT_THROW(
        (RoomService(online, config)),
        std::invalid_argument
    );
    config.resultRetention = std::chrono::hours(1);
    EXPECT_NO_THROW((RoomService(online, config)));
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
