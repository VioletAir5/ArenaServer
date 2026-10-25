#include "net/MatchmakingService.h"

#include <array>
#include <atomic>
#include <barrier>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include "net/ArenaProtocol.h"
#include "net/MatchHandler.h"
#include "net/PacketBuffer.h"
#include "protocol/EnvelopeCodec.hpp"

#include "../gateway/net/ProtocolTestHarness.h"

using namespace service::gateway;
using namespace std::chrono_literals;
namespace wire = arena::protocol;
namespace
{
struct Scheduler
{
    struct Timer
    {
        std::shared_ptr<bool> live;
        std::function<void()> action;
    };
    std::mutex mutex;
    std::deque<std::function<void()>> posts;
    std::vector<Timer> timers;
    int schedules{0}, failAt{0};
    bool failPost{false};
    ConnectionScheduler get()
    {
        return {
            [this](auto action)
            {
                std::lock_guard lock(mutex);
                if (failPost)
                    throw std::runtime_error("post failed");
                posts.push_back(std::move(action));
            },
            [this](auto, auto action)
            {
                if (++schedules == failAt)
                    throw std::runtime_error("timer failed");
                auto live = std::make_shared<bool>(true);
                timers.push_back({live, std::move(action)});
                return ConnectionTimer([live] { *live = false; });
            }
        };
    }
    void run()
    {
        for (;;)
        {
            std::function<void()> action;
            {
                std::lock_guard lock(mutex);
                if (posts.empty())
                    return;
                action = std::move(posts.front());
                posts.pop_front();
            }
            action();
        }
    }
    void fire(std::size_t index, bool alreadyReady = false)
    {
        auto timer = timers.at(index);
        if (alreadyReady || *timer.live)
            timer.action();
    }
};
service::auth::JwtTokens jwt()
{
    service::auth::JwtConfig c;
    c.key = "0123456789abcdef0123456789abcdef";
    c.now = [] { return std::int64_t{1700000000}; };
    return service::auth::JwtTokens(std::move(c));
}
struct MatchmakingServiceTest : testing::Test
{
    Scheduler scheduler;
    std::shared_ptr<OnlineRegistry>
        online = std::make_shared<OnlineRegistry>();
    service::auth::JwtTokens tokens = jwt();
    std::shared_ptr<MatchmakingService> matcher;
    SessionGrant
    login(std::uint64_t player = 42, std::uint64_t connection = 7)
    {
        auto result = online->install(
            arena::domain::PlayerId{player},
            connection,
            {},
            tokens,
            [] {}
        );
        if (!result)
            throw std::runtime_error("test login failed");
        return std::move(*result);
    }
    void start(std::size_t pending = 1024)
    {
        matcher = std::make_shared<MatchmakingService>(
            online,
            [](auto) -> std::optional<std::uint32_t> { return 1000; },
            service::match::QueueConfig{},
            1ms,
            pending
        );
        matcher->start(scheduler.get());
    }
};

TEST_F(
    MatchmakingServiceTest,
    RejectsInvalidDependenciesAndStartConfiguration
)
{
    auto ratings = [](auto) -> std::optional<std::uint32_t>
    { return 1000; };
    EXPECT_THROW(
        (MatchmakingService({}, ratings)),
        std::invalid_argument
    );
    EXPECT_THROW(
        (MatchmakingService(online, {})),
        std::invalid_argument
    );
    EXPECT_THROW(
        (MatchmakingService(online, ratings, {}, 0ms)),
        std::invalid_argument
    );
    start();
    EXPECT_THROW(matcher->start(scheduler.get()), std::logic_error);
    std::thread foreign(
        [&] { EXPECT_THROW(matcher->metrics(), std::logic_error); }
    );
    foreign.join();
}

TEST_F(
    MatchmakingServiceTest,
    ForeignJoinRunsCompletionOnManagementOwnerAndReplays
)
{
    auto grant = login();
    start();
    std::optional<MatchmakingService::Result> result;
    const auto owner = std::this_thread::get_id();
    std::thread worker(
        [&]
        {
            EXPECT_TRUE(matcher->join(
                grant.lease->binding,
                grant.snapshot.sessionId,
                99,
                [&](auto reply)
                {
                    EXPECT_EQ(std::this_thread::get_id(), owner);
                    result.emplace(std::move(reply));
                }
            ));
        }
    );
    worker.join();
    EXPECT_FALSE(result);
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_TRUE(*result);
    const auto ticket = (**result).ticketId;
    EXPECT_EQ((**result).mmr, 1000U);
    result.reset();
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        99,
        [&](auto reply) { result.emplace(std::move(reply)); }
    ));
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_TRUE(*result);
    EXPECT_EQ((**result).ticketId, ticket);
    EXPECT_EQ(matcher->metrics().joined, 1U);
}

TEST_F(
    MatchmakingServiceTest,
    ThirtyTwoConcurrentSameAndDifferentKeysKeepOneActiveTicket
)
{
    auto grant = login();
    start();
    std::uint64_t identity = 0;
    for (bool differentKeys : {false, true})
    {
        std::barrier gate(33);
        std::array<bool, 32> admitted{};
        std::array<std::uint64_t, 32> tickets{};
        std::vector<std::thread> workers;
        for (std::size_t index = 0; index < 32; ++index)
            workers.emplace_back(
                [&, index]
                {
                    gate.arrive_and_wait();
                    admitted[index] = matcher->join(
                        grant.lease->binding,
                        grant.snapshot.sessionId,
                        differentKeys ? index + 100 : 99,
                        [&, index](auto result)
                        {
                            ASSERT_TRUE(result);
                            tickets[index] = result->ticketId.value();
                        }
                    );
                }
            );
        gate.arrive_and_wait();
        for (auto& worker : workers)
            worker.join();
        scheduler.run();
        if (!identity)
            identity = tickets[0];
        ASSERT_NE(identity, 0U);
        for (std::size_t index = 0; index < 32; ++index)
        {
            EXPECT_TRUE(admitted[index]);
            EXPECT_EQ(tickets[index], identity);
        }
    }
    EXPECT_EQ(matcher->metrics().joined, 1U);
    EXPECT_EQ(matcher->metrics().queued, 1U);
}

TEST_F(
    MatchmakingServiceTest,
    ProducersRacingOwnerStopSettleOnlyAcceptedCommandsExactlyOnce
)
{
    auto grant = login();
    start(64);
    std::atomic<int> callbacks{0};
    std::atomic<int> accepted{0};
    std::array<std::atomic<int>, 32> perRequest{};
    std::array<bool, 32> admitted{};
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto result)
        {
            ++callbacks;
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    ++accepted;
    std::barrier gate(33);
    std::vector<std::thread> workers;
    for (std::size_t index = 0; index < 32; ++index)
        workers.emplace_back(
            [&, index]
            {
                gate.arrive_and_wait();
                admitted[index] = matcher->join(
                    grant.lease->binding,
                    grant.snapshot.sessionId,
                    index + 2,
                    [&, index](auto result)
                    {
                        ++callbacks;
                        ++perRequest[index];
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
    matcher->stop();
    for (auto& worker : workers)
        worker.join();
    EXPECT_EQ(callbacks.load(), accepted.load());
    for (std::size_t index = 0; index < 32; ++index)
        EXPECT_EQ(perRequest[index].load(), admitted[index] ? 1 : 0);
    const auto completed = callbacks.load();
    scheduler.run();
    scheduler.fire(0, true);
    EXPECT_EQ(callbacks.load(), completed);
    EXPECT_EQ(scheduler.timers.size(), 1U);
    EXPECT_EQ(matcher->metrics().queued, 0U);
    EXPECT_EQ(matcher->metrics().retained, 0U);
}

TEST_F(
    MatchmakingServiceTest,
    DeferredOldBindingCannotJoinAfterSessionReplacement
)
{
    auto old = login();
    start();
    std::optional<MatchmakingService::Result> result;
    ASSERT_TRUE(matcher->join(
        old.lease->binding,
        old.snapshot.sessionId,
        1,
        [&](auto reply) { result.emplace(std::move(reply)); }
    ));
    const auto replacement = login(42, 8);
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), wire::ERR_UNAUTHENTICATED);
    EXPECT_EQ(matcher->metrics().joined, 0U);
    EXPECT_TRUE(replacement.lease->current());
}

TEST_F(
    MatchmakingServiceTest,
    JwtRecoveryKeepsTicketButRejectsOldTransportCommand
)
{
    auto grant = login();
    start();
    std::optional<MatchmakingService::Result> joined;
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        9,
        [&](auto reply) { joined.emplace(std::move(reply)); }
    ));
    scheduler.run();
    ASSERT_TRUE(joined);
    ASSERT_TRUE(*joined);
    const auto ticket = (**joined).ticketId;
    std::optional<MatchmakingService::Result> stale;
    ASSERT_TRUE(matcher->cancel(
        grant.lease->binding,
        grant.snapshot.sessionId,
        ticket,
        [&](auto reply) { stale.emplace(std::move(reply)); }
    ));
    grant.lease->release();
    const auto claims = tokens.verify(grant.token);
    ASSERT_TRUE(claims);
    auto resumed = online->install(
        arena::domain::PlayerId{42},
        8,
        *claims,
        tokens,
        [] {}
    );
    ASSERT_TRUE(resumed);
    EXPECT_EQ(resumed->snapshot.sessionId, grant.snapshot.sessionId);
    scheduler.run();
    ASSERT_TRUE(stale);
    ASSERT_FALSE(*stale);
    EXPECT_EQ(stale->error(), wire::ERR_UNAUTHENTICATED);
    std::optional<MatchmakingService::Result> recovered;
    ASSERT_TRUE(matcher->join(
        resumed->lease->binding,
        resumed->snapshot.sessionId,
        9,
        [&](auto reply) { recovered.emplace(std::move(reply)); }
    ));
    scheduler.run();
    ASSERT_TRUE(recovered);
    ASSERT_TRUE(*recovered);
    EXPECT_EQ((**recovered).ticketId, ticket);
    EXPECT_EQ(matcher->metrics().joined, 1U);
}

TEST_F(
    MatchmakingServiceTest,
    SaturationRejectsWithoutCallbackAndStopSettlesAcceptedOnce
)
{
    auto grant = login();
    start(1);
    int accepted = 0, rejected = 0;
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto result)
        {
            ++accepted;
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), wire::ERR_UNAVAILABLE);
        }
    ));
    EXPECT_FALSE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        2,
        [&](auto) { ++rejected; }
    ));
    matcher->stop();
    EXPECT_EQ(accepted, 1);
    EXPECT_EQ(rejected, 0);
    scheduler.run();
    scheduler.fire(0, true);
    EXPECT_EQ(accepted, 1);
    EXPECT_EQ(scheduler.timers.size(), 1U);
    EXPECT_FALSE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        3,
        [&](auto) { ++rejected; }
    ));
    EXPECT_EQ(matcher->metrics().queued, 0U);
    EXPECT_EQ(matcher->metrics().retained, 0U);
}

TEST_F(
    MatchmakingServiceTest,
    FailedPostDoesNotRetainCommandOrConsumeCapacity
)
{
    auto grant = login();
    start(1);
    int calls = 0;
    scheduler.failPost = true;
    EXPECT_FALSE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto) { ++calls; }
    ));
    scheduler.failPost = false;
    EXPECT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto result)
        {
            ++calls;
            EXPECT_TRUE(result);
        }
    ));
    scheduler.run();
    EXPECT_EQ(calls, 1);
}

TEST_F(
    MatchmakingServiceTest,
    TimerRearmFailureClosesMailboxAndClearsQueue
)
{
    auto grant = login();
    start();
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [](auto result) { EXPECT_TRUE(result); }
    ));
    scheduler.run();
    EXPECT_EQ(matcher->metrics().queued, 1U);
    scheduler.failAt = 2;
    scheduler.fire(0);
    EXPECT_EQ(matcher->metrics().joined, 1U);
    EXPECT_EQ(matcher->metrics().queued, 0U);
    EXPECT_EQ(matcher->metrics().retained, 0U);
    EXPECT_FALSE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        2,
        [](auto) {}
    ));
}

TEST_F(
    MatchmakingServiceTest,
    RetainedOwnerCallbacksAfterDestructionAreHarmless
)
{
    auto grant = login();
    start();
    int calls = 0;
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto) { ++calls; }
    ));
    matcher.reset();
    EXPECT_EQ(calls, 1);
    scheduler.run();
    scheduler.fire(0, true);
    EXPECT_EQ(calls, 1);
}

TEST_F(
    MatchmakingServiceTest,
    CompletionExceptionsDoNotBlockLaterCommands
)
{
    auto grant = login();
    start();
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [](auto) { throw std::runtime_error("client completion"); }
    ));
    int calls = 0;
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto result)
        {
            ++calls;
            EXPECT_TRUE(result);
        }
    ));
    EXPECT_NO_THROW(scheduler.run());
    EXPECT_EQ(calls, 1);
}

TEST_F(
    MatchmakingServiceTest,
    MissingRatingAndExistingRoomRejectAdmission
)
{
    auto grant = login();
    matcher = std::make_shared<MatchmakingService>(
        online,
        [](auto) -> std::optional<std::uint32_t> { return {}; }
    );
    matcher->start(scheduler.get());
    std::optional<MatchmakingService::Result> result;
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto r) { result.emplace(std::move(r)); }
    ));
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), wire::ERR_UNAVAILABLE);
    ASSERT_TRUE(online->joinRoom(
        grant.lease->binding,
        arena::domain::RoomId{77}
    ));
    result.reset();
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [&](auto r) { result.emplace(std::move(r)); }
    ));
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), wire::ERR_BUSINESS_FAILURE);
}

TEST_F(
    MatchmakingServiceTest,
    RetryReturnsAdmissionRatingWithoutCallingChangedOrUnavailableProvider
)
{
    auto grant = login();
    std::optional<std::uint32_t> rating = 1000;
    bool throwing = false;
    int providerCalls = 0;
    matcher = std::make_shared<MatchmakingService>(
        online,
        [&](auto) -> std::optional<std::uint32_t>
        {
            ++providerCalls;
            if (throwing)
                throw std::runtime_error("rating unavailable");
            return rating;
        }
    );
    matcher->start(scheduler.get());
    std::uint64_t identity = 0;
    const auto retry = [&](std::uint64_t key)
    {
        std::optional<MatchmakingService::Result> result;
        EXPECT_TRUE(matcher->join(
            grant.lease->binding,
            grant.snapshot.sessionId,
            key,
            [&](auto value) { result.emplace(std::move(value)); }
        ));
        scheduler.run();
        EXPECT_TRUE(result);
        if (!result)
            return;
        EXPECT_TRUE(*result);
        if (!*result)
            return;
        EXPECT_EQ((**result).mmr, 1000U);
        if (!identity)
            identity = (**result).ticketId.value();
        EXPECT_EQ((**result).ticketId.value(), identity);
    };
    retry(7);
    ASSERT_EQ(providerCalls, 1);
    rating = 1100;
    retry(7);
    rating.reset();
    retry(7);
    retry(8);
    throwing = true;
    retry(7);
    retry(9);
    EXPECT_EQ(providerCalls, 1);
    throwing = false;
    auto newcomer = login(43, 8);
    std::optional<MatchmakingService::Result> result;
    ASSERT_TRUE(matcher->join(
        newcomer.lease->binding,
        newcomer.snapshot.sessionId,
        1,
        [&](auto value) { result.emplace(std::move(value)); }
    ));
    scheduler.run();
    ASSERT_TRUE(result);
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), wire::ERR_UNAVAILABLE);
    EXPECT_EQ(providerCalls, 2);
}

class ThrowingSink final : public spdlog::sinks::base_sink<std::mutex>
{
    void sink_it_(const spdlog::details::log_msg&) override
    {
        throw std::runtime_error("log allocation");
    }
    void flush_() override
    {
    }
};
TEST_F(
    MatchmakingServiceTest,
    LogFailureCannotStopMatchingUpkeepOrClearAcceptedQueue
)
{
    auto grant = login();
    start();
    ASSERT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [](auto result) { EXPECT_TRUE(result); }
    ));
    scheduler.run();
    struct RestoreDefault
    {
        std::shared_ptr<spdlog::logger>
            original = spdlog::default_logger();
        ~RestoreDefault()
        {
            spdlog::set_default_logger(std::move(original));
        }
    } restore;
    auto throwing = std::make_shared<spdlog::logger>(
        "throwing-match-test",
        std::make_shared<ThrowingSink>()
    );
    throwing->set_error_handler(
        [](const std::string&)
        { throw std::runtime_error("log handler failed"); }
    );
    spdlog::set_default_logger(throwing);
    EXPECT_NO_THROW(scheduler.fire(0));
    EXPECT_EQ(scheduler.timers.size(), 2U);
    EXPECT_EQ(matcher->metrics().queued, 1U);
    EXPECT_TRUE(matcher->join(
        grant.lease->binding,
        grant.snapshot.sessionId,
        1,
        [](auto result) { EXPECT_TRUE(result); }
    ));
    scheduler.run();
}

struct MatchRouteTest : service::gateway::test::ProtocolTest
{
    Scheduler manager;
    std::shared_ptr<OnlineRegistry>
        online = std::make_shared<OnlineRegistry>();
    service::auth::JwtTokens tokens = jwt();
    std::shared_ptr<MatchmakingService>
        matcher = std::make_shared<MatchmakingService>(
            online,
            [](auto) -> std::optional<std::uint32_t> { return 1200; }
        );
    void setup(bool authenticated = true)
    {
        matcher->start(manager.get());
        auto registry = std::make_shared<MessageRegistry>();
        registerMatchRoutes(*registry, online, matcher);
        registry->seal();
        start(makeArenaProtocolHandler(context, registry));
        if (authenticated)
        {
            auto grant = online->install(
                arena::domain::PlayerId{42},
                1,
                {},
                tokens,
                [] {}
            );
            ASSERT_TRUE(grant);
            ASSERT_TRUE(context->session->bind(grant->lease));
        }
    }
    void TearDown() override
    {
        if (host)
        {
            host->close();
            host.reset();
        }
        matcher->stop();
    }
    wire::Envelope rpc(wire::Envelope request)
    {
        auto encoded = service::protocol::EnvelopeCodec::encode(
            request
        );
        EXPECT_TRUE(encoded);
        if (!encoded)
            return {};
        receive(frame(*encoded));
        manager.run();
        wire::Envelope reply;
        service::protocol::PacketCodec codec(kMaxPacketSize);
        int count = 0;
        codec.feed(
            std::as_bytes(std::span(transport->output)),
            [&](auto header, auto payload)
            {
                EXPECT_EQ(header.m_requestId, 7);
                ++count;
                EXPECT_TRUE(
                    service::protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(payload)),
                        reply
                    )
                );
                return true;
            }
        );
        EXPECT_EQ(count, 1);
        transport->output.clear();
        return reply;
    }
};

TEST_F(
    MatchRouteTest,
    ExplicitAndLegacyJoinStatusCancelPreserveCorrelationAndIdentity
)
{
    setup();
    wire::Envelope join;
    join.set_request_id(101);
    join.set_msg_id(wire::MSG_MATCH_JOIN_REQUEST);
    join.mutable_match_join_request()->set_join_key(700);
    auto reply = rpc(join);
    ASSERT_TRUE(reply.has_match_join_response());
    EXPECT_EQ(reply.request_id(), 101U);
    const auto ticket = reply.match_join_response().state();
    EXPECT_EQ(ticket.player_id(), 42U);
    EXPECT_EQ(ticket.mmr(), 1200U);
    EXPECT_EQ(ticket.join_key(), 700U);
    wire::Envelope status;
    status.set_request_id(
        102
    ); // Legacy inference uses the known payload with msg_id=0.
    status.mutable_match_status_request()->set_ticket_id(
        ticket.ticket_id()
    );
    reply = rpc(status);
    ASSERT_TRUE(reply.has_match_status_response());
    EXPECT_EQ(reply.request_id(), 102U);
    EXPECT_EQ(
        reply.match_status_response().state().ticket_id(),
        ticket.ticket_id()
    );
    wire::Envelope cancel;
    cancel.set_request_id(103);
    cancel.set_msg_id(wire::MSG_MATCH_CANCEL_REQUEST);
    cancel.mutable_match_cancel_request()->set_ticket_id(
        ticket.ticket_id()
    );
    reply = rpc(cancel);
    ASSERT_TRUE(reply.has_match_cancel_response());
    EXPECT_EQ(reply.request_id(), 103U);
    EXPECT_EQ(
        reply.match_cancel_response().state().state(),
        wire::MATCH_TICKET_CANCELLED
    );
}

TEST_F(MatchRouteTest, RejectsUnauthenticatedMatchJoin)
{
    setup(false);
    wire::Envelope join;
    join.set_request_id(1);
    join.set_msg_id(wire::MSG_MATCH_JOIN_REQUEST);
    join.mutable_match_join_request()->set_join_key(1);
    EXPECT_EQ(rpc(join).error().code(), wire::ERR_UNAUTHENTICATED);
}
TEST_F(
    MatchRouteTest,
    RejectsZeroIdentityAndWrongPayloadBeforeQueueMutation
)
{
    setup();
    wire::Envelope join;
    join.set_request_id(1);
    join.set_msg_id(wire::MSG_MATCH_JOIN_REQUEST);
    join.mutable_match_join_request();
    EXPECT_EQ(rpc(join).error().code(), wire::ERR_INVALID_REQUEST);
    join.set_request_id(2);
    join.mutable_match_cancel_request()->set_ticket_id(1);
    EXPECT_EQ(rpc(join).error().code(), wire::ERR_INVALID_REQUEST);
    EXPECT_EQ(matcher->metrics().joined, 0U);
}

TEST_F(
    MatchmakingServiceTest,
    OwnerAllocatesPairOnceAndRetriesRecoverSameRoom
)
{
    const auto alice = login(42, 7), bob = login(43, 8);
    std::optional<ManagedRoom> room;
    int calls = 0;
    matcher = std::make_shared<MatchmakingService>(
        online,
        [](auto) -> std::optional<std::uint32_t> { return 1000; },
        service::match::QueueConfig{},
        1ms,
        1024,
        [&](const ManagedRoom& allocated)
        {
            ++calls;
            room = allocated;
            return true;
        }
    );
    matcher->start(scheduler.get());
    std::optional<service::match::TicketSnapshot> first, second;
    ASSERT_TRUE(matcher->join(
        alice.lease->binding,
        alice.snapshot.sessionId,
        10,
        [&](auto r)
        {
            ASSERT_TRUE(r);
            first = *r;
        }
    ));
    ASSERT_TRUE(matcher->join(
        bob.lease->binding,
        bob.snapshot.sessionId,
        20,
        [&](auto r)
        {
            ASSERT_TRUE(r);
            second = *r;
        }
    ));
    scheduler.run();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    scheduler.fire(0);
    ASSERT_TRUE(room);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(matcher->metrics().assignedPlayers, 0U);
    EXPECT_EQ(matcher->metrics().releasedPairs, 1U);
    EXPECT_EQ(
        online->snapshot(alice.snapshot.playerId)->roomId,
        room->roomId
    );
    EXPECT_EQ(
        online->snapshot(bob.snapshot.playerId)->roomId,
        room->roomId
    );
    for (const auto& saved : {first.value(), second.value()})
    {
        const auto& binding = saved.playerId ==
                                      alice.snapshot.playerId
                                  ? alice.lease->binding
                                  : bob.lease->binding;
        std::optional<MatchmakingService::Result> status;
        ASSERT_TRUE(matcher->status(
            binding,
            saved.sessionId,
            saved.ticketId,
            [&](auto r) { status.emplace(std::move(r)); }
        ));
        scheduler.run();
        ASSERT_TRUE(status);
        ASSERT_TRUE(*status);
        EXPECT_EQ((**status).roomId, room->roomId);
        EXPECT_FALSE((**status).active);
    }
    const auto claims = tokens.verify(alice.token);
    ASSERT_TRUE(claims);
    alice.lease->release();
    auto resumed = online->install(
        alice.snapshot.playerId,
        9,
        *claims,
        tokens,
        [] {}
    );
    ASSERT_TRUE(resumed);
    std::optional<MatchmakingService::Result> retry;
    ASSERT_TRUE(matcher->join(
        resumed->lease->binding,
        resumed->snapshot.sessionId,
        10,
        [&](auto r) { retry.emplace(std::move(r)); }
    ));
    scheduler.run();
    ASSERT_TRUE(retry);
    ASSERT_TRUE(*retry);
    EXPECT_EQ((**retry).ticketId, first->ticketId);
    EXPECT_EQ((**retry).roomId, room->roomId);
    retry.reset();
    ASSERT_TRUE(matcher->join(
        resumed->lease->binding,
        resumed->snapshot.sessionId,
        11,
        [&](auto r) { retry.emplace(std::move(r)); }
    ));
    scheduler.run();
    ASSERT_TRUE(retry);
    ASSERT_FALSE(*retry);
    EXPECT_EQ(retry->error(), wire::ERR_BUSINESS_FAILURE);
    scheduler.fire(1);
    EXPECT_EQ(calls, 1);
}

TEST_F(
    MatchmakingServiceTest,
    FailedAllocatorRollsBackBothBindingsAndReleasesFences
)
{
    for (const bool throwing : {false, true})
    {
        if (matcher)
            matcher->stop();
        const auto alice = login(42, 7), bob = login(43, 8);
        const auto timer = scheduler.timers.size();
        int calls = 0;
        matcher = std::make_shared<MatchmakingService>(
            online,
            [](auto) -> std::optional<std::uint32_t> { return 1000; },
            service::match::QueueConfig{},
            1ms,
            1024,
            [&](const ManagedRoom&) -> bool
            {
                ++calls;
                if (throwing)
                    throw 7;
                return false;
            }
        );
        matcher->start(scheduler.get());
        std::optional<service::match::TicketSnapshot> first;
        ASSERT_TRUE(matcher->join(
            alice.lease->binding,
            alice.snapshot.sessionId,
            10,
            [&](auto r)
            {
                ASSERT_TRUE(r);
                first = *r;
            }
        ));
        ASSERT_TRUE(matcher->join(
            bob.lease->binding,
            bob.snapshot.sessionId,
            20,
            [](auto r) { EXPECT_TRUE(r); }
        ));
        scheduler.run();
        scheduler.fire(timer);
        ASSERT_TRUE(first);
        EXPECT_EQ(calls, 1);
        EXPECT_EQ(online->roomCount(), 0U);
        EXPECT_FALSE(
            online->snapshot(alice.snapshot.playerId)->roomId
        );
        EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
        EXPECT_EQ(matcher->metrics().assignedPlayers, 0U);
        std::optional<MatchmakingService::Result> status;
        ASSERT_TRUE(matcher->status(
            alice.lease->binding,
            alice.snapshot.sessionId,
            first->ticketId,
            [&](auto r) { status.emplace(std::move(r)); }
        ));
        scheduler.run();
        ASSERT_TRUE(status);
        ASSERT_FALSE(*status);
        EXPECT_EQ(status->error(), wire::ERR_UNAVAILABLE);
        status.reset();
        ASSERT_TRUE(matcher->join(
            alice.lease->binding,
            alice.snapshot.sessionId,
            11,
            [&](auto r) { status.emplace(std::move(r)); }
        ));
        scheduler.run();
        ASSERT_TRUE(status);
        ASSERT_TRUE(*status);
        EXPECT_EQ(
            (**status).state,
            service::match::TicketState::Queued
        );
    }
}

TEST_F(
    MatchmakingServiceTest,
    RatingCallbackStopDoesNotDestroyActiveQueueFrame
)
{
    const auto alice = login();
    matcher = std::make_shared<MatchmakingService>(
        online,
        [&](auto) -> std::optional<std::uint32_t>
        {
            matcher->stop();
            return 1000;
        }
    );
    matcher->start(scheduler.get());
    std::optional<MatchmakingService::Result> reply;
    ASSERT_TRUE(matcher->join(
        alice.lease->binding,
        alice.snapshot.sessionId,
        1,
        [&](auto r) { reply.emplace(std::move(r)); }
    ));
    EXPECT_NO_THROW(scheduler.run());
    ASSERT_TRUE(reply);
    ASSERT_FALSE(*reply);
    EXPECT_EQ(reply->error(), wire::ERR_UNAVAILABLE);
    EXPECT_EQ(matcher->metrics().retained, 0U);
}

TEST_F(
    MatchmakingServiceTest,
    ExpiryKickStopDefersRetirementUntilEligibilityReturns
)
{
    std::int64_t wall = 1700000000;
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef";
    config.lifetime = 1s;
    config.now = [&] { return wall; };
    service::auth::JwtTokens expiring{config};
    auto grant = online->install(
        arena::domain::PlayerId{42},
        7,
        {},
        expiring,
        [&] { matcher->stop(); }
    );
    ASSERT_TRUE(grant);
    start();
    ASSERT_TRUE(matcher->join(
        grant->lease->binding,
        grant->snapshot.sessionId,
        1,
        [](auto r) { EXPECT_TRUE(r); }
    ));
    scheduler.run();
    ++wall;
    EXPECT_NO_THROW(scheduler.fire(0));
    EXPECT_EQ(matcher->metrics().retained, 0U);
    EXPECT_EQ(scheduler.timers.size(), 1U);
    EXPECT_FALSE(matcher->join(
        grant->lease->binding,
        grant->snapshot.sessionId,
        2,
        [](auto) {}
    ));
}

TEST_F(
    MatchmakingServiceTest,
    AllocatorStopRollsBackBoundRoomBeforeQueueRetirement
)
{
    const auto alice = login(42, 7), bob = login(43, 8);
    matcher = std::make_shared<MatchmakingService>(
        online,
        [](auto) -> std::optional<std::uint32_t> { return 1000; },
        service::match::QueueConfig{},
        1ms,
        1024,
        [&](const ManagedRoom&)
        {
            matcher->stop();
            return true;
        }
    );
    matcher->start(scheduler.get());
    ASSERT_TRUE(matcher->join(
        alice.lease->binding,
        alice.snapshot.sessionId,
        1,
        [](auto r) { EXPECT_TRUE(r); }
    ));
    ASSERT_TRUE(matcher->join(
        bob.lease->binding,
        bob.snapshot.sessionId,
        1,
        [](auto r) { EXPECT_TRUE(r); }
    ));
    scheduler.run();
    EXPECT_NO_THROW(scheduler.fire(0));
    EXPECT_EQ(online->roomCount(), 0U);
    EXPECT_FALSE(online->snapshot(alice.snapshot.playerId)->roomId);
    EXPECT_FALSE(online->snapshot(bob.snapshot.playerId)->roomId);
    EXPECT_EQ(matcher->metrics().retained, 0U);
}

} // namespace
