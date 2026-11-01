#include "lobby/AuthClient.h"

#include <chrono>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "rpc/Server.h"

namespace service::lobby
{
namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Server = arena::internal_rpc::Server;
using Context = arena::internal_rpc::Context;
using Result = AuthClient::Result;

std::int64_t civilNow()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
    )
        .count();
}

enum class Mode
{
    Good,
    MaxLifetime,
    Hold,
    VersionError,
    CycleError,
    Malformed,
    WrongEpoch,
    ZeroPlayer,
    Expired,
    TooFuture,
    BadToken,
    WrongResume,
    WrongPrior
};

struct Peer
{
    Mode mode{Mode::Good};
    std::size_t calls{0}, cancellations{0};
    std::vector<Context> contexts;
    std::vector<Server::Completion> held;
    std::unique_ptr<Server> server;
    explicit Peer(boost::asio::io_context& io)
    {
        Server::Config config;
        config.service = "auth";
        config.callerTokens = {{"lobby", std::string(48, 'a')}};
        Server::Handlers handlers;
        handlers.emplace(
            "auth.authenticate",
            [this](auto request, auto context, auto complete)
            {
                ++calls;
                contexts.push_back(context);
                if (mode == Mode::Hold)
                {
                    held.push_back(std::move(complete));
                    return Server::Cancellation([this]
                                                { ++cancellations; });
                }
                arena::auth::AuthenticateRequest input;
                EXPECT_TRUE(input.ParseFromString(request.payload()));
                arena::rpc::Response response;
                if (mode == Mode::VersionError ||
                    mode == Mode::CycleError)
                    response.set_status(
                        mode == Mode::VersionError
                            ? arena::rpc::INCOMPATIBLE_VERSION
                            : arena::rpc::CALL_CYCLE
                    );
                else if (mode == Mode::Malformed)
                    response.set_payload(
                        std::string(1, static_cast<char>(0xff))
                    );
                else
                {
                    arena::auth::AuthenticateReply reply;
                    reply.set_player_id(
                        mode == Mode::ZeroPlayer ? 0 : 42
                    );
                    reply.set_epoch(
                        input.proposed_epoch() +
                        (mode == Mode::WrongEpoch)
                    );
                    reply.set_expires_at_unix(
                        civilNow() +
                        (mode == Mode::Expired       ? -1
                         : mode == Mode::TooFuture   ? 90000
                         : mode == Mode::MaxLifetime ? 86400
                                                     : 300)
                    );
                    reply.set_session_token(
                        mode == Mode::BadToken
                            ? "invalid token"
                            : "aGVhZGVy.cGF5bG9hZA.c2lnbmF0dXJl"
                    );
                    const bool
                        jwt = input.credential_kind() ==
                              arena::auth::AuthenticateRequest::JWT;
                    reply.set_resumed(
                        mode == Mode::WrongResume ? !jwt : jwt
                    );
                    if (jwt)
                    {
                        reply.set_previous_epoch(
                            mode == Mode::WrongPrior
                                ? input.proposed_epoch()
                                : input.proposed_epoch() - 1
                        );
                        // A renewed token may legitimately have a
                        // shorter lifetime.
                        reply.set_previous_expires_at_unix(
                            civilNow() + 600
                        );
                    }
                    else if (mode == Mode::WrongPrior)
                    {
                        reply.set_previous_epoch(1);
                        reply.set_previous_expires_at_unix(
                            civilNow() + 300
                        );
                    }
                    response.set_payload(reply.SerializeAsString());
                }
                complete(std::move(response));
                return Server::Cancellation{};
            }
        );
        server = std::make_unique<Server>(
            io.get_executor(),
            std::move(config),
            std::move(handlers)
        );
    }
    void release()
    {
        for (auto& complete : held)
        {
            arena::rpc::Response response;
            response.set_status(arena::rpc::CANCELLED);
            complete(std::move(response));
        }
        held.clear();
    }
};

struct Harness
{
    boost::asio::io_context io;
    Peer first{io}, second{io};
    std::unique_ptr<AuthClient> client;
    Harness(std::size_t maximum = 8)
    {
        auto value = config();
        value.maxInflight = maximum;
        client = std::make_unique<AuthClient>(
            io.get_executor(),
            std::move(value)
        );
    }
    AuthClient::Config config()
    {
        AuthClient::Config result;
        result.endpoint = {
            boost::asio::ip::address_v4::loopback(),
            first.server->port()
        };
        result.token = std::string(48, 'a');
        return result;
    }
    ~Harness()
    {
        client->stop();
        first.release();
        second.release();
        first.server->stop();
        second.server->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return !client->inflight() &&
                       first.server->stats().stopped &&
                       second.server->stats().stopped;
            }
        ));
        client.reset();
        first.server.reset();
        second.server.reset();
        io.restart();
        io.poll();
    }
    template <class Predicate>
    bool
    until(Predicate predicate, std::chrono::milliseconds budget = 2s)
    {
        const auto end = Clock::now() + budget;
        while (!predicate() && Clock::now() < end)
        {
            io.restart();
            io.run_for(1ms);
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    static arena::auth::AuthenticateRequest
    request(std::uint64_t epoch = 17, bool jwt = false)
    {
        arena::auth::AuthenticateRequest result;
        result.set_proposed_epoch(epoch);
        result.set_credential_kind(
            jwt ? arena::auth::AuthenticateRequest::JWT
                : arena::auth::AuthenticateRequest::PASSWORD
        );
        if (!jwt)
            result.set_account("alice");
        result.set_credential(
            jwt ? "old.opaque.token" : "private fixture password"
        );
        return result;
    }
    static Context context(std::chrono::milliseconds budget = 1s)
    {
        return {
            Clock::now() + budget,
            "lobby-auth-parent",
            {"gateway", "lobby"}
        };
    }
    Result call(arena::auth::AuthenticateRequest input = request())
    {
        std::optional<Result> result;
        auto accepted = client->authenticate(
            std::move(input),
            context(),
            [&](auto value) { result = std::move(value); }
        );
        if (!accepted)
            throw std::runtime_error("fixture admission failed");
        if (!until([&] { return result.has_value(); }))
            throw std::runtime_error("fixture reply timed out");
        return std::move(*result);
    }
    arena::runtime::ConfigSnapshot::Ptr
    snapshot(std::uint64_t version, bool firstReady, bool secondReady)
    {
        const auto node =
            [&](const char* id, std::uint16_t port, bool ready)
        {
            return std::string(
                       "{\"service\":\"auth\",\"nodeId\":\""
                   ) +
                   id + "\",\"host\":\"127.0.0.1\",\"port\":" +
                   std::to_string(port) +
                   ",\"ready\":" + (ready ? "true" : "false") + "}";
        };
        const auto document = std::string("{\"version\":\"") +
                              std::to_string(version) +
                              "\",\"desiredReady\":true,"
                              "\"drainGraceMs\":1000,\"nodes\":[" +
                              node(
                                  "auth-a",
                                  first.server->port(),
                                  firstReady
                              ) +
                              "," +
                              node(
                                  "auth-b",
                                  second.server->port(),
                                  secondReady
                              ) +
                              "]}";
        auto result = arena::runtime::ConfigSnapshot::parse(document);
        if (!result)
            throw std::runtime_error("invalid test snapshot");
        return *result;
    }
};

TEST(
    AuthClientTest,
    TypedPasswordAndJwtPreserveParentMetadataAndShorterRenewal
)
{
    Harness fixture;
    auto password = fixture.call();
    ASSERT_TRUE(password);
    EXPECT_EQ(password->player_id(), 42);
    EXPECT_EQ(password->epoch(), 17);
    EXPECT_FALSE(password->resumed());
    auto jwt = fixture.call(Harness::request(18, true));
    ASSERT_TRUE(jwt);
    EXPECT_TRUE(jwt->resumed());
    EXPECT_EQ(jwt->previous_epoch(), 17);
    EXPECT_LT(
        jwt->expires_at_unix(),
        jwt->previous_expires_at_unix()
    );
    ASSERT_EQ(fixture.first.contexts.size(), 2);
    EXPECT_EQ(
        fixture.first.contexts[0].correlationId,
        "lobby-auth-parent"
    );
    EXPECT_EQ(
        fixture.first.contexts[0].servicePath,
        (std::vector<std::string>{"gateway", "lobby", "auth"})
    );
    EXPECT_LE(fixture.first.contexts[0].deadline, Clock::now() + 1s);
    fixture.first.mode = Mode::MaxLifetime;
    auto maximum = fixture.call(
        Harness::request(std::numeric_limits<std::uint64_t>::max())
    );
    ASSERT_TRUE(maximum);
    EXPECT_EQ(
        maximum->epoch(),
        std::numeric_limits<std::uint64_t>::max()
    );
    EXPECT_EQ(fixture.client->inflight(), 0);
}

TEST(AuthClientTest, RemoteVersionAndCycleErrorsRemainTyped)
{
    Harness fixture;
    fixture.first.mode = Mode::VersionError;
    auto version = fixture.call();
    ASSERT_FALSE(version);
    EXPECT_EQ(version.error(), arena::rpc::INCOMPATIBLE_VERSION);
    fixture.first.mode = Mode::CycleError;
    auto cycle = fixture.call();
    ASSERT_FALSE(cycle);
    EXPECT_EQ(cycle.error(), arena::rpc::CALL_CYCLE);
    EXPECT_EQ(fixture.client->inflight(), 0);
}

TEST(
    AuthClientTest,
    MalformedAndInconsistentSignerCandidatesAreRejected
)
{
    Harness fixture;
    for (auto mode :
         {Mode::Malformed,
          Mode::WrongEpoch,
          Mode::ZeroPlayer,
          Mode::Expired,
          Mode::TooFuture,
          Mode::BadToken,
          Mode::WrongResume,
          Mode::WrongPrior})
    {
        fixture.first.mode = mode;
        auto response = fixture.call();
        ASSERT_FALSE(response);
        EXPECT_EQ(response.error(), arena::rpc::INTERNAL);
        EXPECT_EQ(fixture.client->inflight(), 0);
    }
    fixture.first.mode = Mode::WrongPrior;
    auto prior = fixture.call(Harness::request(18, true));
    ASSERT_FALSE(prior);
    EXPECT_EQ(prior.error(), arena::rpc::INTERNAL);
}

TEST(
    AuthClientTest,
    DeadlineCancelAndBoundedAdmissionUseTheExistingWeakHandle
)
{
    Harness fixture(1);
    fixture.first.mode = Mode::Hold;
    std::optional<Result> expired;
    const auto started = Clock::now();
    auto call = fixture.client->authenticate(
        Harness::request(),
        Harness::context(60ms),
        [&](auto value) { expired = std::move(value); }
    );
    ASSERT_TRUE(call);
    bool rejectedCalled = false;
    auto rejected = fixture.client->authenticate(
        Harness::request(18),
        Harness::context(),
        [&](auto) { rejectedCalled = true; }
    );
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), arena::rpc::OVERLOADED);
    ASSERT_TRUE(fixture.until([&] { return expired.has_value(); }));
    ASSERT_FALSE(*expired);
    EXPECT_EQ(expired->error(), arena::rpc::DEADLINE_EXCEEDED);
    EXPECT_LT(Clock::now() - started, 250ms);
    EXPECT_FALSE(rejectedCalled);
    EXPECT_EQ(fixture.client->inflight(), 0);
    fixture.first.release();
    ASSERT_TRUE(fixture.until(
        [&] { return fixture.first.server->stats().active == 0; }
    ));
    std::optional<Result> cancelled;
    auto next = fixture.client->authenticate(
        Harness::request(19),
        Harness::context(),
        [&](auto value) { cancelled = std::move(value); }
    );
    ASSERT_TRUE(next);
    next->cancel();
    next->cancel();
    ASSERT_TRUE(fixture.until([&] { return cancelled.has_value(); }));
    ASSERT_FALSE(*cancelled);
    EXPECT_EQ(cancelled->error(), arena::rpc::CANCELLED);
    EXPECT_EQ(fixture.client->inflight(), 0);
}

TEST(
    AuthClientTest,
    RouteIsCapturedAtAdmissionAndEmptyDiscoveryNeverFallsBack
)
{
    Harness fixture;
    auto store = std::make_shared<arena::runtime::ConfigStore>(
        fixture.snapshot(1, true, false)
    );
    auto config = fixture.config();
    config.runtime = store;
    fixture.client = std::make_unique<AuthClient>(
        fixture.io.get_executor(),
        std::move(config)
    );
    std::optional<Result> oldReply, newReply;
    auto old = fixture.client->authenticate(
        Harness::request(),
        Harness::context(),
        [&](auto r) { oldReply = std::move(r); }
    );
    ASSERT_TRUE(old);
    ASSERT_TRUE(store->publish(fixture.snapshot(2, false, true)));
    auto next = fixture.client->authenticate(
        Harness::request(18),
        Harness::context(),
        [&](auto r) { newReply = std::move(r); }
    );
    ASSERT_TRUE(next);
    ASSERT_TRUE(fixture.until([&] { return oldReply && newReply; }));
    ASSERT_TRUE(*oldReply);
    ASSERT_TRUE(*newReply);
    EXPECT_EQ(fixture.first.calls, 1);
    EXPECT_EQ(fixture.second.calls, 1);
    ASSERT_TRUE(store->publish(fixture.snapshot(3, false, false)));
    bool called = false;
    auto absent = fixture.client->authenticate(
        Harness::request(19),
        Harness::context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error(), arena::rpc::UNAVAILABLE);
    fixture.io.restart();
    fixture.io.poll();
    EXPECT_FALSE(called);
    EXPECT_EQ(fixture.first.calls, 1);
    EXPECT_EQ(fixture.second.calls, 1);
    ASSERT_TRUE(store->publish(fixture.snapshot(4, true, false)));
    ASSERT_TRUE(fixture.call(Harness::request(20)));
    EXPECT_EQ(fixture.first.calls, 2);
}

TEST(
    AuthClientTest,
    InvalidInputAndInheritedCycleNeverCreateTransportWork
)
{
    Harness fixture;
    bool called = false;
    auto invalid = fixture.client->authenticate(
        Harness::request(0),
        Harness::context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), arena::rpc::INVALID_ARGUMENT);
    auto parent = Harness::context();
    parent.servicePath = {"gateway", "auth", "lobby"};
    auto cycle = fixture.client->authenticate(
        Harness::request(),
        parent,
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(cycle);
    EXPECT_EQ(cycle.error(), arena::rpc::CALL_CYCLE);
    parent = Harness::context();
    parent.deadline = Clock::now() - 1ms;
    auto expired = fixture.client->authenticate(
        Harness::request(),
        parent,
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error(), arena::rpc::DEADLINE_EXCEEDED);
    EXPECT_FALSE(called);
    EXPECT_EQ(fixture.first.calls, 0);
    EXPECT_EQ(fixture.client->inflight(), 0);
}

TEST(
    AuthClientTest, StopCancelsAcceptedCallsAndDescriptorCountRecovers
)
{
    const auto descriptors = []
    {
        return std::distance(
            std::filesystem::directory_iterator("/proc/self/fd"),
            std::filesystem::directory_iterator{}
        );
    };
    const auto baseline = descriptors();
    {
        Harness fixture;
        fixture.first.mode = Mode::Hold;
        std::vector<Result> replies;
        auto first = fixture.client->authenticate(
            Harness::request(),
            Harness::context(),
            [&](auto r) { replies.push_back(std::move(r)); }
        );
        auto second = fixture.client->authenticate(
            Harness::request(18),
            Harness::context(),
            [&](auto r) { replies.push_back(std::move(r)); }
        );
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
        ASSERT_TRUE(fixture.until(
            [&] { return fixture.first.server->stats().active == 2; }
        ));
        fixture.client->stop();
        fixture.client->stop();
        bool called = false;
        auto rejected = fixture.client->authenticate(
            Harness::request(19),
            Harness::context(),
            [&](auto) { called = true; }
        );
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), arena::rpc::UNAVAILABLE);
        ASSERT_TRUE(fixture.until([&]
                                  { return replies.size() == 2; }));
        for (const auto& result : replies)
        {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), arena::rpc::CANCELLED);
        }
        EXPECT_FALSE(called);
        EXPECT_EQ(fixture.client->inflight(), 0);
        fixture.first.release();
        ASSERT_TRUE(fixture.until(
            [&] { return fixture.first.server->stats().active == 0; }
        ));
    }
    EXPECT_EQ(descriptors(), baseline);
}
} // namespace
} // namespace service::lobby
