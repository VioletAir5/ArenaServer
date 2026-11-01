#include "auth/AuthService.h"

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include <openssl/evp.h>

#include "rpc/Server.h"
#include "storage.pb.h"

namespace service::auth
{
namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using arena::internal_rpc::Context;
using arena::internal_rpc::Server;

std::string digestFor(int rounds)
{
    std::array<unsigned char, 16> salt{};
    std::array<unsigned char, 32> key{};
    if (PKCS5_PBKDF2_HMAC(
            "correct horse",
            13,
            salt.data(),
            salt.size(),
            rounds,
            EVP_sha256(),
            key.size(),
            key.data()
        ) != 1)
        throw std::runtime_error("test digest derivation failed");
    constexpr char hex[] = "0123456789abcdef";
    std::string encoded = "$arena-pbkdf2-sha256$" +
                          std::to_string(rounds) + "$" +
                          std::string(32, '0') + "$";
    for (auto value : key)
    {
        encoded.push_back(hex[value >> 4]);
        encoded.push_back(hex[value & 15]);
    }
    return encoded;
}

struct AuthHarness
{
    boost::asio::io_context io;
    std::unique_ptr<Server> storage;
    std::shared_ptr<AuthService> service;
    std::string digest{digestFor(100000)};
    std::string state{"active"};
    std::uint64_t player{42};
    std::size_t lookups{0}, cancelled{0};
    bool hold{false}, malformed{false};
    std::vector<Server::Completion> held;
    std::vector<Context> contexts;

    static JwtConfig jwt()
    {
        JwtConfig result;
        result.key = std::string(48, 'K');
        result.now = [] { return 1700000000; };
        return result;
    }
    explicit AuthHarness(
        std::size_t workers = 2,
        std::size_t queued = 4,
        std::size_t inflight = 8
    )
    {
        Server::Config server;
        server.service = "storage";
        server.callerTokens.emplace("auth", std::string(48, 'S'));
        Server::Handlers methods;
        methods.emplace(
            "health.ready",
            [](auto, auto, auto complete)
            {
                complete(arena::rpc::Response{});
                return Server::Cancellation{};
            }
        );
        methods.emplace(
            "account.find",
            [this](auto request, auto context, auto complete)
            {
                ++lookups;
                contexts.push_back(context);
                arena::storage::FindAccountRequest query;
                EXPECT_TRUE(query.ParseFromString(request.payload()));
                EXPECT_EQ(query.account_name(), "alice");
                if (hold)
                {
                    held.push_back(std::move(complete));
                    return Server::Cancellation([this]
                                                { ++cancelled; });
                }
                arena::rpc::Response response;
                if (malformed)
                    response.set_payload("not protobuf");
                else
                {
                    arena::storage::FindAccountReply record;
                    auto* account = record.mutable_account();
                    account->set_account_id(7);
                    account->set_account_name("alice");
                    account->set_state(state);
                    account->set_password_digest(digest);
                    auto* profile = record.mutable_player();
                    profile->set_account_id(7);
                    profile->set_player_id(player);
                    profile->set_display_name("Alice");
                    profile->set_state("active");
                    profile->set_mmr(1500);
                    response.set_payload(record.SerializeAsString());
                }
                complete(std::move(response));
                return Server::Cancellation{};
            }
        );
        storage = std::make_unique<Server>(
            io.get_executor(),
            std::move(server),
            std::move(methods)
        );
        AuthService::Config config;
        config.jwt = jwt();
        config.storageEndpoint = {
            boost::asio::ip::address_v4::loopback(),
            storage->port()
        };
        config.storageToken = std::string(48, 'S');
        config.maxWorkers = workers;
        config.maxQueued = queued;
        config.maxInflight = inflight;
        service = std::make_shared<AuthService>(
            io.get_executor(),
            std::move(config)
        );
    }
    ~AuthHarness()
    {
        service->stop();
        for (auto& complete : held)
        {
            arena::rpc::Response response;
            response.set_status(arena::rpc::CANCELLED);
            complete(std::move(response));
        }
        held.clear();
        storage->stop();
        const bool stopped = until(
            [&]
            {
                return service->stats().stopped &&
                       storage->stats().stopped;
            },
            5s
        );
        EXPECT_TRUE(stopped);
        EXPECT_EQ(service->stats().active, 0);
        EXPECT_EQ(service->stats().workersAlive, 0);
        service.reset();
        storage.reset();
        io.restart();
        io.poll();
    }
    template <class Predicate>
    bool
    until(Predicate predicate, std::chrono::milliseconds budget = 2s)
    {
        const auto deadline = Clock::now() + budget;
        while (!predicate() && Clock::now() < deadline)
        {
            io.restart();
            io.run_for(1ms);
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    Context context(std::chrono::milliseconds budget = 1s)
    {
        return {
            Clock::now() + budget,
            "auth-business-contract",
            {"gateway", "lobby", "auth"}
        };
    }
    static arena::auth::AuthenticateRequest
    password(std::uint64_t epoch = 9)
    {
        arena::auth::AuthenticateRequest request;
        request.set_proposed_epoch(epoch);
        request.set_account("alice");
        request.set_credential("correct horse");
        return request;
    }
    arena::rpc::Response
    call(arena::auth::AuthenticateRequest request)
    {
        std::optional<arena::rpc::Response> result;
        auto accepted = service->authenticate(
            std::move(request),
            context(),
            [&](auto response) { result = std::move(response); }
        );
        if (!accepted)
            throw std::runtime_error("test call was not admitted");
        if (!until([&] { return result.has_value(); }))
            throw std::runtime_error("Auth completion timed out");
        return std::move(*result);
    }
};

TEST(
    AuthServiceTest,
    PasswordLookupRealKdfAndSigningUseCanonicalIdentity
)
{
    AuthHarness harness;
    auto response = harness.call(AuthHarness::password());
    ASSERT_EQ(response.status(), arena::rpc::OK);
    arena::auth::AuthenticateReply grant;
    ASSERT_TRUE(grant.ParseFromString(response.payload()));
    EXPECT_EQ(grant.player_id(), 42);
    EXPECT_EQ(grant.epoch(), 9);
    EXPECT_FALSE(grant.resumed());
    JwtTokens tokens(AuthHarness::jwt());
    auto verified = tokens.verify(grant.session_token());
    ASSERT_TRUE(verified);
    EXPECT_EQ(verified->playerId.value(), 42);
    EXPECT_EQ(verified->epoch, 9);
    EXPECT_EQ(verified->expiresAt, grant.expires_at_unix());
    ASSERT_EQ(harness.contexts.size(), 1);
    EXPECT_EQ(
        harness.contexts[0].servicePath,
        (std::vector<
            std::string>{"gateway", "lobby", "auth", "storage"})
    );
    EXPECT_LE(harness.contexts[0].deadline, Clock::now() + 1s);
    EXPECT_EQ(harness.service->stats().active, 0);
    auto wrong = AuthHarness::password(10);
    wrong.set_credential("wrong password");
    auto denied = harness.call(std::move(wrong));
    EXPECT_EQ(denied.status(), arena::rpc::UNAUTHENTICATED);
    EXPECT_TRUE(denied.payload().empty());
    harness.state = "disabled";
    EXPECT_EQ(
        harness.call(AuthHarness::password(11)).status(),
        arena::rpc::UNAUTHENTICATED
    );
    harness.state = "active";
    harness.malformed = true;
    EXPECT_EQ(
        harness.call(AuthHarness::password(12)).status(),
        arena::rpc::UNAVAILABLE
    );
}

TEST(
    AuthServiceTest,
    JwtCandidatePreservesPriorClaimsWithoutPasswordOrStorageLookup
)
{
    AuthHarness harness;
    JwtTokens tokens(AuthHarness::jwt());
    auto initial = tokens.issue(arena::domain::PlayerId{43}, 5);
    ASSERT_TRUE(initial);
    auto request = AuthHarness::password(6);
    request.set_credential_kind(
        arena::auth::AuthenticateRequest::JWT
    );
    request.clear_account();
    request.set_credential(initial->encoded);
    auto response = harness.call(request);
    ASSERT_EQ(response.status(), arena::rpc::OK);
    arena::auth::AuthenticateReply grant;
    ASSERT_TRUE(grant.ParseFromString(response.payload()));
    EXPECT_EQ(grant.player_id(), 43);
    EXPECT_EQ(grant.epoch(), 6);
    EXPECT_TRUE(grant.resumed());
    EXPECT_EQ(grant.previous_epoch(), 5);
    EXPECT_EQ(
        grant.previous_expires_at_unix(),
        initial->claims.expiresAt
    );
    auto next = tokens.verify(grant.session_token());
    ASSERT_TRUE(next);
    EXPECT_EQ(next->epoch, 6);
    EXPECT_EQ(harness.lookups, 0);
    request.set_proposed_epoch(5);
    EXPECT_EQ(
        harness.call(request).status(),
        arena::rpc::UNAUTHENTICATED
    );
    request.set_proposed_epoch(7);
    request.set_credential(initial->encoded + "x");
    EXPECT_EQ(
        harness.call(request).status(),
        arena::rpc::UNAUTHENTICATED
    );
    EXPECT_EQ(harness.lookups, 0);
}

TEST(
    AuthServiceTest,
    InvalidOrExpiredCallsRejectWithoutCallbackOrNewJob
)
{
    AuthHarness harness;
    bool called = false;
    auto invalid = AuthHarness::password(0);
    auto accepted = harness.service->authenticate(
        invalid,
        harness.context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(accepted);
    EXPECT_EQ(accepted.error(), arena::rpc::INVALID_ARGUMENT);
    auto expired = harness.context();
    expired.deadline = Clock::now() - 1ms;
    accepted = harness.service->authenticate(
        AuthHarness::password(),
        expired,
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(accepted);
    EXPECT_EQ(accepted.error(), arena::rpc::DEADLINE_EXCEEDED);
    auto badJwt = AuthHarness::password();
    badJwt.set_credential_kind(arena::auth::AuthenticateRequest::JWT);
    accepted = harness.service->authenticate(
        badJwt,
        harness.context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(accepted);
    EXPECT_EQ(accepted.error(), arena::rpc::INVALID_ARGUMENT);
    EXPECT_EQ(harness.service->stats().active, 0);
    EXPECT_FALSE(called);
    EXPECT_EQ(harness.lookups, 0);
}

TEST(
    AuthServiceTest,
    CancelledKdfRetainsItsSlotUntilActualWorkerCompletion
)
{
    AuthHarness harness(1, 1, 1);
    harness.digest = digestFor(2000000);
    std::optional<arena::rpc::Response> result;
    auto accepted = harness.service->authenticate(
        AuthHarness::password(),
        harness.context(),
        [&](auto response) { result = std::move(response); }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(harness.until(
        [&] { return harness.service->stats().busy == 1; }
    ));
    const auto started = Clock::now();
    (*accepted)();
    EXPECT_LT(Clock::now() - started, 20ms);
    EXPECT_EQ(harness.service->stats().active, 1);
    bool secondCalled = false;
    auto second = harness.service->authenticate(
        AuthHarness::password(10),
        harness.context(),
        [&](auto) { secondCalled = true; }
    );
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error(), arena::rpc::OVERLOADED);
    EXPECT_FALSE(secondCalled);
    ASSERT_TRUE(harness.until([&] { return result.has_value(); }));
    EXPECT_EQ(result->status(), arena::rpc::CANCELLED);
    EXPECT_TRUE(result->payload().empty());
    EXPECT_EQ(harness.service->stats().active, 0);
    harness.digest = digestFor(100000);
    auto recovered = harness.call(AuthHarness::password(10));
    EXPECT_EQ(recovered.status(), arena::rpc::OK);
}

TEST(AuthServiceTest, BoundedQueueAndStopRetainRealJobsAndExitWorkers)
{
    AuthHarness harness(1, 1, 3);
    harness.digest = digestFor(2000000);
    std::array<std::optional<arena::rpc::Response>, 3> results;
    auto first = harness.service->authenticate(
        AuthHarness::password(),
        harness.context(),
        [&](auto r) { results[0] = std::move(r); }
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(harness.until(
        [&] { return harness.service->stats().busy == 1; }
    ));
    JwtTokens tokens(AuthHarness::jwt());
    auto initial = tokens.issue(arena::domain::PlayerId{43}, 1);
    ASSERT_TRUE(initial);
    auto jwt = AuthHarness::password(2);
    jwt.set_credential_kind(arena::auth::AuthenticateRequest::JWT);
    jwt.clear_account();
    jwt.set_credential(initial->encoded);
    auto second = harness.service->authenticate(
        jwt,
        harness.context(),
        [&](auto r) { results[1] = std::move(r); }
    );
    ASSERT_TRUE(second);
    ASSERT_TRUE(harness.until(
        [&] { return harness.service->stats().queued == 1; }
    ));
    jwt.set_proposed_epoch(3);
    auto third = harness.service->authenticate(
        jwt,
        harness.context(),
        [&](auto r) { results[2] = std::move(r); }
    );
    ASSERT_TRUE(third);
    ASSERT_TRUE(harness.until([&]
                              { return results[2].has_value(); }));
    EXPECT_EQ(results[2]->status(), arena::rpc::OVERLOADED);
    harness.service->stop();
    harness.service->stop();
    ASSERT_TRUE(harness.until(
        [&] { return harness.service->stats().stopped; }
    ));
    ASSERT_TRUE(results[0]);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(results[0]->status(), arena::rpc::CANCELLED);
    EXPECT_EQ(results[1]->status(), arena::rpc::CANCELLED);
    auto stats = harness.service->stats();
    EXPECT_EQ(stats.active, 0);
    EXPECT_EQ(stats.queued, 0);
    EXPECT_EQ(stats.busy, 0);
    EXPECT_EQ(stats.workersAlive, 0);
    EXPECT_EQ(stats.accepted, stats.completed);
    auto closed = harness.service->authenticate(
        AuthHarness::password(),
        harness.context(),
        [](auto) {}
    );
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error(), arena::rpc::UNAVAILABLE);
}

TEST(
    AuthServiceTest,
    CancelledStorageWaitHasNoWorkerOrCredentialResidue
)
{
    AuthHarness harness;
    harness.hold = true;
    std::optional<arena::rpc::Response> result;
    auto accepted = harness.service->authenticate(
        AuthHarness::password(),
        harness.context(),
        [&](auto r) { result = std::move(r); }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(harness.until([&] { return harness.lookups == 1; }));
    (*accepted)();
    ASSERT_TRUE(harness.until([&] { return result.has_value(); }));
    EXPECT_EQ(result->status(), arena::rpc::CANCELLED);
    EXPECT_EQ(harness.service->stats().active, 0);
    EXPECT_EQ(harness.service->stats().busy, 0);
    EXPECT_TRUE(result->payload().empty());
}
TEST(
    AuthServiceTest,
    RepeatedJwtCompletionsAndShutdownRecoverPhysicalDescriptors
)
{
    const auto descriptorCount = []
    {
        return std::distance(
            std::filesystem::directory_iterator("/proc/self/fd"),
            std::filesystem::directory_iterator{}
        );
    };
    const auto baseline = descriptorCount();
    {
        AuthHarness harness;
        JwtTokens tokens(AuthHarness::jwt());
        auto initial = tokens.issue(arena::domain::PlayerId{42}, 1);
        ASSERT_TRUE(initial);
        for (std::uint64_t epoch = 2; epoch < 66; ++epoch)
        {
            auto request = AuthHarness::password(epoch);
            request.clear_account();
            request.set_credential_kind(
                arena::auth::AuthenticateRequest::JWT
            );
            request.set_credential(initial->encoded);
            EXPECT_EQ(
                harness.call(std::move(request)).status(),
                arena::rpc::OK
            );
            EXPECT_EQ(harness.service->stats().active, 0);
        }
        EXPECT_EQ(harness.lookups, 0);
    }
    EXPECT_EQ(descriptorCount(), baseline);
}
} // namespace
} // namespace service::auth
