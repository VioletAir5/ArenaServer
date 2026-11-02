#include <array>
#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "net/ArenaProtocol.h"
#include "net/RemoteLobbyRoutes.h"
#include "protocol/EnvelopeCodec.hpp"

#include "ProtocolTestHarness.h"
#include "rpc/Server.h"
#include "runtime/RuntimeConfig.h"

namespace
{
using namespace service::gateway;
using namespace std::chrono_literals;
namespace wire = arena::protocol;
using Server = arena::internal_rpc::Server;
using Clock = std::chrono::steady_clock;
using Binding = service::lobby::SessionBinding;
std::int64_t futureExpiry()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
           )
               .count() +
           300;
}
std::size_t descriptors()
{
    return std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}
    );
}
arena::rpc::Response
response(arena::rpc::Status status = arena::rpc::OK)
{
    arena::rpc::Response result;
    result.set_status(status);
    return result;
}
LobbyClient::Context
context(std::string correlation = "remote-contract")
{
    return {
        Clock::now() + 900ms,
        std::move(correlation),
        {"gateway"}
    };
}
arena::lobby::ExecuteRequest command(
    unsigned payload = wire::MSG_LOGIN_REQUEST, std::uint64_t id = 1
)
{
    arena::lobby::ExecuteRequest value;
    value.set_connection_id(1);
    auto* message = value.mutable_message();
    message->set_request_id(id);
    message->set_msg_id(payload);
    const auto* field = message->GetDescriptor()->FindFieldByNumber(
        payload
    );
    message->GetReflection()->MutableMessage(message, field);
    if (payload == wire::MSG_LOGIN_REQUEST)
    {
        message->mutable_login_request()->set_account("alice");
        message->mutable_login_request()->set_credential(
            "test-password"
        );
        message->mutable_login_request()->set_operation_key(
            std::string(32, 'a')
        );
    }
    else
    {
        value.set_player_id(42);
        value.set_session_epoch(7);
    }
    return value;
}
struct Network
{
    struct Held
    {
        arena::lobby::ExecuteReply reply;
        Server::Completion complete;
    };
    boost::asio::io_context io;
    std::unique_ptr<Server> server;
    std::shared_ptr<LobbyClient> client;
    std::vector<Held> held;
    std::vector<arena::lobby::ExecuteRequest> received;
    std::vector<arena::rpc::Request> metadata;
    std::vector<arena::internal_rpc::Context> parents;
    std::vector<std::vector<std::uint64_t>> batches;
    std::vector<arena::lobby::DetachRequest> detached;
    std::vector<std::uint64_t> revokeIds;
    arena::rpc::Status executeStatus{arena::rpc::OK},
        heartbeatStatus{arena::rpc::OK};
    bool hold{false}, publicFailure{false};
    wire::ErrorCode publicFailureCode{wire::ERR_UNAVAILABLE};
    unsigned fault{0}, cancelled{0}, healthCalls{0};
    bool authReady{true};
    std::vector<arena::rpc::Request> healthMetadata;
    explicit Network(
        std::size_t maxInflight = 1,
        std::size_t maxConnections = 600,
        bool wrongRole = false,
        std::shared_ptr<arena::runtime::ConfigStore> runtime = {}
    )
    {
        Server::Config configured;
        configured.service = "lobby";
        configured.callerTokens = {
            {wrongRole ? "auth" : "gateway", std::string(32, 'g')}
        };
        Server::Handlers handlers;
        handlers.emplace(
            "lobby.execute",
            [this](auto request, auto parent, auto complete)
                -> Server::Cancellation
            {
                metadata.push_back(request);
                parents.push_back(parent);
                arena::lobby::ExecuteRequest input;
                if (!input.ParseFromString(request.payload()))
                {
                    complete(response(arena::rpc::INVALID_ARGUMENT));
                    return {};
                }
                received.push_back(input);
                if (executeStatus != arena::rpc::OK)
                {
                    complete(response(executeStatus));
                    return {};
                }
                arena::lobby::ExecuteReply reply;
                reply.set_committed_revision(19);
                auto* message = reply.mutable_message();
                message->set_request_id(input.message().request_id());
                const auto expected = static_cast<unsigned>(
                                          input.message()
                                              .payload_case()
                                      ) +
                                      1;
                message->set_msg_id(expected);
                message->GetReflection()->MutableMessage(
                    message,
                    message->GetDescriptor()->FindFieldByNumber(
                        expected
                    )
                );
                if (input.message().has_login_request())
                {
                    auto* login = message->mutable_login_response();
                    login->set_accepted(true);
                    login->set_player_id(42);
                    login->set_session_epoch(7);
                    login->set_session_id(900);
                    login->set_session_token(
                        "opaque-lobby-issued-token"
                    );
                }
                auto* session = reply.mutable_session();
                session->set_player_id(42);
                session->set_epoch(7);
                session->set_session_id(900);
                session->set_expires_at_unix(futureExpiry());
                if (publicFailure)
                {
                    message->set_msg_id(wire::MSG_ERROR);
                    message->mutable_error()->set_code(
                        publicFailureCode
                    );
                    reply.clear_session();
                }
                if (fault == 1)
                {
                    message->mutable_room_leave_response();
                    message->set_msg_id(
                        wire::MSG_ROOM_LEAVE_RESPONSE
                    );
                }
                if (fault == 2)
                    session->set_epoch(8);
                if (fault == 3)
                    session->set_expires_at_unix(0);
                if (fault == 4)
                    message->set_request_id(999999);
                if (fault == 5)
                    reply.clear_session();
                if (hold)
                    held.push_back(
                        {std::move(reply), std::move(complete)}
                    );
                else
                {
                    auto result = response();
                    reply.SerializeToString(result.mutable_payload());
                    complete(std::move(result));
                }
                return [this] { ++cancelled; };
            }
        );
        handlers.emplace(
            "lobby.detach",
            [this](auto request, auto, auto complete)
                -> Server::Cancellation
            {
                arena::lobby::DetachRequest input;
                EXPECT_TRUE(input.ParseFromString(request.payload()));
                detached.push_back(input);
                arena::lobby::DetachReply reply;
                reply.set_committed_revision(20);
                auto result = response();
                reply.SerializeToString(result.mutable_payload());
                complete(std::move(result));
                return {};
            }
        );
        handlers.emplace(
            "lobby.heartbeat",
            [this](auto request, auto, auto complete)
                -> Server::Cancellation
            {
                arena::lobby::HeartbeatRequest input;
                EXPECT_TRUE(input.ParseFromString(request.payload()));
                batches.emplace_back(
                    input.connection_ids().begin(),
                    input.connection_ids().end()
                );
                auto result = response(heartbeatStatus);
                if (heartbeatStatus == arena::rpc::OK)
                {
                    arena::lobby::HeartbeatReply reply;
                    reply.set_committed_revision(21);
                    for (const auto id : revokeIds)
                        reply.add_revoked_connection_ids(id);
                    reply.SerializeToString(result.mutable_payload());
                }
                complete(std::move(result));
                return {};
            }
        );
        handlers.emplace(
            "health.ready",
            [this](auto request, auto, auto complete)
                -> Server::Cancellation
            {
                ++healthCalls;
                healthMetadata.push_back(request);
                complete(response(
                    authReady ? arena::rpc::OK
                              : arena::rpc::UNAVAILABLE
                ));
                return {};
            }
        );
        server = std::make_unique<Server>(
            io.get_executor(),
            std::move(configured),
            std::move(handlers)
        );
        LobbyClient::Config config;
        config.endpoint.port(server->port());
        config.serviceToken = std::string(32, 'g');
        config.gatewayInstance = std::string(32, 'a');
        config.maxInflight = maxInflight;
        config.maxConnections = maxConnections;
        config.runtime = std::move(runtime);
        client = std::make_shared<LobbyClient>(
            io.get_executor(),
            std::move(config)
        );
    }
    bool until(
        const std::function<bool()>& predicate,
        std::chrono::milliseconds timeout = 2000ms
    )
    {
        const auto end = Clock::now() + timeout;
        do
        {
            io.restart();
            io.poll();
            if (predicate())
                return true;
            std::this_thread::sleep_for(1ms);
        } while (Clock::now() < end);
        io.restart();
        io.poll();
        return predicate();
    }
    void release()
    {
        auto pending = std::exchange(held, {});
        for (auto& job : pending)
        {
            auto result = response();
            job.reply.SerializeToString(result.mutable_payload());
            job.complete(std::move(result));
        }
    }
    ~Network()
    {
        client->stop();
        release();
        server->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return client->stats().stopped &&
                       server->stats().stopped;
            }
        ));
        client.reset();
        server.reset();
        io.restart();
        io.poll();
    }
};
class RemoteLobbyTest : public service::gateway::test::ProtocolTest
{
protected:
    std::unique_ptr<Network> network;
    void SetUp() override
    {
        network = std::make_unique<Network>();
        start(makeArenaProtocolHandler(
            context,
            makeRemoteLobbyMessageRegistry(network->client)
        ));
    }
    void flush()
    {
        auto ready = std::exchange(posts, {});
        for (auto& action : ready)
            action();
    }
    bool until(const std::function<bool()>& predicate)
    {
        return network->until(
            [&]
            {
                flush();
                return predicate();
            }
        );
    }
    void send(wire::Envelope request)
    {
        const auto encoded = service::protocol::EnvelopeCodec::encode(
            request
        );
        ASSERT_TRUE(encoded);
        receive(frame(*encoded));
    }
    std::vector<wire::Envelope> replies()
    {
        std::vector<wire::Envelope> result;
        service::protocol::PacketCodec decoder;
        decoder.feed(
            std::as_bytes(std::span(transport->output)),
            [&](const auto&, auto bytes)
            {
                wire::Envelope reply;
                EXPECT_TRUE(
                    service::protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(bytes)),
                        reply
                    )
                );
                result.push_back(std::move(reply));
                return true;
            }
        );
        return result;
    }
    void TearDown() override
    {
        if (host)
            host->close();
        flush();
        if (network)
        {
            network->release();
            network->until(
                [&]
                {
                    flush();
                    return network->client->stats()
                               .businessInflight == 0;
                }
            );
        }
        host.reset();
        posts.clear();
        deadlines.clear();
        network.reset();
    }
};

TEST_F(
    RemoteLobbyTest,
    LoginStateRoomAndInputForwardOriginalPayloadWithOwnerProjection
)
{
    auto login = command();
    login.mutable_message()->set_request_id(
        0
    ); // Dispatcher normalizes legacy identity.
    send(login.message());
    ASSERT_TRUE(
        until([&] { return context->session->binding().has_value(); })
    );
    ASSERT_EQ(network->received.size(), 1U);
    EXPECT_NE(network->received[0].message().request_id(), 0U);
    EXPECT_EQ(
        network->received[0]
            .message()
            .login_request()
            .operation_key(),
        std::string(32, 'a')
    );
    EXPECT_EQ(
        network->received[0].gateway_instance(),
        std::string(32, 'a')
    );
    EXPECT_EQ(network->metadata[0].caller_service(), "gateway");
    EXPECT_EQ(network->metadata[0].target_service(), "lobby");
    EXPECT_EQ(network->metadata[0].protocol_version(), 1U);
    EXPECT_EQ(
        network->parents[0].servicePath,
        (std::vector<std::string>{"gateway", "lobby"})
    );
    EXPECT_FALSE(network->parents[0].correlationId.empty());
    EXPECT_LE(network->parents[0].deadline, Clock::now() + 1000ms);
    auto state = command(wire::MSG_SESSION_STATE_REQUEST, 2);
    send(state.message());
    ASSERT_TRUE(until([&] { return replies().size() == 2; }));
    auto room = command(wire::MSG_ROOM_JOIN_REQUEST, 3);
    room.mutable_message()->mutable_room_join_request()->set_room_id(
        99
    );
    send(room.message());
    ASSERT_TRUE(until([&] { return replies().size() == 3; }));
    auto input = command(wire::MSG_ROOM_INPUT_REQUEST, 4);
    auto* control = input.mutable_message()
                        ->mutable_room_input_request();
    control->set_actor_incarnation(31);
    control->set_membership_epoch(17);
    control->set_seq(9);
    control->set_dx(-1);
    control->set_dy(1);
    send(input.message());
    ASSERT_TRUE(until([&] { return replies().size() == 4; }));
    EXPECT_EQ(
        network->received.back().message().SerializeAsString(),
        input.message().SerializeAsString()
    );
    EXPECT_EQ(network->received.back().player_id(), 42U);
    EXPECT_EQ(network->received.back().session_epoch(), 7U);
    EXPECT_EQ(network->client->stats().tracked, 1U);
    EXPECT_EQ(
        replies().front().login_response().session_token(),
        "opaque-lobby-issued-token"
    );
    context->session->disconnect();
    context->session->disconnect();
    ASSERT_TRUE(
        network->until([&] { return network->detached.size() == 1; })
    );
    EXPECT_EQ(network->detached[0].player_id(), 42U);
    EXPECT_EQ(network->detached[0].session_epoch(), 7U);
}

TEST_F(
    RemoteLobbyTest,
    PublicErrorsReachFrontendWithRequestIdentityAndRejectionCounts
)
{
    network->publicFailure = true;
    std::size_t count = 0;
    for (const auto code :
         {wire::ERR_UNAUTHENTICATED, wire::ERR_UNAVAILABLE})
    {
        SCOPED_TRACE(static_cast<int>(code));
        network->publicFailureCode = code;
        const auto requestId = 101 + count;
        send(command(wire::MSG_LOGIN_REQUEST, requestId).message());
        ++count;
        ASSERT_TRUE(until([&] { return replies().size() == count; }));
        const auto output = replies();
        ASSERT_TRUE(output.back().has_error());
        EXPECT_EQ(output.back().msg_id(), wire::MSG_ERROR);
        EXPECT_EQ(output.back().request_id(), requestId);
        EXPECT_EQ(output.back().error().code(), code);
        ASSERT_EQ(network->received.size(), count);
        EXPECT_EQ(
            network->received.back().message().request_id(),
            requestId
        );
        EXPECT_FALSE(context->session->binding());
        EXPECT_EQ(
            context->session->loginState(),
            ConnectionSession::LoginState::Pending
        );
        EXPECT_EQ(network->client->stats().tracked, 0U);
        EXPECT_EQ(
            counters->count(RejectionReason::Unauthenticated),
            1U
        );
        EXPECT_EQ(
            counters->count(RejectionReason::HandlerRejected),
            count - 1
        );
        EXPECT_EQ(
            counters->count(RejectionReason::InvalidEnvelope),
            0U
        );
        EXPECT_EQ(
            counters->count(RejectionReason::CallbackFailure),
            0U
        );
    }
    network->publicFailure = false;
    send(command(wire::MSG_LOGIN_REQUEST, 103).message());
    ASSERT_TRUE(
        until([&] { return context->session->binding().has_value(); })
    );
    ASSERT_EQ(replies().size(), 3U);
    EXPECT_TRUE(replies().back().has_login_response());
    EXPECT_EQ(replies().back().request_id(), 103U);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1U);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1U);
}
TEST_F(
    RemoteLobbyTest,
    LateOwnerReplyAfterDisconnectDoesNotBindAndConditionallyDetaches
)
{
    send(command().message());
    bool receivedReply = false;
    // A foreign RPC completion must enqueue behind the idle owner.
    // Polling on the owner would intentionally install it inline.
    std::thread rpcThread(
        [&]
        {
            receivedReply = network->until(
                [&]
                {
                    return network->received.size() == 1 &&
                           network->client->stats()
                                   .businessInflight == 0;
                }
            );
        }
    );
    rpcThread.join();
    ASSERT_TRUE(receivedReply);
    ASSERT_FALSE(posts.empty());
    ASSERT_EQ(
        context->session->loginState(),
        ConnectionSession::LoginState::Pending
    );
    EXPECT_EQ(network->client->stats().tracked, 0U);
    host->close();
    flush();
    ASSERT_TRUE(network->until(
        [&]
        {
            flush();
            return network->detached.size() == 1;
        }
    ));
    EXPECT_FALSE(context->session->binding());
    EXPECT_EQ(network->client->stats().tracked, 0U);
    EXPECT_TRUE(transport->output.empty());
    EXPECT_EQ(network->detached[0].connection_id(), 1U);
    EXPECT_EQ(network->detached[0].player_id(), 42U);
    EXPECT_EQ(network->detached[0].session_epoch(), 7U);
}

TEST_F(
    RemoteLobbyTest,
    DeadlineCancellationAndLateCompletionCannotAuthenticate
)
{
    network->hold = true;
    send(command().message());
    ASSERT_TRUE(
        network->until([&] { return network->held.size() == 1; })
    );
    ASSERT_FALSE(deadlines.empty());
    deadlines.back()();
    flush();
    ASSERT_TRUE(network->until([&]
                               { return network->cancelled != 0; }));
    network->release();
    ASSERT_TRUE(
        until([&] { return network->server->stats().active == 0; })
    );
    EXPECT_FALSE(context->session->binding());
    EXPECT_EQ(network->client->stats().tracked, 0U);
    const auto output = replies();
    ASSERT_EQ(output.size(), 1U);
    EXPECT_TRUE(output[0].has_error());
}

TEST(
    RemoteLobbyClientTest,
    TypedMismatchVersionCycleAndUnknownOutcomeAreNeverRetried
)
{
    Network network;
    for (const auto status :
         {arena::rpc::INCOMPATIBLE_VERSION,
          arena::rpc::CALL_CYCLE,
          arena::rpc::OUTCOME_UNKNOWN})
    {
        network.executeStatus = status;
        std::optional<LobbyClient::ExecuteResult> result;
        ASSERT_TRUE(network.client->execute(
            command(),
            context(),
            [&](auto reply) { result = std::move(reply); }
        ));
        ASSERT_TRUE(network.until([&]
                                  { return result.has_value(); }));
        ASSERT_FALSE(*result);
        EXPECT_EQ(result->error(), status);
    }
    EXPECT_EQ(network.received.size(), 3U);
    network.executeStatus = arena::rpc::OK;
    for (unsigned fault = 1; fault != 6; ++fault)
    {
        SCOPED_TRACE(fault);
        network.fault = fault;
        std::optional<LobbyClient::ExecuteResult> result;
        ASSERT_TRUE(network.client->execute(
            command(),
            context(),
            [&](auto reply) { result = std::move(reply); }
        ));
        ASSERT_TRUE(network.until([&]
                                  { return result.has_value(); }));
        ASSERT_FALSE(*result);
        EXPECT_EQ(result->error(), arena::rpc::INTERNAL);
    }
    network.fault = 0;
    network.publicFailure = true;
    std::optional<LobbyClient::ExecuteResult> result;
    ASSERT_TRUE(network.client->execute(
        command(),
        context(),
        [&](auto reply) { result = std::move(reply); }
    ));
    ASSERT_TRUE(network.until([&] { return result.has_value(); }));
    ASSERT_TRUE(*result);
    EXPECT_TRUE((*result)->message().has_error());
    EXPECT_FALSE((*result)->has_session());
}

TEST(
    RemoteLobbyClientTest,
    WrongRoleRejectsBeforeHandlerAndNeverFallsBack
)
{
    Network network(1, 2, true);
    std::optional<LobbyClient::ExecuteResult> result;
    ASSERT_TRUE(network.client->execute(
        command(),
        context(),
        [&](auto reply) { result = std::move(reply); }
    ));
    ASSERT_TRUE(network.until([&] { return result.has_value(); }));
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), arena::rpc::UNAUTHENTICATED);
    EXPECT_TRUE(network.received.empty());
    auto snapshot = arena::runtime::ConfigSnapshot::parse(
        R"({"version":"1","desiredReady":true,"drainGraceMs":0,"nodes":[{"service":"auth","nodeId":"auth-a","host":"127.0.0.1","port":7101,"ready":true}]})"
    );
    ASSERT_TRUE(snapshot);
    auto runtime = std::make_shared<arena::runtime::ConfigStore>(
        *snapshot
    );
    Network missing(1, 2, false, runtime);
    bool called = false;
    const auto rejected = missing.client->execute(
        command(),
        context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), arena::rpc::UNAVAILABLE);
    missing.io.poll();
    EXPECT_FALSE(called);
    EXPECT_TRUE(missing.received.empty());
}

TEST(
    RemoteLobbyClientTest,
    BusinessOverloadCannotStarveHeartbeatAndOutagesCannotRevokeOrRevive
)
{
    Network network;
    network.hold = true;
    auto closes = std::make_shared<std::atomic<unsigned>>(0);
    auto first = network.client->track(
        {arena::domain::PlayerId{42}, 1, 7},
        futureExpiry(),
        [closes] { ++*closes; }
    );
    auto second = network.client->track(
        {arena::domain::PlayerId{43}, 2, 8},
        futureExpiry(),
        [closes] { ++*closes; }
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    std::optional<LobbyClient::ExecuteResult> held;
    ASSERT_TRUE(network.client->execute(
        command(),
        context(),
        [&](auto reply) { held = std::move(reply); }
    ));
    ASSERT_TRUE(network.until([&]
                              { return network.held.size() == 1; }));
    bool unexpected = false;
    const auto overloaded = network.client->execute(
        command(),
        context(),
        [&](auto) { unexpected = true; }
    );
    ASSERT_FALSE(overloaded);
    EXPECT_EQ(overloaded.error(), arena::rpc::OVERLOADED);
    EXPECT_FALSE(unexpected);
    arena::lobby::HeartbeatRequest heartbeat;
    heartbeat.add_connection_ids(1);
    heartbeat.add_connection_ids(2);
    for (unsigned scenario = 0; scenario != 4; ++scenario)
    {
        network.revokeIds = scenario == 0
                                ? std::vector<std::uint64_t>{999}
                            : scenario == 2
                                ? std::vector<std::uint64_t>{1}
                                : std::vector<std::uint64_t>{};
        network.heartbeatStatus = scenario == 1
                                      ? arena::rpc::UNAVAILABLE
                                      : arena::rpc::OK;
        std::optional<LobbyClient::HeartbeatResult> result;
        ASSERT_TRUE(network.client->heartbeat(
            heartbeat,
            context(),
            [&](auto reply) { result = std::move(reply); }
        ));
        ASSERT_TRUE(network.until([&]
                                  { return result.has_value(); }));
        if (scenario < 2)
        {
            EXPECT_FALSE(*result);
            EXPECT_TRUE((*first)->load());
            EXPECT_TRUE((*second)->load());
        }
        else
        {
            ASSERT_TRUE(*result);
            EXPECT_FALSE((*first)->load());
            EXPECT_TRUE((*second)->load());
        }
    }
    EXPECT_EQ(closes->load(), 1U);
    network.client->invalidate({arena::domain::PlayerId{42}, 1, 7});
    EXPECT_EQ(closes->load(), 1U);
    network.release();
    ASSERT_TRUE(network.until([&] { return held.has_value(); }));
}

TEST(
    RemoteLobbyClientTest,
    AutomaticHeartbeatsUseFiniteRoundRobinBatchesAndTrackBound
)
{
    Network network(1, 513);
    for (std::uint64_t id = 1; id != 514; ++id)
        ASSERT_TRUE(network.client->track(
            {arena::domain::PlayerId{id}, id, id},
            futureExpiry(),
            [] {}
        ));
    auto full = network.client->track(
        {arena::domain::PlayerId{514}, 514, 514},
        futureExpiry(),
        [] {}
    );
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error(), arena::rpc::OVERLOADED);
    ASSERT_TRUE(
        network.until([&] { return network.batches.size() >= 2; })
    );
    std::set<std::uint64_t> seen;
    for (const auto& batch : network.batches)
    {
        EXPECT_LE(batch.size(), 512U);
        seen.insert(batch.begin(), batch.end());
    }
    EXPECT_EQ(seen.size(), 513U);
}

TEST(
    RemoteLobbyClientTest,
    StopCancelsPhysicalCallsAndReturnsDescriptorsToBaseline
)
{
    const auto before = descriptors();
    {
        Network network;
        network.hold = true;
        std::optional<LobbyClient::ExecuteResult> result;
        const auto accepted = network.client->execute(
            command(),
            context(),
            [&](auto reply) { result = std::move(reply); }
        );
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(
            network.until([&] { return network.held.size() == 1; })
        );
        accepted->cancel();
        accepted->cancel();
        ASSERT_TRUE(network.until([&]
                                  { return result.has_value(); }));
        ASSERT_FALSE(*result);
        EXPECT_EQ(result->error(), arena::rpc::CANCELLED);
        network.client->stop();
        network.client->stop();
        network.release();
        ASSERT_TRUE(network.until(
            [&]
            {
                return network.client->stats().stopped &&
                       network.server->stats().active == 0;
            }
        ));
        EXPECT_EQ(network.client->stats().businessInflight, 0U);
        EXPECT_EQ(network.client->stats().heartbeatInflight, 0U);
    }
    EXPECT_EQ(descriptors(), before);
}

TEST(
    RemoteLobbyProjectionTest,
    CleanupIsOnceAndFinalForeignReleaseHasNoOwnerIo
)
{
    auto released = std::make_shared<std::atomic<unsigned>>(0);
    auto valid = std::make_shared<std::atomic<bool>>(true);
    const Binding binding{arena::domain::PlayerId{42}, 17, 7};
    auto session = std::make_shared<ConnectionSession>();
    ASSERT_TRUE(session->bind(
        {binding,
         [valid] { return valid->load(); },
         [released] { ++*released; }}
    ));
    session->disconnect();
    session->disconnect();
    session.reset();
    EXPECT_EQ(released->load(), 1U);
    session = std::make_shared<ConnectionSession>();
    ASSERT_TRUE(session->bind(
        {binding,
         [valid] { return valid->load(); },
         [released] { ++*released; }}
    ));
    std::thread finalOwner([retained = std::move(session)]() mutable
                           { retained.reset(); });
    finalOwner.join();
    EXPECT_EQ(released->load(), 2U);
}
TEST(
    RemoteLobbyClientTest,
    ProbesRealLobbyHealthBeforeLoginAndFencesAuthAndLobbyOutages
)
{
    Network network;
    EXPECT_FALSE(network.client->stats().dependencyReady);
    ASSERT_TRUE(network.until(
        [&] { return network.client->stats().dependencyReady; }
    ));
    EXPECT_EQ(network.client->stats().tracked, 0U);
    ASSERT_FALSE(network.healthMetadata.empty());
    EXPECT_EQ(network.healthMetadata[0].method(), "health.ready");
    EXPECT_EQ(network.healthMetadata[0].caller_service(), "gateway");
    EXPECT_EQ(network.healthMetadata[0].target_service(), "lobby");
    EXPECT_EQ(network.healthMetadata[0].protocol_version(), 1U);
    network.authReady = false;
    ASSERT_TRUE(network.until(
        [&] { return !network.client->stats().dependencyReady; }
    ));
    network.authReady = true;
    ASSERT_TRUE(network.until(
        [&] { return network.client->stats().dependencyReady; }
    ));
    network.server->stop();
    ASSERT_TRUE(network.until(
        [&] { return !network.client->stats().dependencyReady; }
    ));
    network.client->stop();
    ASSERT_TRUE(
        network.until([&] { return network.client->stats().stopped; })
    );
    EXPECT_EQ(network.client->stats().probeInflight, 0U);
}

TEST(
    RemoteLobbyClientTest, RejectsActualLegacyVersionReplyWithoutRetry
)
{
    using Tcp = boost::asio::ip::tcp;
    boost::asio::io_context io;
    Tcp::acceptor acceptor(
        io,
        {boost::asio::ip::address_v4::loopback(), 0}
    );
    unsigned requests = 0;
    std::function<void()> acceptNext;
    acceptNext = [&]
    {
        auto socket = std::make_shared<Tcp::socket>(io);
        acceptor.async_accept(
            *socket,
            [&, socket](boost::system::error_code error)
            {
                if (error)
                    return;
                acceptNext();
                auto prefix = std::make_shared<
                    std::array<unsigned char, 4>>();
                auto body = std::make_shared<std::string>();
                boost::asio::async_read(
                    *socket,
                    boost::asio::buffer(*prefix),
                    [&, socket, prefix, body](auto error, auto)
                    {
                        ASSERT_FALSE(error);
                        const auto size =
                            (static_cast<std::uint32_t>((*prefix)[0])
                             << 24) |
                            (static_cast<std::uint32_t>((*prefix)[1])
                             << 16) |
                            (static_cast<std::uint32_t>((*prefix)[2])
                             << 8) |
                            (*prefix)[3];
                        ASSERT_LE(
                            size,
                            arena::internal_rpc::Client::maxFrame
                        );
                        body->resize(size);
                        boost::asio::async_read(
                            *socket,
                            boost::asio::buffer(*body),
                            [&, socket, body](auto error, auto)
                            {
                                ASSERT_FALSE(error);
                                arena::rpc::Request input;
                                ASSERT_TRUE(
                                    input.ParseFromString(*body)
                                );
                                if (input.method() == "lobby.execute")
                                    ++requests;
                                auto legacy = response();
                                legacy.set_request_id(
                                    input.request_id()
                                );
                                legacy.set_correlation_id(
                                    input.correlation_id()
                                );
                                legacy.set_protocol_version(0);
                                const auto payload =
                                    legacy.SerializeAsString();
                                auto frame = std::make_shared<
                                    std::string>(4, '\0');
                                for (unsigned i = 0; i != 4; ++i)
                                    (*frame)[i] = static_cast<char>(
                                        payload.size() >>
                                        ((3 - i) * 8)
                                    );
                                frame->append(payload);
                                boost::asio::async_write(
                                    *socket,
                                    boost::asio::buffer(*frame),
                                    [socket, frame](auto, auto)
                                    {
                                        boost::system::error_code
                                            ignored;
                                        socket->close(ignored);
                                    }
                                );
                            }
                        );
                    }
                );
            }
        );
    };
    acceptNext();
    LobbyClient::Config config;
    config.endpoint = acceptor.local_endpoint();
    config.serviceToken = std::string(32, 'g');
    config.gatewayInstance = std::string(32, 'a');
    LobbyClient client(io.get_executor(), std::move(config));
    std::optional<LobbyClient::ExecuteResult> result;
    ASSERT_TRUE(client.execute(
        command(),
        context(),
        [&](auto reply) { result = std::move(reply); }
    ));
    const auto until = [&](auto ready)
    {
        const auto deadline = Clock::now() + 2000ms;
        do
        {
            io.restart();
            io.poll();
            if (ready())
                return true;
            std::this_thread::sleep_for(1ms);
        } while (Clock::now() < deadline);
        return ready();
    };
    ASSERT_TRUE(until([&] { return result.has_value(); }));
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), arena::rpc::INCOMPATIBLE_VERSION);
    EXPECT_EQ(requests, 1U);
    EXPECT_FALSE(client.stats().dependencyReady);
    client.stop();
    ASSERT_TRUE(until([&] { return client.stats().stopped; }));
    boost::system::error_code ignored;
    acceptor.close(ignored);
    io.restart();
    io.poll();
    acceptNext = {};
}
} // namespace
