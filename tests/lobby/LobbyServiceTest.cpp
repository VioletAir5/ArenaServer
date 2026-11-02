#include "lobby/LobbyService.h"

#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "rpc/Server.h"
#include "security/SecureErase.h"

namespace service::lobby
{
namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Server = arena::internal_rpc::Server;
using Context = arena::internal_rpc::Context;
namespace wire = arena::protocol;
namespace rpc = arena::rpc;
namespace dto = arena::storage;
using Execute = LobbyService::Result<arena::lobby::ExecuteReply>;
const std::string gatewayId(32, '1');
const std::string password = "lobby-private-password-fixture";

std::int64_t civilNow()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
    )
        .count();
}

// Auth and Storage are external collaborator fixtures. Authority,
// WAL, Queue, RoomService, clients and actual RPC sockets remain real
// in every case.
struct Dependencies
{
    struct Claims
    {
        std::uint64_t player, epoch;
        std::int64_t expires;
    };
    boost::asio::io_context& io;
    std::unique_ptr<Server> auth, storage;
    bool holdAuth{false}, authReady{true}, storageReady{true};
    rpc::Status authError{rpc::OK}, playerError{rpc::OK},
        saveError{rpc::OK};
    bool persistUnknown{false};
    unsigned authCalls{0}, playerCalls{0}, saveCalls{0},
        matchCalls{0}, receiptCalls{0};
    unsigned cancellations{0};
    std::uint64_t issued{0}, nextId{1000};
    std::map<std::string, Claims> tokens;
    std::map<std::uint64_t, dto::MatchRecord> saved;
    std::vector<std::pair<Server::Completion, arena::rpc::Response>>
        held;
    std::vector<std::string> settlementKeys;
    explicit Dependencies(boost::asio::io_context& context)
        : io(context)
    {
        Server::Config config;
        config.service = "auth";
        config.callerTokens = {{"lobby", std::string(48, 'a')}};
        Server::Handlers handlers;
        handlers.emplace(
            "health.ready",
            [this](auto, auto, auto complete)
            {
                arena::rpc::Response result;
                if (!authReady)
                    result.set_status(rpc::UNAVAILABLE);
                complete(std::move(result));
                return Server::Cancellation{};
            }
        );
        handlers.emplace(
            "auth.authenticate",
            [this](auto request, auto, auto complete)
            {
                ++authCalls;
                arena::auth::AuthenticateRequest input;
                const bool parsed = input.ParseFromString(
                    request.payload()
                );
                arena::security::eraseSecret(
                    *request.mutable_payload()
                );
                arena::rpc::Response response;
                std::optional<Claims> prior;
                std::uint64_t player = 0;
                if (parsed &&
                    input.credential_kind() ==
                        arena::auth::AuthenticateRequest::PASSWORD &&
                    input.credential() == password)
                {
                    if (input.account() == "alice")
                        player = 42;
                    if (input.account() == "bob")
                        player = 43;
                }
                else if (
                    parsed &&
                    input.credential_kind() ==
                        arena::auth::AuthenticateRequest::JWT
                )
                {
                    const auto found = tokens.find(
                        input.credential()
                    );
                    if (found != tokens.end() &&
                        found->second.expires > civilNow() &&
                        input.proposed_epoch() > found->second.epoch)
                    {
                        prior = found->second;
                        player = prior->player;
                    }
                }
                arena::security::eraseSecret(
                    *input.mutable_credential()
                );
                if (authError != rpc::OK)
                    response.set_status(authError);
                else if (!player || !input.proposed_epoch())
                    response.set_status(rpc::UNAUTHENTICATED);
                else
                {
                    arena::auth::AuthenticateReply reply;
                    reply.set_player_id(player);
                    reply.set_epoch(input.proposed_epoch());
                    reply.set_expires_at_unix(civilNow() + 300);
                    const auto token = "aGVhZGVy.Y2xhaW1z.c2ln" +
                                       std::to_string(++issued);
                    tokens.emplace(
                        token,
                        Claims{
                            player,
                            input.proposed_epoch(),
                            reply.expires_at_unix()
                        }
                    );
                    reply.set_session_token(token);
                    reply.set_resumed(prior.has_value());
                    if (prior)
                    {
                        reply.set_previous_epoch(prior->epoch);
                        reply.set_previous_expires_at_unix(
                            prior->expires
                        );
                    }
                    reply.SerializeToString(
                        response.mutable_payload()
                    );
                    arena::security::eraseSecret(
                        *reply.mutable_session_token()
                    );
                }
                if (holdAuth)
                {
                    held.emplace_back(
                        std::move(complete),
                        std::move(response)
                    );
                    return Server::Cancellation([this]
                                                { ++cancellations; });
                }
                complete(std::move(response));
                return Server::Cancellation{};
            }
        );
        auth = std::make_unique<Server>(
            io.get_executor(),
            std::move(config),
            std::move(handlers)
        );
        config = {};
        config.service = "storage";
        config.callerTokens = {{"lobby", std::string(48, 's')}};
        handlers.clear();
        for (const auto* method :
             {"health.ready",
              "player.get",
              "match.allocate_ids",
              "match.save_result",
              "match.get",
              "settlement.get"})
            handlers.emplace(
                method,
                [this, method](auto request, auto, auto complete)
                {
                    auto response = handle(method, request);
                    complete(std::move(response));
                    return Server::Cancellation{};
                }
            );
        storage = std::make_unique<Server>(
            io.get_executor(),
            std::move(config),
            std::move(handlers)
        );
    }
    void release()
    {
        auto replies = std::move(held);
        held.clear();
        for (auto& [complete, response] : replies)
            complete(std::move(response));
    }
    arena::rpc::Response handle(
        std::string_view method, const arena::rpc::Request& request
    )
    {
        arena::rpc::Response response;
        if (method == "health.ready")
        {
            if (!storageReady)
                response.set_status(rpc::UNAVAILABLE);
            return response;
        }
        if (method == "player.get")
        {
            ++playerCalls;
            if (playerError != rpc::OK)
            {
                response.set_status(playerError);
                return response;
            }
            dto::PlayerGetRequest input;
            if (!input.ParseFromString(request.payload()))
            {
                response.set_status(rpc::INVALID_ARGUMENT);
                return response;
            }
            dto::PlayerGetReply reply;
            auto* player = reply.mutable_player();
            player->set_player_id(input.player_id());
            player->set_account_id(input.player_id());
            player->set_state("active");
            player->set_display_name("Fixture player");
            player->set_mmr(input.player_id() == 42 ? 1100 : 1105);
            reply.SerializeToString(response.mutable_payload());
            return response;
        }
        if (method == "match.allocate_ids")
        {
            dto::AllocateIdsRequest input;
            if (!input.ParseFromString(request.payload()) ||
                !input.count())
            {
                response.set_status(rpc::INVALID_ARGUMENT);
                return response;
            }
            dto::AllocateIdsReply reply;
            reply.set_first(nextId);
            nextId += input.count();
            reply.set_last(nextId - 1);
            reply.SerializeToString(response.mutable_payload());
            return response;
        }
        if (method == "match.save_result")
        {
            ++saveCalls;
            settlementKeys.push_back(request.idempotency_key());
            dto::SaveResultRequest input;
            if (!input.ParseFromString(request.payload()) ||
                !input.has_match())
            {
                response.set_status(rpc::INVALID_ARGUMENT);
                return response;
            }
            if (saveError == rpc::OK ||
                (saveError == rpc::OUTCOME_UNKNOWN && persistUnknown))
                saved.insert_or_assign(
                    input.match().match_id(),
                    input.match()
                );
            if (saveError != rpc::OK)
            {
                response.set_status(saveError);
                return response;
            }
            dto::SaveResultReply reply;
            *reply.mutable_match() = input.match();
            receipt(
                input.match().match_id(),
                *reply.mutable_settlement()
            );
            reply.SerializeToString(response.mutable_payload());
            return response;
        }
        if (method == "match.get")
        {
            ++matchCalls;
            dto::MatchGetRequest input;
            if (!input.ParseFromString(request.payload()))
            {
                response.set_status(rpc::INVALID_ARGUMENT);
                return response;
            }
            const auto found = saved.find(input.match_id());
            if (found == saved.end())
            {
                response.set_status(rpc::NOT_FOUND);
                return response;
            }
            dto::MatchGetReply reply;
            *reply.mutable_match() = found->second;
            reply.SerializeToString(response.mutable_payload());
            return response;
        }
        ++receiptCalls;
        dto::SettlementGetRequest input;
        if (!input.ParseFromString(request.payload()))
        {
            response.set_status(rpc::INVALID_ARGUMENT);
            return response;
        }
        if (!saved.contains(input.settlement_id()))
        {
            response.set_status(rpc::NOT_FOUND);
            return response;
        }
        dto::SettlementGetReply reply;
        receipt(input.settlement_id(), *reply.mutable_settlement());
        reply.SerializeToString(response.mutable_payload());
        return response;
    }
    static void
    receipt(std::uint64_t id, dto::SettlementRecord& value)
    {
        value.set_settlement_id(id);
        value.set_match_id(id);
        value.set_event_id("01020304-0506-5708-890a-0b0c0d0e0f10");
        value.set_replayed(true);
    }
};

struct Harness
{
    boost::asio::io_context io;
    Dependencies dependencies{io};
    std::filesystem::path directory;
    LobbyService::Config config;
    std::unique_ptr<LobbyService> service;
    std::uint64_t requestId{0};
    explicit Harness(bool startNow = true)
    {
        std::string
            pattern = (std::filesystem::temp_directory_path() /
                       "arena-lobby-service-XXXXXX")
                          .string();
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error(
                "cannot create private Lobby journal directory"
            );
        directory = pattern;
        config.authority.journal.directory = directory;
        config.authority.bootId = "lobby-service-test-boot";
        config.authority.wallNow = civilNow;
        config.authority.rooms.externalTicks = true;
        config.authority.rooms.externalSettlement = true;
        config.authority.rooms.battleDurationTicks = 6;
        config.authority.rooms.readyTimeoutTicks = 200;
        config.authority.rooms.resultRetention = 5s;
        config.authority.registry.grace = 1s;
        config.auth.endpoint = {
            boost::asio::ip::address_v4::loopback(),
            dependencies.auth->port()
        };
        config.auth.token = std::string(48, 'a');
        config.auth.maxInflight = 16;
        config.storage.endpoint = {
            boost::asio::ip::address_v4::loopback(),
            dependencies.storage->port()
        };
        config.storage.token = std::string(48, 's');
        config.storage.maxInflight = 16;
        config.maxJobs = 16;
        config.transportLease = 5s;
        config.receiptRetention = 10s;
        config.settlementMaxElapsed = 2s;
        config.settlementMaxAttempts = 4;
        config.retryBase = 10ms;
        config.retryMax = 20ms;
        if (startNow)
            start();
    }
    ~Harness()
    {
        stop();
        dependencies.release();
        dependencies.auth->stop();
        dependencies.storage->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return dependencies.auth->stats().stopped &&
                       dependencies.storage->stats().stopped;
            }
        ));
        dependencies.auth.reset();
        dependencies.storage.reset();
        io.restart();
        io.poll();
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    template <class Predicate>
    bool
    until(Predicate predicate, std::chrono::milliseconds budget = 4s)
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
    void pump(std::chrono::milliseconds duration)
    {
        const auto end = Clock::now() + duration;
        while (Clock::now() < end)
        {
            io.restart();
            io.run_for(1ms);
        }
    }
    void start()
    {
        service = std::make_unique<LobbyService>(
            io.get_executor(),
            config
        );
        if (!until(
                [&]
                {
                    return service->stats().initialized ||
                           service->stats().failed;
                }
            ) ||
            service->stats().failed)
            throw std::runtime_error("Lobby initialization failed");
        if (!until([&] { return service->stats().ready; }))
            throw std::runtime_error("Lobby readiness timed out");
    }
    void stop()
    {
        if (!service)
            return;
        service->stop();
        dependencies.release();
        EXPECT_TRUE(until([&] { return service->stats().stopped; }));
        EXPECT_EQ(service->stats().active, 0u);
        EXPECT_EQ(service->stats().authInflight, 0u);
        EXPECT_EQ(service->stats().storageInflight, 0u);
        service.reset();
        io.restart();
        io.poll();
    }
    void restart()
    {
        stop();
        start();
    }
    static Context context(std::chrono::milliseconds budget = 2s)
    {
        return {
            Clock::now() + budget,
            "gateway-lobby-fixture",
            {"gateway", "lobby"}
        };
    }
    arena::lobby::ExecuteRequest request(
        std::uint64_t connection,
        const arena::lobby::ExecuteReply* session = nullptr
    )
    {
        arena::lobby::ExecuteRequest value;
        value.set_gateway_instance(gatewayId);
        value.set_connection_id(connection);
        value.mutable_message()->set_request_id(++requestId);
        if (session)
        {
            value.set_player_id(session->session().player_id());
            value.set_session_epoch(session->session().epoch());
        }
        return value;
    }
    Execute execute(arena::lobby::ExecuteRequest request)
    {
        std::optional<Execute> result;
        auto accepted = service->execute(
            std::move(request),
            context(),
            [&](auto reply) { result.emplace(std::move(reply)); }
        );
        if (!accepted)
            return std::unexpected(accepted.error());
        if (!until([&] { return result.has_value(); }))
            throw std::runtime_error("Lobby execution timed out");
        return std::move(*result);
    }
    arena::lobby::ExecuteRequest loginRequest(
        std::string account,
        std::uint64_t connection,
        std::string operation,
        std::string credential = password,
        bool jwt = false
    )
    {
        auto value = request(connection);
        value.mutable_message()->set_msg_id(wire::MSG_LOGIN_REQUEST);
        auto* login = value.mutable_message()
                          ->mutable_login_request();
        login->set_account(std::move(account));
        login->set_credential(std::move(credential));
        login->set_operation_key(std::move(operation));
        login->set_credential_kind(
            jwt ? wire::CREDENTIAL_JWT : wire::CREDENTIAL_PASSWORD
        );
        return value;
    }
    arena::lobby::ExecuteReply login(
        std::string account,
        std::uint64_t connection,
        std::string operation,
        std::string credential = password,
        bool jwt = false
    )
    {
        auto result = execute(loginRequest(
            std::move(account),
            connection,
            std::move(operation),
            std::move(credential),
            jwt
        ));
        if (!result || !result->message().has_login_response() ||
            !result->message().login_response().accepted())
            throw std::runtime_error("fixture login failed");
        return std::move(*result);
    }
    Execute state(
        std::uint64_t connection,
        const arena::lobby::ExecuteReply& session
    )
    {
        auto value = request(connection, &session);
        value.mutable_message()->mutable_session_state_request();
        return execute(std::move(value));
    }
    Execute roomStatus(
        std::uint64_t connection,
        const arena::lobby::ExecuteReply& session
    )
    {
        auto value = request(connection, &session);
        value.mutable_message()->mutable_room_status_request();
        return execute(std::move(value));
    }
    void joinRoom(
        std::uint64_t connection,
        const arena::lobby::ExecuteReply& session,
        std::uint64_t room
    )
    {
        auto value = request(connection, &session);
        value.mutable_message()
            ->mutable_room_join_request()
            ->set_room_id(room);
        auto result = execute(std::move(value));
        ASSERT_TRUE(result);
        ASSERT_TRUE(result->message().has_room_join_response());
    }
    arena::lobby::HeartbeatReply
    heartbeat(std::vector<std::uint64_t> ids)
    {
        arena::lobby::HeartbeatRequest request;
        request.set_gateway_instance(gatewayId);
        for (const auto id : ids)
            request.add_connection_ids(id);
        std::optional<
            LobbyService::Result<arena::lobby::HeartbeatReply>>
            reply;
        auto accepted = service->heartbeat(
            request,
            context(),
            [&](auto r) { reply.emplace(std::move(r)); }
        );
        if (!accepted || !until([&] { return reply.has_value(); }) ||
            !*reply)
            throw std::runtime_error("heartbeat failed");
        return std::move(**reply);
    }
    void detach(
        std::uint64_t connection,
        const arena::lobby::ExecuteReply& session
    )
    {
        arena::lobby::DetachRequest request;
        request.set_gateway_instance(gatewayId);
        request.set_connection_id(connection);
        request.set_player_id(session.session().player_id());
        request.set_session_epoch(session.session().epoch());
        std::optional<LobbyService::Result<arena::lobby::DetachReply>>
            reply;
        auto accepted = service->detach(
            request,
            context(),
            [&](auto r) { reply.emplace(std::move(r)); }
        );
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(until([&] { return reply.has_value(); }));
        ASSERT_TRUE(*reply);
        EXPECT_GT((**reply).committed_revision(), 0u);
    }
    struct Match
    {
        std::uint64_t id, room, incarnation, aliceEpoch, bobEpoch;
        arena::lobby::ExecuteReply alice, bob;
    };
    Match match()
    {
        auto alice = login("alice", 1, "match-alice-00000001");
        auto bob = login("bob", 2, "match-bob-0000000001");
        for (const auto* session : {&alice, &bob})
        {
            auto value = request(session == &alice ? 1 : 2, session);
            value.mutable_message()
                ->mutable_match_join_request()
                ->set_join_key(9);
            auto joined = execute(std::move(value));
            if (!joined ||
                !joined->message().has_match_join_response())
                throw std::runtime_error("match join failed");
            EXPECT_EQ(
                joined->message().match_join_response().state().mmr(),
                session == &alice ? 1100u : 1105u
            );
        }
        std::optional<arena::protocol::RoomStatus> status;
        if (!until(
                [&]
                {
                    auto result = roomStatus(1, alice);
                    if (result &&
                        result->message().has_room_status_response())
                        status = result->message()
                                     .room_status_response()
                                     .state();
                    return status.has_value();
                }
            ))
            throw std::runtime_error("managed room not allocated");
        if (status->members_size() != 2)
            throw std::runtime_error("incomplete match cohort");
        Match result{
            status->match_id(),
            status->snapshot().room_id(),
            status->snapshot().incarnation(),
            0,
            0,
            std::move(alice),
            std::move(bob)
        };
        for (const auto& member : status->members())
            (member.player_id() == 42
                 ? result.aliceEpoch
                 : result.bobEpoch) = member.member_epoch();
        return result;
    }
    void ready(const Match& match)
    {
        for (unsigned i = 0; i != 2; ++i)
        {
            const auto& session = i ? match.bob : match.alice;
            auto value = request(i + 1, &session);
            auto* ready = value.mutable_message()
                              ->mutable_room_ready_request();
            ready->set_ready(true);
            ready->set_actor_incarnation(match.incarnation);
            ready->set_membership_epoch(
                i ? match.bobEpoch : match.aliceEpoch
            );
            auto result = execute(std::move(value));
            ASSERT_TRUE(result);
            ASSERT_TRUE(result->message().has_room_ready_response());
        }
    }
};
arena::lobby_meta::Metadata durableMetadata(Harness& fixture)
{
    fixture.stop();
    arena::lobby_meta::Metadata metadata;
    {
        DurableStateStore reader(
            fixture.io.get_executor(),
            fixture.config.authority.journal
        );
        const auto snapshot = reader.snapshot();
        const auto found = snapshot->values.find("authority/meta");
        if (found == snapshot->values.end() ||
            !metadata.ParseFromString(found->second))
            throw std::runtime_error("durable metadata missing");
        for (const auto& [key, value] : snapshot->values)
            if (key.starts_with("replay/"))
            {
                const auto log = decodeReplayLog(value);
                EXPECT_TRUE(log);
                if (log)
                {
                    EXPECT_TRUE(
                        service::room::RoomActor::replay(*log)
                    );
                }
            }
        reader.stop();
        EXPECT_TRUE(fixture.until([&]
                                  { return reader.stats().closed; }));
    }
    return metadata;
}
void businessError(const Execute& result, wire::ErrorCode code)
{
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->message().has_error());
    EXPECT_EQ(
        result->message().error().code(),
        static_cast<unsigned>(code)
    );
    EXPECT_GT(result->committed_revision(), 0u);
}

TEST(
    LobbyServiceTest,
    ConcurrentLoginsReserveDifferentEpochsAndCommitExactSessionProjection
)
{
    Harness h;
    std::optional<Execute> alice, bob;
    auto a = h.service->execute(
        h.loginRequest("alice", 1, "alice-concurrent-0001"),
        Harness::context(),
        [&](auto r) { alice.emplace(std::move(r)); }
    );
    auto b = h.service->execute(
        h.loginRequest("bob", 2, "bob-concurrent-000001"),
        Harness::context(),
        [&](auto r) { bob.emplace(std::move(r)); }
    );
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(h.until([&] { return alice && bob; }));
    ASSERT_TRUE(*alice);
    ASSERT_TRUE(*bob);
    ASSERT_TRUE((*alice)->message().has_login_response());
    ASSERT_TRUE((*bob)->message().has_login_response());
    EXPECT_NE((*alice)->session().epoch(), (*bob)->session().epoch());
    EXPECT_NE(
        (*alice)->session().session_id(),
        (*bob)->session().session_id()
    );
    for (const auto* reply : {&**alice, &**bob})
    {
        EXPECT_GT(reply->committed_revision(), 0u);
        EXPECT_EQ(
            reply->session().player_id(),
            reply->message().login_response().player_id()
        );
        EXPECT_EQ(
            reply->session().epoch(),
            reply->message().login_response().session_epoch()
        );
        EXPECT_EQ(
            reply->session().session_id(),
            reply->message().login_response().session_id()
        );
        EXPECT_GT(reply->session().expires_at_unix(), civilNow());
    }
    EXPECT_EQ(h.dependencies.authCalls, 2u);
    EXPECT_EQ(h.dependencies.playerCalls, 2u);
    EXPECT_FALSE(h.service->stats().failed);
}

TEST(
    LobbyServiceTest,
    ManualRoutesAndInputAcknowledgementsUseTheRealRoomTick
)
{
    Harness h;
    auto alice = h.login("alice", 1, "manual-alice-00000001");
    h.joinRoom(1, alice, 7);
    auto state = h.state(1, alice);
    ASSERT_TRUE(state);
    ASSERT_TRUE(state->message().has_session_state_response());
    EXPECT_EQ(
        state->message().session_state_response().room_id(),
        7u
    );
    auto snapshotRequest = h.request(1, &alice);
    snapshotRequest.mutable_message()
        ->mutable_room_snapshot_request();
    auto snapshot = h.execute(snapshotRequest);
    ASSERT_TRUE(snapshot);
    ASSERT_TRUE(snapshot->message().has_room_snapshot_response());
    const auto
        first = snapshot->message().room_snapshot_response().state();
    ASSERT_EQ(first.players_size(), 1);
    EXPECT_GT(first.tick(), 0u);
    auto input = h.request(1, &alice);
    auto* control = input.mutable_message()
                        ->mutable_room_input_request();
    control->set_actor_incarnation(first.incarnation());
    control->set_membership_epoch(first.players(0).member_epoch());
    control->set_seq(1);
    control->set_dx(1);
    control->set_dy(0);
    const auto requestId = input.message().request_id();
    auto applied = h.execute(input);
    ASSERT_TRUE(applied);
    ASSERT_TRUE(applied->message().has_room_input_response());
    EXPECT_EQ(applied->message().request_id(), requestId);
    EXPECT_EQ(
        applied->message().msg_id(),
        wire::MSG_ROOM_INPUT_RESPONSE
    );
    EXPECT_EQ(
        applied->message().room_input_response().result(),
        wire::ROOM_INPUT_APPLIED
    );
    EXPECT_GT(
        applied->message().room_input_response().state().tick(),
        first.tick()
    );
    EXPECT_EQ(
        applied->message()
            .room_input_response()
            .state()
            .players(0)
            .last_seq(),
        1u
    );
    input.mutable_message()->set_request_id(++h.requestId);
    auto duplicate = h.execute(input);
    ASSERT_TRUE(duplicate);
    ASSERT_TRUE(duplicate->message().has_room_input_response());
    EXPECT_EQ(
        duplicate->message().room_input_response().result(),
        wire::ROOM_INPUT_DUPLICATE
    );
    input.mutable_message()
        ->mutable_room_input_request()
        ->set_actor_incarnation(first.incarnation() + 1);
    input.mutable_message()->set_request_id(++h.requestId);
    businessError(h.execute(input), wire::ERR_BUSINESS_FAILURE);
    auto ping = h.request(1, &alice);
    ping.mutable_message()->mutable_ping()->set_nonce(99);
    auto pong = h.execute(std::move(ping));
    ASSERT_TRUE(pong);
    EXPECT_EQ(pong->message().pong().nonce(), 99u);
    auto leave = h.request(1, &alice);
    leave.mutable_message()->mutable_room_leave_request();
    auto left = h.execute(std::move(leave));
    ASSERT_TRUE(left);
    ASSERT_TRUE(left->message().has_room_leave_response());
    EXPECT_EQ(
        left->message().room_leave_response().state().room_id(),
        0u
    );
}

TEST(
    LobbyServiceTest,
    PasswordReceiptSurvivesRestartAndCannotStealANewerPasswordSession
)
{
    Harness h;
    const std::string operation = "password-operation-0001";
    auto first = h.login("alice", 1, operation);
    h.joinRoom(1, first, 7);
    const auto firstRevision = first.committed_revision();
    h.restart();
    auto recovered = h.login("alice", 2, operation);
    EXPECT_EQ(
        recovered.session().session_id(),
        first.session().session_id()
    );
    EXPECT_GT(recovered.session().epoch(), first.session().epoch());
    EXPECT_GT(recovered.committed_revision(), firstRevision);
    EXPECT_TRUE(recovered.message().login_response().resumed());
    EXPECT_EQ(recovered.message().login_response().room_id(), 7u);
    const auto beforeReplay = h.dependencies.authCalls;
    auto repeated = h.login("alice", 2, operation);
    EXPECT_EQ(
        repeated.session().epoch(),
        recovered.session().epoch()
    );
    EXPECT_EQ(
        repeated.session().session_id(),
        recovered.session().session_id()
    );
    EXPECT_EQ(h.dependencies.authCalls, beforeReplay + 1);
    auto fresh = h.login("alice", 3, "password-new-session-0001");
    EXPECT_NE(
        fresh.session().session_id(),
        recovered.session().session_id()
    );
    EXPECT_FALSE(fresh.message().login_response().resumed());
    businessError(
        h.execute(h.loginRequest("alice", 4, operation)),
        wire::ERR_UNAUTHENTICATED
    );
    auto current = h.state(3, fresh);
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->message().has_session_state_response());
    EXPECT_EQ(
        current->message().session_state_response().session_id(),
        fresh.session().session_id()
    );
    h.stop();
    std::ifstream input(
        h.directory / "authority.wal",
        std::ios::binary
    );
    const std::string contents(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>{}
    );
    ASSERT_FALSE(contents.empty());
    EXPECT_EQ(contents.find(password), std::string::npos);
    EXPECT_EQ(contents.find(std::string(48, 'a')), std::string::npos);
    EXPECT_EQ(contents.find(std::string(48, 's')), std::string::npos);
}

TEST(
    LobbyServiceTest,
    OldJwtIsRecoverableOnlyForTheExactDurableOperationReceipt
)
{
    Harness h;
    auto passwordLogin = h.login("alice", 1, "jwt-initial-pass-0001");
    h.joinRoom(1, passwordLogin, 7);
    const auto oldToken = passwordLogin.message()
                              .login_response()
                              .session_token();
    auto renewed = h.login(
        "",
        2,
        "jwt-resume-operation-01",
        oldToken,
        true
    );
    h.restart();
    auto recovered = h.login(
        "",
        3,
        "jwt-resume-operation-01",
        oldToken,
        true
    );
    EXPECT_EQ(
        recovered.session().session_id(),
        renewed.session().session_id()
    );
    EXPECT_GT(recovered.session().epoch(), renewed.session().epoch());
    EXPECT_EQ(recovered.message().login_response().room_id(), 7u);
    EXPECT_TRUE(recovered.message().login_response().resumed());
    businessError(
        h.execute(h.loginRequest(
            "",
            4,
            "jwt-other-operation-01",
            oldToken,
            true
        )),
        wire::ERR_UNAUTHENTICATED
    );
    auto current = h.state(3, recovered);
    ASSERT_TRUE(current);
    EXPECT_EQ(
        current->session().epoch(),
        recovered.session().epoch()
    );
}

TEST(
    LobbyServiceTest,
    HeartbeatAndDetachFenceOldPhysicalConnectionsAndKeepCurrentBinding
)
{
    Harness h;
    auto first = h.login("alice", 1, "transport-initial-0001");
    h.joinRoom(1, first, 7);
    auto replacement = h.login(
        "",
        2,
        "transport-resume-00001",
        first.message().login_response().session_token(),
        true
    );
    auto heartbeat = h.heartbeat({1, 2, 99});
    EXPECT_EQ(
        (std::vector<std::uint64_t>(
            heartbeat.revoked_connection_ids().begin(),
            heartbeat.revoked_connection_ids().end()
        )),
        (std::vector<std::uint64_t>{1, 99})
    );
    h.detach(1, first);
    auto stillCurrent = h.state(2, replacement);
    ASSERT_TRUE(stillCurrent);
    ASSERT_TRUE(stillCurrent->message().has_session_state_response());
    h.detach(2, replacement);
    auto detached = h.heartbeat({2});
    ASSERT_EQ(detached.revoked_connection_ids_size(), 1);
    EXPECT_EQ(detached.revoked_connection_ids(0), 2u);
    auto resumed = h.login(
        "",
        3,
        "transport-resume-00002",
        replacement.message().login_response().session_token(),
        true
    );
    EXPECT_EQ(
        resumed.session().session_id(),
        replacement.session().session_id()
    );
    EXPECT_EQ(resumed.message().login_response().room_id(), 7u);
}

TEST(
    LobbyServiceTest,
    LeaseExpiryAndExpiredGraceNeverFallBackToAFreshJwtSession
)
{
    Harness h(false);
    h.config.transportLease = 150ms;
    h.config.authority.registry.grace = 200ms;
    h.start();
    auto alice = h.login("alice", 1, "lease-alice-initial-01");
    h.pump(450ms);
    businessError(h.state(1, alice), wire::ERR_UNAUTHENTICATED);
    businessError(
        h.execute(h.loginRequest(
            "",
            2,
            "lease-expired-jwt-001",
            alice.message().login_response().session_token(),
            true
        )),
        wire::ERR_UNAUTHENTICATED
    );
}

TEST(
    LobbyServiceTest,
    ExpiredManagedMemberJwtIsRejectedWithHistoricalFinishedCohortRetained
)
{
    Harness h(false);
    h.config.authority.registry.grace = 150ms;
    h.config.authority.rooms.battleDurationTicks = 1000;
    h.start();
    const auto match = h.match();
    h.ready(match);
    ASSERT_TRUE(h.until(
        [&]
        {
            const auto response = h.roomStatus(2, match.bob);
            return response &&
                   response->message().has_room_status_response() &&
                   response->message()
                           .room_status_response()
                           .state()
                           .phase() == wire::ROOM_PHASE_PLAYING;
        }
    ));
    h.detach(1, match.alice);
    std::optional<wire::RoomStatus> finished;
    ASSERT_TRUE(h.until(
        [&]
        {
            const auto response = h.roomStatus(2, match.bob);
            if (response &&
                response->message().has_room_status_response())
                finished = response->message()
                               .room_status_response()
                               .state();
            return finished &&
                   finished->phase() == wire::ROOM_PHASE_FINISHED;
        }
    ));
    ASSERT_TRUE(finished->has_settlement());
    EXPECT_EQ(
        finished->settlement().reason(),
        wire::SETTLEMENT_REASON_MEMBER_UNAVAILABLE
    );
    ASSERT_TRUE(h.dependencies.saved.contains(match.id));
    EXPECT_EQ(finished->members_size(), 2);
    const auto bobBefore = h.state(2, match.bob);
    ASSERT_TRUE(bobBefore);
    ASSERT_TRUE(bobBefore->message().has_session_state_response());
    EXPECT_EQ(
        bobBefore->message().session_state_response().room_id(),
        0u
    );

    // A failed resume still commits the candidate's maintenance
    // state. The finished actor keeps its immutable, now historical
    // cohort.
    const auto staleToken = match.alice.message()
                                .login_response()
                                .session_token();
    businessError(
        h.execute(h.loginRequest("", 3, "", staleToken, true)),
        wire::ERR_UNAUTHENTICATED
    );
    const auto bobAfter = h.state(2, match.bob);
    ASSERT_TRUE(bobAfter);
    ASSERT_TRUE(bobAfter->message().has_session_state_response());
    EXPECT_EQ(
        bobAfter->session().session_id(),
        match.bob.session().session_id()
    );
    EXPECT_EQ(
        bobAfter->session().epoch(),
        match.bob.session().epoch()
    );
    const auto metadata = durableMetadata(h);
    EXPECT_EQ(metadata.receipts_size(), 2);
    ASSERT_EQ(metadata.settlements_size(), 1);
    EXPECT_EQ(
        metadata.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
    );
    h.start();
    businessError(
        h.execute(h.loginRequest(
            "",
            4,
            "expired-managed-retry-01",
            staleToken,
            true
        )),
        wire::ERR_UNAUTHENTICATED
    );
    EXPECT_FALSE(h.service->stats().failed);
}
TEST(
    LobbyServiceTest,
    DelayedOldLoginCannotReplaceTheNewerCommittedPlayerEpoch
)
{
    Harness h;
    h.dependencies.holdAuth = true;
    std::optional<Execute> old;
    auto accepted = h.service->execute(
        h.loginRequest("alice", 1, "old-login-operation-01"),
        Harness::context(),
        [&](auto r) { old.emplace(std::move(r)); }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(h.until([&]
                        { return h.dependencies.authCalls == 1; }));
    h.dependencies.holdAuth = false;
    auto latest = h.login("alice", 2, "new-login-operation-01");
    h.dependencies.release();
    ASSERT_TRUE(h.until([&] { return old.has_value(); }));
    businessError(*old, wire::ERR_UNAUTHENTICATED);
    auto current = h.state(2, latest);
    ASSERT_TRUE(current);
    EXPECT_EQ(current->session().epoch(), latest.session().epoch());
    EXPECT_EQ(h.heartbeat({1, 2}).revoked_connection_ids_size(), 1);
}

TEST(
    LobbyServiceTest,
    TwoPlayersReachDurableSettlementAndReplayJournalChecksumsStayValid
)
{
    Harness h;
    auto match = h.match();
    h.ready(match);
    ASSERT_TRUE(h.until(
        [&]
        {
            auto response = h.roomStatus(1, match.alice);
            return response &&
                   response->message().has_room_status_response() &&
                   response->message()
                           .room_status_response()
                           .state()
                           .phase() == wire::ROOM_PHASE_PLAYING;
        }
    ));
    for (unsigned i = 0; i != 2; ++i)
    {
        auto request = h.request(
            i + 1,
            i ? &match.bob : &match.alice
        );
        auto* input = request.mutable_message()
                          ->mutable_room_input_request();
        input->set_actor_incarnation(match.incarnation);
        input->set_membership_epoch(
            i ? match.bobEpoch : match.aliceEpoch
        );
        input->set_seq(1);
        input->set_dx(i ? -1 : 1);
        auto applied = h.execute(std::move(request));
        ASSERT_TRUE(applied);
        ASSERT_TRUE(applied->message().has_room_input_response());
        EXPECT_EQ(
            applied->message().room_input_response().result(),
            wire::ROOM_INPUT_APPLIED
        );
    }
    bool finished = false;
    std::optional<wire::RoomStatus> status;
    ASSERT_TRUE(h.until(
        [&]
        {
            auto response = h.roomStatus(1, match.alice);
            if (response &&
                response->message().has_room_status_response())
            {
                status = response->message()
                             .room_status_response()
                             .state();
                finished = status->phase() ==
                           wire::ROOM_PHASE_FINISHED;
            }
            return finished;
        }
    ));
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->has_settlement());
    EXPECT_EQ(status->settlement().match_id(), match.id);
    EXPECT_EQ(
        status->played_ticks(),
        h.config.authority.rooms.battleDurationTicks
    );
    ASSERT_TRUE(h.dependencies.saved.contains(match.id));
    EXPECT_EQ(
        h.dependencies.saved.at(match.id).final_checksum(),
        status->snapshot().checksum()
    );
    EXPECT_EQ(h.dependencies.saveCalls, 1u);
    ASSERT_EQ(h.dependencies.settlementKeys.size(), 1u);
    EXPECT_EQ(
        h.dependencies.settlementKeys[0],
        "lobby-settlement/" + std::to_string(match.id)
    );
    auto current = h.state(1, match.alice);
    ASSERT_TRUE(current);
    EXPECT_EQ(
        current->message().session_state_response().room_id(),
        0u
    );
    auto receipt = h.request(1, &match.alice);
    receipt.mutable_message()
        ->mutable_settlement_request()
        ->set_match_id(match.id);
    auto result = h.execute(std::move(receipt));
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->message().has_settlement_response());
    EXPECT_EQ(
        result->message()
            .settlement_response()
            .state()
            .final_checksum(),
        status->snapshot().checksum()
    );
    const auto metadata = durableMetadata(h);
    ASSERT_EQ(metadata.settlements_size(), 1);
    EXPECT_EQ(
        metadata.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
    );
    EXPECT_EQ(metadata.settlements(0).attempts(), 1u);
}

TEST(
    LobbyServiceTest,
    UnknownSettlementAcknowledgementIsReconciledWithoutResavingOrEarlyFinish
)
{
    Harness h;
    h.dependencies.saveError = rpc::OUTCOME_UNKNOWN;
    h.dependencies.persistUnknown = true;
    auto match = h.match();
    h.ready(match);
    bool finished = false;
    ASSERT_TRUE(h.until(
        [&]
        {
            auto response = h.roomStatus(1, match.alice);
            finished = response &&
                       response->message()
                           .has_room_status_response() &&
                       response->message()
                               .room_status_response()
                               .state()
                               .phase() == wire::ROOM_PHASE_FINISHED;
            return finished;
        }
    ));
    EXPECT_EQ(h.dependencies.saveCalls, 1u);
    EXPECT_EQ(h.dependencies.matchCalls, 1u);
    EXPECT_EQ(h.dependencies.receiptCalls, 1u);
    const auto metadata = durableMetadata(h);
    ASSERT_EQ(metadata.settlements_size(), 1);
    EXPECT_EQ(metadata.settlements(0).attempts(), 2u);
    EXPECT_EQ(
        metadata.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
    );
    h.start();
    h.pump(150ms);
    EXPECT_EQ(h.dependencies.saveCalls, 1u);
    auto current = h.state(1, match.alice);
    ASSERT_TRUE(current);
    EXPECT_EQ(
        current->message().session_state_response().room_id(),
        0u
    );
}

TEST(
    LobbyServiceTest,
    ExhaustedSettlementBudgetRemainsBlockedAcrossRealJournalRecovery
)
{
    Harness h(false);
    h.config.settlementMaxAttempts = 2;
    h.start();
    h.dependencies.saveError = rpc::UNAVAILABLE;
    auto match = h.match();
    h.ready(match);
    ASSERT_TRUE(h.until(
        [&]
        {
            return h.dependencies.saveCalls == 1 &&
                   h.dependencies.matchCalls == 1;
        }
    ));
    h.pump(150ms);
    auto metadata = durableMetadata(h);
    ASSERT_EQ(metadata.settlements_size(), 1);
    EXPECT_EQ(metadata.settlements(0).attempts(), 2u);
    EXPECT_EQ(
        metadata.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::BLOCKED
    );
    const auto deadline = metadata.settlements(0).deadline_nanos();
    h.start();
    h.pump(200ms);
    EXPECT_EQ(h.dependencies.saveCalls, 1u);
    EXPECT_EQ(h.dependencies.matchCalls, 1u);
    auto status = h.roomStatus(1, match.alice);
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->message().has_room_status_response());
    EXPECT_EQ(
        status->message().room_status_response().state().phase(),
        wire::ROOM_PHASE_SETTLING
    );
    EXPECT_FALSE(status->message()
                     .room_status_response()
                     .state()
                     .has_settlement());
    metadata = durableMetadata(h);
    EXPECT_EQ(metadata.settlements(0).attempts(), 2u);
    EXPECT_EQ(metadata.settlements(0).deadline_nanos(), deadline);
}

TEST(
    LobbyServiceTest,
    DependencyOutageConsumesFrozenElapsedBudgetEvenBeforeFirstSubmission
)
{
    Harness h(false);
    h.config.authority.rooms.battleDurationTicks = 20;
    h.config.settlementMaxElapsed = 1s;
    h.start();
    auto match = h.match();
    h.ready(match);
    h.dependencies.storageReady = false;
    h.pump(2300ms);
    auto metadata = durableMetadata(h);
    ASSERT_EQ(metadata.settlements_size(), 1);
    EXPECT_EQ(metadata.settlements(0).attempts(), 0u);
    EXPECT_EQ(
        metadata.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::BLOCKED
    );
    EXPECT_EQ(h.dependencies.saveCalls, 0u);
    h.dependencies.storageReady = true;
    h.start();
    h.pump(150ms);
    EXPECT_EQ(h.dependencies.saveCalls, 0u);
}

TEST(
    LobbyServiceTest,
    CancellationOverloadAndTypedDependencyErrorsDoNotLeakJobsOrReplaceAuthority
)
{
    Harness h(false);
    h.config.maxJobs = 1;
    h.start();
    h.dependencies.holdAuth = true;
    std::optional<Execute> cancelled;
    std::size_t activeWhenNotified = 0;
    auto accepted = h.service->execute(
        h.loginRequest("alice", 1, "cancel-login-operation-01"),
        Harness::context(),
        [&](auto r)
        {
            activeWhenNotified = h.service->stats().active;
            cancelled.emplace(std::move(r));
        }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(h.until([&]
                        { return h.dependencies.authCalls == 1; }));
    bool called = false;
    auto overload = h.service->execute(
        h.loginRequest("bob", 2, "overload-operation-001"),
        Harness::context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(overload);
    EXPECT_EQ(overload.error(), rpc::OVERLOADED);
    (*accepted)();
    (*accepted)();
    ASSERT_TRUE(h.until([&] { return cancelled.has_value(); }));
    ASSERT_FALSE(*cancelled);
    EXPECT_EQ(cancelled->error(), rpc::CANCELLED);
    EXPECT_GE(activeWhenNotified, 1u);
    EXPECT_FALSE(called);
    ASSERT_TRUE(
        h.until([&] { return h.service->stats().authInflight == 0; })
    );
    h.dependencies.holdAuth = false;
    h.dependencies.release();
    for (const auto status :
         {rpc::INCOMPATIBLE_VERSION,
          rpc::CALL_CYCLE,
          rpc::OUTCOME_UNKNOWN})
    {
        h.dependencies.authError = status;
        auto result = h.execute(
            h.loginRequest("alice", 1, "dependency-failure-001")
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), status);
    }
    h.dependencies.authError = rpc::OK;
    auto alice = h.login("alice", 1, "after-cancel-login-001");
    ASSERT_TRUE(alice.message().has_login_response());
    EXPECT_FALSE(h.service->stats().failed);
}

TEST(
    LobbyServiceTest,
    InvalidInputRejectsBeforeWorkAndStoppedServiceReleasesPhysicalDescriptors
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
        Harness h;
        bool called = false;
        auto request = h.loginRequest(
            "alice",
            1,
            "valid-operation-key-01"
        );
        request.set_gateway_instance("invalid");
        auto rejected = h.service->execute(
            request,
            Harness::context(),
            [&](auto) { called = true; }
        );
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), rpc::INVALID_ARGUMENT);
        request = h.loginRequest("alice", 1, "short");
        EXPECT_FALSE(h.service->execute(
            request,
            Harness::context(),
            [&](auto) { called = true; }
        ));
        request = h.loginRequest(
            "alice",
            1,
            "valid-operation-key-02"
        );
        request.mutable_message()->set_msg_id(
            wire::MSG_ROOM_INPUT_REQUEST
        );
        EXPECT_FALSE(h.service->execute(
            request,
            Harness::context(),
            [&](auto) { called = true; }
        ));
        arena::lobby::HeartbeatRequest heartbeat;
        heartbeat.set_gateway_instance(gatewayId);
        for (unsigned i = 1; i <= 513; ++i)
            heartbeat.add_connection_ids(i);
        EXPECT_FALSE(h.service->heartbeat(
            heartbeat,
            Harness::context(),
            [&](auto) { called = true; }
        ));
        EXPECT_FALSE(called);
        EXPECT_EQ(h.dependencies.authCalls, 0u);
        h.stop();
    }
    EXPECT_EQ(descriptors(), baseline);
}
TEST(
    LobbyServiceTest,
    CorruptJournalSurfacesFatalInitializationWithoutReadiness
)
{
    Harness h(false);
    {
        std::ofstream corrupted(
            h.directory / "authority.wal",
            std::ios::binary
        );
        corrupted << "invalid-private-journal";
    }
    h.service = std::make_unique<LobbyService>(
        h.io.get_executor(),
        h.config
    );
    ASSERT_TRUE(h.until([&] { return h.service->stats().failed; }));
    EXPECT_FALSE(h.service->stats().ready);
    h.stop();
}
TEST(
    LobbyServiceTest,
    ReceiptExpiresExactlyAtItsFrozenCutoffBeforeTickGarbageCollection
)
{
    auto now = Clock::now();
    const auto admitted = now;
    Harness h(false);
    h.config.authority.autoTicks = false;
    h.config.authority.now = [&] { return now; };
    h.config.receiptRetention = 1s;
    h.start();
    const std::string operation = "exact-receipt-expiry-01";
    const auto original = h.login("alice", 1, operation);
    now = admitted + 1s - 1ns;
    auto before = h.execute(h.loginRequest("alice", 1, operation));
    ASSERT_TRUE(before);
    ASSERT_TRUE(before->message().has_login_response());
    EXPECT_EQ(before->session().epoch(), original.session().epoch());
    EXPECT_EQ(
        before->session().session_id(),
        original.session().session_id()
    );
    now = admitted + 1s;
    const auto authentications = h.dependencies.authCalls;
    auto expired = h.execute(h.loginRequest("alice", 1, operation));
    businessError(expired, wire::ERR_UNAUTHENTICATED);
    // The original password still must be checked once. The expired
    // receipt must never cause a second authentication using its
    // retained bearer token.
    EXPECT_EQ(h.dependencies.authCalls, authentications + 1);
    const auto beforeReconnect = h.dependencies.authCalls;
    auto expiredReconnect = h.execute(
        h.loginRequest("alice", 2, operation)
    );
    businessError(expiredReconnect, wire::ERR_UNAUTHENTICATED);
    EXPECT_EQ(h.dependencies.authCalls, beforeReconnect + 1);
    auto current = h.state(1, original);
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->message().has_session_state_response());
    EXPECT_EQ(current->session().epoch(), original.session().epoch());
    EXPECT_EQ(
        current->session().session_id(),
        original.session().session_id()
    );
}

} // namespace
} // namespace service::lobby