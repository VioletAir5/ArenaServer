#include "lobby/StorageClient.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
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
using Status = arena::rpc::Status;
namespace dto = arena::storage;
constexpr auto max64 = std::numeric_limits<std::uint64_t>::max();
constexpr auto managedBit = std::uint64_t{1} << 63;
const std::string event = "01020304-0506-5708-890a-0b0c0d0e0f10";

dto::MatchRecord record(std::uint64_t id = 21)
{
    dto::MatchRecord value;
    value.set_match_id(id);
    value.set_room_id(managedBit | id);
    value.set_state("completed");
    value.set_result_version(1);
    value.set_final_tick(200);
    value.set_final_checksum(max64);
    value.set_finish_reason("completed");
    value.set_created_unix_ms(1000);
    value.set_started_unix_ms(1100);
    value.set_finished_unix_ms(1200);
    for (std::uint32_t i = 0; i != 2; ++i)
    {
        auto* p = value.add_participants();
        p->set_player_id(max64 - i);
        p->set_slot(i + 1);
        p->set_session_id(max64 - 2 - i);
        p->set_membership_epoch(max64 - 4 - i);
        p->set_initial_mmr(1500 + i);
        p->set_last_input_seq(max64);
        p->set_final_x(i ? INT32_MAX : INT32_MIN);
        p->set_final_y(i ? INT32_MIN : INT32_MAX);
        p->set_last_dx(-1);
        p->set_last_dy(1);
    }
    return value;
}

dto::SaveResultRequest saveRequest()
{
    dto::SaveResultRequest value;
    *value.mutable_match() = record();
    value.mutable_match()->set_created_unix_ms(0);
    value.mutable_match()->set_started_unix_ms(0);
    value.mutable_match()->set_finished_unix_ms(0);
    return value;
}

struct StoragePeer
{
    bool hold{false}, malformed{false}, oversized{false},
        nonemptyProbe{false};
    Status error{arena::rpc::OK};
    std::uint64_t firstId{100};
    unsigned calls{0}, cancellations{0};
    std::vector<Context> contexts;
    std::vector<arena::rpc::Request> requests;
    std::vector<Server::Completion> held;
    std::function<void(dto::PlayerGetReply&)> changePlayer;
    std::function<void(dto::AllocateIdsReply&)> changeIds;
    std::function<void(dto::SaveResultReply&)> changeSave;
    std::function<void(dto::MatchGetReply&)> changeMatch;
    std::function<void(dto::SettlementGetReply&)> changeSettlement;
    std::unique_ptr<Server> server;

    explicit StoragePeer(
        boost::asio::io_context& io, std::string service = "storage"
    )
    {
        Server::Config config;
        config.service = std::move(service);
        config.callerTokens = {{"lobby", std::string(48, 's')}};
        Server::Handlers handlers;
        for (const auto* method :
             {"player.get",
              "match.allocate_ids",
              "match.save_result",
              "match.get",
              "settlement.get",
              "health.ready"})
            handlers.emplace(
                method,
                [this,
                 method](auto request, auto context, auto complete)
                {
                    ++calls;
                    contexts.push_back(context);
                    requests.push_back(request);
                    if (hold)
                    {
                        held.push_back(std::move(complete));
                        return Server::Cancellation(
                            [this] { ++cancellations; }
                        );
                    }
                    arena::rpc::Response response;
                    response.set_status(error);
                    if (error == arena::rpc::OK)
                    {
                        if (malformed)
                            response.set_payload(
                                std::string(
                                    1,
                                    static_cast<char>(0xff)
                                )
                            );
                        else if (oversized)
                            response.set_payload(
                                std::string(8193, 'x')
                            );
                        else if (
                            std::string_view(method) == "health.ready"
                        )
                        {
                            EXPECT_TRUE(request.payload().empty());
                            if (nonemptyProbe)
                                response.set_payload(
                                    "unexpected body"
                                );
                        }
                        else
                            response.set_payload(
                                reply(method, request.payload())
                            );
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
    std::string
    reply(std::string_view method, const std::string& payload)
    {
        if (method == "player.get")
        {
            dto::PlayerGetRequest request;
            EXPECT_TRUE(request.ParseFromString(payload));
            dto::PlayerGetReply reply;
            auto* p = reply.mutable_player();
            p->set_player_id(request.player_id());
            p->set_account_id(max64 - 7);
            p->set_display_name("Alice 玩家");
            p->set_state("active");
            p->set_mmr(10000);
            p->set_revision(INT64_MAX);
            if (changePlayer)
                changePlayer(reply);
            return reply.SerializeAsString();
        }
        if (method == "match.allocate_ids")
        {
            dto::AllocateIdsRequest request;
            EXPECT_TRUE(request.ParseFromString(payload));
            dto::AllocateIdsReply reply;
            reply.set_first(firstId);
            reply.set_last(firstId + request.count() - 1);
            if (changeIds)
                changeIds(reply);
            return reply.SerializeAsString();
        }
        if (method == "match.save_result")
        {
            dto::SaveResultRequest request;
            EXPECT_TRUE(request.ParseFromString(payload));
            dto::SaveResultReply reply;
            auto* m = reply.mutable_match();
            *m = request.match();
            if (!m->created_unix_ms())
                m->set_created_unix_ms(1000);
            if (m->state() == "completed" && !m->started_unix_ms())
                m->set_started_unix_ms(m->created_unix_ms());
            if (!m->finished_unix_ms())
                m->set_finished_unix_ms(
                    std::max(
                        m->created_unix_ms(),
                        m->started_unix_ms()
                    ) +
                    100
                );
            if (m->participants(0).slot() > m->participants(1).slot())
                m->mutable_participants()->SwapElements(0, 1);
            auto* s = reply.mutable_settlement();
            s->set_settlement_id(m->match_id());
            s->set_match_id(m->match_id());
            s->set_event_id(event);
            if (changeSave)
                changeSave(reply);
            return reply.SerializeAsString();
        }
        if (method == "match.get")
        {
            dto::MatchGetRequest request;
            EXPECT_TRUE(request.ParseFromString(payload));
            dto::MatchGetReply reply;
            *reply.mutable_match() = record(request.match_id());
            if (changeMatch)
                changeMatch(reply);
            return reply.SerializeAsString();
        }
        dto::SettlementGetRequest request;
        EXPECT_TRUE(request.ParseFromString(payload));
        dto::SettlementGetReply reply;
        auto* s = reply.mutable_settlement();
        s->set_settlement_id(request.settlement_id());
        s->set_match_id(21);
        s->set_event_id(event);
        s->set_replayed(true);
        if (changeSettlement)
            changeSettlement(reply);
        return reply.SerializeAsString();
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

struct StorageHarness
{
    boost::asio::io_context io;
    StoragePeer first{io}, second{io};
    std::unique_ptr<StorageClient> client;
    explicit StorageHarness(std::size_t maximum = 8)
    {
        auto config = settings();
        config.maxInflight = maximum;
        client = std::make_unique<StorageClient>(
            io.get_executor(),
            std::move(config)
        );
    }
    StorageClient::Config settings()
    {
        StorageClient::Config result;
        result.endpoint = {
            boost::asio::ip::address_v4::loopback(),
            first.server->port()
        };
        result.token = std::string(48, 's');
        return result;
    }
    ~StorageHarness()
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
    until(Predicate predicate, std::chrono::milliseconds timeout = 2s)
    {
        const auto end = Clock::now() + timeout;
        while (!predicate() && Clock::now() < end)
        {
            io.restart();
            io.run_for(1ms);
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    static Context context(std::chrono::milliseconds budget = 1s)
    {
        return {
            Clock::now() + budget,
            "lobby-storage-parent",
            {"gateway", "lobby"}
        };
    }
    template <class Reply, class Start>
    StorageClient::Result<Reply> call(Start start)
    {
        std::optional<StorageClient::Result<Reply>> result;
        const auto admitted = start(
            [&](auto r) { result.emplace(std::move(r)); }
        );
        if (!admitted)
            throw std::runtime_error("fixture admission failed");
        if (!until([&] { return result.has_value(); }))
            throw std::runtime_error("fixture reply timed out");
        return std::move(*result);
    }
    StorageClient::Result<dto::PlayerGetReply>
    player(std::uint64_t id = max64)
    {
        dto::PlayerGetRequest request;
        request.set_player_id(id);
        return call<dto::PlayerGetReply>(
            [&](auto complete)
            {
                return client
                    ->player(request, context(), std::move(complete));
            }
        );
    }
    StorageClient::Result<dto::AllocateIdsReply>
    ids(std::uint32_t count = 32)
    {
        dto::AllocateIdsRequest request;
        request.set_count(count);
        return call<dto::AllocateIdsReply>(
            [&](auto complete)
            {
                return client->allocateIds(
                    request,
                    context(),
                    std::move(complete)
                );
            }
        );
    }
    StorageClient::Result<dto::SaveResultReply>
    save(dto::SaveResultRequest request = saveRequest())
    {
        return call<dto::SaveResultReply>(
            [&](auto complete)
            {
                return client->saveResult(
                    request,
                    context(),
                    "lobby-match-21",
                    std::move(complete)
                );
            }
        );
    }
    StorageClient::Result<dto::MatchGetReply> match()
    {
        dto::MatchGetRequest request;
        request.set_match_id(21);
        return call<dto::MatchGetReply>(
            [&](auto complete)
            {
                return client
                    ->match(request, context(), std::move(complete));
            }
        );
    }
    StorageClient::Result<dto::SettlementGetReply>
    settlement(std::uint64_t id = 21)
    {
        dto::SettlementGetRequest request;
        request.set_settlement_id(id);
        return call<dto::SettlementGetReply>(
            [&](auto complete)
            {
                return client->getSettlement(
                    request,
                    context(),
                    std::move(complete)
                );
            }
        );
    }
    StorageClient::Result<void> probe()
    {
        return call<void>(
            [&](auto complete)
            { return client->probe(context(), std::move(complete)); }
        );
    }
    arena::runtime::ConfigSnapshot::Ptr
    snapshot(std::uint64_t version, bool a, bool b)
    {
        const auto node =
            [](const char* id, std::uint16_t port, bool ready)
        {
            return std::string(
                       "{\"service\":\"storage\",\"nodeId\":\""
                   ) +
                   id + "\",\"host\":\"127.0.0.1\",\"port\":" +
                   std::to_string(port) +
                   ",\"ready\":" + (ready ? "true" : "false") + "}";
        };
        auto parsed = arena::runtime::ConfigSnapshot::parse(
            std::string("{\"version\":\"") + std::to_string(version) +
            "\",\"desiredReady\":true,\"drainGraceMs\":1000,"
            "\"nodes\":[" +
            node("storage-a", first.server->port(), a) + "," +
            node("storage-b", second.server->port(), b) + "]}"
        );
        if (!parsed)
            throw std::runtime_error("invalid route fixture");
        return *parsed;
    }
};

TEST(
    StorageClientTest,
    AllTypedMethodsPreserveParentMetadataAndFullWidthRecords
)
{
    StorageHarness h;
    auto player = h.player();
    ASSERT_TRUE(player);
    EXPECT_EQ(player->player().player_id(), max64);
    EXPECT_EQ(player->player().account_id(), max64 - 7);
    EXPECT_EQ(
        player->player().revision(),
        static_cast<std::uint64_t>(INT64_MAX)
    );
    EXPECT_EQ(player->player().mmr(), 10000u);
    auto range = h.ids();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->last() - range->first() + 1, 32u);
    auto saved = h.save();
    ASSERT_TRUE(saved);
    EXPECT_EQ(saved->match().room_id(), managedBit | 21);
    EXPECT_EQ(saved->match().final_checksum(), max64);
    EXPECT_EQ(saved->match().participants(0).last_input_seq(), max64);
    EXPECT_EQ(saved->match().participants(0).final_x(), INT32_MIN);
    EXPECT_EQ(saved->match().participants(1).final_x(), INT32_MAX);
    EXPECT_EQ(saved->settlement().settlement_id(), 21u);
    auto match = h.match();
    ASSERT_TRUE(match);
    EXPECT_EQ(match->match().match_id(), 21u);
    auto receipt = h.settlement(max64);
    ASSERT_TRUE(receipt);
    EXPECT_EQ(receipt->settlement().settlement_id(), max64);
    ASSERT_TRUE(h.probe());
    ASSERT_EQ(h.first.calls, 6u);
    for (std::size_t i = 0; i != h.first.contexts.size(); ++i)
    {
        EXPECT_EQ(
            h.first.contexts[i].correlationId,
            "lobby-storage-parent"
        );
        EXPECT_EQ(
            h.first.contexts[i].servicePath,
            (std::vector<std::string>{"gateway", "lobby", "storage"})
        );
        EXPECT_LE(h.first.contexts[i].deadline, Clock::now() + 1s);
        EXPECT_EQ(h.first.requests[i].caller_service(), "lobby");
        EXPECT_EQ(h.first.requests[i].target_service(), "storage");
        EXPECT_EQ(
            h.first.requests[i].protocol_version(),
            arena::internal_rpc::protocolVersion
        );
    }
    EXPECT_EQ(
        h.first.requests[2].idempotency_key(),
        "lobby-match-21"
    );
    EXPECT_EQ(h.client->inflight(), 0u);
}

TEST(
    StorageClientTest,
    InclusiveRangeBoundaryAndLegalReplayDifferencesAreAccepted
)
{
    StorageHarness h;
    h.first.firstId = managedBit - 1024;
    auto range = h.ids(1024);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->last(), managedBit - 1);
    h.first.changeSave = [](auto& reply)
    {
        reply.set_replayed(true);
        reply.mutable_settlement()->set_replayed(false);
    };
    auto request = saveRequest();
    request.mutable_match()->mutable_participants()->SwapElements(
        0,
        1
    );
    request.mutable_match()->set_final_tick(max64);
    request.mutable_match()->set_result_version(INT32_MAX);
    auto saved = h.save(request);
    ASSERT_TRUE(saved);
    EXPECT_TRUE(saved->replayed());
    EXPECT_FALSE(saved->settlement().replayed());
    EXPECT_GT(saved->match().created_unix_ms(), 0u);
    EXPECT_EQ(saved->match().final_tick(), max64);
    EXPECT_EQ(
        saved->match().result_version(),
        static_cast<std::uint32_t>(INT32_MAX)
    );
    request = saveRequest();
    request.mutable_match()->set_state("aborted");
    request.mutable_match()->set_finish_reason("ready_timeout");
    ASSERT_TRUE(h.save(request));
}

TEST(
    StorageClientTest,
    MalformedOversizedInactiveAndInvalidPlayerRecordsCannotEscape
)
{
    StorageHarness h;
    const auto internal = [&](auto result)
    {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    };
    h.first.malformed = true;
    internal(h.player());
    h.first.malformed = false;
    h.first.oversized = true;
    internal(h.player());
    h.first.oversized = false;
    const std::vector<std::function<void(dto::PlayerGetReply&)>>
        corrupt = {
            [](auto& r) { r.clear_player(); },
            [](auto& r)
            { r.mutable_player()->set_player_id(max64 - 1); },
            [](auto& r) { r.mutable_player()->set_account_id(0); },
            [](auto& r) { r.mutable_player()->set_mmr(10001); },
            [](auto& r) { r.mutable_player()->set_revision(max64); },
            [](auto& r)
            { r.mutable_player()->set_state("invented"); },
            [](auto& r)
            { r.mutable_player()->set_display_name("\x7f"); },
            [](auto& r)
            { r.mutable_player()->set_display_name("\xc2\x80"); },
            [](auto& r)
            {
                r.mutable_player()->set_display_name(
                    std::string(129, 'a')
                );
            },
            [](auto& r)
            { r.mutable_player()->set_display_name("   "); }
        };
    for (const auto& change : corrupt)
    {
        h.first.changePlayer = change;
        internal(h.player());
    }
    for (const auto* state : {"suspended", "retired"})
    {
        h.first.changePlayer = [state](auto& r)
        { r.mutable_player()->set_state(state); };
        auto result = h.player();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::UNAUTHENTICATED);
    }
}

TEST(
    StorageClientTest,
    AllocationAndSettlementIdentityMustMatchTheRequest
)
{
    StorageHarness h;
    const std::vector<std::function<void(dto::AllocateIdsReply&)>>
        ranges = {
            [](auto& r) { r.set_first(0); },
            [](auto& r) { r.set_last(r.first() - 1); },
            [](auto& r) { r.set_last(r.last() + 1); },
            [](auto& r) { r.set_last(managedBit); },
            [](auto& r)
            {
                r.set_first(max64);
                r.set_last(max64);
            }
        };
    for (const auto& change : ranges)
    {
        h.first.changeIds = change;
        auto result = h.ids();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    }
    const std::vector<std::function<void(dto::SettlementGetReply&)>>
        receipts = {
            [](auto& r) { r.clear_settlement(); },
            [](auto& r)
            { r.mutable_settlement()->set_settlement_id(22); },
            [](auto& r) { r.mutable_settlement()->set_match_id(0); },
            [](auto& r)
            {
                r.mutable_settlement()->set_event_id(
                    std::string(36, 'x')
                );
            },
            [](auto& r)
            {
                r.mutable_settlement()->set_event_id(
                    "00000000-0000-0000-0000-000000000000"
                );
            }
        };
    for (const auto& change : receipts)
    {
        h.first.changeSettlement = change;
        auto result = h.settlement();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    }
    h.first.nonemptyProbe = true;
    auto probe = h.probe();
    ASSERT_FALSE(probe);
    EXPECT_EQ(probe.error(), arena::rpc::INTERNAL);
}

TEST(
    StorageClientTest,
    SaveAcknowledgementRequiresEveryImmutableFactAndExactReceipt
)
{
    StorageHarness h;
    const std::vector<std::function<void(dto::SaveResultReply&)>>
        corrupt = {
            [](auto& r) { r.clear_match(); },
            [](auto& r)
            {
                r.mutable_match()->set_match_id(22);
                r.mutable_match()->set_room_id(managedBit | 22);
            },
            [](auto& r) { r.mutable_match()->set_room_id(1); },
            [](auto& r)
            {
                r.mutable_match()->set_state("aborted");
                r.mutable_match()->set_finish_reason(
                    "member_unavailable"
                );
            },
            [](auto& r) { r.mutable_match()->set_result_version(2); },
            [](auto& r)
            { r.mutable_match()->set_winner_player_id(max64); },
            [](auto& r) { r.mutable_match()->set_final_tick(201); },
            [](auto& r)
            { r.mutable_match()->set_final_checksum(max64 - 1); },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_player_id(1);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_session_id(1);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_membership_epoch(1);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_initial_mmr(1499);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_last_input_seq(max64 - 1);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_final_x(0);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_final_y(0);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_last_dx(0);
            },
            [](auto& r)
            {
                r.mutable_match()
                    ->mutable_participants(0)
                    ->set_last_dy(0);
            },
            [](auto& r)
            {
                r.mutable_match()->mutable_participants(0)->set_slot(
                    2
                );
            },
            [](auto& r) { r.clear_settlement(); },
            [](auto& r)
            { r.mutable_settlement()->set_settlement_id(22); },
            [](auto& r) { r.mutable_settlement()->set_match_id(22); },
            [](auto& r)
            { r.mutable_settlement()->set_event_id("not-a-uuid"); },
            [](auto& r)
            { r.mutable_match()->set_finished_unix_ms(0); }
        };
    for (std::size_t i = 0; i != corrupt.size(); ++i)
    {
        SCOPED_TRACE(i);
        h.first.changeSave = corrupt[i];
        auto result = h.save();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    }
    auto explicitTimes = saveRequest();
    explicitTimes.mutable_match()->set_created_unix_ms(1000);
    explicitTimes.mutable_match()->set_started_unix_ms(1100);
    explicitTimes.mutable_match()->set_finished_unix_ms(1200);
    for (const auto field : {0, 1, 2})
    {
        h.first.changeSave = [field](auto& r)
        {
            if (field == 0)
                r.mutable_match()->set_created_unix_ms(1001);
            if (field == 1)
                r.mutable_match()->set_started_unix_ms(1101);
            if (field == 2)
                r.mutable_match()->set_finished_unix_ms(1201);
        };
        auto result = h.save(explicitTimes);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    }
}

TEST(
    StorageClientTest,
    MatchLookupChecksIdentityAndLifecycleWithoutInventingTerminalState
)
{
    StorageHarness h;
    for (const auto* state : {"forming", "running"})
    {
        h.first.changeMatch = [state](auto& r)
        {
            auto* m = r.mutable_match();
            m->set_state(state);
            m->set_final_tick(0);
            m->set_final_checksum(0);
            m->clear_finish_reason();
            m->set_finished_unix_ms(0);
            if (m->state() == "forming")
                m->set_started_unix_ms(0);
        };
        auto result = h.match();
        ASSERT_TRUE(result);
        EXPECT_EQ(result->match().state(), state);
    }
    for (int kind = 0; kind != 4; ++kind)
    {
        h.first.changeMatch = [kind](auto& r)
        {
            if (kind == 0)
                r.mutable_match()->set_match_id(22);
            if (kind == 1)
                r.mutable_match()->set_room_id(22);
            if (kind == 2)
                r.mutable_match()->set_started_unix_ms(999);
            if (kind == 3)
                r.mutable_match()->set_result_version(0);
        };
        auto result = h.match();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), arena::rpc::INTERNAL);
    }
}

TEST(
    StorageClientTest,
    PublicVersionCycleUnknownAndStorageErrorsRemainExactWithoutRetries
)
{
    StorageHarness h;
    for (const auto status :
         {arena::rpc::INCOMPATIBLE_VERSION,
          arena::rpc::CALL_CYCLE,
          arena::rpc::OUTCOME_UNKNOWN,
          arena::rpc::NOT_FOUND,
          arena::rpc::OVERLOADED,
          arena::rpc::UNAVAILABLE,
          arena::rpc::CONFLICT})
    {
        h.first.error = status;
        const auto before = h.first.calls;
        auto result = h.save();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), status);
        h.io.restart();
        h.io.run_for(5ms);
        EXPECT_EQ(h.first.calls, before + 1);
    }
}

TEST(
    StorageClientTest,
    ParentDeadlineCancellationAndOverloadKeepPhysicalLoansBounded
)
{
    StorageHarness h(1);
    h.first.hold = true;
    dto::PlayerGetRequest request;
    request.set_player_id(1);
    std::optional<StorageClient::Result<dto::PlayerGetReply>> expired;
    const auto started = Clock::now();
    auto accepted = h.client->player(
        request,
        StorageHarness::context(100ms),
        [&](auto r) { expired.emplace(std::move(r)); }
    );
    ASSERT_TRUE(accepted);
    bool called = false;
    auto overload = h.client->player(
        request,
        StorageHarness::context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(overload);
    EXPECT_EQ(overload.error(), arena::rpc::OVERLOADED);
    ASSERT_TRUE(h.until([&] { return h.first.calls == 1; }));
    ASSERT_TRUE(h.until([&] { return expired.has_value(); }));
    ASSERT_FALSE(*expired);
    EXPECT_EQ(expired->error(), arena::rpc::DEADLINE_EXCEEDED);
    EXPECT_LT(Clock::now() - started, 1s);
    EXPECT_FALSE(called);
    EXPECT_EQ(h.client->inflight(), 0u);
    EXPECT_EQ(h.first.server->stats().active, 1u);
    h.first.release();
    ASSERT_TRUE(
        h.until([&] { return h.first.server->stats().active == 0; })
    );
    std::optional<StorageClient::Result<dto::PlayerGetReply>>
        cancelled;
    auto next = h.client->player(
        request,
        StorageHarness::context(),
        [&](auto r) { cancelled.emplace(std::move(r)); }
    );
    ASSERT_TRUE(next);
    ASSERT_TRUE(h.until([&] { return h.first.calls == 2; }));
    next->cancel();
    next->cancel();
    ASSERT_TRUE(h.until([&] { return cancelled.has_value(); }));
    ASSERT_FALSE(*cancelled);
    EXPECT_EQ(cancelled->error(), arena::rpc::CANCELLED);
    EXPECT_EQ(h.client->inflight(), 0u);
    h.first.release();
    ASSERT_TRUE(
        h.until([&] { return h.first.server->stats().active == 0; })
    );
}

TEST(
    StorageClientTest,
    RouteIsCapturedPerAdmissionAndUnreadyDiscoveryHasNoFallback
)
{
    StorageHarness h;
    auto runtime = std::make_shared<arena::runtime::ConfigStore>(
        h.snapshot(1, true, false)
    );
    auto config = h.settings();
    config.runtime = runtime;
    h.client = std::make_unique<StorageClient>(
        h.io.get_executor(),
        config
    );
    dto::PlayerGetRequest request;
    request.set_player_id(1);
    std::optional<StorageClient::Result<dto::PlayerGetReply>> first,
        second;
    ASSERT_TRUE(h.client->player(
        request,
        StorageHarness::context(),
        [&](auto r) { first.emplace(std::move(r)); }
    ));
    ASSERT_TRUE(runtime->publish(h.snapshot(2, false, true)));
    ASSERT_TRUE(h.client->player(
        request,
        StorageHarness::context(),
        [&](auto r) { second.emplace(std::move(r)); }
    ));
    ASSERT_TRUE(h.until([&] { return first && second; }));
    EXPECT_TRUE(*first);
    EXPECT_TRUE(*second);
    EXPECT_EQ(h.first.calls, 1u);
    EXPECT_EQ(h.second.calls, 1u);
    ASSERT_TRUE(runtime->publish(h.snapshot(3, false, false)));
    bool called = false;
    auto absent = h.client->player(
        request,
        StorageHarness::context(),
        [&](auto) { called = true; }
    );
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error(), arena::rpc::UNAVAILABLE);
    h.io.restart();
    h.io.poll();
    EXPECT_FALSE(called);
    EXPECT_EQ(h.first.calls, 1u);
    EXPECT_EQ(h.second.calls, 1u);
}

TEST(
    StorageClientTest,
    WrongPeerAndLostAcknowledgementNeverProduceAnInventedCommit
)
{
    StorageHarness h;
    StoragePeer wrong(h.io, "auth");
    auto config = h.settings();
    config.endpoint.port(wrong.server->port());
    h.client = std::make_unique<StorageClient>(
        h.io.get_executor(),
        config
    );
    auto result = h.player();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), arena::rpc::INVALID_ARGUMENT);
    EXPECT_EQ(wrong.calls, 0u);
    wrong.server->stop();
    ASSERT_TRUE(h.until([&]
                        { return wrong.server->stats().stopped; }));
    h.client = std::make_unique<StorageClient>(
        h.io.get_executor(),
        h.settings()
    );
    h.first.hold = true;
    std::optional<StorageClient::Result<dto::SaveResultReply>> lost;
    auto accepted = h.client->saveResult(
        saveRequest(),
        StorageHarness::context(),
        "lobby-match-21",
        [&](auto r) { lost.emplace(std::move(r)); }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(h.until([&] { return h.first.calls == 1; }));
    h.first.server->stop();
    h.first.release();
    ASSERT_TRUE(h.until([&] { return lost.has_value(); }));
    ASSERT_FALSE(*lost);
    EXPECT_EQ(lost->error(), arena::rpc::OUTCOME_UNKNOWN);
    EXPECT_EQ(h.first.calls, 1u);
}

TEST(
    StorageClientTest,
    InvalidAdmissionAndInheritedCycleDoNotCreateHiddenCallbackWork
)
{
    StorageHarness h;
    bool called = false;
    const auto callback = [&](auto) { called = true; };
    dto::PlayerGetRequest player;
    auto invalid = h.client->player(
        player,
        StorageHarness::context(),
        callback
    );
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), arena::rpc::INVALID_ARGUMENT);
    dto::AllocateIdsRequest ids;
    ids.set_count(1025);
    EXPECT_FALSE(h.client->allocateIds(
        ids,
        StorageHarness::context(),
        callback
    ));
    auto save = saveRequest();
    save.mutable_match()->mutable_participants(0)->set_last_dx(2);
    EXPECT_FALSE(h.client->saveResult(
        save,
        StorageHarness::context(),
        "key",
        callback
    ));
    EXPECT_FALSE(h.client->saveResult(
        saveRequest(),
        StorageHarness::context(),
        "",
        callback
    ));
    dto::MatchGetRequest match;
    match.set_match_id(managedBit);
    EXPECT_FALSE(
        h.client->match(match, StorageHarness::context(), callback)
    );
    dto::SettlementGetRequest settlement;
    EXPECT_FALSE(h.client->getSettlement(
        settlement,
        StorageHarness::context(),
        callback
    ));
    auto parent = StorageHarness::context();
    parent.servicePath = {"gateway", "storage", "lobby"};
    player.set_player_id(1);
    auto cycle = h.client->player(player, parent, callback);
    ASSERT_FALSE(cycle);
    EXPECT_EQ(cycle.error(), arena::rpc::CALL_CYCLE);
    parent = StorageHarness::context();
    parent.deadline = Clock::now() - 1ms;
    auto deadline = h.client->player(player, parent, callback);
    ASSERT_FALSE(deadline);
    EXPECT_EQ(deadline.error(), arena::rpc::DEADLINE_EXCEEDED);
    EXPECT_FALSE(h.client->probe(StorageHarness::context(), {}));
    EXPECT_FALSE(called);
    EXPECT_EQ(h.first.calls, 0u);
    EXPECT_EQ(h.client->inflight(), 0u);
}

TEST(
    StorageClientTest,
    StopDestructionAndRepeatedCallsReleaseAllDescriptorsAndCompletions
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
        StorageHarness h;
        for (int i = 0; i != 12; ++i)
            ASSERT_TRUE(h.player());
        h.first.hold = true;
        auto pendingClient = std::make_unique<StorageClient>(
            h.io.get_executor(),
            h.settings()
        );
        unsigned completions = 0;
        std::optional<Status> status;
        dto::PlayerGetRequest request;
        request.set_player_id(1);
        auto accepted = pendingClient->player(
            request,
            StorageHarness::context(),
            [&](auto r)
            {
                ++completions;
                status = r ? arena::rpc::OK : r.error();
            }
        );
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(h.until(
            [&] { return h.first.server->stats().active == 1; }
        ));
        pendingClient->stop();
        pendingClient->stop();
        bool called = false;
        auto rejected = pendingClient->player(
            request,
            StorageHarness::context(),
            [&](auto) { called = true; }
        );
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), arena::rpc::UNAVAILABLE);
        pendingClient.reset();
        accepted->cancel();
        ASSERT_TRUE(h.until([&] { return completions == 1; }));
        EXPECT_EQ(status, arena::rpc::CANCELLED);
        EXPECT_FALSE(called);
        h.first.release();
        ASSERT_TRUE(h.until(
            [&] { return h.first.server->stats().active == 0; }
        ));
        h.io.restart();
        h.io.run_for(5ms);
        EXPECT_EQ(completions, 1u);
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    StorageClientTest,
    InvalidConfigurationIsRejectedBeforeAnySocketWork
)
{
    boost::asio::io_context io;
    StorageClient::Config config;
    config.endpoint = {boost::asio::ip::address_v4::loopback(), 1};
    config.token = std::string(48, 's');
    for (int field = 0; field != 7; ++field)
    {
        auto invalid = config;
        if (field == 0)
            invalid.endpoint.port(0);
        if (field == 1)
            invalid.endpoint.address(
                boost::asio::ip::make_address("192.0.2.1")
            );
        if (field == 2)
            invalid.token.clear();
        if (field == 3)
            invalid.maxInflight = 0;
        if (field == 4)
            invalid.maxInflight = 4097;
        if (field == 5)
            invalid.timeout = 0ms;
        if (field == 6)
            invalid.timeout = 1001ms;
        EXPECT_THROW(
            StorageClient(io.get_executor(), std::move(invalid)),
            std::invalid_argument
        );
    }
    EXPECT_EQ(io.poll(), 0u);
}
} // namespace
} // namespace service::lobby