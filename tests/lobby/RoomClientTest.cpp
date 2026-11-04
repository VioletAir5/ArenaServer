#include "lobby/RoomClient.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <gtest/gtest.h>

#include "room/RoomWire.h"
#include "room/RoomWorker.h"

namespace
{
using namespace std::chrono_literals;
using Client = service::lobby::RoomClient;
namespace proto = arena::room;
namespace rw = service::room::wire;
template <class F> bool until(F f)
{
    const auto end = std::chrono::steady_clock::now() + 5s;
    while (!f() && std::chrono::steady_clock::now() < end)
        std::this_thread::sleep_for(1ms);
    return f();
}
struct Stack
{
    boost::asio::io_context io;
    std::filesystem::path directory;
    std::unique_ptr<service::room::RoomWorker> a, b;
    std::unique_ptr<Client> client;
    std::shared_ptr<arena::runtime::ConfigStore> config;
    std::thread runner;
    Stack()
    {
        std::array<char, 80> name{};
        const std::string pattern = "/tmp/arena-room-client-XXXXXX";
        std::copy(pattern.begin(), pattern.end(), name.begin());
        if (!::mkdtemp(name.data()))
            throw std::runtime_error(
                "temporary Room directory failed"
            );
        directory = name.data();
        service::room::RoomWorker::Config c;
        c.nodeId = "room-a";
        c.token = std::string(32, 'r');
        c.directory = directory / "a";
        a = std::make_unique<service::room::RoomWorker>(
            io.get_executor(),
            c
        );
        c.nodeId = "room-b";
        c.directory = directory / "b";
        b = std::make_unique<service::room::RoomWorker>(
            io.get_executor(),
            c
        );
        auto snapshot = arena::runtime::ConfigSnapshot::parse(
            json(1, true, false)
        );
        if (!snapshot)
            throw std::runtime_error(
                "Room discovery fixture rejected"
            );
        config = std::make_shared<arena::runtime::ConfigStore>(
            *snapshot
        );
        Client::Config cc;
        cc.nodes = {
            {"room-a",
             {boost::asio::ip::address_v4::loopback(), a->port()}},
            {"room-b",
             {boost::asio::ip::address_v4::loopback(), b->port()}}
        };
        cc.token = std::string(32, 'r');
        cc.runtime = config;
        client = std::make_unique<Client>(io.get_executor(), cc);
        runner = std::thread([this] { io.run(); });
        if (!until([&]
                   { return a->stats().ready && b->stats().ready; }))
        {
            client->stop();
            a->stop();
            b->stop();
            until(
                [&]
                {
                    return !client->inflight() &&
                           a->stats().stopped && b->stats().stopped;
                }
            );
            io.stop();
            runner.join();
            std::filesystem::remove_all(directory);
            throw std::runtime_error(
                "real Room workers did not become ready"
            );
        }
    }
    ~Stack()
    {
        client->stop();
        a->stop();
        b->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return !client->inflight() && a->stats().stopped &&
                       b->stats().stopped;
            }
        ));
        io.stop();
        if (runner.joinable())
            runner.join();
        client.reset();
        a.reset();
        b.reset();
        std::filesystem::remove_all(directory);
    }
    std::string
    json(unsigned version, bool includeB, bool poisonB) const
    {
        std::string text =
            "{\"version\":\"" + std::to_string(version) +
            "\",\"desiredReady\":true,\"drainGraceMs\":0,\"nodes\":["
            "{\"service\":\"room\",\"nodeId\":\"room-a\",\"host\":"
            "\"127.0.0.1\",\"port\":" +
            std::to_string(a->port()) + ",\"ready\":true}";
        if (includeB)
            text += ",{\"service\":\"room\",\"nodeId\":\"room-b\","
                    "\"host\":\"127.0.0.1\",\"port\":" +
                    std::to_string(poisonB ? a->port() : b->port()) +
                    ",\"ready\":true}";
        return text + "]}";
    }
    void reload(unsigned version, bool includeB, bool poisonB = false)
    {
        const auto next = arena::runtime::ConfigSnapshot::parse(
            json(version, includeB, poisonB)
        );
        if (!next || !config->publish(*next))
            throw std::runtime_error("Room discovery reload failed");
    }
    arena::internal_rpc::Context context() const
    {
        return {
            std::chrono::steady_clock::now() + 1s,
            "room-client-test",
            {"lobby"}
        };
    }
    template <class Reply, class Send>
    Client::Result<Reply> call(Send send)
    {
        auto result = std::make_shared<
            std::promise<Client::Result<Reply>>>();
        auto future = result->get_future();
        const auto admitted = send(
            [result](Client::Result<Reply> r)
            { result->set_value(std::move(r)); }
        );
        if (!admitted)
            return std::unexpected(admitted.error());
        if (future.wait_for(3s) != std::future_status::ready)
            throw std::runtime_error(
                "Room typed client completion timed out"
            );
        return future.get();
    }
    proto::CreateRequest
    createRequest(const std::string& worker) const
    {
        proto::CreateRequest request;
        request.set_version(1);
        request.set_worker_id(worker);
        request.set_room_id(7);
        request.set_incarnation(19);
        auto* p = request.mutable_policy();
        p->set_max_commands(256);
        p->set_battle_duration_ticks(100);
        p->set_ready_timeout_ticks(600);
        p->set_result_retention_nanos(60'000'000'000);
        p->mutable_actor()->set_max_members(64);
        p->mutable_actor()->set_max_inputs_per_tick(256);
        p->mutable_actor()->set_history_ticks(200);
        p->mutable_actor()->set_position_limit(1000000);
        return request;
    }
    proto::ExecuteRequest execution(
        const std::string& worker,
        std::uint64_t epoch,
        const proto::Image& base,
        bool input
    ) const
    {
        service::room::RuntimeAuthorization auth{
            {service::room::PlayerId{42},
             service::room::SessionId{142},
             1},
            7,
            2,
            true
        };
        service::room::RuntimeCommand command;
        command
            .kind = input
                        ? service::room::RuntimeCommandKind::Input
                        : service::room::RuntimeCommandKind::Snapshot;
        command.caller = auth;
        command.incarnation = 19;
        command.sequence = input ? 1 : 0;
        command.dx = input ? 1 : 0;
        service::room::RuntimeCutoff cutoff;
        cutoff.roster = {auth};
        cutoff.currentAuthorizations = {auth};
        cutoff.nowNanos = static_cast<std::int64_t>(
                              base.logical_tick() + 1
                          ) *
                          50'000'000;
        cutoff.wallMillis = 1000 + base.logical_tick() * 50;
        const auto cmd = rw::encodeCommand(command),
                   tick = rw::encodeCutoff(cutoff);
        if (!cmd || !tick)
            throw std::runtime_error(
                "Room wire fixture encoding failed"
            );
        proto::ExecuteRequest request;
        request.set_version(1);
        request.set_worker_id(worker);
        request.set_owner_epoch(epoch);
        *request.mutable_base() = base;
        auto* op = request.add_operations();
        op->set_cursor(base.cursor() + 1);
        op->set_kind(proto::OP_COMMAND);
        op->set_payload(*cmd);
        op = request.add_operations();
        op->set_cursor(base.cursor() + 2);
        op->set_kind(proto::OP_TICK);
        op->set_logical_tick(base.logical_tick() + 1);
        op->set_payload(*tick);
        return request;
    }
    Client::Result<proto::CacheReceipt> activate(
        const std::string& worker,
        std::uint64_t epoch,
        std::uint64_t revision,
        const proto::Image& image,
        std::string transfer = {}
    )
    {
        proto::ActivateRequest request;
        request.set_version(1);
        request.set_worker_id(worker);
        request.set_owner_epoch(epoch);
        request.set_directory_revision(revision);
        request.set_transfer_id(std::move(transfer));
        *request.mutable_image() = image;
        return call<proto::CacheReceipt>(
            [&](auto cb)
            { return client->activate(request, context(), cb); }
        );
    }
};
TEST(RoomClientTest, ResolvesExactNodeAndNeverFallsBackToReadySibling)
{
    Stack s;
    const auto b = s.call<proto::HealthReply>(
        [&](auto cb)
        { return s.client->probe("room-b", s.context(), cb); }
    );
    ASSERT_TRUE(b);
    EXPECT_EQ(b->worker_id(), s.b->stats().workerId);
    s.reload(2, false);
    std::atomic<unsigned> callbacks{0};
    const auto missing = s.client->create(
        s.createRequest(b->worker_id()),
        s.context(),
        [&](auto) { ++callbacks; }
    );
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), arena::rpc::UNAVAILABLE);
    EXPECT_EQ(callbacks, 0);
    EXPECT_EQ(s.client->inflight(), 0);
    const auto a = s.call<proto::HealthReply>(
        [&](auto cb)
        { return s.client->probe("room-a", s.context(), cb); }
    );
    ASSERT_TRUE(a);
    EXPECT_EQ(a->worker_id(), s.a->stats().workerId);
}
TEST(
    RoomClientTest,
    CapturesEndpointBeforeOwnerQueueAndRejectsWrongInstanceReply
)
{
    Stack s;
    std::promise<void> entered, go;
    auto release = go.get_future().share();
    boost::asio::post(
        s.io,
        [&]
        {
            entered.set_value();
            release.wait();
        }
    );
    const bool blocked = entered.get_future().wait_for(1s) ==
                         std::future_status::ready;
    if (!blocked)
    {
        go.set_value();
        FAIL() << "owner barrier did not enter";
    }
    auto reply = std::make_shared<
        std::promise<Client::Result<proto::HealthReply>>>();
    auto future = reply->get_future();
    const auto admitted = s.client->probe(
        "room-b",
        s.context(),
        [reply](auto r) { reply->set_value(std::move(r)); }
    );
    s.reload(2, true, true);
    go.set_value();
    ASSERT_TRUE(admitted);
    ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
    const auto old = future.get();
    ASSERT_TRUE(old);
    EXPECT_EQ(old->worker_id(), s.b->stats().workerId);
    const auto wrong = s.call<proto::HealthReply>(
        [&](auto cb)
        { return s.client->probe("room-b", s.context(), cb); }
    );
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), arena::rpc::INTERNAL);
}
TEST(
    RoomClientTest,
    RealWorkerTransferPreservesSequenceAndVolatileTickCache
)
{
    Stack s;
    const auto aid = s.a->stats().workerId,
               bid = s.b->stats().workerId;
    const auto created = s.call<proto::CreateReply>(
        [&](auto cb)
        {
            return s.client
                ->create(s.createRequest(aid), s.context(), cb);
        }
    );
    ASSERT_TRUE(created);
    ASSERT_TRUE(s.activate(aid, 1, 1, created->image()));
    const auto first = s.call<proto::ExecuteReply>(
        [&](auto cb)
        {
            return s.client->execute(
                s.execution(aid, 1, created->image(), false),
                s.context(),
                cb
            );
        }
    );
    ASSERT_TRUE(first);
    EXPECT_EQ(first->image().physical_tick(), 1);
    const auto before = s.a->stats().journalFileBytes;
    ASSERT_TRUE(s.activate(aid, 1, 2, first->image()));
    EXPECT_EQ(s.a->stats().journalFileBytes, before);
    proto::HintRequest hint;
    hint.set_version(1);
    hint.set_worker_id(aid);
    hint.set_owner_epoch(1);
    hint.set_directory_revision(3);
    hint.set_room_id(7);
    hint.set_incarnation(19);
    ASSERT_TRUE(s.call<proto::CacheReceipt>(
        [&](auto cb)
        { return s.client->freeze(hint, s.context(), cb); }
    ));
    const auto frozen = s.call<proto::ExecuteReply>(
        [&](auto cb)
        {
            return s.client->execute(
                s.execution(aid, 1, first->image(), true),
                s.context(),
                cb
            );
        }
    );
    EXPECT_FALSE(frozen);
    proto::StageRequest stage;
    stage.set_version(1);
    stage.set_worker_id(bid);
    stage.set_transfer_id("move-7");
    stage.set_owner_epoch(2);
    *stage.mutable_image() = first->image();
    const auto staged = s.call<proto::StageReceipt>(
        [&](auto cb)
        { return s.client->stage(stage, s.context(), cb); }
    );
    ASSERT_TRUE(staged);
    EXPECT_EQ(staged->fingerprint(), first->image().fingerprint());
    ASSERT_TRUE(s.activate(bid, 2, 4, first->image(), "move-7"));
    const auto next = s.call<proto::ExecuteReply>(
        [&](auto cb)
        {
            return s.client->execute(
                s.execution(bid, 2, first->image(), true),
                s.context(),
                cb
            );
        }
    );
    ASSERT_TRUE(next);
    const auto final = rw::unpackImage(next->image());
    ASSERT_TRUE(final);
    ASSERT_EQ(final->actor.players.size(), 1);
    EXPECT_EQ(final->actor.players[0].x, 1);
    EXPECT_EQ(final->actor.players[0].sequence, 1);
    EXPECT_EQ(final->logicalCutoff, 2);
}
TEST(
    RoomClientTest, InvalidWorkerAndExpiredBudgetDoNotCreateTransport
)
{
    Stack s;
    std::atomic<unsigned> callbacks{0};
    auto request = s.createRequest("room-a:not-an-instance");
    const auto bad = s.client->create(
        request,
        s.context(),
        [&](auto) { ++callbacks; }
    );
    ASSERT_FALSE(bad);
    EXPECT_EQ(bad.error(), arena::rpc::INVALID_ARGUMENT);
    auto expired = s.context();
    expired.deadline = std::chrono::steady_clock::now() - 1ms;
    const auto timed = s.client->probe(
        "room-a",
        expired,
        [&](auto) { ++callbacks; }
    );
    ASSERT_FALSE(timed);
    EXPECT_EQ(timed.error(), arena::rpc::DEADLINE_EXCEEDED);
    EXPECT_EQ(callbacks, 0);
    EXPECT_EQ(s.client->inflight(), 0);
}
} // namespace
