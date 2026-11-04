#include "room/RoomWorker.h"

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "room/RoomWire.h"

namespace
{
std::atomic<bool> holdSync{false}, syncEntered{false},
    failSync{false};
}
extern "C" int __real_fdatasync(int);
/** @brief Test-only linker barrier, never a production worker
 * callback. */
extern "C" int __wrap_fdatasync(int descriptor)
{
    char path[64], target[4096];
    const auto length = std::snprintf(
        path,
        sizeof(path),
        "/proc/self/fd/%d",
        descriptor
    );
    const auto size = length > 0 ? ::readlink(
                                       path,
                                       target,
                                       sizeof(target) - 1
                                   )
                                 : -1;
    if (size > 0)
    {
        target[size] = 0;
        if (std::string_view(target, static_cast<std::size_t>(size))
                    .find("room-cache-") != std::string_view::npos &&
            holdSync.load())
        {
            syncEntered.store(true);
            while (holdSync.load())
                ::usleep(500);
        }
        if (std::string_view(target, static_cast<std::size_t>(size))
                    .find("room-cache-") != std::string_view::npos &&
            failSync.exchange(false))
        {
            errno = EIO;
            return -1;
        }
    }
    return __real_fdatasync(descriptor);
}
namespace
{
using namespace std::chrono_literals;
using namespace service::room;
namespace pb = arena::room;
using Client = arena::internal_rpc::Client;
using Status = arena::rpc::Status;
struct SyncRelease
{
    ~SyncRelease()
    {
        holdSync.store(false);
    }
};
RuntimeAuthorization auth(std::uint64_t player = 1)
{
    return {
        {PlayerId{player}, SessionId{player + 10}, player + 20},
        player + 30,
        player + 40,
        true
    };
}
RuntimeCutoff cutoff(const RuntimeImage& current)
{
    return {
        {auth()},
        {auth()},
        {},
        static_cast<std::int64_t>(current.logicalCutoff + 1) *
            50000000,
        100 + current.logicalCutoff
    };
}
template <class T> T unwrap(std::expected<T, wire::CodecError> value)
{
    if (!value)
        throw std::runtime_error("wire fixture encoding failed");
    return std::move(*value);
}
struct Harness
{
    boost::asio::io_context io;
    std::filesystem::path directory;
    std::unique_ptr<RoomWorker> worker;
    std::unique_ptr<Client> client;
    std::string node;
    explicit Harness(
        std::string name = "node-a",
        std::size_t maxInflight = 8,
        std::size_t maxRooms = 16
    )
        : node(std::move(name))
    {
        std::string
            pattern = (std::filesystem::temp_directory_path() /
                       "arena-room-worker-XXXXXX")
                          .string();
        std::vector<char> text(pattern.begin(), pattern.end());
        text.push_back(0);
        auto* created = ::mkdtemp(text.data());
        if (!created)
            throw std::runtime_error(
                "private test directory unavailable"
            );
        directory = created;
        start(maxInflight, maxRooms);
    }
    void start(
        std::size_t maxInflight = 8,
        std::size_t maxRooms = 16,
        bool waitReady = true
    )
    {
        RoomWorker::Config config;
        config.nodeId = node;
        config.token = std::string(48, 'R');
        config.directory = directory;
        config.maxInflight = maxInflight;
        config.maxRooms = maxRooms;
        config.maxStages = 16;
        worker = std::make_unique<RoomWorker>(
            io.get_executor(),
            std::move(config)
        );
        client = std::make_unique<Client>(
            io.get_executor(),
            boost::asio::ip::tcp::endpoint{
                boost::asio::ip::address_v4::loopback(),
                worker->port()
            },
            32
        );
        if (waitReady)
        {
            EXPECT_TRUE(until([&] { return worker->stats().ready; }));
        }
    }
    template <class Predicate>
    bool
    until(Predicate predicate, std::chrono::milliseconds budget = 3s)
    {
        const auto deadline = std::chrono::steady_clock::now() +
                              budget;
        while (!predicate() &&
               std::chrono::steady_clock::now() < deadline)
        {
            io.restart();
            io.run_for(1ms);
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    void close()
    {
        holdSync.store(false);
        if (client)
            client->stop();
        if (worker)
            worker->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return (!client || !client->inflight()) &&
                       (!worker || worker->stats().stopped);
            },
            5s
        ));
        if (worker)
        {
            auto stats = worker->stats();
            EXPECT_EQ(stats.activeJobs, 0);
            EXPECT_EQ(stats.journalPending, 0);
            EXPECT_EQ(stats.rpcActive, 0);
            EXPECT_EQ(stats.rpcPeers, 0);
            EXPECT_TRUE(stats.journalClosed);
        }
        client.reset();
        worker.reset();
        io.restart();
        io.poll();
    }
    ~Harness()
    {
        close();
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    arena::rpc::Request request(
        std::string method,
        std::string payload,
        std::string caller = "lobby"
    ) const
    {
        arena::rpc::Request r;
        r.set_method(std::move(method));
        r.set_payload(std::move(payload));
        r.set_service_token(std::string(48, 'R'));
        r.set_protocol_version(1);
        r.set_caller_service(caller);
        r.set_target_service("room");
        r.add_service_path(caller);
        r.set_correlation_id("room-worker-contract");
        return r;
    }
    arena::rpc::Response callRequest(arena::rpc::Request request)
    {
        auto result = std::make_shared<
            std::optional<arena::rpc::Response>>();
        auto accepted = client->call(
            std::move(request),
            {},
            [result](auto value) { *result = std::move(value); }
        );
        if (!accepted)
        {
            arena::rpc::Response r;
            r.set_status(accepted.error());
            return r;
        }
        if (!until([&] { return result->has_value(); }))
            throw std::runtime_error(
                "real Room RPC fixture timed out"
            );
        return std::move(**result);
    }
    arena::rpc::Response call(
        std::string method,
        std::string payload,
        std::string caller = "lobby"
    )
    {
        return callRequest(request(
            std::move(method),
            std::move(payload),
            std::move(caller)
        ));
    }
    template <class Message>
    arena::rpc::Response
    call(std::string method, const Message& message)
    {
        return call(
            std::move(method),
            unwrap(wire::serializeMessage(message))
        );
    }
    template <class Message>
    Message reply(const arena::rpc::Response& response)
    {
        EXPECT_EQ(response.status(), arena::rpc::OK);
        Message message;
        if (!wire::parseMessage(response.payload(), message))
            throw std::runtime_error("typed Room reply invalid");
        return message;
    }
    pb::Image
    create(std::uint64_t room = 7, std::uint64_t incarnation = 9)
    {
        pb::CreateRequest r;
        r.set_version(1);
        r.set_worker_id(worker->stats().workerId);
        r.set_room_id(room);
        r.set_incarnation(incarnation);
        *r.mutable_policy() = unwrap(
            wire::packPolicy(RuntimePolicy{})
        );
        return reply<pb::CreateReply>(call("room.create", r)).image();
    }
    pb::ActivateRequest activation(
        const pb::Image& image,
        std::uint64_t epoch = 1,
        std::uint64_t revision = 1,
        std::string transfer = {}
    ) const
    {
        pb::ActivateRequest r;
        r.set_version(1);
        r.set_worker_id(worker->stats().workerId);
        r.set_owner_epoch(epoch);
        r.set_directory_revision(revision);
        *r.mutable_image() = image;
        r.set_transfer_id(std::move(transfer));
        return r;
    }
    pb::StageRequest staging(
        const pb::Image& image,
        std::uint64_t epoch = 2,
        std::string transfer = "transfer-1"
    ) const
    {
        pb::StageRequest r;
        r.set_version(1);
        r.set_worker_id(worker->stats().workerId);
        r.set_owner_epoch(epoch);
        r.set_transfer_id(std::move(transfer));
        *r.mutable_image() = image;
        return r;
    }
    pb::ExecuteRequest execution(
        const pb::Image& image,
        std::uint64_t epoch = 1,
        std::vector<RuntimeCommand> commands = {}
    ) const
    {
        auto current = unwrap(wire::unpackImage(image));
        auto cut = cutoff(current);
        pb::ExecuteRequest r;
        r.set_version(1);
        r.set_worker_id(worker->stats().workerId);
        r.set_owner_epoch(epoch);
        *r.mutable_base() = image;
        auto cursor = image.cursor();
        for (const auto& command : commands)
        {
            auto& op = *r.add_operations();
            op.set_cursor(++cursor);
            op.set_logical_tick(0);
            op.set_kind(pb::OP_COMMAND);
            op.set_payload(unwrap(wire::encodeCommand(command)));
        }
        auto& op = *r.add_operations();
        op.set_cursor(++cursor);
        op.set_logical_tick(image.logical_tick() + 1);
        op.set_kind(pb::OP_TICK);
        op.set_payload(unwrap(wire::encodeCutoff(cut)));
        return r;
    }
};
RuntimeCommand input(std::uint64_t sequence)
{
    RuntimeCommand c;
    c.kind = RuntimeCommandKind::Input;
    c.caller = auth();
    c.incarnation = 9;
    c.sequence = sequence;
    c.dx = 1;
    return c;
}
std::size_t descriptors()
{
    std::size_t result = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        (void)entry;
        ++result;
    }
    return result;
}
TEST(
    RoomWorker,
    RealRpcCreateExecuteAndForwardCacheHaveOneCanonicalImage
)
{
    Harness h;
    auto image = h.create();
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    auto request = h.execution(image);
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::CONFLICT
    );
    auto active = h.activation(image);
    ASSERT_EQ(
        h.call("room.activate", active).status(),
        arena::rpc::OK
    );
    ASSERT_TRUE(
        h.until([&] { return h.worker->stats().journalPending == 0; })
    );
    const auto disk = h.worker->stats().journalFileBytes;
    auto executed = h.reply<pb::ExecuteReply>(
        h.call("room.execute", h.execution(image, 1, {input(1)}))
    );
    ASSERT_EQ(executed.results_size(), 1);
    ASSERT_TRUE(executed.results(0).has_input());
    EXPECT_EQ(executed.results(0).input(), pb::APPLIED);
    EXPECT_EQ(executed.image().physical_tick(), 1);
    EXPECT_EQ(executed.image().cursor(), 2);
    EXPECT_EQ(executed.image().logical_tick(), 1);
    ASSERT_TRUE(executed.has_replay());
    auto tick = wire::decodeTick(executed.replay());
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->checksum, executed.image().checksum());
    EXPECT_EQ(
        h.worker->stats().journalFileBytes,
        disk
    ); // execute is an isolated proposal.
    auto next = h.activation(executed.image(), 1, 2);
    ASSERT_EQ(h.call("room.activate", next).status(), arena::rpc::OK);
    EXPECT_EQ(
        h.worker->stats().journalFileBytes,
        disk
    ); // same-epoch install is process-local.
    EXPECT_EQ(
        h.call("room.activate", active).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.execute", h.execution(image)).status(),
        arena::rpc::CONFLICT
    );
    auto duplicate = h.reply<pb::ExecuteReply>(h.call(
        "room.execute",
        h.execution(executed.image(), 1, {input(1)})
    ));
    ASSERT_EQ(duplicate.results_size(), 1);
    EXPECT_EQ(duplicate.results(0).input(), pb::DUPLICATE);
}
TEST(
    RoomWorker,
    StageIsDurableInactiveIdempotentAndRequiresExactTransferActivation
)
{
    Harness source("source"), target("target");
    auto image = source.create();
    auto stage = target.staging(image);
    auto receipt = target.reply<pb::StageReceipt>(
        target.call("room.stage", stage)
    );
    EXPECT_EQ(receipt.fingerprint(), image.fingerprint());
    EXPECT_EQ(target.worker->stats().stages, 1);
    EXPECT_EQ(target.worker->stats().activeRoutes, 0);
    EXPECT_GT(target.worker->stats().journalFileBytes, 0);
    auto first = target.worker->stats().journalFileBytes;
    EXPECT_EQ(
        target.call("room.stage", stage).status(),
        arena::rpc::OK
    );
    EXPECT_EQ(target.worker->stats().journalFileBytes, first);
    auto conflicting = stage;
    conflicting.set_transfer_id("other");
    EXPECT_EQ(
        target.call("room.stage", conflicting).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        target.call("room.execute", target.execution(image, 2))
            .status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        target
            .call(
                "room.activate",
                target.activation(image, 2, 9, "wrong")
            )
            .status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        target
            .call(
                "room.activate",
                target.activation(image, 2, 9, "transfer-1")
            )
            .status(),
        arena::rpc::OK
    );
    EXPECT_EQ(target.worker->stats().stages, 0);
    EXPECT_EQ(target.worker->stats().activeRoutes, 1);
    auto future = target.reply<pb::ExecuteReply>(target.call(
        "room.execute",
        target.execution(image, 2, {input(1)})
    ));
    EXPECT_EQ(future.image().incarnation(), image.incarnation());
    EXPECT_EQ(future.image().physical_tick(), 1);
    const auto durableBytes = target.worker->stats().journalFileBytes;
    EXPECT_EQ(
        target
            .call(
                "room.activate",
                target.activation(future.image(), 2, 10, "transfer-1")
            )
            .status(),
        arena::rpc::OK
    );
    EXPECT_EQ(target.worker->stats().journalFileBytes, durableBytes);
}
TEST(
    RoomWorker,
    RestartNeverAuthorizesOldInstanceAndNewEpochResumesCurrentImage
)
{
    Harness h;
    auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image)).status(),
        arena::rpc::OK
    );
    auto proposal = h.reply<pb::ExecuteReply>(
        h.call("room.execute", h.execution(image, 1, {input(1)}))
    );
    ASSERT_EQ(
        h.call("room.activate", h.activation(proposal.image(), 1, 2))
            .status(),
        arena::rpc::OK
    );
    const auto old = h.worker->stats().workerId;
    h.close();
    h.start();
    ASSERT_NE(h.worker->stats().workerId, old);
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    EXPECT_EQ(
        h.call("room.execute", h.execution(proposal.image()))
            .status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(proposal.image(), 1, 2))
            .status(),
        arena::rpc::CONFLICT
    );
    ASSERT_EQ(
        h.call("room.activate", h.activation(proposal.image(), 2, 3))
            .status(),
        arena::rpc::OK
    );
    auto next = h.reply<pb::ExecuteReply>(h.call(
        "room.execute",
        h.execution(proposal.image(), 2, {input(2)})
    ));
    auto resumed = wire::unpackImage(next.image());
    ASSERT_TRUE(resumed);
    ASSERT_EQ(resumed->actor.players.size(), 1);
    EXPECT_EQ(resumed->actor.players.front().sequence, 2);
    EXPECT_EQ(resumed->actor.tick, 2);
    EXPECT_EQ(resumed->actor.players.front().x, 2);
}
TEST(RoomWorker, WrongRoleWorkerEpochAndMalformedKernelAreRejected)
{
    Harness h;
    auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image)).status(),
        arena::rpc::OK
    );
    auto request = h.execution(image);
    auto bytes = unwrap(wire::serializeMessage(request));
    EXPECT_EQ(
        h.call("room.execute", bytes, "gateway").status(),
        arena::rpc::UNAUTHENTICATED
    );
    auto legacy = h.request("room.execute", bytes);
    legacy.set_protocol_version(0);
    legacy.clear_caller_service();
    legacy.clear_target_service();
    legacy.clear_service_path();
    EXPECT_EQ(
        h.callRequest(std::move(legacy)).status(),
        arena::rpc::INCOMPATIBLE_VERSION
    );
    request.set_worker_id("node-a:00000000000000000000000000000000");
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::CONFLICT
    );
    request = h.execution(image, 2);
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::CONFLICT
    );
    auto invalid = unwrap(wire::unpackImage(image));
    ++invalid.actor.checksum;
    auto packed = unwrap(wire::packImage(invalid, 0));
    EXPECT_EQ(
        h.call("room.stage", h.staging(packed)).status(),
        arena::rpc::INVALID_ARGUMENT
    );
    request = h.execution(image, 1, {input(1)});
    request.mutable_operations(0)->set_logical_tick(
        image.logical_tick() + 1
    );
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::INVALID_ARGUMENT
    );
    request = h.execution(image);
    request.mutable_operations(0)->set_cursor(100);
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::INVALID_ARGUMENT
    );
    request = h.execution(image);
    request.mutable_operations(0)->set_kind(pb::OP_COMMAND);
    EXPECT_EQ(
        h.call("room.execute", request).status(),
        arena::rpc::INVALID_ARGUMENT
    );
    pb::CreateRequest old;
    old.set_version(2);
    old.set_worker_id(h.worker->stats().workerId);
    EXPECT_EQ(
        h.call("room.create", old).status(),
        arena::rpc::INCOMPATIBLE_VERSION
    );
}
TEST(
    RoomWorker,
    FreezeAndRetireKeepEpochFencesAndCannotPublishProposals
)
{
    Harness h;
    auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image, 4, 10)).status(),
        arena::rpc::OK
    );
    pb::HintRequest hint;
    hint.set_version(1);
    hint.set_worker_id(h.worker->stats().workerId);
    hint.set_owner_epoch(4);
    hint.set_directory_revision(11);
    hint.set_room_id(7);
    hint.set_incarnation(9);
    EXPECT_EQ(h.call("room.freeze", hint).status(), arena::rpc::OK);
    EXPECT_EQ(
        h.call("room.execute", h.execution(image, 4)).status(),
        arena::rpc::CONFLICT
    );
    hint.set_directory_revision(12);
    EXPECT_EQ(h.call("room.retire", hint).status(), arena::rpc::OK);
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 4, 13)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.stage", h.staging(image, 3)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 5, 13)).status(),
        arena::rpc::OK
    );
}
TEST(
    RoomWorker,
    OptionalLegacyReplayIsActuallyValidatedAgainstCompleteCurrentSnapshot
)
{
    Harness h;
    auto initial = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(initial)).status(),
        arena::rpc::OK
    );
    auto next = h.reply<pb::ExecuteReply>(
        h.call("room.execute", h.execution(initial, 1, {input(1)}))
    );
    auto initialValue = unwrap(wire::unpackImage(initial));
    ReplayLog log{
        initialValue.policy.actor,
        initialValue.actor,
        {unwrap(wire::decodeTick(next.replay()))}
    };
    auto stage = h.staging(next.image(), 2);
    stage.set_legacy_replay(unwrap(wire::encodeReplayLog(log)));
    EXPECT_EQ(h.call("room.stage", stage).status(), arena::rpc::OK);
    ++log.ticks.front().checksum;
    stage.set_owner_epoch(3);
    stage.set_transfer_id("bad-replay");
    stage.set_legacy_replay(unwrap(wire::encodeReplayLog(log)));
    EXPECT_EQ(
        h.call("room.stage", stage).status(),
        arena::rpc::INVALID_ARGUMENT
    );
}
TEST(
    RoomWorker,
    CancelledDurableJobKeepsPhysicalAdmissionUntilActualSyncAndStop
)
{
    Harness h("node-a", 1);
    auto image = h.create();
    holdSync.store(true);
    syncEntered.store(false);
    SyncRelease release;
    auto result = std::make_shared<
        std::optional<arena::rpc::Response>>();
    auto stage = h.staging(image);
    auto call = h.client->call(
        h.request(
            "room.stage",
            unwrap(wire::serializeMessage(stage))
        ),
        {},
        [result](auto r) { *result = std::move(r); }
    );
    ASSERT_TRUE(call);
    ASSERT_TRUE(h.until([&] { return syncEntered.load(); }));
    EXPECT_EQ(h.worker->stats().activeJobs, 1);
    EXPECT_EQ(h.worker->stats().journalPending, 1);
    EXPECT_EQ(h.worker->stats().stages, 0);
    EXPECT_EQ(
        h.call("room.create", pb::CreateRequest{}).status(),
        arena::rpc::OVERLOADED
    );
    call->cancel();
    ASSERT_TRUE(h.until([&] { return result->has_value(); }));
    EXPECT_EQ((**result).status(), arena::rpc::CANCELLED);
    h.worker->stop();
    EXPECT_EQ(h.worker->stats().activeJobs, 1);
    EXPECT_FALSE(h.worker->stats().stopped);
    holdSync.store(false);
    ASSERT_TRUE(h.until([&] { return h.worker->stats().stopped; }));
    EXPECT_EQ(h.worker->stats().activeJobs, 0);
    EXPECT_EQ(h.worker->stats().journalPending, 0);
    EXPECT_EQ(h.worker->stats().stages, 1);
}
TEST(RoomWorker, HealthReportsRealCacheAndStopReleasesDescriptors)
{
    const auto baseline = descriptors();
    {
        Harness h;
        auto image = h.create();
        ASSERT_EQ(
            h.call("room.stage", h.staging(image)).status(),
            arena::rpc::OK
        );
        auto response = h.call("health.ready", std::string{});
        auto health = h.reply<pb::HealthReply>(response);
        EXPECT_TRUE(health.ready());
        EXPECT_EQ(health.worker_id(), h.worker->stats().workerId);
        EXPECT_EQ(health.stats().inactive_stages(), 1);
        EXPECT_GT(health.stats().physical_descriptors(), baseline);
        EXPECT_GT(health.stats().cache_bytes(), 0);
        h.close();
    }
    EXPECT_EQ(descriptors(), baseline);
}
TEST(
    RoomWorker,
    RoomCapacityIsBoundedAndRejectedActivationHasNoPartialCache
)
{
    Harness h("node-a", 8, 1);
    auto first = h.create(7), second = h.create(8);
    ASSERT_EQ(
        h.call("room.activate", h.activation(first)).status(),
        arena::rpc::OK
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(second)).status(),
        arena::rpc::OVERLOADED
    );
    EXPECT_EQ(h.worker->stats().activeRoutes, 1);
    EXPECT_EQ(
        h.call("room.execute", h.execution(second)).status(),
        arena::rpc::CONFLICT
    );
}
TEST(
    RoomWorker,
    IdenticalInactiveInitialProofCanBeRevalidatedAfterPreCreateCrash
)
{
    Harness h;
    const auto image = h.create();
    auto initial = h.staging(image, 1, "initial-7-9");
    ASSERT_EQ(h.call("room.stage", initial).status(), arena::rpc::OK);
    const auto old = h.worker->stats().workerId;
    h.close();
    h.start();
    ASSERT_NE(h.worker->stats().workerId, old);
    auto conflict = h.staging(image, 1, "initial-wrong");
    EXPECT_EQ(
        h.call("room.stage", conflict).status(),
        arena::rpc::CONFLICT
    );
    auto replacement = h.staging(image, 1, "initial-7-9");
    ASSERT_EQ(
        h.call("room.stage", replacement).status(),
        arena::rpc::OK
    );
    ASSERT_EQ(
        h.call("room.activate", h.activation(image, 1, 1)).status(),
        arena::rpc::OK
    );
    h.close();
    h.start();
    EXPECT_EQ(
        h.call("room.stage", h.staging(image, 1, "initial-7-9"))
            .status(),
        arena::rpc::CONFLICT
    );
}
TEST(
    RoomWorker,
    NewIncarnationStartsAtEpochOneWithoutWeakeningOldLifetimeFence
)
{
    Harness h;
    auto first = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(first, 4, 1)).status(),
        arena::rpc::OK
    );
    arena::room::HintRequest retired;
    retired.set_version(1);
    retired.set_worker_id(h.worker->stats().workerId);
    retired.set_owner_epoch(4);
    retired.set_directory_revision(2);
    retired.set_room_id(7);
    retired.set_incarnation(9);
    ASSERT_EQ(
        h.call("room.retire", retired).status(),
        arena::rpc::OK
    );
    const auto next = h.create(7, 10);
    ASSERT_EQ(
        h.call("room.stage", h.staging(next, 1, "initial-7-10"))
            .status(),
        arena::rpc::OK
    );
    ASSERT_EQ(
        h.call(
             "room.activate",
             h.activation(next, 1, 3, "initial-7-10")
        )
            .status(),
        arena::rpc::OK
    );
    EXPECT_EQ(
        h.call("room.execute", h.execution(first, 4)).status(),
        arena::rpc::CONFLICT
    );
    auto proposal = h.reply<arena::room::ExecuteReply>(
        h.call("room.execute", h.execution(next, 1))
    );
    EXPECT_EQ(proposal.image().incarnation(), 10);
    EXPECT_EQ(proposal.image().physical_tick(), 1);
    EXPECT_EQ(h.worker->stats().activeRoutes, 1);
}
TEST(
    RoomWorker,
    StageOnlyRetirementPersistsFenceAndCannotReactivateSameEpoch
)
{
    Harness h;
    const auto image = h.create();
    ASSERT_EQ(
        h.call("room.stage", h.staging(image, 2, "migration-7-9-1"))
            .status(),
        arena::rpc::OK
    );
    pb::HintRequest hint;
    hint.set_version(1);
    hint.set_worker_id(h.worker->stats().workerId);
    hint.set_owner_epoch(2);
    hint.set_directory_revision(10);
    hint.set_room_id(7);
    hint.set_incarnation(9);
    ASSERT_EQ(h.call("room.retire", hint).status(), arena::rpc::OK);
    EXPECT_EQ(h.worker->stats().stages, 0);
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    EXPECT_EQ(
        h.call("room.stage", h.staging(image, 2, "migration-7-9-1"))
            .status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call(
             "room.activate",
             h.activation(image, 2, 11, "migration-7-9-1")
        )
            .status(),
        arena::rpc::CONFLICT
    );
    h.close();
    h.start();
    EXPECT_EQ(
        h.call("room.stage", h.staging(image, 2, "migration-7-9-1"))
            .status(),
        arena::rpc::CONFLICT
    );
    ASSERT_EQ(
        h.call("room.stage", h.staging(image, 4, "migration-7-9-3"))
            .status(),
        arena::rpc::OK
    );
    EXPECT_EQ(
        h.call(
             "room.activate",
             h.activation(image, 4, 12, "migration-7-9-3")
        )
            .status(),
        arena::rpc::OK
    );
}
TEST(RoomWorker, OldFreezeAndRetireKeepHigherEpochStageProof)
{
    Harness h;
    const auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image, 1, 1)).status(),
        arena::rpc::OK
    );
    ASSERT_EQ(
        h.call("room.stage", h.staging(image, 3, "migration-7-9-2"))
            .status(),
        arena::rpc::OK
    );
    pb::HintRequest hint;
    hint.set_version(1);
    hint.set_worker_id(h.worker->stats().workerId);
    hint.set_owner_epoch(1);
    hint.set_directory_revision(2);
    hint.set_room_id(7);
    hint.set_incarnation(9);
    const auto beforeFreeze = h.worker->stats().cacheBytes;
    ASSERT_EQ(h.call("room.freeze", hint).status(), arena::rpc::OK);
    EXPECT_LT(h.worker->stats().cacheBytes, beforeFreeze);
    EXPECT_EQ(h.worker->stats().stages, 1);
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 1, 3)).status(),
        arena::rpc::CONFLICT
    );
    hint.set_directory_revision(3);
    ASSERT_EQ(h.call("room.retire", hint).status(), arena::rpc::OK);
    EXPECT_EQ(h.worker->stats().stages, 1);
    ASSERT_EQ(
        h.call(
             "room.activate",
             h.activation(image, 3, 4, "migration-7-9-2")
        )
            .status(),
        arena::rpc::OK
    );
    EXPECT_EQ(
        h.call("room.freeze", hint).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(h.worker->stats().activeRoutes, 1);
}
TEST(
    RoomWorker,
    RestartReleasesDeadInstanceImagesAndPreservesEpochFences
)
{
    Harness h;
    const auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image, 2, 4)).status(),
        arena::rpc::OK
    );
    ASSERT_EQ(
        h.call("room.stage", h.staging(image, 3, "migration-next"))
            .status(),
        arena::rpc::OK
    );
    const auto oldRequest = h.execution(image, 2);
    const auto retained = h.worker->stats().cacheBytes;
    h.close();
    h.start();
    EXPECT_LT(h.worker->stats().cacheBytes, retained);
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    EXPECT_EQ(h.worker->stats().stages, 1);
    EXPECT_EQ(
        h.call("room.execute", oldRequest).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 2, 4)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.stage", h.staging(image, 3, "migration-next"))
            .status(),
        arena::rpc::CONFLICT
    );
    ASSERT_EQ(
        h.call("room.activate", h.activation(image, 4, 5)).status(),
        arena::rpc::OK
    );
    EXPECT_EQ(h.worker->stats().stages, 0);
    EXPECT_EQ(
        h.call("room.execute", h.execution(image, 4)).status(),
        arena::rpc::OK
    );
}
TEST(
    RoomWorker,
    StartupFenceCleanupWaitsForRealSyncAndStopDrainsItsLoan
)
{
    Harness h;
    const auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image)).status(),
        arena::rpc::OK
    );
    const auto before = h.worker->stats().cacheBytes;
    h.close();
    SyncRelease release;
    syncEntered.store(false);
    h.start(8, 16, false);
    holdSync.store(true);
    ASSERT_TRUE(h.until([] { return syncEntered.load(); }, 1s));
    EXPECT_FALSE(h.worker->stats().ready);
    EXPECT_GT(h.worker->stats().journalPending, 0);
    EXPECT_EQ(
        h.call(
             "room.create",
             [&]
             {
                 pb::CreateRequest request;
                 request.set_version(1);
                 request.set_worker_id(h.worker->stats().workerId);
                 request.set_room_id(8);
                 request.set_incarnation(10);
                 *request.mutable_policy() = unwrap(
                     wire::packPolicy(RuntimePolicy{})
                 );
                 return request;
             }()
        )
            .status(),
        arena::rpc::UNAVAILABLE
    );
    h.worker->stop();
    EXPECT_FALSE(h.worker->stats().stopped);
    holdSync.store(false);
    ASSERT_TRUE(h.until([&] { return h.worker->stats().stopped; }));
    EXPECT_FALSE(h.worker->stats().ready);
    h.close();
    h.start();
    EXPECT_TRUE(h.worker->stats().ready);
    EXPECT_LT(h.worker->stats().cacheBytes, before);
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 1, 2)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 2, 3)).status(),
        arena::rpc::OK
    );
}
TEST(RoomWorker, StartupSyncFailureStaysUnreadyAndReopenRetainsFences)
{
    Harness h;
    const auto image = h.create();
    ASSERT_EQ(
        h.call("room.activate", h.activation(image)).status(),
        arena::rpc::OK
    );
    const auto before = h.worker->stats().cacheBytes;
    h.close();
    h.start(8, 16, false);
    failSync.store(true);
    ASSERT_TRUE(h.until([&] { return h.worker->stats().stopped; }));
    EXPECT_FALSE(failSync.load());
    EXPECT_FALSE(h.worker->stats().ready);
    EXPECT_TRUE(h.worker->stats().journalClosed);
    EXPECT_EQ(h.worker->stats().journalPending, 0);
    h.close();
    h.start();
    EXPECT_TRUE(h.worker->stats().ready);
    EXPECT_LT(h.worker->stats().cacheBytes, before);
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 1, 2)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 2, 3)).status(),
        arena::rpc::OK
    );
}
TEST(RoomWorker, StartupCleansMultipleBatchesBeforeReadiness)
{
    Harness h("node-a", 8, 512);
    for (std::uint64_t id = 1; id <= 300; ++id)
    {
        const auto image = h.create(id, id);
        ASSERT_EQ(
            h.call("room.activate", h.activation(image, 1, id))
                .status(),
            arena::rpc::OK
        );
    }
    const auto before = h.worker->stats().cacheBytes;
    h.close();
    h.start(8, 512);
    EXPECT_TRUE(h.worker->stats().ready);
    EXPECT_EQ(h.worker->stats().activeRoutes, 0);
    EXPECT_LT(h.worker->stats().cacheBytes, before);
    const auto retained = h.worker->stats().cacheBytes;
    h.close();
    h.start(8, 512);
    EXPECT_EQ(h.worker->stats().cacheBytes, retained);
    const auto image = h.create(300, 300);
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 1, 301)).status(),
        arena::rpc::CONFLICT
    );
    EXPECT_EQ(
        h.call("room.activate", h.activation(image, 2, 302)).status(),
        arena::rpc::OK
    );
}
} // namespace