#include "lobby/RoomCoordinator.h"

#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "room/RoomWire.h"
#include "room/RoomWorker.h"

namespace
{
using namespace std::chrono_literals;
using namespace service::lobby;
using Authority = LobbyAuthority;
namespace rw = service::room::wire;
using Commit = std::
    expected<Authority::CommitReply, Authority::Error>;
/** @brief Forwards real worker frames and can hold one selected
 * completed reply. */
class ReplyProxy
{
    using Tcp = boost::asio::ip::tcp;
    struct Peer : std::enable_shared_from_this<Peer>
    {
        ReplyProxy& owner;
        Tcp::socket front, back;
        std::array<unsigned char, 4> header{};
        std::string request, response, method;
        bool closed{false};
        Peer(ReplyProxy& value)
            : owner(value)
            , front(value.io)
            , back(value.io)
        {
        }
        void close()
        {
            if (closed)
                return;
            closed = true;
            boost::system::error_code ignored;
            front.close(ignored);
            back.close(ignored);
            owner.peers.erase(shared_from_this());
        }
        void readRequest()
        {
            auto self = shared_from_this();
            boost::asio::async_read(
                front,
                boost::asio::buffer(header),
                [self](auto error, auto)
                {
                    if (error)
                    {
                        self->close();
                        return;
                    }
                    const auto size = self->length();
                    if (!size || size > 256 * 1024)
                    {
                        self->close();
                        return;
                    }
                    self->request.resize(size);
                    boost::asio::async_read(
                        self->front,
                        boost::asio::buffer(self->request),
                        [self](auto error, auto)
                        {
                            arena::rpc::Request parsed;
                            if (error || !parsed.ParseFromString(
                                             self->request
                                         ))
                            {
                                self->close();
                                return;
                            }
                            self->method = parsed.method();
                            self->back.async_connect(
                                self->owner.target,
                                [self](auto error)
                                {
                                    if (error)
                                    {
                                        self->close();
                                        return;
                                    }
                                    self->request.insert(
                                        0,
                                        reinterpret_cast<const char*>(
                                            self->header.data()
                                        ),
                                        4
                                    );
                                    boost::asio::async_write(
                                        self->back,
                                        boost::asio::buffer(
                                            self->request
                                        ),
                                        [self](auto error, auto)
                                        {
                                            if (error)
                                            {
                                                self->close();
                                                return;
                                            }
                                            self->readResponse();
                                        }
                                    );
                                }
                            );
                        }
                    );
                }
            );
        }
        std::uint32_t length() const
        {
            return (std::uint32_t(header[0]) << 24) |
                   (std::uint32_t(header[1]) << 16) |
                   (std::uint32_t(header[2]) << 8) | header[3];
        }
        void readResponse()
        {
            auto self = shared_from_this();
            boost::asio::async_read(
                back,
                boost::asio::buffer(header),
                [self](auto error, auto)
                {
                    if (error)
                    {
                        self->close();
                        return;
                    }
                    const auto size = self->length();
                    if (!size || size > 256 * 1024)
                    {
                        self->close();
                        return;
                    }
                    self->response.resize(size);
                    boost::asio::async_read(
                        self->back,
                        boost::asio::buffer(self->response),
                        [self](auto error, auto)
                        {
                            if (error)
                            {
                                self->close();
                                return;
                            }
                            if (self->method == "health.ready")
                            {
                                ++self->owner.probes;
                                if (self->owner.holdNextProbe)
                                {
                                    self->owner.holdNextProbe = false;
                                    self->owner.held = self;
                                    return;
                                }
                            }
                            if (self->method == "room.freeze" ||
                                self->method == "room.retire")
                            {
                                ++self->owner.hints;
                                if (self->owner.holdNextHint)
                                {
                                    self->owner.holdNextHint = false;
                                    self->owner.held = self;
                                    return;
                                }
                            }
                            arena::rpc::Response parsed;
                            if (!parsed.ParseFromString(
                                    self->response
                                ) ||
                                parsed.status() != arena::rpc::OK)
                            {
                                self->send();
                                return;
                            }
                            if (self->method ==
                                self->owner.holdMethod)
                            {
                                ++self->owner.executions;
                                if (self->owner.holdNext)
                                {
                                    self->owner.holdNext = false;
                                    self->owner.held = self;
                                    return;
                                }
                            }
                            self->send();
                        }
                    );
                }
            );
        }
        void send()
        {
            auto self = shared_from_this();
            response.insert(
                0,
                reinterpret_cast<const char*>(header.data()),
                4
            );
            boost::asio::async_write(
                front,
                boost::asio::buffer(response),
                [self](auto, auto) { self->close(); }
            );
        }
    };

public:
    boost::asio::io_context& io;
    Tcp::acceptor acceptor;
    Tcp::endpoint target;
    std::set<std::shared_ptr<Peer>> peers;
    std::shared_ptr<Peer> held;
    bool holdNext{false}, holdNextHint{false}, holdNextProbe{false};
    std::string holdMethod{"room.execute"};
    std::size_t executions{0}, hints{0}, probes{0};
    ReplyProxy(boost::asio::io_context& context, std::uint16_t port)
        : io(context)
        , acceptor(
              context, {boost::asio::ip::address_v4::loopback(), 0}
          )
        , target(boost::asio::ip::address_v4::loopback(), port)
    {
        accept();
    }
    void accept()
    {
        auto peer = std::make_shared<Peer>(*this);
        acceptor.async_accept(
            peer->front,
            [this, peer](auto error)
            {
                if (!error)
                {
                    peers.insert(peer);
                    peer->readRequest();
                    accept();
                }
            }
        );
    }
    std::uint16_t port() const
    {
        return acceptor.local_endpoint().port();
    }
    void release()
    {
        if (auto peer = std::exchange(held, {}))
            peer->send();
    }
    ~ReplyProxy()
    {
        boost::system::error_code ignored;
        acceptor.close(ignored);
        held.reset();
        while (!peers.empty())
            (*peers.begin())->close();
    }
};
struct Stack
{
    boost::asio::io_context io;
    std::filesystem::path directory;
    const Authority::Clock::time_point
        origin = Authority::Clock::now();
    std::atomic<std::int64_t> elapsed{0};
    std::unique_ptr<service::room::RoomWorker> a, b;
    std::shared_ptr<arena::runtime::ConfigStore> discovery;
    std::unique_ptr<Authority> authority;
    std::unique_ptr<RoomCoordinator> coordinator;
    unsigned configVersion{0};
    std::optional<std::uint16_t> proxyA, proxyB;
    explicit Stack(std::size_t maxRooms = 32)
    {
        std::string pattern = "/tmp/arena-room-coordinator-XXXXXX";
        std::vector<char> text(pattern.begin(), pattern.end());
        text.push_back(0);
        auto* created = ::mkdtemp(text.data());
        if (!created)
            throw std::runtime_error("private directory failed");
        directory = created;
        a = openWorker("room-a");
        b = openWorker("room-b");
        reload();
        Authority::Config config;
        config.bootId = "same-test-boot";
        config.journal.directory = directory / "lobby";
        config.autoTicks = false;
        config.rooms.maxPendingPerRoom = 256;
        config.rooms.maxRooms = maxRooms;
        config.now = [this] { return now(); };
        config.wallNow = [] { return std::int64_t{1700000000}; };
        authority = std::make_unique<Authority>(
            io.get_executor(),
            std::move(config)
        );
        if (!until([&] { return authority->stats().ready; }))
            throw std::runtime_error("Authority startup failed");
        openCoordinator();
        if (!until(
                [&]
                {
                    return coordinator->stats().ready &&
                           coordinator->stats().healthyWorkers == 2;
                }
            ))
            throw std::runtime_error("real workers not qualified");
    }
    Authority::Clock::time_point now() const
    {
        return origin + std::chrono::milliseconds(elapsed.load());
    }
    std::unique_ptr<service::room::RoomWorker>
    openWorker(const std::string& node)
    {
        service::room::RoomWorker::Config config;
        config.nodeId = node;
        config.token = std::string(48, 'R');
        config.directory = directory / node;
        return std::make_unique<service::room::RoomWorker>(
            io.get_executor(),
            std::move(config)
        );
    }
    void reload()
    {
        const std::string json =
            "{\"version\":\"" + std::to_string(++configVersion) +
            "\",\"desiredReady\":true,\"drainGraceMs\":0,\"nodes\":["
            "{\"service\":\"room\",\"nodeId\":\"room-a\",\"host\":"
            "\"127.0.0.1\",\"port\":" +
            std::to_string(proxyA.value_or(a->port())) +
            ",\"ready\":true},"
            "{\"service\":\"room\",\"nodeId\":\"room-b\",\"host\":"
            "\"127.0.0.1\",\"port\":" +
            std::to_string(proxyB.value_or(b->port())) +
            ",\"ready\":true}]}";
        auto snapshot = arena::runtime::ConfigSnapshot::parse(json);
        if (!snapshot)
            throw std::runtime_error("discovery fixture invalid");
        if (!discovery)
            discovery = std::make_shared<arena::runtime::ConfigStore>(
                *snapshot
            );
        else if (!discovery->publish(*snapshot))
            throw std::runtime_error("discovery publication failed");
    }
    void openCoordinator()
    {
        RoomCoordinator::Config config;
        config.client.token = std::string(48, 'R');
        config.client.runtime = discovery;
        config.client.nodes = {
            {"room-a",
             {boost::asio::ip::address_v4::loopback(), a->port()}},
            {"room-b",
             {boost::asio::ip::address_v4::loopback(), b->port()}}
        };
        config.client.maxInflight = 64;
        config.maxInflight = 32;
        config.maxBatch = 32;
        config.poll = 2ms;
        config.probe = 20ms;
        config.now = [this] { return now(); };
        config.wallNow = [] { return std::int64_t{1700000000}; };
        coordinator = std::make_unique<RoomCoordinator>(
            io.get_executor(),
            *authority,
            std::move(config)
        );
    }
    template <class Predicate>
    bool
    until(Predicate predicate, std::chrono::milliseconds budget = 4s)
    {
        const auto end = std::chrono::steady_clock::now() + budget;
        while (!predicate() && std::chrono::steady_clock::now() < end)
        {
            io.restart();
            io.run_for(1ms);
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    void stopCoordinator()
    {
        if (!coordinator)
            return;
        coordinator->stop();
        EXPECT_TRUE(until([&]
                          { return coordinator->stats().stopped; }));
        EXPECT_EQ(coordinator->stats().rpcInflight, 0);
        EXPECT_EQ(coordinator->stats().walInflight, 0);
        EXPECT_EQ(coordinator->stats().migrations, 0);
        coordinator.reset();
    }
    ~Stack()
    {
        stopCoordinator();
        authority->stop();
        a->stop();
        b->stop();
        EXPECT_TRUE(until(
            [&]
            {
                return authority->stats().stopped &&
                       a->stats().stopped && b->stats().stopped;
            }
        ));
        authority.reset();
        a.reset();
        b.reset();
        io.restart();
        io.poll();
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    Authority::CommitReply transact(Authority::Mutation mutation)
    {
        auto result = std::make_shared<std::optional<Commit>>();
        auto call = authority->submit(
            std::move(mutation),
            now() + 3s,
            [result](auto r) { *result = std::move(r); }
        );
        if (!call || !until([&] { return result->has_value(); }) ||
            !**result)
            throw std::runtime_error(
                "Authority fixture transaction failed"
            );
        return std::move(***result);
    }
    static void capture(Authority::Capture complete)
    {
        arena::protocol::Envelope reply;
        reply.set_msg_id(arena::protocol::MSG_PONG);
        reply.mutable_pong();
        complete(std::move(reply));
    }
    RoomIdentity reserve(std::uint64_t room = 7)
    {
        auto result = transact(
            [room](auto& work, auto complete)
            {
                auto* pending = work.meta.add_pending_rooms();
                pending->set_room_id(room);
                pending->set_incarnation(
                    work.meta.incarnation_high_water() + 1
                );
                work.meta.set_incarnation_high_water(
                    pending->incarnation()
                );
                pending->set_created_unix_ms(1700000000000);
                pending->set_deadline_nanos(
                    std::chrono::duration_cast<
                        std::chrono::nanoseconds>(
                        work.now.time_since_epoch() + 10s
                    )
                        .count()
                );
                capture(complete);
            }
        );
        const auto& pending = result.committed->meta.pending_rooms(
            result.committed->meta.pending_rooms_size() - 1
        );
        return {pending.room_id(), pending.incarnation()};
    }
    void join(RoomIdentity id, std::uint64_t player = 1)
    {
        transact(
            [id, player](auto& work, auto complete)
            {
                const auto epoch = work.meta.reserved_epoch() + 1;
                work.meta.set_reserved_epoch(epoch);
                const auto handle = work.meta.transport_high_water() +
                                    1;
                work.meta.set_transport_high_water(handle);
                service::auth::IssuedToken verified{
                    "verified-fixture-candidate",
                    {arena::domain::PlayerId{player},
                     epoch,
                     1700003600}
                };
                SessionBinding binding{
                    verified.claims.playerId,
                    handle,
                    epoch
                };
                auto installed = work.online->installVerified(
                    verified,
                    handle,
                    {},
                    work.leaseClock(),
                    work.kickFactory()(binding)
                );
                if (!installed || !work.online->joinRoom(
                                      binding,
                                      arena::domain::RoomId{id.room}
                                  ))
                    throw std::runtime_error(
                        "verified test join failed"
                    );
                auto* transport = work.meta.add_transports();
                transport->set_gateway_instance("test-gateway");
                transport->set_connection_id(handle + 100);
                transport->set_handle(handle);
                transport->set_player_id(player);
                transport->set_epoch(epoch);
                transport->set_token_expires_at_unix(1700003600);
                transport->set_lease_deadline_nanos(
                    std::chrono::duration_cast<
                        std::chrono::nanoseconds>(
                        work.now.time_since_epoch() + 1h
                    )
                        .count()
                );
                auto* pending = work.meta.mutable_pending_rooms();
                for (int i = pending->size(); i-- > 0;)
                    if (pending->Get(i).room_id() == id.room)
                        pending->DeleteSubrange(i, 1);
                capture(complete);
            }
        );
    }
    RoomIdentity
    manual(std::uint64_t room = 7, std::uint64_t player = 1)
    {
        const auto id = reserve(room);
        if (!until(
                [&]
                {
                    return authority->snapshot()->directory->find(
                               id
                           ) != nullptr;
                }
            ))
            throw std::runtime_error(
                "worker create was not committed"
            );
        join(id, player);
        tick(id);
        return id;
    }
    void tick(RoomIdentity id)
    {
        const auto old = authority->snapshot()
                             ->directory->find(id)
                             ->image.logicalTick;
        elapsed += 50;
        authority->requestTick();
        coordinator->wake();
        if (!until(
                [&]
                {
                    return authority->snapshot()
                               ->directory->find(id)
                               ->image.logicalTick > old;
                }
            ))
            throw std::runtime_error(
                "real worker cutoff did not commit"
            );
    }
    std::string input(
        RoomIdentity id,
        std::uint64_t sequence,
        std::uint64_t player = 1
    )
    {
        const auto snapshot = authority->snapshot();
        const auto entry = std::find_if(
            snapshot->core.registry.entries.begin(),
            snapshot->core.registry.entries.end(),
            [&](const auto& entry)
            { return entry.snapshot.playerId.value() == player; }
        );
        if (entry == snapshot->core.registry.entries.end())
            throw std::runtime_error("missing real registry session");
        const auto& session = entry->snapshot;
        service::room::RuntimeCommand command;
        command.kind = service::room::RuntimeCommandKind::Input;
        command.caller = service::room::RuntimeAuthorization{
            {session.playerId, session.sessionId, session.roomEpoch},
            session.epoch,
            session.connectionId,
            true
        };
        command.incarnation = id.incarnation;
        command.sequence = sequence;
        command.dx = 1;
        auto encoded = rw::encodeCommand(command);
        if (!encoded)
            throw std::runtime_error("input encoding failed");
        const auto key = "input/" + std::to_string(sequence);
        const auto epoch = snapshot->directory->find(id)->ownerEpoch;
        transact(
            [id,
             epoch,
             key,
             bytes = *encoded](auto& work, auto complete)
            {
                work.roomChanges.push_back(
                    RoomAccept{
                        id,
                        epoch,
                        {key, RoomIngressKind::Input, 0, bytes}
                    }
                );
                capture(complete);
            }
        );
        return key;
    }
};
TEST(
    RoomCoordinator,
    ManualCreationRemainsPinnedUntilJoinThenRealCutoffPublishes
)
{
    Stack s;
    const auto id = s.reserve();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()->directory->find(id) !=
                   nullptr;
        }
    ));
    auto before = s.authority->snapshot();
    ASSERT_EQ(before->meta.pending_rooms_size(), 1);
    EXPECT_EQ(before->directory->find(id)->image.tick, 0);
    s.elapsed += 50;
    s.authority->requestTick();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()->revision >
                   before->revision;
        }
    ));
    EXPECT_TRUE(
        s.authority->snapshot()->directory->find(id)->inbox.empty()
    );
    const auto control = s.authority->snapshot()
                             ->directory->controlRevision();
    s.join(id);
    EXPECT_EQ(
        s.authority->snapshot()->directory->controlRevision(),
        control
    );
    s.tick(id);
    auto image = s.authority->snapshot()->roomImages.at(id);
    EXPECT_EQ(image->actor.players.size(), 1);
    EXPECT_EQ(image->actor.tick, 1);
}
TEST(
    RoomCoordinator,
    ClosingIngressDrainsBeforeLastImageRetiresAndRoomIdCanBeReused
)
{
    Stack s;
    const auto id = s.manual();
    s.stopCoordinator();
    const auto operation = s.input(id, 1);
    const auto oldTick = s.authority->snapshot()
                             ->directory->find(id)
                             ->image.logicalTick;
    s.elapsed += 50;
    s.authority->requestTick();
    ASSERT_TRUE(s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return std::any_of(
                route->inbox.begin(),
                route->inbox.end(),
                [](const auto& item)
                {
                    return item.operation.kind ==
                           RoomIngressKind::Tick;
                }
            );
        }
    ));
    const auto binding = s.authority->snapshot()
                             ->core.registry.entries.front()
                             .binding;
    s.transact(
        [id, binding](auto& work, auto complete)
        {
            if (!work.online->leaveRoom(binding))
                throw std::runtime_error("fixture leave failed");
            const auto& route = *work.base->directory->find(id);
            work.roomChanges.push_back(
                RoomRetire{
                    id,
                    route.owner,
                    route.ownerEpoch,
                    route.image.cursor,
                    roomImageFingerprint(id, route.image)
                }
            );
            Stack::capture(complete);
        }
    );
    ASSERT_EQ(
        s.authority->snapshot()->directory->find(id)->lifecycle,
        RoomRouteLifecycle::Closing
    );
    ASSERT_TRUE(s.authority->snapshot()->roomImages.contains(id));
    s.openCoordinator();
    ASSERT_TRUE(s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return route->inbox.empty() &&
                   route->receipts.at(operation).applied;
        }
    ));
    EXPECT_GT(
        s.authority->snapshot()
            ->directory->find(id)
            ->image.logicalTick,
        oldTick
    );
    s.transact(
        [id](auto& work, auto complete)
        {
            const auto& route = *work.base->directory->find(id);
            work.roomChanges.push_back(
                RoomRetire{
                    id,
                    route.owner,
                    route.ownerEpoch,
                    route.image.cursor,
                    roomImageFingerprint(id, route.image)
                }
            );
            Stack::capture(complete);
        }
    );
    ASSERT_EQ(
        s.authority->snapshot()->directory->find(id)->lifecycle,
        RoomRouteLifecycle::Retired
    );
    ASSERT_TRUE(s.authority->snapshot()->roomImages.empty());
    s.coordinator->wake();
    ASSERT_TRUE(s.until(
        [&]
        {
            return !s.coordinator->stats().cacheHintPending &&
                   !s.coordinator->stats().cacheHintInflight &&
                   !s.a->stats().activeRoutes &&
                   !s.b->stats().activeRoutes;
        }
    ));
    const auto replacement = s.manual(id.room);
    ASSERT_NE(replacement.incarnation, id.incarnation);
    EXPECT_EQ(s.authority->snapshot()->directory->size(), 2);
    EXPECT_FALSE(s.authority->snapshot()->roomImages.contains(id));
    EXPECT_TRUE(
        s.authority->snapshot()->roomImages.contains(replacement)
    );
    const auto freshInput = s.input(replacement, 1);
    s.tick(replacement);
    const auto& receipt = s.authority->snapshot()
                              ->directory->find(replacement)
                              ->receipts.at(freshInput);
    EXPECT_TRUE(receipt.applied);
    EXPECT_EQ(
        s.authority->snapshot()
            ->roomImages.at(replacement)
            ->actor.players.front()
            .sequence,
        1
    );
}
TEST(
    RoomCoordinator,
    PublicInputReceiptUsesWorkerOutcomeAndOneCommittedSnapshot
)
{
    Stack s;
    const auto id = s.manual();
    const auto key = s.input(id, 1);
    s.tick(id);
    const auto* route = s.authority->snapshot()->directory->find(id);
    const auto& receipt = route->receipts.at(key);
    ASSERT_TRUE(receipt.applied);
    arena::protocol::Envelope reply;
    ASSERT_TRUE(reply.ParseFromString(receipt.response));
    EXPECT_EQ(reply.request_id(), 0);
    ASSERT_TRUE(reply.has_room_input_response());
    EXPECT_EQ(
        reply.room_input_response().result(),
        arena::protocol::ROOM_INPUT_APPLIED
    );
    EXPECT_EQ(
        reply.room_input_response().state().checksum(),
        route->image.checksum
    );
    EXPECT_EQ(
        reply.room_input_response().state().players(0).last_seq(),
        1
    );
}
TEST(
    RoomCoordinator,
    PrepareAckPrecedesBackgroundTransferAndTargetKeepsInputSequence
)
{
    Stack s;
    const auto id = s.manual();
    s.input(id, 1);
    s.tick(id);
    const auto before = *s.authority->snapshot()->directory->find(id);
    const auto node = RoomClient::nodeOf(before.owner);
    const std::string target = *node == "room-a" ? "room-b"
                                                 : "room-a";
    auto result = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    auto admitted = s.coordinator->startMigration(
        id,
        before.ownerEpoch,
        1,
        target,
        s.now() + 1s,
        [result](auto r) { *result = std::move(r); }
    );
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(s.until([&] { return result->has_value(); }));
    ASSERT_TRUE(**result);
    EXPECT_EQ((**result)->phase, RoomMigrationPhase::Preparing);
    ASSERT_TRUE(s.until(
        [&]
        {
            auto* route = s.authority->snapshot()->directory->find(
                id
            );
            return route->ownerEpoch == 2 &&
                   route->handoff->phase ==
                       RoomMigrationPhase::Committed;
        }
    ));
    s.input(id, 2);
    s.tick(id);
    const auto image = s.authority->snapshot()->roomImages.at(id);
    EXPECT_EQ(image->actor.players.front().sequence, 2);
    EXPECT_EQ(image->actor.players.front().x, 2);
    auto duplicate = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    ASSERT_TRUE(s.coordinator->startMigration(
        id,
        before.ownerEpoch,
        1,
        target,
        s.now() + 1s,
        [duplicate](auto r) { *duplicate = std::move(r); }
    ));
    ASSERT_TRUE(s.until([&] { return duplicate->has_value(); }));
    ASSERT_TRUE(**duplicate);
    EXPECT_EQ((**duplicate)->targetEpoch, 2);
}
TEST(
    RoomCoordinator, LostSourceCreatesNewDurableEpochOnHealthySibling
)
{
    Stack s;
    const auto id = s.manual();
    const auto before = *s.authority->snapshot()->directory->find(id);
    const auto node = RoomClient::nodeOf(before.owner);
    auto& failed = *node == "room-a" ? s.a : s.b;
    failed->stop();
    ASSERT_TRUE(s.until([&] { return failed->stats().stopped; }));
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()
                       ->directory->find(id)
                       ->ownerEpoch > before.ownerEpoch;
        }
    ));
    const auto* next = s.authority->snapshot()->directory->find(id);
    EXPECT_NE(next->owner, before.owner);
    EXPECT_EQ(next->image, before.image);
    const auto fresh = s.manual(8, 2);
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(fresh)->owner,
        next->owner
    );
    const auto inputKey = s.input(id, 1);
    s.tick(id);
    ASSERT_TRUE(s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return route->receipts.at(inputKey).applied;
        }
    ));
    EXPECT_EQ(
        s.authority->snapshot()
            ->roomImages.at(id)
            ->actor.players.front()
            .sequence,
        1
    );
}
TEST(
    RoomCoordinator,
    UnclaimedManualRoomExpiresWithoutDanglingPendingOrActor
)
{
    Stack s;
    const auto id = s.reserve();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()->directory->find(id) !=
                   nullptr;
        }
    ));
    s.elapsed += 11000;
    s.coordinator->wake();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()
                       ->meta.pending_rooms_size() == 0;
        }
    ));
    const auto* route = s.authority->snapshot()->directory->find(id);
    ASSERT_NE(route, nullptr);
    EXPECT_EQ(route->lifecycle, RoomRouteLifecycle::Retired);
    EXPECT_FALSE(s.authority->snapshot()->roomImages.contains(id));
    EXPECT_TRUE(
        s.authority->snapshot()->core.registry.entries.empty()
    );
}
TEST(
    RoomCoordinator,
    CancelledAndExpiredAdminDoNotPublishPrepareAndStopDrains
)
{
    Stack s;
    const auto id = s.manual();
    const auto route = *s.authority->snapshot()->directory->find(id);
    const auto target = *RoomClient::nodeOf(route.owner) == "room-a"
                            ? "room-b"
                            : "room-a";
    auto result = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    auto call = s.coordinator->startMigration(
        id,
        route.ownerEpoch,
        1,
        target,
        s.now() + 1s,
        [result](auto r) { *result = std::move(r); }
    );
    ASSERT_TRUE(call);
    (*call)();
    ASSERT_TRUE(s.until([&] { return result->has_value(); }));
    ASSERT_FALSE(**result);
    EXPECT_FALSE(
        s.authority->snapshot()->directory->find(id)->handoff
    );
    auto rejected = s.coordinator->startMigration(
        id,
        route.ownerEpoch,
        1,
        target,
        s.now() - 1ms,
        [](auto) {}
    );
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), arena::rpc::DEADLINE_EXCEEDED);
    s.stopCoordinator();
    EXPECT_EQ(s.a->stats().journalPending, 0);
    EXPECT_EQ(s.b->stats().journalPending, 0);
}
TEST(
    RoomCoordinator,
    FrozenControlAndNewOwnerTickDoNotWaitForOldCalculatedReply
)
{
    Stack s;
    const auto id = s.manual();
    const auto before = *s.authority->snapshot()->directory->find(id);
    const bool sourceA = *RoomClient::nodeOf(before.owner) ==
                         "room-a";
    ReplyProxy source(s.io, sourceA ? s.a->port() : s.b->port()),
        target(s.io, sourceA ? s.b->port() : s.a->port());
    if (sourceA)
    {
        s.proxyA = source.port();
        s.proxyB = target.port();
    }
    else
    {
        s.proxyB = source.port();
        s.proxyA = target.port();
    }
    s.reload();
    source.holdNext = true;
    s.input(id, 1);
    s.elapsed += 50;
    s.authority->requestTick();
    s.coordinator->wake();
    ASSERT_TRUE(s.until([&] { return bool(source.held); }, 300ms));
    arena::rpc::Response actual;
    ASSERT_TRUE(actual.ParseFromString(source.held->response));
    arena::room::ExecuteReply calculated;
    ASSERT_TRUE(calculated.ParseFromString(actual.payload()));
    EXPECT_EQ(
        calculated.image().logical_tick(),
        before.image.logicalTick + 1
    );
    auto prepared = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    ASSERT_TRUE(s.coordinator->startMigration(
        id,
        before.ownerEpoch,
        1,
        sourceA ? "room-b" : "room-a",
        s.now() + 1s,
        [prepared](auto value) { *prepared = std::move(value); }
    ));
    ASSERT_TRUE(
        s.until([&] { return prepared->has_value(); }, 300ms)
    );
    ASSERT_TRUE(**prepared);
    const bool advanced = s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return route->ownerEpoch == 2 &&
                   route->image.logicalTick >
                       before.image.logicalTick;
        },
        300ms
    );
    EXPECT_TRUE(advanced)
        << "the actual old execute response is still held; "
           "Freeze/Commit and target cutoff must already be durable";
    if (advanced)
    {
        target.holdNext = true;
        const auto current = *s.authority->snapshot()
                                  ->directory->find(id);
        s.elapsed += 50;
        s.authority->requestTick();
        s.coordinator->wake();
        EXPECT_TRUE(
            s.until([&] { return bool(target.held); }, 200ms)
        );
        const auto calls = target.executions;
        source.release();
        s.until([] { return false; }, 30ms);
        EXPECT_EQ(
            s.authority->snapshot()->directory->find(id)->image,
            current.image
        );
        EXPECT_EQ(target.executions, calls)
            << "old completion released the new owner's generation";
        target.release();
        EXPECT_TRUE(s.until(
            [&]
            {
                return s.authority->snapshot()
                           ->directory->find(id)
                           ->image.logicalTick >
                       current.image.logicalTick;
            }
        ));
    }
    else
        source.release();
    source.release();
    target.release();
    EXPECT_TRUE(s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return route->ownerEpoch == 2 &&
                   route->image.logicalTick >
                       before.image.logicalTick;
        }
    ));
    EXPECT_EQ(
        s.authority->snapshot()
            ->roomImages.at(id)
            ->actor.players.front()
            .sequence,
        1
    );
    // Destroy the coordinator before the transparent listeners and
    // pending peers.
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    RejoinDuringCalculatedEmptyCutoffPreservesApplyAndNewMembership
)
{
    Stack s;
    const auto id = s.manual();
    const auto before = *s.authority->snapshot()->directory->find(id);
    const bool sourceA = *RoomClient::nodeOf(before.owner) ==
                         "room-a";
    ReplyProxy proxy(s.io, sourceA ? s.a->port() : s.b->port());
    if (sourceA)
        s.proxyA = proxy.port();
    else
        s.proxyB = proxy.port();
    s.reload();
    const auto binding = s.authority->snapshot()
                             ->core.registry.entries.front()
                             .binding;
    s.transact(
        [binding](auto& work, auto complete)
        {
            if (!work.online->leaveRoom(binding))
                throw std::runtime_error("leave failed");
            Stack::capture(complete);
        }
    );
    proxy.holdNext = true;
    s.elapsed += 50;
    s.authority->requestTick();
    s.coordinator->wake();
    ASSERT_TRUE(s.until([&] { return bool(proxy.held); }, 300ms));
    arena::rpc::Response frame;
    ASSERT_TRUE(frame.ParseFromString(proxy.held->response));
    arena::room::ExecuteReply calculated;
    ASSERT_TRUE(calculated.ParseFromString(frame.payload()));
    ASSERT_TRUE(calculated.retire());
    s.join(id, 2);
    const auto another = s.manual(8, 3);
    proxy.release();
    const bool consumed = s.until(
        [&]
        {
            const auto* route = s.authority->snapshot()
                                    ->directory->find(id);
            return route->image.cursor >= calculated.image().cursor();
        },
        300ms
    );
    EXPECT_TRUE(consumed)
        << "the accepted empty cutoff must commit even though a new "
           "member suppressed retirement";
    EXPECT_FALSE(s.authority->stats().failed);
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(id)->lifecycle,
        RoomRouteLifecycle::Active
    );
    if (consumed)
    {
        const auto key = s.input(id, 1, 2);
        s.tick(id);
        ASSERT_TRUE(s.until(
            [&]
            {
                return s.authority->snapshot()
                    ->directory->find(id)
                    ->receipts.at(key)
                    .applied;
            }
        ));
        const auto image = s.authority->snapshot()->roomImages.at(id);
        ASSERT_EQ(image->actor.players.size(), 1);
        EXPECT_EQ(
            image->actor.players.front().member.playerId.value(),
            2
        );
        EXPECT_EQ(image->actor.players.front().sequence, 1);
        s.tick(another);
        EXPECT_GT(
            s.authority->snapshot()
                ->directory->find(another)
                ->image.logicalTick,
            0
        );
    }
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    PrepareUsesOwnerFenceWhileAnAlreadyCalculatedTickCommitsAheadOfIt
)
{
    Stack s;
    const auto id = s.manual();
    const auto initial = *s.authority->snapshot()->directory->find(
        id
    );
    const bool sourceA = *RoomClient::nodeOf(initial.owner) ==
                         "room-a";
    ReplyProxy proxy(s.io, sourceA ? s.a->port() : s.b->port());
    if (sourceA)
        s.proxyA = proxy.port();
    else
        s.proxyB = proxy.port();
    s.reload();
    proxy.holdNext = true;
    s.elapsed += 50;
    s.authority->requestTick();
    s.coordinator->wake();
    ASSERT_TRUE(s.until([&] { return bool(proxy.held); }, 300ms));
    arena::rpc::Response frame;
    ASSERT_TRUE(frame.ParseFromString(proxy.held->response));
    ASSERT_EQ(frame.status(), arena::rpc::OK);
    arena::room::ExecuteReply actual;
    ASSERT_TRUE(actual.ParseFromString(frame.payload()));
    const auto before = *s.authority->snapshot()->directory->find(id);
    ASSERT_EQ(before.inbox.size(), 1);
    ASSERT_EQ(
        before.inbox.front().operation.kind,
        RoomIngressKind::Tick
    );
    RoomApply apply{
        id,
        before.owner,
        before.ownerEpoch,
        {actual.image().physical_tick(),
         actual.image().checksum(),
         actual.image().cursor(),
         actual.image().opaque(),
         actual.image().logical_tick()},
        {{before.inbox.front().sequence, {}}},
        std::nullopt
    };
    ASSERT_TRUE(actual.has_replay());
    auto tick = rw::decodeTick(actual.replay());
    auto old = rw::decodeImage(before.image.checkpoint);
    ASSERT_TRUE(tick);
    ASSERT_TRUE(old);
    auto replay = rw::encodeReplayLog(
        {old->policy.actor, old->actor, {*tick}}
    );
    ASSERT_TRUE(replay);
    apply.replay = RoomReplayExtent{
        tick->tick,
        tick->tick,
        std::move(*replay)
    };
    auto first = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    auto duplicate = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    bool captured = false;
    s.transact(
        [&](auto& work, auto complete)
        {
            // Keep this Authority mutation active while the other
            // strand captures Prepare. Its write must queue behind
            // this real worker TickApply.
            auto one = s.coordinator->startMigration(
                id,
                before.ownerEpoch,
                1,
                sourceA ? "room-b" : "room-a",
                s.now() + 1s,
                [first](auto result) { *first = std::move(result); }
            );
            auto two = s.coordinator->startMigration(
                id,
                before.ownerEpoch,
                1,
                sourceA ? "room-b" : "room-a",
                s.now() + 1s,
                [duplicate](auto result)
                { *duplicate = std::move(result); }
            );
            EXPECT_TRUE(one);
            EXPECT_TRUE(two);
            const auto deadline = std::chrono::steady_clock::now() +
                                  300ms;
            while (!s.coordinator->stats().walInflight &&
                   std::chrono::steady_clock::now() < deadline)
                s.io.run_one_for(1ms);
            captured = s.coordinator->stats().walInflight == 1;
            // Drain queued coordinator posts, including the identical
            // second start. This strand remains occupied, so neither
            // queued Prepare can mutate yet.
            s.io.poll();
            EXPECT_FALSE(first->has_value());
            EXPECT_FALSE(duplicate->has_value());
            work.roomChanges.push_back(apply);
            Stack::capture(complete);
        }
    );
    ASSERT_TRUE(captured);
    EXPECT_GE(
        s.authority->snapshot()
            ->directory->find(id)
            ->image.logicalTick,
        actual.image().logical_tick()
    );
    ASSERT_TRUE(s.until(
        [&] { return first->has_value() && duplicate->has_value(); },
        300ms
    ));
    EXPECT_TRUE(**first)
        << "a forward TickApply is not an owner fence conflict";
    EXPECT_TRUE(**duplicate)
        << "the same durable Prepare must remain idempotent";
    if (**first && **duplicate)
    {
        EXPECT_EQ((***first).deadline, (***duplicate).deadline);
        EXPECT_EQ((***first).source, (***duplicate).source);
        EXPECT_EQ((***first).target, (***duplicate).target);
        EXPECT_EQ((***first).id, (***duplicate).id);
    }
    proxy.release();
    EXPECT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()
                       ->directory->find(id)
                       ->ownerEpoch == 2;
        }
    ));
    EXPECT_GE(
        s.authority->snapshot()
            ->directory->find(id)
            ->image.logicalTick,
        actual.image().logical_tick()
    );
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    RetiredHistoryIdleDiagnosticKeepsExactHintsBounded
)
{
    Stack s(512);
    s.stopCoordinator();
    ReplyProxy proxy(s.io, s.a->port());
    s.proxyA = proxy.port();
    s.reload();
    constexpr std::size_t rooms = 450;
    const auto owner = s.a->stats().workerId;
    for (std::size_t start = 0; start < rooms; start += 64)
    {
        std::vector<RoomDirectoryCommand> changes;
        for (auto index = start; index < rooms && index < start + 64;
             ++index)
        {
            const RoomIdentity id{1000 + index, 1000 + index};
            auto runtime = service::room::RoomRuntime::create(
                service::room::RoomId{id.room},
                id.incarnation,
                {}
            );
            ASSERT_TRUE(runtime);
            auto packed = rw::packImage(*runtime, 0);
            ASSERT_TRUE(packed);
            RoomWorkerImage image{
                packed->physical_tick(),
                packed->checksum(),
                0,
                packed->opaque(),
                packed->logical_tick()
            };
            changes.push_back(RoomCreate{id, owner, image});
            changes.push_back(
                RoomRetire{id, owner, 1, 0, packed->fingerprint()}
            );
        }
        s.transact(
            [&](auto& work, auto complete)
            {
                work.meta.set_incarnation_high_water(
                    1000 + std::min(rooms, start + 64) - 1
                );
                for (auto index = start;
                     index < rooms && index < start + 64;
                     ++index)
                {
                    auto* pending = work.meta.add_pending_rooms();
                    pending->set_room_id(1000 + index);
                    pending->set_incarnation(1000 + index);
                    pending->set_created_unix_ms(1700000000000);
                    pending->set_deadline_nanos(
                        std::chrono::duration_cast<
                            std::chrono::nanoseconds>(
                            (s.now() + 10s).time_since_epoch()
                        )
                            .count()
                    );
                }
                Stack::capture(complete);
            }
        );
        s.transact(
            [&](auto& work, auto complete)
            {
                work.roomChanges = changes;
                work.meta.clear_pending_rooms();
                Stack::capture(complete);
            }
        );
    }
    ASSERT_EQ(s.authority->snapshot()->directory->size(), rooms);
    s.openCoordinator();
    ASSERT_TRUE(s.until(
        [&]
        {
            return proxy.hints == rooms &&
                   !s.coordinator->stats().walInflight &&
                   !s.authority->stats().pending;
        },
        4s
    ));
    EXPECT_TRUE(s.authority->snapshot()->roomImages.empty());
    ASSERT_TRUE(s.until(
        [&]
        {
            const auto stats = s.coordinator->stats();
            return !stats.cacheHintPending &&
                   !stats.cacheHintInflight;
        }
    ));
    const auto reconciliations = s.coordinator->stats()
                                     .cacheReconciliations;
    const auto cpuBefore = std::clock();
    const auto wallBefore = std::chrono::steady_clock::now();
    s.io.restart();
    s.io.run_for(250ms);
    const auto cpuNanos = static_cast<std::uint64_t>(
        (std::clock() - cpuBefore) *
        (1'000'000'000.0 / CLOCKS_PER_SEC)
    );
    const auto wallNanos = std::chrono::duration_cast<
                               std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() -
                               wallBefore
    )
                               .count();
    RecordProperty("retired_rooms", rooms);
    RecordProperty("idle_cpu_ns", std::to_string(cpuNanos));
    RecordProperty("idle_wall_ns", std::to_string(wallNanos));
    std::cout << "retired_idle rooms=" << rooms
              << " cpu_ns=" << cpuNanos << " wall_ns=" << wallNanos
              << " hints=" << proxy.hints << '\n';
    EXPECT_EQ(proxy.hints, rooms);
    EXPECT_EQ(
        s.coordinator->stats().cacheReconciliations,
        reconciliations
    );
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    ControlRevisionIgnoresTickReceiptAndCoreWritesButRetirementReconciles
)
{
    Stack s;
    const auto id = s.manual();
    auto before = s.authority->snapshot();
    const bool sourceA = *RoomClient::nodeOf(
                             before->directory->find(id)->owner
                         ) == "room-a";
    ReplyProxy proxy(s.io, sourceA ? s.a->port() : s.b->port());
    if (sourceA)
        s.proxyA = proxy.port();
    else
        s.proxyB = proxy.port();
    s.reload();
    const auto control = before->directory->controlRevision();
    const auto operation = s.input(id, 1);
    EXPECT_EQ(
        s.authority->snapshot()->directory->controlRevision(),
        control
    );
    s.tick(id);
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()
                ->directory->find(id)
                ->receipts.at(operation)
                .applied;
        }
    ));
    EXPECT_EQ(
        s.authority->snapshot()->directory->controlRevision(),
        control
    );
    EXPECT_GT(s.authority->snapshot()->revision, control);
    s.transact(
        [](auto& work, auto complete)
        {
            work.meta.set_reserved_epoch(
                work.meta.reserved_epoch() + 1
            );
            Stack::capture(complete);
        }
    );
    EXPECT_EQ(
        s.authority->snapshot()->directory->controlRevision(),
        control
    );
    s.elapsed += 11000;
    s.transact(
        [id](auto& work, auto complete)
        {
            work.roomChanges.push_back(RoomPruneReceipts{id});
            Stack::capture(complete);
        }
    );
    EXPECT_EQ(
        s.authority->snapshot()->directory->controlRevision(),
        control
    );
    ASSERT_TRUE(
        s.authority->snapshot()->directory->find(id)->receipts.empty()
    );
    const auto binding = s.authority->snapshot()
                             ->core.registry.entries.front()
                             .binding;
    const auto previousReconciliations = s.coordinator->stats()
                                             .cacheReconciliations;
    s.transact(
        [id, binding](auto& work, auto complete)
        {
            if (!work.online->leaveRoom(binding))
                throw std::runtime_error("fixture leave failed");
            const auto& route = *work.base->directory->find(id);
            work.roomChanges.push_back(
                RoomRetire{
                    id,
                    route.owner,
                    route.ownerEpoch,
                    route.image.cursor,
                    roomImageFingerprint(id, route.image)
                }
            );
            Stack::capture(complete);
        }
    );
    const auto retired = s.authority->snapshot();
    EXPECT_EQ(
        retired->directory->controlRevision(),
        retired->revision
    );
    EXPECT_GT(retired->directory->controlRevision(), control);
    ASSERT_TRUE(s.until(
        [&]
        {
            return proxy.hints == 1 &&
                   !s.coordinator->stats().cacheHintInflight;
        }
    ));
    EXPECT_EQ(
        s.coordinator->stats().cacheReconciliations,
        previousReconciliations + 1
    );
    EXPECT_EQ((sourceA ? s.a : s.b)->stats().activeRoutes, 0);
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    FrozenHintReplyCannotResolveNewCommittedRetirementIntent
)
{
    Stack s;
    const auto id = s.manual();
    const auto before = *s.authority->snapshot()->directory->find(id);
    const bool sourceA = *RoomClient::nodeOf(before.owner) ==
                         "room-a";
    ReplyProxy source(s.io, sourceA ? s.a->port() : s.b->port());
    if (sourceA)
        s.proxyA = source.port();
    else
        s.proxyB = source.port();
    s.reload();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.coordinator->stats().ready &&
                   s.coordinator->stats().healthyWorkers == 2;
        },
        300ms
    ));
    source.holdNextHint = true;
    auto prepared = std::make_shared<std::optional<
        std::expected<RoomHandoff, arena::rpc::Status>>>();
    ASSERT_TRUE(s.coordinator->startMigration(
        id,
        before.ownerEpoch,
        1,
        sourceA ? "room-b" : "room-a",
        s.now() + 1s,
        [prepared](auto result) { *prepared = std::move(result); }
    ));
    ASSERT_TRUE(s.until(
        [&]
        {
            return source.held && s.authority->snapshot()
                                          ->directory->find(id)
                                          ->ownerEpoch == 2;
        },
        300ms
    ));
    ASSERT_TRUE(prepared->has_value());
    ASSERT_TRUE(**prepared);
    EXPECT_EQ(source.hints, 1);
    source.release();
    ASSERT_TRUE(s.until(
        [&]
        {
            return source.hints == 2 &&
                   !s.coordinator->stats().cacheHintInflight;
        },
        300ms
    ));
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(id)->ownerEpoch,
        2
    );
    EXPECT_EQ((sourceA ? s.a : s.b)->stats().activeRoutes, 0);
    s.stopCoordinator();
}
TEST(
    RoomCoordinator,
    ReloadFencesReadinessAndOldHealthCannotQualifyNewRoute
)
{
    Stack s;
    const auto id = s.manual();
    ReplyProxy a(s.io, s.a->port()), b(s.io, s.b->port());
    s.proxyA = a.port();
    s.proxyB = b.port();
    s.reload();
    ASSERT_TRUE(s.until(
        [&]
        {
            return a.probes && b.probes &&
                   s.coordinator->stats().ready &&
                   s.coordinator->stats().healthyWorkers == 2;
        }
    ));
    a.holdNextProbe = b.holdNextProbe = true;
    ASSERT_TRUE(s.until([&] { return a.held && b.held; }));
    ASSERT_TRUE(s.coordinator->stats().ready);
    const auto previous = s.authority->snapshot()
                              ->directory->find(id)
                              ->image;
    const auto previousOwner = *s.authority->snapshot()
                                    ->directory->find(id);
    const auto executions = a.executions + b.executions;
    s.reload();
    EXPECT_FALSE(s.coordinator->stats().ready);
    EXPECT_EQ(s.coordinator->stats().healthyWorkers, 0);
    a.holdNextProbe = b.holdNextProbe = true;
    a.release();
    b.release();
    const auto input = s.input(id, 1);
    s.elapsed += 50;
    s.authority->requestTick();
    s.coordinator->wake();
    ASSERT_TRUE(s.until([&] { return a.held && b.held; }));
    s.until([] { return false; }, 30ms);
    EXPECT_FALSE(s.coordinator->stats().ready);
    EXPECT_EQ(s.coordinator->stats().healthyWorkers, 0);
    EXPECT_EQ(a.executions + b.executions, executions);
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(id)->image,
        previous
    );
    EXPECT_FALSE(s.authority->snapshot()
                     ->directory->find(id)
                     ->receipts.at(input)
                     .applied);
    const bool sourceA = *RoomClient::nodeOf(previousOwner.owner) ==
                         "room-a";
    (sourceA ? b : a).release();
    ASSERT_TRUE(s.until(
        [&] { return s.coordinator->stats().healthyWorkers == 1; }
    ));
    s.until([] { return false; }, 30ms);
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(id)->ownerEpoch,
        previousOwner.ownerEpoch
    );
    EXPECT_EQ(
        s.authority->snapshot()->directory->find(id)->owner,
        previousOwner.owner
    );
    EXPECT_EQ(a.executions + b.executions, executions);
    (sourceA ? a : b).release();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.coordinator->stats().ready &&
                   s.coordinator->stats().healthyWorkers == 2 &&
                   s.authority->snapshot()
                       ->directory->find(id)
                       ->receipts.at(input)
                       .applied;
        }
    ));
    EXPECT_GT(a.executions + b.executions, executions);
    s.stopCoordinator();
}
} // namespace
TEST(
    RoomCoordinator,
    ExpiredCreateReplyLeavesNoPrivateStageAfterWorkerRestart
)
{
    Stack s;
    ReplyProxy pa(s.io, s.a->port()), pb(s.io, s.b->port());
    pa.holdMethod = pb.holdMethod = "room.create";
    pa.holdNext = pb.holdNext = true;
    s.proxyA = pa.port();
    s.proxyB = pb.port();
    s.reload();
    const auto id = s.reserve();
    ASSERT_TRUE(s.until([&]
                        { return bool(pa.held) || bool(pb.held); }));
    ASSERT_EQ(s.authority->snapshot()->directory->find(id), nullptr);
    EXPECT_EQ(s.a->stats().stages + s.b->stats().stages, 0);
    s.elapsed += 11000;
    pa.release();
    pb.release();
    s.coordinator->wake();
    ASSERT_TRUE(s.until(
        [&]
        {
            return s.authority->snapshot()
                           ->meta.pending_rooms_size() == 0 &&
                   s.coordinator->stats().inflight == 0;
        }
    ));
    EXPECT_EQ(s.authority->snapshot()->directory->find(id), nullptr);
    EXPECT_EQ(s.a->stats().stages + s.b->stats().stages, 0);
    EXPECT_EQ(s.a->stats().cacheBytes + s.b->stats().cacheBytes, 0);
    s.stopCoordinator();
    s.a->stop();
    s.b->stop();
    ASSERT_TRUE(s.until(
        [&] { return s.a->stats().stopped && s.b->stats().stopped; }
    ));
    s.a.reset();
    s.b.reset();
    s.a = s.openWorker("room-a");
    s.b = s.openWorker("room-b");
    EXPECT_EQ(s.a->stats().stages + s.b->stats().stages, 0);
    EXPECT_EQ(s.a->stats().cacheBytes + s.b->stats().cacheBytes, 0);
}
