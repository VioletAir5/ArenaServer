#include "lobby/LobbyAuthority.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "lobby/LegacyRoomImport.h"
#include "lobby/ReplayArchive.h"
#include "lobby_state.pb.h"
#include "room/RoomRuntime.h"
#include "room/RoomWire.h"
#include "storage.pb.h"

namespace
{
std::atomic<bool> holdNextSync{false}, failNextSync{false},
    syncHeld{false};
std::mutex syncMutex;
std::condition_variable syncChanged;
bool releaseSync{true};
void unblockSync()
{
    std::lock_guard lock(syncMutex);
    releaseSync = true;
    syncChanged.notify_all();
}
} // namespace
extern "C" int __real_fdatasync(int);
extern "C" int __wrap_fdatasync(int descriptor)
{
    std::array<char, 4096> path{};
    const auto name = "/proc/self/fd/" + std::to_string(descriptor);
    const auto count = ::readlink(
        name.c_str(),
        path.data(),
        path.size()
    );
    if (count > 0 &&
        std::string_view(path.data(), static_cast<std::size_t>(count))
            .ends_with("/authority.wal"))
    {
        if (holdNextSync.exchange(false))
        {
            std::unique_lock lock(syncMutex);
            syncHeld = true;
            syncChanged.notify_all();
            syncChanged.wait(lock, [] { return releaseSync; });
        }
        if (failNextSync.exchange(false))
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
using Authority = service::lobby::LobbyAuthority;
using namespace service::lobby;
using arena::domain::PlayerId;
using arena::domain::RoomId;
using Result = std::
    expected<Authority::CommitReply, Authority::Error>;

arena::protocol::Envelope pong(std::uint64_t value = 0)
{
    arena::protocol::Envelope reply;
    reply.set_msg_id(arena::protocol::MSG_PONG);
    reply.set_request_id(value);
    reply.mutable_pong();
    return reply;
}
std::string bytes(const std::filesystem::path& path)
{
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), {}};
}
struct Pending
{
    std::shared_ptr<std::optional<Result>> result;
    Authority::Cancellation cancel;
};
class LobbyAuthorityTest : public testing::Test
{
protected:
    boost::asio::io_context context;
    std::filesystem::path directory;
    std::atomic<std::int64_t> milliseconds{0}, wall{1700000000};
    Authority::Config config;
    std::unique_ptr<Authority> authority;
    std::unique_ptr<service::auth::JwtTokens> tokens;

    void SetUp() override
    {
        std::array<char, 64> pattern{};
        const std::string name = "/tmp/arena-lobby-owner-XXXXXX";
        std::copy(name.begin(), name.end(), pattern.begin());
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error("mkdtemp failed");
        directory = pattern.data();
        config.journal.directory = directory;
        config.bootId = "same-linux-test-boot";
        config.autoTicks = false;
        config.registry.grace = 100ms;
        config.rooms.battleDurationTicks = 1000;
        config.now = [this]
        {
            return Authority::Clock::time_point{} +
                   std::chrono::milliseconds(milliseconds.load());
        };
        config.wallNow = [this] { return wall.load(); };
        service::auth::JwtConfig jwt;
        jwt.key = "0123456789abcdef0123456789abcdef";
        jwt.now = [this] { return wall.load(); };
        tokens = std::make_unique<service::auth::JwtTokens>(
            std::move(jwt)
        );
    }
    bool until(
        const std::function<bool()>& predicate,
        std::chrono::milliseconds budget = 4s
    )
    {
        const auto end = Authority::Clock::now() + budget;
        while (Authority::Clock::now() < end)
        {
            context.restart();
            context.run_for(2ms);
            if (predicate())
                return true;
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }
    void open()
    {
        authority = std::make_unique<Authority>(
            context.get_executor(),
            config
        );
        if (!until([&] { return authority->stats().initialized; }) ||
            !authority->stats().ready)
            throw std::runtime_error("authority did not initialize");
    }
    void close()
    {
        if (!authority)
            return;
        authority->stop();
        if (!until([&] { return authority->stats().stopped; }))
            throw std::runtime_error(
                "authority physical shutdown timed out"
            );
        if (!authority->stats().journal.closed ||
            authority->stats().pending)
            throw std::runtime_error(
                "authority retained physical work after shutdown"
            );
        authority.reset();
        context.restart();
        context.poll();
    }
    void TearDown() override
    {
        holdNextSync = false;
        failNextSync = false;
        unblockSync();
        if (authority)
        {
            authority->stop();
            EXPECT_TRUE(
                until([&] { return authority->stats().stopped; })
            );
            authority.reset();
            context.restart();
            context.poll();
        }
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    Pending submit(Authority::Mutation mutation, bool tick = false)
    {
        Pending pending{
            std::make_shared<std::optional<Result>>(),
            {}
        };
        auto admitted = authority->submit(
            std::move(mutation),
            Authority::Clock::now() + 3s,
            [result = pending.result](Result value)
            { result->emplace(std::move(value)); },
            tick
        );
        if (!admitted)
            throw std::runtime_error("fixture transaction rejected");
        pending.cancel = std::move(*admitted);
        return pending;
    }
    Result await(Pending& pending)
    {
        if (!until([&] { return pending.result->has_value(); }))
            throw std::runtime_error(
                "transaction completion missing"
            );
        return std::move(**pending.result);
    }
    Authority::CommitReply
    call(Authority::Mutation mutation, bool tick = false)
    {
        auto pending = submit(std::move(mutation), tick);
        if (tick)
        {
            milliseconds += 50;
            authority->requestTick();
        }
        auto result = await(pending);
        if (!result)
            throw std::runtime_error(
                "transaction did not commit: " +
                std::to_string(int(result.error()))
            );
        return std::move(*result);
    }
    arena::protocol::LoginResponse
    login(std::uint64_t player, std::uint64_t room = 0)
    {
        auto reserved = call(
            [](auto& candidate, auto capture)
            {
                candidate.meta.set_reserved_epoch(
                    candidate.meta.reserved_epoch() + 1
                );
                capture(pong());
            }
        );
        auto signedToken = tokens->issue(
            PlayerId{player},
            reserved.committed->meta.reserved_epoch()
        );
        if (!signedToken)
            throw std::runtime_error("real token signing failed");
        auto result = call(
            [token = std::move(*signedToken),
             room](auto& candidate, auto capture)
            {
                const auto handle = candidate.meta
                                        .transport_high_water() +
                                    1;
                candidate.meta.set_transport_high_water(handle);
                const auto binding = SessionBinding{
                    token.claims.playerId,
                    handle,
                    token.claims.epoch
                };
                auto installed = candidate.online->installVerified(
                    token,
                    handle,
                    {},
                    candidate.leaseClock(),
                    candidate.kickFactory()(binding)
                );
                if (!installed)
                    throw std::runtime_error(
                        "verified candidate install failed"
                    );
                if (room && !candidate.online
                                 ->joinRoom(binding, RoomId{room}))
                    throw std::runtime_error(
                        "manual room join failed"
                    );
                auto* transport = candidate.meta.add_transports();
                transport->set_gateway_instance("test-gateway");
                transport->set_connection_id(handle + 100);
                transport->set_handle(handle);
                transport->set_player_id(
                    token.claims.playerId.value()
                );
                transport->set_epoch(token.claims.epoch);
                transport->set_token_expires_at_unix(
                    token.claims.expiresAt
                );
                transport->set_lease_deadline_nanos(
                    std::chrono::duration_cast<
                        std::chrono::nanoseconds>(
                        candidate.now.time_since_epoch() + 1h
                    )
                        .count()
                );
                arena::protocol::Envelope reply;
                reply.set_msg_id(arena::protocol::MSG_LOGIN_RESPONSE);
                auto* body = reply.mutable_login_response();
                body->set_accepted(true);
                body->set_session_token(installed->token);
                body->set_player_id(token.claims.playerId.value());
                body->set_session_epoch(installed->snapshot.epoch);
                body->set_session_id(
                    installed->snapshot.sessionId.value()
                );
                body->set_room_id(room);
                capture(std::move(reply));
            }
        );
        const auto reply = result.reply->login_response();
        if (!tokens->verify(reply.session_token()))
            throw std::runtime_error(
                "real token verification failed"
            );
        return reply;
    }
    service::room::RuntimePolicy workerPolicy() const
    {
        return {
            config.rooms.actor,
            config.rooms.maxPendingPerRoom,
            config.rooms.battleDurationTicks,
            config.rooms.readyTimeoutTicks,
            config.rooms.resultRetention.count() * 1'000'000
        };
    }
    RoomWorkerImage workerImage(
        const service::room::RuntimeImage& image, std::uint64_t cursor
    )
    {
        const auto opaque = service::room::wire::encodeImage(image);
        if (!opaque)
            throw std::runtime_error("worker image encoding failed");
        return {
            image.actor.tick,
            image.actor.checksum,
            cursor,
            *opaque,
            image.logicalCutoff
        };
    }
    RoomIdentity
    createPending(const arena::lobby_meta::PendingRoom& reservation)
    {
        std::optional<service::room::RuntimeAllocation> allocation;
        if (reservation.match_id())
        {
            const auto snapshot = authority->snapshot();
            const auto found = std::find_if(
                snapshot->core.registry.rooms.begin(),
                snapshot->core.registry.rooms.end(),
                [&](const auto& room)
                {
                    return room.roomId.value() ==
                           reservation.room_id();
                }
            );
            if (found == snapshot->core.registry.rooms.end() ||
                !found->managed)
                throw std::runtime_error("missing reserved cohort");
            const auto& a = found->managed->members[0];
            const auto& b = found->managed->members[1];
            allocation = service::room::RuntimeAllocation{
                reservation.match_id(),
                {{{a.playerId, a.sessionId, a.roomEpoch},
                  {b.playerId, b.sessionId, b.roomEpoch}}},
                {a.initialMmr, b.initialMmr}
            };
        }
        auto image = service::room::RoomRuntime::create(
            RoomId{reservation.room_id()},
            reservation.incarnation(),
            workerPolicy(),
            allocation,
            reservation.created_unix_ms()
        );
        if (!image)
            throw std::runtime_error(
                "real worker initialization failed"
            );
        const RoomIdentity id{
            reservation.room_id(),
            reservation.incarnation()
        };
        const auto value = workerImage(*image, 0);
        call(
            [id, value, reservation](auto& work, auto capture)
            {
                work.roomChanges.push_back(
                    RoomCreate{id, "worker-a", value}
                );
                if (reservation.match_id() &&
                    !work.queue->release(
                        reservation.match_id(),
                        work.now,
                        RoomId{id.room}
                    ))
                    throw std::runtime_error(
                        "assignment pin release failed"
                    );
                auto* pending = work.meta.mutable_pending_rooms();
                for (int i = pending->size(); i-- > 0;)
                    if (pending->Get(i).room_id() == id.room)
                        pending->DeleteSubrange(i, 1);
                capture(pong());
            }
        );
        return id;
    }
    RoomIdentity manual(std::uint64_t room = 55)
    {
        auto reserved = call(
            [room](auto& work, auto capture)
            {
                auto* pending = work.meta.add_pending_rooms();
                pending->set_room_id(room);
                pending->set_incarnation(
                    work.meta.incarnation_high_water() + 1
                );
                work.meta.set_incarnation_high_water(
                    pending->incarnation()
                );
                pending->set_created_unix_ms(
                    static_cast<std::uint64_t>(work.wallNow) * 1000
                );
                pending->set_deadline_nanos(
                    work.now.time_since_epoch().count() +
                    10'000'000'000LL
                );
                capture(pong());
            }
        );
        return createPending(
            reserved.committed->meta.pending_rooms(0)
        );
    }
    RoomIdentity match()
    {
        const auto first = login(7), second = login(8);
        call(
            [first, second](auto& work, auto capture)
            {
                work.meta.set_ticket_next(10);
                work.meta.set_ticket_last(1000);
                if (!work.queue->join(
                        PlayerId{7},
                        arena::domain::SessionId{first.session_id()},
                        1,
                        1000,
                        work.now
                    ) ||
                    !work.queue->join(
                        PlayerId{8},
                        arena::domain::SessionId{second.session_id()},
                        1,
                        1000,
                        work.now
                    ))
                    throw std::runtime_error(
                        "matching admission failed"
                    );
                capture(pong());
            }
        );
        milliseconds += 50;
        authority->requestTick();
        if (!until(
                [&]
                {
                    return authority->snapshot()
                               ->meta.pending_rooms_size() == 1;
                }
            ))
            throw std::runtime_error("durable pending room missing");
        if (!authority->snapshot()->core.rooms.actors.empty() ||
            !authority->snapshot()->roomImages.empty() ||
            authority->snapshot()
                    ->core.queue.metrics.assignedPlayers != 2)
            throw std::runtime_error(
                "pending match ran a local kernel or lost its pin"
            );
        return createPending(
            authority->snapshot()->meta.pending_rooms(0)
        );
    }
    service::room::RuntimeCommand workerCommand(
        RoomIdentity id,
        service::room::RuntimeCommandKind kind,
        std::uint64_t player
    )
    {
        service::room::RuntimeCommand command;
        command.kind = kind;
        command.incarnation = id.incarnation;
        const auto snapshot = authority->snapshot();
        for (const auto& entry : snapshot->core.registry.entries)
            if (entry.snapshot.playerId.value() == player)
                command.caller = service::room::RuntimeAuthorization{
                    {entry.snapshot.playerId,
                     entry.snapshot.sessionId,
                     entry.snapshot.roomEpoch},
                    entry.snapshot.epoch,
                    entry.snapshot.connectionId,
                    entry.snapshot.presence == Presence::Online
                };
        return command;
    }
    Authority::CommitReply workerStep(
        RoomIdentity id,
        std::vector<service::room::RuntimeCommand> commands = {},
        bool retire = false,
        bool blockSettlement = false
    )
    {
        const auto base = authority->snapshot();
        const auto* route = base->directory->find(id);
        const auto image = base->roomImages.at(id);
        service::room::RuntimeCutoff cutoff;
        milliseconds += 50;
        cutoff.nowNanos = milliseconds.load() * 1'000'000;
        cutoff.wallMillis = static_cast<std::uint64_t>(wall.load()) *
                            1000;
        for (const auto& entry : base->core.registry.entries)
        {
            const auto& v = entry.snapshot;
            service::room::RuntimeAuthorization auth{
                {v.playerId, v.sessionId, v.roomEpoch},
                v.epoch,
                v.connectionId,
                v.presence == Presence::Online
            };
            cutoff.currentAuthorizations.push_back(auth);
            if (v.roomId && v.roomId->value() == id.room)
                cutoff.roster.push_back(auth);
        }
        std::vector<RoomDirectoryCommand> accept;
        for (std::size_t i = 0; i != commands.size(); ++i)
        {
            auto bytes = service::room::wire::encodeCommand(
                commands[i]
            );
            if (!bytes)
                throw std::runtime_error(
                    "worker command encoding failed"
                );
            accept.push_back(
                RoomAccept{
                    id,
                    route->ownerEpoch,
                    {"op/" +
                         std::to_string(image->logicalCutoff + 1) +
                         "/" + std::to_string(i),
                     commands[i].kind ==
                             service::room::RuntimeCommandKind::Ready
                         ? RoomIngressKind::Ready
                     : commands[i].kind ==
                             service::room::RuntimeCommandKind::
                                 SettlementAck
                         ? RoomIngressKind::Settlement
                         : RoomIngressKind::Input,
                     0,
                     std::move(*bytes)}
                }
            );
        }
        auto context = service::room::wire::encodeCutoff(cutoff);
        if (!context)
            throw std::runtime_error("worker cutoff encoding failed");
        accept.push_back(
            RoomAccept{
                id,
                route->ownerEpoch,
                {"tick/" + std::to_string(image->logicalCutoff + 1),
                 RoomIngressKind::Tick,
                 image->logicalCutoff + 1,
                 std::move(*context)}
            }
        );
        call(
            [accept](auto& work, auto capture)
            {
                work.roomChanges.insert(
                    work.roomChanges.end(),
                    accept.begin(),
                    accept.end()
                );
                capture(pong());
            }
        );
        if (authority->snapshot()->roomImages.at(id) != image)
            throw std::runtime_error(
                "ingress replaced an unchanged immutable view"
            );
        cutoff.commands = std::move(commands);
        auto proposal = service::room::RoomRuntime::step(
            *image,
            cutoff
        );
        if (!proposal)
            throw std::runtime_error("real worker cutoff failed");
        const auto cursor = route->nextSequence + accept.size() - 1;
        const auto value = workerImage(proposal->image, cursor);
        std::vector<RoomAppliedReply> replies;
        for (std::size_t i = 0; i != accept.size(); ++i)
            replies.push_back(
                {route->nextSequence + i,
                 i + 1 == accept.size()
                     ? std::string{}
                     : pong(proposal->image.actor.checksum)
                           .SerializeAsString()}
            );
        std::optional<RoomReplayExtent> replay;
        if (proposal->replay)
        {
            auto bytes = service::room::wire::encodeReplayLog(
                {image->policy.actor,
                 image->actor,
                 {*proposal->replay}}
            );
            if (!bytes)
                throw std::runtime_error(
                    "worker replay encoding failed"
                );
            replay = RoomReplayExtent{
                proposal->replay->tick,
                proposal->replay->tick,
                std::move(*bytes)
            };
        }
        const auto release = proposal->releaseMembership;
        const auto matchId = proposal->image.managed
                                 ? proposal->image.managed->allocation
                                       .matchId
                                 : 0;
        return call(
            [id,
             owner = route->owner,
             epoch = route->ownerEpoch,
             value,
             replies,
             replay,
             release,
             retire,
             blockSettlement,
             matchId](auto& work, auto capture)
            {
                work.roomChanges.push_back(
                    RoomApply{
                        id,
                        owner,
                        epoch,
                        value,
                        replies,
                        replay
                    }
                );
                if (release && !work.online->finishMatch(
                                   RoomId{id.room},
                                   matchId
                               ))
                    throw std::runtime_error(
                        "membership release failed"
                    );
                if (retire)
                {
                    const auto registry = work.online->checkpoint();
                    for (const auto& entry : registry.entries)
                        if (entry.snapshot.roomId &&
                            entry.snapshot.roomId->value() == id.room)
                            work.online->leaveRoom(entry.binding);
                    work.roomChanges.push_back(
                        RoomRetire{
                            id,
                            owner,
                            epoch,
                            value.cursor,
                            roomImageFingerprint(id, value)
                        }
                    );
                }
                if (blockSettlement)
                    work.meta.mutable_settlements(0)->set_phase(
                        arena::lobby_meta::SettlementDelivery::BLOCKED
                    );
                capture(pong(value.checksum));
            }
        );
    }
    void ready(RoomIdentity id)
    {
        auto a = workerCommand(
            id,
            service::room::RuntimeCommandKind::Ready,
            7
        );
        auto b = workerCommand(
            id,
            service::room::RuntimeCommandKind::Ready,
            8
        );
        a.ready = b.ready = true;
        workerStep(id, {a, b});
        workerStep(id);
    }
    void settlementIntent(RoomIdentity id)
    {
        const auto image = authority->snapshot()->roomImages.at(id);
        const auto& match = *image->managed;
        arena::storage::SaveResultRequest request;
        auto* record = request.mutable_match();
        record->set_match_id(match.allocation.matchId);
        record->set_room_id(id.room);
        record->set_result_version(1);
        record->set_state("completed");
        record->set_finish_reason("completed");
        record->set_final_tick(match.result->finalTick);
        record->set_final_checksum(match.result->finalChecksum);
        for (std::size_t i = 0; i != 2; ++i)
        {
            auto* member = record->add_participants();
            member->set_player_id(
                match.allocation.members[i].playerId.value()
            );
            member->set_session_id(
                match.allocation.members[i].sessionId.value()
            );
            member->set_membership_epoch(
                match.allocation.members[i].epoch
            );
            member->set_initial_mmr(match.allocation.initialMmr[i]);
            member->set_slot(i + 1);
        }
        call(
            [request](auto& work, auto capture)
            {
                const auto now = work.now.time_since_epoch().count();
                auto* delivery = work.meta.add_settlements();
                delivery->set_match_id(request.match().match_id());
                delivery->set_stable_idempotency_key(
                    "settlement/" +
                    std::to_string(request.match().match_id())
                );
                delivery->set_max_attempts(8);
                delivery->set_first_attempt_nanos(now);
                delivery->set_deadline_nanos(now + 1'000'000'000);
                delivery->set_next_attempt_nanos(now);
                delivery->set_retry_base_ms(10);
                delivery->set_retry_max_ms(100);
                delivery->set_request(request.SerializeAsString());
                capture(pong());
            }
        );
    }
    SessionBinding binding(std::uint64_t player)
    {
        for (const auto& entry :
             authority->snapshot()->core.registry.entries)
            if (entry.snapshot.playerId.value() == player)
                return entry.binding;
        throw std::runtime_error("missing fixture binding");
    }
};

TEST_F(
    LobbyAuthorityTest,
    SignedLoginRoomAndHighWatermarksSurviveSameBootReopen
)
{
    open();
    auto loginReply = login(7, 55);
    const auto saved = authority->snapshot();
    const auto encoded = encodeCoreCheckpoint(saved->core).value();
    close();
    open();
    EXPECT_EQ(authority->snapshot()->revision, saved->revision);
    EXPECT_EQ(
        encodeCoreCheckpoint(authority->snapshot()->core).value(),
        encoded
    );
    EXPECT_EQ(
        authority->snapshot()->meta.SerializeAsString(),
        saved->meta.SerializeAsString()
    );
    EXPECT_EQ(
        authority->snapshot()
            ->core.registry.entries.front()
            .snapshot.roomId,
        RoomId{55}
    );
    auto claims = tokens->verify(loginReply.session_token());
    ASSERT_TRUE(claims);
    EXPECT_EQ(
        claims->epoch,
        saved->core.registry.entries.front().snapshot.epoch
    );
}

TEST_F(
    LobbyAuthorityTest,
    PhysicalSyncHoldsPublicationAndCancellationAfterAdmissionStillCommits
)
{
    config.maxPending = 1;
    open();
    login(7);
    const auto current = binding(7);
    auto reserved = call(
        [](auto& work, auto capture)
        {
            auto* pending = work.meta.add_pending_rooms();
            pending->set_room_id(55);
            pending->set_incarnation(1);
            pending->set_created_unix_ms(
                static_cast<std::uint64_t>(work.wallNow) * 1000
            );
            pending->set_deadline_nanos(10'000'000'000LL);
            work.meta.set_incarnation_high_water(1);
            capture(pong());
        }
    );
    const auto created = service::room::RoomRuntime::create(
        RoomId{55},
        1,
        workerPolicy()
    );
    ASSERT_TRUE(created);
    const auto value = workerImage(*created, 0);
    const RoomIdentity id{55, 1};
    const auto old = authority->snapshot();
    ASSERT_TRUE(old->roomImages.empty());
    {
        std::lock_guard lock(syncMutex);
        releaseSync = false;
        syncHeld = false;
        holdNextSync = true;
    }
    auto pending = submit(
        [current, id, value](auto& candidate, auto capture)
        {
            candidate.roomChanges.push_back(
                RoomCreate{id, "worker-a", value}
            );
            candidate.meta.clear_pending_rooms();
            if (!candidate.online->joinRoom(current, RoomId{55}))
                throw std::runtime_error("join failed");
            capture(pong());
        }
    );
    ASSERT_TRUE(until([] { return syncHeld.load(); }));
    EXPECT_FALSE(pending.result->has_value());
    EXPECT_EQ(authority->snapshot(), old);
    EXPECT_TRUE(authority->stats().writeInFlight);
    auto forbidden = authority->submit(
        [](auto&, auto capture) { capture(pong()); },
        Authority::Clock::now() + 1s,
        [](Result)
        { ADD_FAILURE() << "rejected operation called back"; }
    );
    ASSERT_FALSE(forbidden);
    EXPECT_EQ(forbidden.error(), Authority::Error::Capacity);
    pending.cancel();
    authority->stop();
    EXPECT_FALSE(authority->stats().stopped);
    EXPECT_EQ(authority->snapshot(), old);
    unblockSync();
    auto result = await(pending);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->committed->revision, old->revision + 1);
    EXPECT_TRUE(old->roomImages.empty());
    EXPECT_EQ(
        result->committed->roomImages.at(id)->actor,
        created->actor
    );
    EXPECT_NE(result->committed->directory, old->directory);
    EXPECT_EQ(
        result->committed->directory->revision(),
        result->committed->revision
    );
    EXPECT_TRUE(result->committed->core.rooms.actors.empty());
    EXPECT_EQ(
        result->committed->core.registry.entries.front()
            .snapshot.roomId,
        RoomId{55}
    );
    ASSERT_TRUE(until([&] { return authority->stats().stopped; }));
    EXPECT_TRUE(authority->stats().journal.closed);
}

TEST_F(
    LobbyAuthorityTest,
    PreAdmissionCancellationAndQueuedStopDoNotMutateCommittedState
)
{
    open();
    const auto old = authority->snapshot();
    auto cancelled = submit(
        [](auto& candidate, auto capture)
        {
            candidate.meta.set_reserved_epoch(
                candidate.meta.reserved_epoch() + 1
            );
            capture(pong());
        }
    );
    cancelled.cancel();
    auto result = await(cancelled);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), Authority::Error::Cancelled);
    EXPECT_EQ(authority->snapshot(), old);
    auto waiting = submit(
        [](auto&, auto capture) { capture(pong()); },
        true
    );
    authority->stop();
    auto stopped = await(waiting);
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error(), Authority::Error::Cancelled);
    EXPECT_EQ(authority->snapshot(), old);
    ASSERT_TRUE(until([&] { return authority->stats().stopped; }));
    EXPECT_EQ(authority->stats().pending, 0u);
}

TEST_F(
    LobbyAuthorityTest,
    JournalCapacityFailureKeepsTheOldSnapshotAndCandidateEffects
)
{
    config.journal.maxRecordBytes = 8192;
    config.journal.maxBytes = 32768;
    open();
    login(7);
    const auto old = authority->snapshot();
    auto pending = submit(
        [](auto& candidate, auto capture)
        {
            const auto entry = candidate.online->checkpoint()
                                   .entries.front();
            arena::protocol::Envelope reply;
            reply.set_msg_id(arena::protocol::MSG_ERROR);
            reply.mutable_error()->set_message(
                std::string(16384, 'x')
            );
            auto* receipt = candidate.meta.add_receipts();
            receipt->set_key("oversized-record");
            receipt->set_method(
                arena::protocol::MSG_SESSION_STATE_REQUEST
            );
            receipt->set_request_fingerprint(std::string(32, 'f'));
            receipt->set_player_id(entry.snapshot.playerId.value());
            receipt->set_session_id(entry.snapshot.sessionId.value());
            receipt->set_epoch(entry.snapshot.epoch);
            receipt->set_expires_nanos(1000000000);
            receipt->set_reply(reply.SerializeAsString());
            capture(pong());
        }
    );
    auto result = await(pending);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), Authority::Error::Capacity);
    EXPECT_EQ(authority->snapshot(), old);
    EXPECT_TRUE(authority->stats().ready);
    EXPECT_EQ(authority->stats().pending, 0u);
}

TEST_F(
    LobbyAuthorityTest,
    UncertainSyncQuarantinesWithoutPublishingAndReopenReconcilesActualBytes
)
{
    open();
    login(7);
    const auto old = authority->snapshot();
    const auto current = binding(7);
    failNextSync = true;
    auto pending = submit(
        [current](auto& candidate, auto capture)
        {
            if (!candidate.online->joinRoom(current, RoomId{55}))
                throw std::runtime_error("join failed");
            capture(pong());
        }
    );
    auto result = await(pending);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), Authority::Error::OutcomeUnknown);
    EXPECT_EQ(authority->snapshot(), old);
    EXPECT_FALSE(authority->stats().ready);
    EXPECT_TRUE(authority->stats().failed);
    ASSERT_TRUE(until([&] { return authority->stats().stopped; }));
    close();
    open();
    EXPECT_EQ(authority->snapshot()->revision, old->revision + 1);
    EXPECT_EQ(
        authority->snapshot()
            ->core.registry.entries.front()
            .snapshot.roomId,
        RoomId{55}
    );
}

TEST_F(
    LobbyAuthorityTest,
    TwoPlayersMatchReadyAndOneHundredWorkerCutoffsRecoverTheSameChecksums
)
{
    open();
    const auto id = match();
    ready(id);
    ASSERT_EQ(
        authority->snapshot()->roomImages.at(id)->managed->phase,
        service::room::RuntimePhase::Playing
    );
    auto reference = service::room::RoomActor::restore(
        authority->snapshot()->roomImages.at(id)->actor,
        config.rooms.actor
    );
    ASSERT_TRUE(reference);
    std::mt19937 random(0xA11CEu);
    for (std::uint64_t sequence = 1; sequence <= 100; ++sequence)
    {
        const auto before = (*reference)->snapshot();
        std::vector<service::room::Member> members;
        for (const auto& player : before.players)
            members.push_back(player.member);
        const auto dx = static_cast<std::int32_t>(random() % 3) - 1;
        const auto dy = static_cast<std::int32_t>(random() % 3) - 1;
        const auto expected = (*reference)
                                  ->step(
                                      members,
                                      {{before.players.front().member,
                                        sequence,
                                        dx,
                                        dy}}
                                  )
                                  .snapshot;
        auto command = workerCommand(
            id,
            service::room::RuntimeCommandKind::Input,
            7
        );
        command.sequence = sequence;
        command.dx = dx;
        command.dy = dy;
        const auto committed = workerStep(id, {command});
        EXPECT_EQ(
            committed.committed->roomImages.at(id)->actor,
            expected
        );
        EXPECT_EQ(committed.reply->request_id(), expected.checksum);
        EXPECT_TRUE(committed.committed->core.rooms.actors.empty());
        EXPECT_TRUE(committed.committed->core.distributedRooms);
        EXPECT_EQ(
            committed.committed->directory->revision(),
            committed.committed->revision
        );
        if (sequence % 10 == 0)
        {
            close();
            open();
            EXPECT_EQ(
                authority->snapshot()->roomImages.at(id)->actor,
                expected
            );
        }
    }
    const auto
        final = authority->snapshot()->roomImages.at(id)->actor;
    close();
    AtomicJournal journal(context.get_executor(), config.journal);
    const auto archives = assembleReplayArchives(
        *journal.snapshot(),
        {},
        config.journal.maxKeys
    );
    ASSERT_TRUE(archives);
    ASSERT_EQ(archives->size(), 1u);
    EXPECT_EQ(archives->front().finalSnapshot, final);
    EXPECT_GE(archives->front().log.ticks.size(), 100u);
    journal.stop();
    ASSERT_TRUE(until([&] { return journal.stats().closed; }));
}

TEST_F(
    LobbyAuthorityTest,
    ZeroAttemptBlockedSettlementKeepsItsOriginalBudgetAcrossReopen
)
{
    config.rooms.battleDurationTicks = 1;
    open();
    const auto id = match();
    ready(id);
    const auto image = authority->snapshot()->roomImages.at(id);
    ASSERT_EQ(
        image->managed->phase,
        service::room::RuntimePhase::Settling
    );
    const auto& match = *image->managed;
    settlementIntent(id);
    const auto original = authority->snapshot()->meta.settlements(0);
    milliseconds += 1000;
    service::room::RuntimeCommand failed;
    failed.kind = service::room::RuntimeCommandKind::SettlementAck;
    failed.matchId = match.allocation.matchId;
    failed.committed = false;
    workerStep(id, {failed}, false, true);
    close();
    open();
    const auto restored = authority->snapshot()->meta.settlements(0);
    EXPECT_EQ(
        restored.phase(),
        arena::lobby_meta::SettlementDelivery::BLOCKED
    );
    EXPECT_EQ(restored.attempts(), 0u);
    EXPECT_EQ(restored.deadline_nanos(), original.deadline_nanos());
    const auto before = authority->snapshot();
    auto reset = submit(
        [](auto& work, auto capture)
        {
            work.meta.mutable_settlements(0)->set_deadline_nanos(
                work.meta.settlements(0).deadline_nanos() + 1
            );
            capture(pong());
        }
    );
    ASSERT_FALSE(await(reset));
    EXPECT_EQ(authority->snapshot(), before);
    auto dropped = submit(
        [](auto& work, auto capture)
        {
            work.meta.clear_settlements();
            capture(pong());
        }
    );
    EXPECT_FALSE(await(dropped));
    EXPECT_EQ(authority->snapshot(), before);
}

TEST_F(
    LobbyAuthorityTest,
    DurableSettlementAckMayPrecedeWorkerFinishAndSurvivesReopen
)
{
    config.rooms.battleDurationTicks = 1;
    open();
    const auto id = match();
    ready(id);
    settlementIntent(id);
    service::room::RuntimeCommand ack;
    ack.kind = service::room::RuntimeCommandKind::SettlementAck;
    ack.matchId = authority->snapshot()
                      ->roomImages.at(id)
                      ->managed->allocation.matchId;
    ack.committed = true;
    const auto bytes = service::room::wire::encodeCommand(ack);
    ASSERT_TRUE(bytes);
    const auto before = authority->snapshot();
    auto forged = submit(
        [](auto& work, auto capture)
        {
            auto* intent = work.meta.mutable_settlements(0);
            intent->set_attempts(1);
            intent->set_phase(
                arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
            );
            capture(pong());
        }
    );
    EXPECT_FALSE(await(forged));
    EXPECT_EQ(authority->snapshot(), before);
    call(
        [id, payload = *bytes](auto& work, auto capture)
        {
            auto* intent = work.meta.mutable_settlements(0);
            intent->set_attempts(1);
            intent->set_phase(
                arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
            );
            intent->set_retention_deadline_nanos(
                work.now.time_since_epoch().count() + 30'000'000'000LL
            );
            work.roomChanges.push_back(
                RoomAccept{
                    id,
                    1,
                    {"ack", RoomIngressKind::Settlement, 0, payload}
                }
            );
            capture(pong());
        }
    );
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->managed->phase,
        service::room::RuntimePhase::Settling
    );
    close();
    open();
    EXPECT_EQ(
        authority->snapshot()->meta.settlements(0).phase(),
        arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED
    );
    call([](auto&, auto capture) { capture(pong()); }, true);
    const auto snapshot = authority->snapshot();
    const auto* route = snapshot->directory->find(id);
    ASSERT_EQ(route->inbox.size(), 2u);
    auto cutoff = service::room::wire::decodeCutoff(
        route->inbox.back().operation.payload
    );
    ASSERT_TRUE(cutoff);
    cutoff->commands.push_back(ack);
    const auto proposal = service::room::RoomRuntime::step(
        *snapshot->roomImages.at(id),
        *cutoff
    );
    ASSERT_TRUE(proposal);
    ASSERT_TRUE(proposal->releaseMembership);
    ASSERT_FALSE(proposal->replay);
    const auto value = workerImage(
        proposal->image,
        route->inbox.back().sequence
    );
    const std::vector<RoomAppliedReply> replies{
        {route->inbox[0].sequence, {}},
        {route->inbox[1].sequence, {}}
    };
    call(
        [id,
         value,
         replies,
         match = ack.matchId](auto& work, auto capture)
        {
            work.roomChanges.push_back(
                RoomApply{id, "worker-a", 1, value, replies, {}}
            );
            if (!work.online->finishMatch(RoomId{id.room}, match))
                throw std::runtime_error("finish failed");
            capture(pong());
        }
    );
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->managed->phase,
        service::room::RuntimePhase::Finished
    );
    EXPECT_TRUE(authority->snapshot()->core.registry.rooms.empty());
    close();
    open();
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->managed->phase,
        service::room::RuntimePhase::Finished
    );
}

TEST_F(
    LobbyAuthorityTest,
    FinalApplyAndRetireMoveKnownCountersExactlyOnce
)
{
    open();
    login(7, 55);
    const auto id = manual();
    auto input = workerCommand(
        id,
        service::room::RuntimeCommandKind::Input,
        7
    );
    input.sequence = 1;
    input.dx = 1;
    workerStep(id, {input}, true);
    const auto snapshot = authority->snapshot();
    EXPECT_TRUE(snapshot->roomImages.empty());
    EXPECT_EQ(snapshot->core.rooms.unattributedCounters.ticks, 1u);
    EXPECT_EQ(snapshot->core.rooms.unattributedCounters.applied, 1u);
    const auto route = *snapshot->directory->find(id);
    ASSERT_EQ(route.lifecycle, RoomRouteLifecycle::Retired);
    call(
        [id, route](auto& work, auto capture)
        {
            work.roomChanges.push_back(
                RoomRetire{
                    id,
                    route.owner,
                    route.ownerEpoch,
                    route.image.cursor,
                    roomImageFingerprint(id, route.image)
                }
            );
            capture(pong());
        }
    );
    EXPECT_EQ(
        authority->snapshot()->core.rooms.unattributedCounters.ticks,
        1u
    );
    EXPECT_EQ(
        authority->snapshot()
            ->core.rooms.unattributedCounters.applied,
        1u
    );
    close();
    open();
    EXPECT_EQ(
        authority->snapshot()
            ->core.rooms.unattributedCounters.applied,
        1u
    );
    EXPECT_TRUE(authority->snapshot()->roomImages.empty());
}

TEST_F(
    LobbyAuthorityTest,
    PendingRoomReservationCannotRenewOrPublishWithoutItsDurableIdentity
)
{
    open();
    login(7);
    call(
        [](auto& work, auto capture)
        {
            auto* p = work.meta.add_pending_rooms();
            p->set_room_id(55);
            p->set_incarnation(1);
            p->set_created_unix_ms(1700000000000);
            p->set_deadline_nanos(10'000'000'000LL);
            work.meta.set_incarnation_high_water(1);
            capture(pong());
        }
    );
    const auto before = authority->snapshot();
    auto renewed = submit(
        [](auto& work, auto capture)
        {
            work.meta.mutable_pending_rooms(0)->set_deadline_nanos(
                11'000'000'000LL
            );
            capture(pong());
        }
    );
    EXPECT_FALSE(await(renewed));
    EXPECT_EQ(authority->snapshot(), before);
    const RoomIdentity id{55, 1};
    const auto created = service::room::RoomRuntime::create(
        RoomId{55},
        1,
        workerPolicy()
    );
    ASSERT_TRUE(created);
    const auto value = workerImage(*created, 0);
    call(
        [id, value](auto& work, auto capture)
        {
            work.roomChanges.push_back(
                RoomCreate{id, "worker-a", value}
            );
            capture(pong());
        }
    );
    call([](auto&, auto capture) { capture(pong()); }, true);
    EXPECT_TRUE(
        authority->snapshot()->directory->find(id)->inbox.empty()
    );
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->logicalCutoff,
        0u
    );
    EXPECT_EQ(authority->snapshot()->meta.pending_rooms_size(), 1);
    const auto current = binding(7);
    call(
        [current](auto& work, auto capture)
        {
            if (!work.online->joinRoom(current, RoomId{55}))
                throw std::runtime_error("final join failed");
            work.meta.clear_pending_rooms();
            capture(pong());
        }
    );
    call([](auto&, auto capture) { capture(pong()); }, true);
    ASSERT_EQ(
        authority->snapshot()->directory->find(id)->inbox.size(),
        1u
    );
    EXPECT_EQ(
        authority->snapshot()
            ->directory->find(id)
            ->inbox.front()
            .operation.kind,
        RoomIngressKind::Tick
    );
    EXPECT_TRUE(authority->snapshot()->meta.pending_rooms().empty());
    EXPECT_TRUE(authority->snapshot()->core.rooms.actors.empty());
}

TEST_F(
    LobbyAuthorityTest,
    PureValueCutoffsAreBoundedFairAndDoNotDuplicatePendingTicks
)
{
    config.directoryLimits.maxCommands = 2;
    open();
    const std::array ids{manual(55), manual(56), manual(57)};
    const auto tick = [&]
    {
        const auto revision = authority->snapshot()->revision;
        milliseconds += 50;
        authority->requestTick();
        ASSERT_TRUE(until(
            [&] { return authority->snapshot()->revision > revision; }
        ));
    };
    tick();
    std::size_t admitted = 0;
    for (const auto id : ids)
        admitted += !authority->snapshot()
                         ->directory->find(id)
                         ->inbox.empty();
    EXPECT_EQ(admitted, 2u);
    tick();
    for (const auto id : ids)
    {
        const auto* route = authority->snapshot()->directory->find(
            id
        );
        ASSERT_EQ(route->inbox.size(), 1u);
        EXPECT_EQ(
            route->inbox[0].operation.kind,
            RoomIngressKind::Tick
        );
        EXPECT_EQ(route->inbox[0].operation.tick, 1u);
        EXPECT_EQ(
            authority->snapshot()->roomImages.at(id)->logicalCutoff,
            0u
        );
        EXPECT_EQ(
            authority->snapshot()->roomImages.at(id)->actor.tick,
            0u
        );
    }
    tick();
    for (const auto id : ids)
        EXPECT_EQ(
            authority->snapshot()->directory->find(id)->inbox.size(),
            1u
        );
}

TEST_F(
    LobbyAuthorityTest,
    CutoffSamplesCurrentAuthorizationAfterMembershipChanges
)
{
    open();
    login(7, 55);
    const auto id = manual();
    auto command = workerCommand(
        id,
        service::room::RuntimeCommandKind::Snapshot,
        7
    );
    const auto encoded = service::room::wire::encodeCommand(command);
    ASSERT_TRUE(encoded);
    const auto current = binding(7);
    call(
        [id, current, bytes = *encoded](auto& work, auto capture)
        {
            work.roomChanges.push_back(
                RoomAccept{
                    id,
                    1,
                    {"query", RoomIngressKind::Input, 0, bytes}
                }
            );
            if (!work.online->leaveRoom(current))
                throw std::runtime_error("leave failed");
            capture(pong());
        },
        true
    );
    const auto snapshot = authority->snapshot();
    const auto* route = snapshot->directory->find(id);
    ASSERT_EQ(route->inbox.size(), 2u);
    EXPECT_EQ(
        route->inbox.back().operation.kind,
        RoomIngressKind::Tick
    );
    auto cutoff = service::room::wire::decodeCutoff(
        route->inbox.back().operation.payload
    );
    ASSERT_TRUE(cutoff);
    EXPECT_TRUE(cutoff->roster.empty());
    ASSERT_EQ(cutoff->currentAuthorizations.size(), 1u);
    EXPECT_EQ(
        cutoff->currentAuthorizations[0].member.playerId,
        PlayerId{7}
    );
    EXPECT_EQ(
        cutoff->currentAuthorizations[0].connectionHandle,
        current.connectionId
    );
    cutoff->commands.push_back(command);
    const auto proposal = service::room::RoomRuntime::step(
        *snapshot->roomImages.at(id),
        *cutoff
    );
    ASSERT_TRUE(proposal);
    ASSERT_EQ(proposal->results.size(), 1u);
    EXPECT_EQ(
        proposal->results[0].error,
        service::room::RuntimeError::BusinessFailure
    );
    EXPECT_EQ(snapshot->roomImages.at(id)->logicalCutoff, 0u);
}

TEST_F(
    LobbyAuthorityTest,
    LegacyV1IsAdoptedAsExactValuesWithoutLobbyKernelExecution
)
{
    open();
    login(7, 55);
    const auto saved = authority->snapshot();
    const auto current = binding(7);
    auto online = OnlineRegistry::restore(
        saved->core.registry,
        config.registry,
        [this] { return wall.load(); },
        [](SessionBinding) { return [] {}; }
    );
    ASSERT_TRUE(online);
    auto oldConfig = config.rooms;
    oldConfig.externalTicks = oldConfig.externalSettlement = true;
    oldConfig.incarnationSource = [] { return 1u; };
    oldConfig.now = [this]
    {
        return Authority::Clock::time_point{} +
               std::chrono::milliseconds(milliseconds.load());
    };
    oldConfig.wallNow = [this]
    {
        return std::chrono::system_clock::time_point{
            std::chrono::seconds(wall.load())
        };
    };
    RoomService legacy(*online, oldConfig);
    legacy.start({});
    ASSERT_TRUE(legacy.snapshot(
        current,
        saved->core.registry.entries.front().snapshot.sessionId,
        RoomId{55},
        [](auto) {}
    ));
    ASSERT_TRUE(legacy.advance());
    const auto checkpoint = legacy.checkpoint();
    const auto history = legacy.replayLog(RoomId{55});
    ASSERT_TRUE(checkpoint);
    ASSERT_TRUE(history);
    const auto imported = splitLegacyRooms(*checkpoint);
    ASSERT_TRUE(imported);
    ASSERT_EQ(imported->images.size(), 1u);
    // Only this fixture executes the old kernel. The online owner
    // sees the v1 bytes below.
    legacy.stop();
    online->reset();
    auto core = saved->core;
    core.rooms = *checkpoint;
    core.distributedRooms = false;
    auto coreBytes = encodeCoreCheckpoint(core);
    ASSERT_TRUE(coreBytes);
    arena::lobby_state::CoreCheckpoint v1;
    ASSERT_TRUE(v1.ParseFromString(*coreBytes));
    v1.set_version(1);
    v1.clear_distributed_rooms();
    auto meta = saved->meta;
    meta.set_incarnation_high_water(1);
    auto replay = encodeReplayLog(*history);
    ASSERT_TRUE(replay);
    close();
    {
        AtomicJournal journal(context.get_executor(), config.journal);
        std::optional<AtomicJournal::Result> result;
        ASSERT_TRUE(journal.commit(
            journal.snapshot()->revision,
            {{"authority/core", v1.SerializeAsString()},
             {"authority/meta", meta.SerializeAsString()},
             {"replay/55/1/1", *replay}},
            [&](auto value) { result = value; }
        ));
        ASSERT_TRUE(until([&] { return result.has_value(); }));
        ASSERT_TRUE(*result);
        journal.stop();
        ASSERT_TRUE(until([&] { return journal.stats().closed; }));
    }
    open();
    ASSERT_EQ(authority->snapshot()->core.rooms.actors.size(), 1u);
    EXPECT_TRUE(authority->snapshot()->roomImages.empty());
    const auto staged = imported->images.front();
    ASSERT_TRUE(service::room::RoomRuntime::validate(staged));
    const RoomIdentity id{55, 1};
    const auto value = workerImage(staged, 0);
    call(
        [id, value](auto& work, auto capture)
        {
            work.roomChanges.push_back(
                RoomCreate{id, "worker-a", value}
            );
            work.legacyRooms.actors.clear();
            work.legacyRooms.matches.clear();
            work.legacyRooms.roomCounters.clear();
            capture(pong());
        }
    );
    EXPECT_TRUE(authority->snapshot()->core.distributedRooms);
    EXPECT_TRUE(authority->snapshot()->core.rooms.actors.empty());
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->actor,
        staged.actor
    );
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->counters,
        staged.counters
    );
    close();
    open();
    EXPECT_EQ(
        authority->snapshot()->roomImages.at(id)->actor,
        staged.actor
    );
    auto input = workerCommand(
        id,
        service::room::RuntimeCommandKind::Input,
        7
    );
    input.sequence = 1;
    input.dx = 1;
    workerStep(id, {input});
    const auto
        final = authority->snapshot()->roomImages.at(id)->actor;
    close();
    AtomicJournal journal(context.get_executor(), config.journal);
    const auto archives = assembleReplayArchives(
        *journal.snapshot(),
        {},
        config.journal.maxKeys
    );
    ASSERT_TRUE(archives);
    ASSERT_EQ(archives->size(), 1u);
    EXPECT_EQ(archives->front().finalSnapshot, final);
    EXPECT_EQ(archives->front().log.ticks.size(), 2u);
    journal.stop();
    ASSERT_TRUE(until([&] { return journal.stats().closed; }));
}

TEST_F(
    LobbyAuthorityTest,
    LeaseAndReceiptExpiryAreExclusiveAndGraceIsNotResetByReopen
)
{
    open();
    const auto reply = login(7, 55);
    const auto current = binding(7);
    call(
        [reply](auto& candidate, auto capture)
        {
            candidate.meta.mutable_transports(0)
                ->set_lease_deadline_nanos(200000000);
            auto* receipt = candidate.meta.add_receipts();
            receipt->set_key("expiring-operation");
            receipt->set_method(arena::protocol::MSG_LOGIN_REQUEST);
            receipt->set_request_fingerprint(std::string(32, 'f'));
            receipt->set_player_id(reply.player_id());
            receipt->set_session_id(reply.session_id());
            receipt->set_epoch(reply.session_epoch());
            receipt->set_expires_nanos(100000000);
            receipt->set_reply(pong().SerializeAsString());
            capture(pong());
        }
    );
    const auto tick = [&](std::int64_t time)
    {
        const auto revision = authority->snapshot()->revision;
        milliseconds = time;
        authority->requestTick();
        EXPECT_TRUE(until(
            [&] { return authority->snapshot()->revision > revision; }
        ));
    };
    tick(100);
    EXPECT_EQ(authority->snapshot()->meta.receipts_size(), 0);
    EXPECT_EQ(
        authority->snapshot()
            ->core.registry.entries.front()
            .snapshot.presence,
        Presence::Online
    );
    tick(200);
    ASSERT_EQ(
        authority->snapshot()->core.registry.entries.size(),
        1u
    );
    EXPECT_EQ(authority->snapshot()->meta.transports_size(), 0);
    const auto deadline = authority->snapshot()
                              ->core.registry.entries.front()
                              .snapshot.graceDeadline;
    EXPECT_EQ(deadline, Authority::Clock::time_point{} + 300ms);
    EXPECT_EQ(
        authority->snapshot()
            ->core.registry.entries.front()
            .binding.epoch,
        current.epoch
    );
    close();
    open();
    EXPECT_EQ(
        authority->snapshot()
            ->core.registry.entries.front()
            .snapshot.graceDeadline,
        deadline
    );
    tick(300);
    EXPECT_TRUE(authority->snapshot()->core.registry.entries.empty());
    EXPECT_TRUE(authority->snapshot()->core.registry.rooms.empty());
}

TEST_F(
    LobbyAuthorityTest, BootMismatchFailsClosedAndPreservesTheJournal
)
{
    open();
    login(7, 55);
    close();
    const auto before = bytes(directory / "authority.wal");
    config.bootId = "different-linux-boot";
    authority = std::make_unique<Authority>(
        context.get_executor(),
        config
    );
    ASSERT_TRUE(until(
        [&]
        {
            return authority->stats().initialized &&
                   authority->stats().stopped;
        }
    ));
    EXPECT_TRUE(authority->stats().failed);
    EXPECT_FALSE(authority->stats().ready);
    EXPECT_FALSE(authority->snapshot());
    EXPECT_EQ(bytes(directory / "authority.wal"), before);
}

TEST_F(
    LobbyAuthorityTest,
    CorruptCommittedFrameCannotBecomeAFreshAuthority
)
{
    open();
    login(7);
    close();
    auto corrupted = bytes(directory / "authority.wal");
    ASSERT_FALSE(corrupted.empty());
    corrupted.back() ^= 0x5a;
    {
        std::ofstream file(
            directory / "authority.wal",
            std::ios::binary | std::ios::trunc
        );
        file.write(
            corrupted.data(),
            static_cast<std::streamsize>(corrupted.size())
        );
    }
    authority = std::make_unique<Authority>(
        context.get_executor(),
        config
    );
    ASSERT_TRUE(until(
        [&]
        {
            return authority->stats().initialized &&
                   authority->stats().stopped;
        }
    ));
    EXPECT_TRUE(authority->stats().failed);
    EXPECT_FALSE(authority->stats().ready);
    EXPECT_FALSE(authority->snapshot());
    EXPECT_EQ(bytes(directory / "authority.wal"), corrupted);
}

TEST_F(
    LobbyAuthorityTest,
    InvalidCandidatesAndDuplicateTransportsCannotPublish
)
{
    open();
    login(7);
    login(8);
    const auto old = authority->snapshot();
    auto noReply = submit(
        [](auto& candidate, auto)
        {
            candidate.meta.set_reserved_epoch(
                candidate.meta.reserved_epoch() + 1
            );
        }
    );
    auto missing = await(noReply);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), Authority::Error::InvalidState);
    EXPECT_EQ(authority->snapshot(), old);
    auto duplicate = submit(
        [](auto& candidate, auto capture)
        {
            const auto copy = candidate.meta.transports(0);
            *candidate.meta.add_transports() = copy;
            capture(pong());
        }
    );
    auto invalid = await(duplicate);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), Authority::Error::InvalidState);
    EXPECT_EQ(authority->snapshot(), old);
    auto reset = submit(
        [](auto& candidate, auto capture)
        {
            candidate.meta.set_reserved_epoch(
                candidate.meta.reserved_epoch() - 1
            );
            capture(pong());
        }
    );
    EXPECT_FALSE(await(reset));
    EXPECT_EQ(authority->snapshot(), old);
    auto orphan = submit(
        [](auto& candidate, auto capture)
        {
            const auto a = candidate.online->snapshot(PlayerId{7});
            const auto b = candidate.online->snapshot(PlayerId{8});
            if (!a || !b ||
                !candidate.online->allocateMatch(
                    77,
                    {{{a->playerId, a->sessionId, 1500},
                      {b->playerId, b->sessionId, 1500}}}
                ))
                throw std::runtime_error("allocation fixture failed");
            capture(pong());
        }
    );
    EXPECT_FALSE(await(orphan));
    EXPECT_EQ(authority->snapshot(), old);
}

TEST_F(
    LobbyAuthorityTest,
    CallbackDeliveryRetainsItsSlotAndExceptionsDoNotLeakAdmission
)
{
    config.maxPending = 1;
    open();
    auto complete = std::make_shared<bool>(false);
    auto admitted = authority->submit(
        [](auto&, auto capture) { capture(pong()); },
        Authority::Clock::now() + 1s,
        [this, complete](Result result)
        {
            EXPECT_TRUE(result);
            auto nested = authority->submit(
                [](auto&, auto capture) { capture(pong()); },
                Authority::Clock::now() + 1s,
                [](Result)
                { ADD_FAILURE() << "rejected callback ran"; }
            );
            EXPECT_FALSE(nested);
            EXPECT_EQ(nested.error(), Authority::Error::Capacity);
            *complete = true;
            throw std::runtime_error(
                "injected caller callback exception"
            );
        }
    );
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(until(
        [&] { return *complete && authority->stats().pending == 0; }
    ));
    EXPECT_EQ(authority->stats().callbackFailures, 1u);
    EXPECT_TRUE(
        call([](auto&, auto capture) { capture(pong()); }).reply
    );
}

TEST_F(
    LobbyAuthorityTest, StopBeforeBootstrapNeverStartsAPhysicalWriter
)
{
    authority = std::make_unique<Authority>(
        context.get_executor(),
        config
    );
    authority->stop();
    ASSERT_TRUE(until([&] { return authority->stats().stopped; }));
    EXPECT_FALSE(authority->snapshot());
    EXPECT_EQ(authority->stats().pending, 0u);
    EXPECT_FALSE(
        std::filesystem::exists(directory / "authority.wal")
    );
}
} // namespace
