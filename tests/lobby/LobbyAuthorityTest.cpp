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
    const auto old = authority->snapshot();
    const auto current = binding(7);
    {
        std::lock_guard lock(syncMutex);
        releaseSync = false;
        syncHeld = false;
        holdNextSync = true;
    }
    auto pending = submit(
        [current](auto& candidate, auto capture)
        {
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
    TwoPlayersMatchReadyAndOneHundredRandomCutoffsRecoverTheSameChecksums
)
{
    open();
    const auto alice = login(7), bob = login(8);
    call(
        [alice, bob](auto& candidate, auto capture)
        {
            candidate.meta.set_ticket_next(10);
            candidate.meta.set_ticket_last(1000);
            if (!candidate.queue->join(
                    PlayerId{7},
                    arena::domain::SessionId{alice.session_id()},
                    1,
                    1000,
                    candidate.now
                ) ||
                !candidate.queue->join(
                    PlayerId{8},
                    arena::domain::SessionId{bob.session_id()},
                    1,
                    1010,
                    candidate.now
                ))
                throw std::runtime_error(
                    "real queue admission failed"
                );
            capture(pong());
        }
    );
    milliseconds = 50;
    authority->requestTick();
    ASSERT_TRUE(until(
        [&]
        { return !authority->snapshot()->core.rooms.matches.empty(); }
    ));
    auto current = authority->snapshot();
    ASSERT_EQ(current->core.rooms.actors.size(), 1u);
    const auto actor = current->core.rooms.actors.front();
    const auto room = actor.roomId;
    const auto firstBinding = binding(7), secondBinding = binding(8);
    call(
        [alice, bob, firstBinding, secondBinding, actor](
            auto& candidate,
            auto capture
        )
        {
            auto pending = std::make_shared<unsigned>(2);
            auto failed = std::make_shared<bool>(false);
            auto complete = [pending, failed, capture](
                                RoomService::StatusResult status
                            )
            {
                *failed = *failed || !status;
                if (--*pending == 0)
                {
                    if (*failed)
                        capture(
                            std::unexpected(
                                arena::protocol::ERR_INTERNAL
                            )
                        );
                    else
                        capture(pong());
                }
            };
            const auto entries = candidate.online->checkpoint()
                                     .entries;
            if (!candidate.rooms->ready(
                    firstBinding,
                    arena::domain::SessionId{alice.session_id()},
                    actor.roomId,
                    actor.incarnation,
                    entries[0].snapshot.roomEpoch,
                    true,
                    complete
                ) ||
                !candidate.rooms->ready(
                    secondBinding,
                    arena::domain::SessionId{bob.session_id()},
                    actor.roomId,
                    actor.incarnation,
                    entries[1].snapshot.roomEpoch,
                    true,
                    complete
                ))
                throw std::runtime_error(
                    "readiness mailbox rejected"
                );
        },
        true
    );
    for (unsigned i = 0;
         i < 4 &&
         authority->snapshot()->core.rooms.matches.front().phase !=
             RoomPhase::Playing;
         ++i)
    {
        const auto revision = authority->snapshot()->revision;
        milliseconds += 50;
        authority->requestTick();
        ASSERT_TRUE(until(
            [&] { return authority->snapshot()->revision > revision; }
        ));
    }
    ASSERT_EQ(
        authority->snapshot()->core.rooms.matches.front().phase,
        RoomPhase::Playing
    );
    auto reference = service::room::RoomActor::restore(
        authority->snapshot()->core.rooms.actors.front(),
        config.rooms.actor
    );
    ASSERT_TRUE(reference);
    std::mt19937 random(0xA11CEu);
    for (std::uint64_t sequence = 1; sequence <= 100; ++sequence)
    {
        const auto dx = static_cast<std::int32_t>(random() % 3) - 1;
        const auto dy = static_cast<std::int32_t>(random() % 3) - 1;
        const auto before = (*reference)->snapshot();
        std::vector<service::room::Member> members;
        for (const auto& player : before.players)
            members.push_back(player.member);
        const auto member = before.players.front().member;
        const auto expected = (*reference)
                                  ->step(
                                      members,
                                      {{member, sequence, dx, dy}}
                                  )
                                  .snapshot;
        auto committed = call(
            [firstBinding,
             alice,
             room,
             actor,
             member,
             sequence,
             dx,
             dy](auto& candidate, auto capture)
            {
                auto metadata = candidate.metadataHandle();
                const auto
                    expiry = std::chrono::duration_cast<
                                 std::chrono::nanoseconds>(
                                 candidate.now.time_since_epoch() + 1h
                    )
                                 .count();
                if (!candidate.rooms->input(
                        firstBinding,
                        arena::domain::SessionId{alice.session_id()},
                        room,
                        actor.incarnation,
                        member.epoch,
                        sequence,
                        dx,
                        dy,
                        [metadata,
                         capture,
                         member,
                         firstBinding,
                         expiry,
                         sequence](RoomService::InputResult result)
                        {
                            if (!result)
                            {
                                capture(
                                    std::unexpected(result.error())
                                );
                                return;
                            }
                            auto reply = pong(
                                result->snapshot.checksum
                            );
                            auto* receipt = metadata->add_receipts();
                            receipt->set_key(
                                "input-" + std::to_string(sequence)
                            );
                            receipt->set_method(
                                arena::protocol::
                                    MSG_ROOM_INPUT_REQUEST
                            );
                            receipt->set_request_fingerprint(
                                std::string(32, 'f')
                            );
                            receipt->set_player_id(
                                member.playerId.value()
                            );
                            receipt->set_session_id(
                                member.sessionId.value()
                            );
                            receipt->set_epoch(firstBinding.epoch);
                            receipt->set_expires_nanos(expiry);
                            receipt->set_reply(
                                reply.SerializeAsString()
                            );
                            capture(std::move(reply));
                        }
                    ))
                    throw std::runtime_error(
                        "input mailbox rejected"
                    );
            },
            true
        );
        ASSERT_TRUE(committed.reply);
        EXPECT_EQ(
            committed.committed->core.rooms.actors.front(),
            expected
        );
        EXPECT_EQ(committed.reply->request_id(), expected.checksum);
        EXPECT_EQ(
            committed.committed->meta.receipts_size(),
            static_cast<int>(sequence)
        );
        if (sequence % 10 == 0)
        {
            const auto revision = committed.committed->revision;
            close();
            open();
            EXPECT_EQ(authority->snapshot()->revision, revision);
            EXPECT_EQ(
                authority->snapshot()->core.rooms.actors.front(),
                expected
            );
            EXPECT_EQ(
                authority->snapshot()->meta.receipts_size(),
                static_cast<int>(sequence)
            );
        }
    }
    const auto final = authority->snapshot()
                           ->core.rooms.actors.front();
    close();
    DurableStateStore store(context.get_executor(), config.journal);
    std::map<std::uint64_t, service::room::ReplayLog> segments;
    for (const auto& [key, value] : store.snapshot()->values)
        if (key.starts_with("replay/"))
        {
            auto decoded = decodeReplayLog(value);
            ASSERT_TRUE(decoded);
            segments.emplace(
                decoded->ticks.front().tick,
                std::move(*decoded)
            );
        }
    std::optional<service::room::Snapshot> replayed;
    for (const auto& [tick, log] : segments)
    {
        (void)tick;
        if (replayed)
        {
            EXPECT_EQ(log.checkpoint, *replayed);
        }
        auto replay = service::room::RoomActor::replay(log);
        ASSERT_TRUE(replay);
        replayed = replay->snapshot;
    }
    ASSERT_TRUE(replayed);
    EXPECT_EQ(*replayed, final);
    EXPECT_GE(segments.size(), 100u);
    store.stop();
    ASSERT_TRUE(until([&] { return store.stats().closed; }));
}

TEST_F(
    LobbyAuthorityTest,
    ZeroAttemptBlockedSettlementKeepsItsOriginalBudgetAcrossReopen
)
{
    config.rooms.battleDurationTicks = 1;
    open();
    const auto alice = login(7), bob = login(8);
    const auto a = binding(7), b = binding(8);
    call(
        [alice, bob](auto& candidate, auto capture)
        {
            candidate.meta.set_ticket_next(1);
            candidate.meta.set_ticket_last(100);
            if (!candidate.queue->join(
                    PlayerId{7},
                    arena::domain::SessionId{alice.session_id()},
                    1,
                    1000,
                    candidate.now
                ) ||
                !candidate.queue->join(
                    PlayerId{8},
                    arena::domain::SessionId{bob.session_id()},
                    1,
                    1000,
                    candidate.now
                ))
                throw std::runtime_error("queue admission failed");
            capture(pong());
        }
    );
    milliseconds = 50;
    authority->requestTick();
    ASSERT_TRUE(until(
        [&]
        { return !authority->snapshot()->core.rooms.matches.empty(); }
    ));
    const auto initial = authority->snapshot();
    const auto actor = initial->core.rooms.actors.front();
    call(
        [a, b, alice, bob, actor](auto& candidate, auto capture)
        {
            const auto entries = candidate.online->checkpoint()
                                     .entries;
            auto remaining = std::make_shared<unsigned>(2);
            auto complete =
                [remaining, capture](RoomService::StatusResult status)
            {
                if (!status)
                    throw std::runtime_error("ready cutoff rejected");
                if (--*remaining == 0)
                    capture(pong());
            };
            if (!candidate.rooms->ready(
                    a,
                    arena::domain::SessionId{alice.session_id()},
                    actor.roomId,
                    actor.incarnation,
                    entries[0].snapshot.roomEpoch,
                    true,
                    complete
                ) ||
                !candidate.rooms->ready(
                    b,
                    arena::domain::SessionId{bob.session_id()},
                    actor.roomId,
                    actor.incarnation,
                    entries[1].snapshot.roomEpoch,
                    true,
                    complete
                ))
                throw std::runtime_error("ready mailbox rejected");
        },
        true
    );
    for (unsigned attempt = 0;
         attempt < 8 &&
         authority->snapshot()->core.rooms.matches.front().phase !=
             RoomPhase::Settling;
         ++attempt)
    {
        const auto revision = authority->snapshot()->revision;
        milliseconds += 50;
        authority->requestTick();
        ASSERT_TRUE(until(
            [&] { return authority->snapshot()->revision > revision; }
        ));
    }
    ASSERT_EQ(
        authority->snapshot()->core.rooms.matches.front().phase,
        RoomPhase::Settling
    );
    const auto match = authority->snapshot()
                           ->core.rooms.matches.front();
    ASSERT_TRUE(match.result);
    arena::storage::SaveResultRequest request;
    auto* record = request.mutable_match();
    record->set_match_id(match.allocation.matchId);
    record->set_room_id(match.allocation.roomId.value());
    record->set_result_version(1);
    record->set_state("completed");
    record->set_finish_reason("completed");
    record->set_final_tick(match.result->finalTick);
    record->set_final_checksum(match.result->finalChecksum);
    for (std::size_t index = 0;
         index < match.allocation.members.size();
         ++index)
    {
        const auto& member = match.allocation.members[index];
        auto* participant = record->add_participants();
        participant->set_player_id(member.playerId.value());
        participant->set_session_id(member.sessionId.value());
        participant->set_membership_epoch(member.roomEpoch);
        participant->set_initial_mmr(member.initialMmr);
        participant->set_slot(static_cast<unsigned>(index + 1));
    }
    call(
        [request](auto& candidate, auto capture)
        {
            const auto first = std::chrono::duration_cast<
                                   std::chrono::nanoseconds>(
                                   candidate.now.time_since_epoch()
            )
                                   .count();
            auto* delivery = candidate.meta.add_settlements();
            delivery->set_match_id(request.match().match_id());
            delivery->set_stable_idempotency_key(
                "settlement/" +
                std::to_string(request.match().match_id())
            );
            delivery->set_max_attempts(8);
            delivery->set_first_attempt_nanos(first);
            delivery->set_deadline_nanos(first + 1000000000);
            delivery->set_next_attempt_nanos(first);
            delivery->set_retry_base_ms(10);
            delivery->set_retry_max_ms(100);
            delivery->set_request(request.SerializeAsString());
            capture(pong());
        }
    );
    const auto original = authority->snapshot()->meta.settlements(0);
    milliseconds += 1000;
    call(
        [match](auto& candidate, auto capture)
        {
            candidate.meta.mutable_settlements(0)->set_phase(
                arena::lobby_meta::SettlementDelivery::BLOCKED
            );
            if (!candidate.rooms->acknowledgeSettlement(
                    match.allocation.matchId,
                    false
                ))
                throw std::runtime_error(
                    "blocked settlement acknowledgement failed"
                );
            capture(pong());
        }
    );
    EXPECT_EQ(
        authority->snapshot()->meta.settlements(0).attempts(),
        0u
    );
    close();
    open();
    const auto restored = authority->snapshot()->meta.settlements(0);
    EXPECT_EQ(
        restored.phase(),
        arena::lobby_meta::SettlementDelivery::BLOCKED
    );
    EXPECT_EQ(restored.attempts(), 0u);
    EXPECT_EQ(
        restored.first_attempt_nanos(),
        original.first_attempt_nanos()
    );
    EXPECT_EQ(restored.deadline_nanos(), original.deadline_nanos());
    const auto saved = authority->snapshot();
    auto reset = submit(
        [](auto& candidate, auto capture)
        {
            candidate.meta.mutable_settlements(0)->set_deadline_nanos(
                candidate.meta.settlements(0).deadline_nanos() + 1
            );
            capture(pong());
        }
    );
    auto resetResult = await(reset);
    ASSERT_FALSE(resetResult);
    EXPECT_EQ(resetResult.error(), Authority::Error::InvalidState);
    EXPECT_EQ(authority->snapshot(), saved);
    auto dropped = submit(
        [](auto& candidate, auto capture)
        {
            candidate.meta.clear_settlements();
            capture(pong());
        }
    );
    EXPECT_FALSE(await(dropped));
    EXPECT_EQ(authority->snapshot(), saved);
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
