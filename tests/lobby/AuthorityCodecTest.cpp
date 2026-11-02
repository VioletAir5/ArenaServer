#include "lobby/AuthorityCodec.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include <google/protobuf/message.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "auth/JwtTokens.h"
#include "lobby_state.pb.h"

namespace
{
using namespace service::lobby;
using namespace std::chrono_literals;
namespace wire = arena::lobby_state;
namespace room = service::room;
namespace match = service::match;
using arena::domain::PlayerId;
using arena::domain::RoomId;
using arena::domain::SessionId;
using Clock = std::chrono::steady_clock;

class AuthorityCodecTest : public testing::Test
{
protected:
    Clock::time_point now{Clock::duration{-250000000}};
    std::int64_t wall{1000};
    std::uint64_t nextTicket{1000}, nextActor{1};
    bool rejectClocks{false};
    std::unique_ptr<service::auth::JwtTokens> tokens;
    std::shared_ptr<OnlineRegistry> registry;
    std::unique_ptr<match::MatchQueue> queue;
    std::unique_ptr<RoomService> rooms;
    std::vector<SessionGrant> grants;
    std::optional<ManagedRoom> allocation;

    OnlineRegistryConfig registryConfig()
    {
        OnlineRegistryConfig config;
        config.grace = 100ms;
        config.now = [this]
        {
            if (rejectClocks)
                throw std::runtime_error("codec read clock");
            return now;
        };
        return config;
    }
    std::function<std::int64_t()> wallClock()
    {
        return [this]
        {
            if (rejectClocks)
                throw std::runtime_error("codec read clock");
            return wall;
        };
    }
    match::QueueConfig queueConfig()
    {
        match::QueueConfig config;
        config.ticketIdSource = [this]
        { return std::optional(nextTicket++); };
        return config;
    }
    RoomServiceConfig roomConfig()
    {
        RoomServiceConfig config;
        config.externalTicks = true;
        config.externalSettlement = true;
        config.battleDurationTicks = 20;
        config.incarnationSource = [this] { return nextActor++; };
        config.now = [this]
        {
            if (rejectClocks)
                throw std::runtime_error("codec read clock");
            return now;
        };
        config.wallNow = [this]
        {
            if (rejectClocks)
                throw std::runtime_error("codec read clock");
            return std::chrono::system_clock::time_point{
                std::chrono::seconds{wall}
            };
        };
        return config;
    }
    void SetUp() override
    {
        service::auth::JwtConfig auth;
        auth.key = "0123456789abcdef0123456789abcdef";
        auth.now = wallClock();
        tokens = std::make_unique<service::auth::JwtTokens>(
            std::move(auth)
        );
        registry = std::make_shared<OnlineRegistry>(registryConfig());
        queue = std::make_unique<match::MatchQueue>(queueConfig());
        rooms = std::make_unique<RoomService>(registry, roomConfig());
        rooms->start({});
        for (std::uint64_t player = 1; player != 9; ++player)
        {
            auto grant = registry->install(
                PlayerId{player},
                100 + player,
                {},
                *tokens,
                [] {}
            );
            ASSERT_TRUE(grant);
            grants.push_back(std::move(*grant));
        }
        auto managed = registry->allocateMatch(
            101,
            {{{PlayerId{1}, grants[0].snapshot.sessionId, 1400},
              {PlayerId{2}, grants[1].snapshot.sessionId, 1600}}}
        );
        ASSERT_TRUE(managed);
        allocation = *managed;
        ASSERT_TRUE(rooms->openMatch(*managed));
        ASSERT_TRUE(
            registry->joinRoom(grants[2].lease->binding, RoomId{99})
        );
        ASSERT_TRUE(
            registry->joinRoom(grants[3].lease->binding, RoomId{99})
        );
        ASSERT_TRUE(rooms->snapshot(
            grants[2].lease->binding,
            grants[2].snapshot.sessionId,
            RoomId{99},
            [](auto) {}
        ));
        auto initial = rooms->replayLog(managed->roomId);
        ASSERT_TRUE(initial);
        for (unsigned i = 0; i != 2; ++i)
            ASSERT_TRUE(rooms->ready(
                grants[i].lease->binding,
                grants[i].snapshot.sessionId,
                managed->roomId,
                initial->checkpoint.incarnation,
                managed->members[i].roomEpoch,
                true,
                [](auto) {}
            ));
        ASSERT_TRUE(rooms->advance());
        ASSERT_TRUE(rooms->advance());
        auto log = rooms->replayLog(managed->roomId);
        ASSERT_TRUE(log);
        ASSERT_TRUE(rooms->input(
            grants[0].lease->binding,
            grants[0].snapshot.sessionId,
            managed->roomId,
            log->checkpoint.incarnation,
            managed->members[0].roomEpoch,
            1,
            1,
            -1,
            [](auto) {}
        ));
        ASSERT_TRUE(rooms->advance());
        grants[3].lease->release();
        ASSERT_TRUE(queue->join(
            PlayerId{5},
            grants[4].snapshot.sessionId,
            501,
            1000,
            now
        ));
        ASSERT_TRUE(queue->join(
            PlayerId{6},
            grants[5].snapshot.sessionId,
            601,
            1000,
            now
        ));
        ASSERT_EQ(queue->advance(now).size(), 1U);
        auto cancelled = queue->join(
            PlayerId{7},
            grants[6].snapshot.sessionId,
            701,
            1200,
            now
        );
        ASSERT_TRUE(cancelled);
        ASSERT_TRUE(queue->cancel(
            PlayerId{7},
            grants[6].snapshot.sessionId,
            cancelled->ticketId,
            now
        ));
        ASSERT_TRUE(queue->join(
            PlayerId{8},
            grants[7].snapshot.sessionId,
            801,
            1500,
            now
        ));
    }
    void TearDown() override
    {
        rejectClocks = false;
        if (rooms)
            rooms->stop();
        rooms.reset();
        queue.reset();
        if (registry)
            registry->shutdown();
        grants.clear();
        registry.reset();
        tokens.reset();
    }
    CoreCheckpoint image()
    {
        auto checkpoint = rooms->checkpoint();
        if (!checkpoint)
            throw std::runtime_error("room cut not quiescent");
        return {
            registry->checkpoint(),
            queue->checkpoint(),
            std::move(*checkpoint)
        };
    }
    wire::CoreCheckpoint wireImage()
    {
        const auto bytes = encodeCoreCheckpoint(image());
        if (!bytes)
            throw std::runtime_error("fixture encode failed");
        wire::CoreCheckpoint result;
        if (!result.ParseFromString(*bytes))
            throw std::runtime_error("fixture parse failed");
        return result;
    }
};

TEST_F(
    AuthorityCodecTest,
    RoundTripsRealAuthorityAndContinuesExistingRestorePaths
)
{
    const auto original = image();
    const auto ids = std::pair{nextTicket, nextActor};
    rejectClocks = true;
    auto bytes = encodeCoreCheckpoint(original);
    ASSERT_TRUE(bytes);
    auto decoded = decodeCoreCheckpoint(*bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->queue, original.queue);
    EXPECT_EQ(encodeCoreCheckpoint(*decoded), bytes);
    EXPECT_EQ((std::pair{nextTicket, nextActor}), ids);
    rejectClocks = false;
    auto online = OnlineRegistry::restore(
        decoded->registry,
        registryConfig(),
        wallClock(),
        [](SessionBinding) { return [] {}; }
    );
    ASSERT_TRUE(online);
    auto restoredQueue = match::MatchQueue::restore(
        decoded->queue,
        queueConfig()
    );
    ASSERT_TRUE(restoredQueue);
    auto restoredRooms = RoomService::restore(
        *online,
        decoded->rooms,
        roomConfig()
    );
    ASSERT_TRUE(restoredRooms);
    const auto roomId = allocation->roomId;
    const auto before = rooms->replayLog(roomId);
    ASSERT_TRUE(before);
    for (auto* service : {rooms.get(), restoredRooms->get()})
        ASSERT_TRUE(service->input(
            grants[0].lease->binding,
            grants[0].snapshot.sessionId,
            roomId,
            before->checkpoint.incarnation,
            allocation->members[0].roomEpoch,
            2,
            -1,
            1,
            [](auto) {}
        ));
    ASSERT_TRUE(rooms->advance());
    ASSERT_TRUE((*restoredRooms)->advance());
    const auto live = rooms->checkpoint();
    const auto recovered = (*restoredRooms)->checkpoint();
    ASSERT_TRUE(live);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(live->actors, recovered->actors);
    EXPECT_EQ(
        encodeCoreCheckpoint(
            CoreCheckpoint{
                (*online)->checkpoint(),
                (*restoredQueue)->checkpoint(),
                *recovered
            }
        ),
        encodeCoreCheckpoint(image())
    );
}

TEST_F(
    AuthorityCodecTest,
    PreservesExpiredDetachedAndHistoricalManagedCohorts
)
{
    auto replacement = registry->install(
        PlayerId{1},
        201,
        {},
        *tokens,
        [] {}
    );
    ASSERT_TRUE(replacement);
    grants[1].lease->release();
    now += 100ms;
    EXPECT_EQ(
        registry->sweep(),
        2U
    ); // Both detached sessions reach their original exclusive
       // cutoff.
    auto bytes = encodeCoreCheckpoint(image());
    ASSERT_TRUE(bytes);
    auto decoded = decodeCoreCheckpoint(*bytes);
    ASSERT_TRUE(decoded);
    auto online = OnlineRegistry::restore(
        decoded->registry,
        registryConfig(),
        wallClock(),
        [](SessionBinding) { return [] {}; }
    );
    ASSERT_TRUE(online);
    auto recovered = RoomService::restore(
        *online,
        decoded->rooms,
        roomConfig()
    );
    ASSERT_TRUE(recovered);
    EXPECT_EQ(
        (*online)->managedRoom(allocation->roomId)->members,
        allocation->members
    );
    ASSERT_TRUE((*recovered)->advance());
    auto terminal = (*recovered)->checkpoint();
    ASSERT_TRUE(terminal);
    ASSERT_EQ(terminal->matches.size(), 1U);
    EXPECT_EQ(terminal->matches[0].phase, RoomPhase::Settling);
    EXPECT_EQ(
        terminal->matches[0].result->reason,
        SettlementReason::MemberUnavailable
    );
}

TEST_F(
    AuthorityCodecTest,
    RetainsTerminalIntentAcknowledgementAndExhaustedBudget
)
{
    for (unsigned i = 0; i != 30; ++i)
    {
        const auto checkpoint = rooms->checkpoint();
        ASSERT_TRUE(checkpoint);
        if (checkpoint->matches[0].phase == RoomPhase::Settling)
            break;
        ASSERT_TRUE(rooms->advance());
    }
    const auto terminal = image();
    ASSERT_EQ(terminal.rooms.matches[0].phase, RoomPhase::Settling);
    ASSERT_TRUE(terminal.rooms.matches[0].result);
    auto bytes = encodeCoreCheckpoint(terminal);
    ASSERT_TRUE(bytes);
    auto decoded = decodeCoreCheckpoint(*bytes);
    ASSERT_TRUE(decoded);
    auto online = OnlineRegistry::restore(
        decoded->registry,
        registryConfig(),
        wallClock(),
        [](SessionBinding) { return [] {}; }
    );
    ASSERT_TRUE(online);
    auto recovered = RoomService::restore(
        *online,
        decoded->rooms,
        roomConfig()
    );
    ASSERT_TRUE(recovered);
    ASSERT_TRUE(
        (*recovered)
            ->acknowledgeSettlement(allocation->matchId, false)
    );
    const auto blocked = (*recovered)->checkpoint();
    ASSERT_TRUE(blocked);
    EXPECT_TRUE(blocked->matches[0].persistenceBlocked);
    EXPECT_EQ(blocked->matches[0].admissionAttempts, 1U);
    const auto blockedBytes = encodeCoreCheckpoint(
        CoreCheckpoint{
            (*online)->checkpoint(),
            decoded->queue,
            *blocked
        }
    );
    ASSERT_TRUE(blockedBytes);
    const auto blockedImage = decodeCoreCheckpoint(*blockedBytes);
    ASSERT_TRUE(blockedImage);
    auto failed = RoomService::restore(
        *online,
        blockedImage->rooms,
        roomConfig()
    );
    ASSERT_TRUE(failed);
    EXPECT_FALSE(
        (*failed)->acknowledgeSettlement(allocation->matchId, true)
    );
    EXPECT_EQ(
        (*failed)->checkpoint()->matches[0].admissionAttempts,
        1U
    );
    ASSERT_TRUE(
        rooms->acknowledgeSettlement(allocation->matchId, true)
    );
    const auto completed = image();
    ASSERT_EQ(completed.rooms.matches[0].phase, RoomPhase::Finished);
    ASSERT_TRUE(completed.rooms.matches[0].expires);
    const auto completedBytes = encodeCoreCheckpoint(completed);
    ASSERT_TRUE(completedBytes);
    const auto completedImage = decodeCoreCheckpoint(*completedBytes);
    ASSERT_TRUE(completedImage);
    const auto completedRegistry = OnlineRegistry::restore(
        completedImage->registry,
        registryConfig(),
        wallClock(),
        [](SessionBinding) { return [] {}; }
    );
    ASSERT_TRUE(completedRegistry);
    const auto completedRooms = RoomService::restore(
        *completedRegistry,
        completedImage->rooms,
        roomConfig()
    );
    ASSERT_TRUE(completedRooms);
    EXPECT_EQ(encodeCoreCheckpoint(*completedImage), completedBytes);
}
TEST_F(
    AuthorityCodecTest,
    PreservesFullWidthIntegersAndNegativeClockExtremes
)
{
    auto value = image();
    auto& entry = value.registry.entries[0];
    entry.snapshot.playerId = PlayerId{UINT64_MAX};
    entry.snapshot.sessionId = SessionId{UINT64_MAX};
    entry.snapshot.epoch = UINT64_MAX;
    entry.snapshot.graceDeadline = Clock::time_point::min();
    entry.binding = {PlayerId{UINT64_MAX}, UINT64_MAX, UINT64_MAX};
    value.registry
        .epochWatermark = value.registry
                              .sessionWatermark = UINT64_MAX;
    value.queue.lastNow = Clock::time_point::min();
    value.queue.entries[0].admitted = Clock::time_point::max();
    value.queue.entries[0].lifecycle.revision = UINT64_MAX;
    value.queue.metrics.totalMatchedWaitMs = UINT64_MAX;
    value.rooms.deadline = Clock::time_point::min();
    value.rooms.matches[0].retryAt = Clock::time_point::max();
    value.rooms.matches[0].expires = Clock::time_point::min();
    value.rooms.matches[0]
        .admissionAttempts = std::numeric_limits<std::size_t>::max();
    value.rooms.matches[0].persistenceOutcome = 2;
    value.rooms.actors[0].checksum = UINT64_MAX;
    auto& player = value.rooms.actors[0].players[0];
    player.x = INT32_MIN;
    player.y = INT32_MAX;
    player.sequence = UINT64_MAX;
    auto bytes = encodeCoreCheckpoint(value);
    ASSERT_TRUE(bytes);
    auto decoded = decodeCoreCheckpoint(*bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(
        decoded->registry.entries[0].binding.connectionId,
        UINT64_MAX
    );
    EXPECT_EQ(
        decoded->registry.entries[0].snapshot.graceDeadline,
        Clock::time_point::min()
    );
    EXPECT_EQ(decoded->queue, value.queue);
    EXPECT_EQ(decoded->rooms.deadline, Clock::time_point::min());
    EXPECT_EQ(
        decoded->rooms.matches[0].retryAt,
        Clock::time_point::max()
    );
    EXPECT_EQ(
        decoded->rooms.matches[0].expires,
        Clock::time_point::min()
    );
    EXPECT_EQ(
        decoded->rooms.matches[0].admissionAttempts,
        std::numeric_limits<std::size_t>::max()
    );
    EXPECT_EQ(decoded->rooms.matches[0].persistenceOutcome, 2);
    EXPECT_EQ(decoded->rooms.actors[0], value.rooms.actors[0]);
    EXPECT_EQ(encodeCoreCheckpoint(*decoded), bytes);
}

TEST_F(
    AuthorityCodecTest,
    RejectsUnknownNestedFieldsVersionsAndMalformedOptionals
)
{
    const auto valid = wireImage();
    const auto rejects =
        [&](auto change, CodecError expected = CodecError::Invalid)
    {
        auto broken = valid;
        change(broken);
        auto result = decodeCoreCheckpoint(
            broken.SerializeAsString()
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), expected);
    };
    rejects(
        [](auto& w) { w.set_version(2); },
        CodecError::UnsupportedVersion
    );
    rejects(
        [](auto& w) { w.mutable_queue()->set_version(2); },
        CodecError::UnsupportedVersion
    );
    rejects(
        [](auto& w)
        {
            w.GetReflection()->MutableUnknownFields(&w)->AddVarint(
                100,
                1
            );
        }
    );
    rejects(
        [](auto& w)
        {
            auto* nested = w.mutable_rooms()
                               ->mutable_actors(0)
                               ->mutable_players(0)
                               ->mutable_member();
            nested->GetReflection()
                ->MutableUnknownFields(nested)
                ->AddVarint(100, 1);
        }
    );
    rejects(
        [](auto& w)
        {
            w.mutable_registry()
                ->mutable_entries(0)
                ->mutable_binding()
                ->set_player_id(0);
        }
    );
    rejects(
        [](auto& w)
        {
            w.mutable_registry()
                ->mutable_entries(0)
                ->mutable_snapshot()
                ->mutable_room_id()
                ->set_value(0);
        }
    );
    rejects(
        [](auto& w)
        {
            w.mutable_queue()
                ->mutable_metrics()
                ->mutable_matched_wait()
                ->RemoveLast();
        }
    );
    rejects(
        [](auto& w)
        {
            w.mutable_rooms()
                ->mutable_matches(0)
                ->mutable_ready()
                ->RemoveLast();
        }
    );
    rejects([](auto& w) { w.mutable_queue()->clear_policy(); });
    rejects(
        [](auto& w)
        {
            auto* snapshot = w.mutable_registry()
                                 ->mutable_entries(0)
                                 ->mutable_snapshot();
            snapshot->GetReflection()->SetEnumValue(
                snapshot,
                snapshot->GetDescriptor()->FindFieldByName(
                    "presence"
                ),
                99
            );
        }
    );
    auto bytes = valid.SerializeAsString();
    bytes.append(
        "\x08\x01",
        2
    ); // Duplicate singular version is not silently last-wins.
    EXPECT_FALSE(decodeCoreCheckpoint(bytes));
    bytes.pop_back();
    EXPECT_FALSE(decodeCoreCheckpoint(bytes));
    EXPECT_FALSE(decodeCoreCheckpoint(""));
    const auto canonical = valid.SerializeAsString();
    EXPECT_FALSE(decodeCoreCheckpoint(
        std::string("\x08\x81\x00", 3) + canonical.substr(2)
    ));
}

TEST_F(
    AuthorityCodecTest,
    RejectsWireNarrowingAndNonBooleanVarintsBeforeParsing
)
{
    const auto rejects = [&](auto change)
    {
        auto broken = wireImage();
        change(broken);
        auto result = decodeCoreCheckpoint(
            broken.SerializeAsString()
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), CodecError::Invalid);
    };
    rejects(
        [](auto& w)
        {
            auto* policy = w.mutable_queue()->mutable_policy();
            policy->set_initial_window(0);
            policy->GetReflection()
                ->MutableUnknownFields(policy)
                ->AddVarint(1, std::uint64_t{1} << 32);
        }
    );
    rejects(
        [](auto& w)
        {
            auto* state = w.mutable_rooms();
            state->set_running(false);
            state->GetReflection()
                ->MutableUnknownFields(state)
                ->AddVarint(6, 2);
        }
    );
    rejects(
        [](auto& w)
        {
            auto* actor = w.mutable_rooms()
                              ->mutable_policy()
                              ->mutable_actor();
            actor->set_position_limit(0);
            actor->GetReflection()
                ->MutableUnknownFields(actor)
                ->AddVarint(4, UINT64_MAX);
        }
    );
}

TEST_F(
    AuthorityCodecTest,
    BoundsWireAndStructureBeforeAllocatingBusinessCollections
)
{
    auto oversized = decodeCoreCheckpoint(
        std::string(authorityCodecMaxBytes + 1, 'x')
    );
    ASSERT_FALSE(oversized);
    EXPECT_EQ(oversized.error(), CodecError::Capacity);
    wire::CoreCheckpoint structure;
    structure.set_version(1);
    for (std::size_t i = 0; i <= authorityCodecMaxItems; ++i)
        structure.mutable_registry()->add_entries();
    ASSERT_LT(structure.ByteSizeLong(), authorityCodecMaxBytes);
    auto invalid = decodeCoreCheckpoint(
        structure.SerializeAsString()
    );
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), CodecError::Capacity);
    room::ReplayLog log{
        {},
        room::Snapshot{RoomId{1}, 1, 0, 0, {}},
        {}
    };
    log.ticks.resize(
        authorityCodecMaxItems + 1,
        room::TickRecord{1, {}, {}, 0}
    );
    auto rejected = encodeReplayLog(log);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), CodecError::Capacity);
}

TEST(
    AuthorityReplayCodecTest,
    OneHundredRandomActorLogsRoundTripAndReplayEveryChecksum
)
{
    std::mt19937_64 random(0xc033c0de);
    for (unsigned round = 0; round != 100; ++round)
    {
        SCOPED_TRACE(round);
        room::ActorConfig config;
        config.positionLimit = 500;
        room::RoomActor actor(
            RoomId{round + 1},
            UINT64_MAX - round,
            config
        );
        room::ReplayLog log{config, actor.snapshot(), {}};
        const std::vector<room::Member> members{
            {PlayerId{1}, SessionId{11}, 1},
            {PlayerId{2}, SessionId{12}, 2}
        };
        for (unsigned tick = 0; tick != 20; ++tick)
        {
            const auto before = actor.snapshot();
            std::vector<room::Input> inputs;
            for (std::size_t index = 0; index != members.size();
                 ++index)
            {
                const auto sequence = before.players.empty()
                                          ? 1
                                          : before.players[index]
                                                    .sequence +
                                                1;
                inputs.push_back(
                    {members[index],
                     sequence,
                     static_cast<std::int32_t>(random() % 3) - 1,
                     static_cast<std::int32_t>(random() % 3) - 1}
                );
            }
            if (tick % 5 == 0)
                inputs.push_back(
                    {{PlayerId{1}, SessionId{11}, 0}, 0, 2, INT32_MIN}
                );
            std::shuffle(inputs.begin(), inputs.end(), random);
            const auto result = actor.step(
                members,
                std::move(inputs)
            );
            auto encoded = encodeTickRecord(result.record);
            ASSERT_TRUE(encoded);
            auto decoded = decodeTickRecord(*encoded);
            ASSERT_TRUE(decoded);
            EXPECT_EQ(decoded->tick, result.record.tick);
            EXPECT_EQ(decoded->members, result.record.members);
            EXPECT_EQ(decoded->inputs, result.record.inputs);
            EXPECT_EQ(decoded->checksum, result.record.checksum);
            log.ticks.push_back(std::move(*decoded));
        }
        auto bytes = encodeReplayLog(log);
        ASSERT_TRUE(bytes);
        auto decoded = decodeReplayLog(*bytes);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(encodeReplayLog(*decoded), bytes);
        const auto replayed = room::RoomActor::replay(*decoded);
        ASSERT_TRUE(replayed);
        EXPECT_EQ(replayed->snapshot, actor.snapshot());
        auto restored = room::RoomActor::restore(
            decoded->checkpoint,
            decoded->config
        );
        ASSERT_TRUE(restored);
        for (const auto& tick : decoded->ticks)
            EXPECT_EQ(
                (*restored)
                    ->step(tick.members, tick.inputs)
                    .snapshot.checksum,
                tick.checksum
            );
        EXPECT_EQ((*restored)->snapshot(), actor.snapshot());
    }
}

TEST(
    AuthorityReplayCodecTest,
    RejectsUnknownVersionsUnsortedAttemptsAndDiscontinuousSegments
)
{
    room::RoomActor actor(RoomId{1}, 1);
    const room::Member member{PlayerId{1}, SessionId{11}, 1};
    room::ReplayLog log{{}, actor.snapshot(), {}};
    log.ticks.push_back(
        actor.step({member}, {{member, 1, 1, 0}}).record
    );
    const auto bytes = encodeReplayLog(log);
    ASSERT_TRUE(bytes);
    wire::ReplayLog wireLog;
    ASSERT_TRUE(wireLog.ParseFromString(*bytes));
    wireLog.set_version(2);
    EXPECT_EQ(
        decodeReplayLog(wireLog.SerializeAsString()).error(),
        CodecError::UnsupportedVersion
    );
    wireLog.set_version(1);
    wireLog.mutable_ticks(0)->set_tick(2);
    EXPECT_FALSE(decodeReplayLog(wireLog.SerializeAsString()));
    auto invalid = log.ticks[0];
    invalid.inputs.push_back({member, 0, 0, 0});
    EXPECT_FALSE(encodeTickRecord(invalid));
    const auto encodedTick = encodeTickRecord(log.ticks[0]);
    ASSERT_TRUE(encodedTick);
    wire::TickEnvelope tick;
    ASSERT_TRUE(tick.ParseFromString(*encodedTick));
    tick.mutable_record()
        ->GetReflection()
        ->MutableUnknownFields(tick.mutable_record())
        ->AddVarint(100, 1);
    EXPECT_FALSE(decodeTickRecord(tick.SerializeAsString()));
}
} // namespace
