#include "room/RoomRuntime.h"

#include <limits>
#include <random>
#include <stdexcept>

#include <gtest/gtest.h>

namespace
{
using namespace service::room;
constexpr std::uint64_t managedRoom = (std::uint64_t{1} << 63) | 7;
RuntimeAuthorization auth(std::uint64_t player)
{
    return {
        {PlayerId{player}, SessionId{player + 10}, player + 99},
        player + 20,
        player + 1000,
        true
    };
}
RuntimeAllocation allocation()
{
    return {7, {auth(1).member, auth(2).member}, {1000, 2000}};
}
RuntimeImage make(bool managed = false, std::uint64_t duration = 3)
{
    RuntimePolicy policy;
    policy.battleDurationTicks = duration;
    auto value = RoomRuntime::create(
        RoomId{managed ? managedRoom : 7},
        9,
        policy,
        managed ? std::optional(allocation()) : std::nullopt,
        100
    );
    if (!value)
        throw std::runtime_error("runtime create failed");
    return std::move(*value);
}
RuntimeCutoff cutoff(const RuntimeImage& image)
{
    return {
        {auth(1), auth(2)},
        {auth(1), auth(2)},
        {},
        static_cast<std::int64_t>(
            (image.logicalCutoff + 1) * 50'000'000
        ),
        100 + image.logicalCutoff * 50
    };
}
RuntimeCommand
command(RuntimeCommandKind kind, std::uint64_t player = 1)
{
    RuntimeCommand value;
    value.kind = kind;
    value.caller = auth(player);
    value.incarnation = 9;
    value.matchId = 7;
    return value;
}
RuntimeCommand input(std::uint64_t sequence, int dx = 1)
{
    auto value = command(RuntimeCommandKind::Input);
    value.sequence = sequence;
    value.dx = dx;
    return value;
}
RuntimeCommand ack(bool committed)
{
    RuntimeCommand value;
    value.kind = RuntimeCommandKind::SettlementAck;
    value.matchId = 7;
    value.committed = committed;
    return value;
}
RuntimeProposal step(
    const RuntimeImage& image,
    std::vector<RuntimeCommand> commands = {}
)
{
    auto cut = cutoff(image);
    cut.commands = std::move(commands);
    auto result = RoomRuntime::step(image, cut);
    if (!result)
        throw std::runtime_error(
            "runtime step failed: " +
            std::to_string(static_cast<int>(result.error()))
        );
    return std::move(*result);
}
RuntimeImage playing(std::uint64_t duration = 3)
{
    auto image = make(true, duration);
    auto a = command(RuntimeCommandKind::Ready, 1);
    auto b = command(RuntimeCommandKind::Ready, 2);
    a.ready = b.ready = true;
    image = step(image, {a, b}).image;
    if (image.managed->phase != RuntimePhase::Ready)
        throw std::runtime_error("first ready cutoff was skipped");
    return step(image).image;
}
RuntimeImage settling()
{
    auto image = playing(2);
    return step(image, {input(1)}).image;
}

TEST(
    RoomRuntimeTest,
    IsolatedCandidatePreservesCommittedBaseAndCanonicalReplay
)
{
    auto original = make();
    const auto before = original.actor;
    const auto first = step(original, {input(1)});
    const auto repeat = step(original, {input(1)});
    EXPECT_EQ(original.actor, before);
    EXPECT_EQ(original.logicalCutoff, 0u);
    EXPECT_EQ(first.image.actor, repeat.image.actor);
    ASSERT_TRUE(first.replay);
    ASSERT_EQ(first.results.size(), 1u);
    EXPECT_EQ(first.results[0].input, InputStatus::Applied);
    auto replay = RoomActor::replay(
        {original.policy.actor, original.actor, {*first.replay}}
    );
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, first.image.actor);
    EXPECT_EQ(first.image.logicalCutoff, 1u);
}

TEST(
    RoomRuntimeTest,
    ManagedLifecycleWaitsForDurableSettlementAcknowledgement
)
{
    auto image = playing(2);
    EXPECT_EQ(image.managed->phase, RuntimePhase::Playing);
    EXPECT_EQ(image.managed->playedTicks, 1u);
    auto ended = step(image, {input(1)});
    ASSERT_EQ(ended.image.managed->phase, RuntimePhase::Settling);
    EXPECT_FALSE(ended.releaseMembership);
    EXPECT_EQ(ended.image.counters.battlesCompleted, 0u);
    const auto frozen = ended.image.actor;
    auto query = step(
        ended.image,
        {command(RuntimeCommandKind::Status),
         command(RuntimeCommandKind::Settlement)}
    );
    EXPECT_EQ(query.image.actor, frozen);
    EXPECT_FALSE(query.replay);
    EXPECT_EQ(
        query.image.logicalCutoff,
        ended.image.logicalCutoff + 1
    );
    EXPECT_FALSE(query.results[0].error);
    EXPECT_EQ(query.results[1].error, RuntimeError::Unavailable);
    auto committed = step(query.image, {ack(true)});
    EXPECT_EQ(committed.image.managed->phase, RuntimePhase::Finished);
    EXPECT_TRUE(committed.releaseMembership);
    EXPECT_EQ(committed.image.actor, frozen);
    EXPECT_FALSE(committed.replay);
    EXPECT_EQ(committed.image.counters.battlesCompleted, 1u);
    EXPECT_EQ(committed.image.counters.settlementsCommitted, 1u);
    auto repeated = step(committed.image, {ack(true)});
    EXPECT_FALSE(repeated.releaseMembership);
    EXPECT_EQ(repeated.image.counters, committed.image.counters);
}

TEST(
    RoomRuntimeTest,
    ExactCurrentTransportAndRoomMembershipAreDifferentFences
)
{
    auto image = make();
    auto cut = cutoff(image);
    auto old = input(1);
    --old.caller->sessionEpoch;
    auto badMember = input(1);
    ++badMember.caller->member.epoch;
    auto badActor = input(1);
    ++badActor.incarnation;
    cut.commands = {old, badMember, badActor};
    auto result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    EXPECT_EQ(
        result->results[0].error,
        RuntimeError::Unauthenticated
    );
    EXPECT_EQ(
        result->results[1].error,
        RuntimeError::BusinessFailure
    );
    EXPECT_EQ(
        result->results[2].error,
        RuntimeError::BusinessFailure
    );
    cut.roster.erase(cut.roster.begin());
    cut.commands = {command(RuntimeCommandKind::Snapshot)};
    result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    EXPECT_EQ(
        result->results[0].error,
        RuntimeError::BusinessFailure
    );
}

TEST(
    RoomRuntimeTest,
    DetachedCohortSurvivesGraceAndReconnectRetainsSequence
)
{
    auto image = playing(20);
    image = step(image, {input(1)}).image;
    auto cut = cutoff(image);
    cut.roster[0].online = false;
    cut.roster[0].connectionHandle = 0;
    cut.currentAuthorizations[0] = cut.roster[0];
    cut.commands = {input(2)};
    auto detached = RoomRuntime::step(image, cut);
    ASSERT_TRUE(detached);
    EXPECT_EQ(detached->image.managed->phase, RuntimePhase::Playing);
    EXPECT_FALSE(detached->image.managed->online[0]);
    EXPECT_EQ(
        detached->results[0].error,
        RuntimeError::Unauthenticated
    );
    EXPECT_EQ(detached->image.actor.players[0].sequence, 1u);
    auto resumed = cutoff(detached->image);
    ++resumed.roster[0].sessionEpoch;
    ++resumed.roster[0].connectionHandle;
    resumed.currentAuthorizations = resumed.roster;
    auto request = input(2);
    request.caller = resumed.roster[0];
    resumed.commands = {request};
    const auto applied = RoomRuntime::step(detached->image, resumed);
    ASSERT_TRUE(applied);
    EXPECT_EQ(applied->results[0].input, InputStatus::Applied);
    EXPECT_EQ(applied->image.actor.players[0].sequence, 2u);
}

TEST(
    RoomRuntimeTest,
    ExpiredCohortFreezesLastStateWithoutInventingActorTick
)
{
    auto image = playing(20);
    image = step(image, {input(1)}).image;
    const auto before = image.actor;
    auto cut = cutoff(image);
    cut.roster.erase(cut.roster.begin());
    cut.currentAuthorizations.erase(
        cut.currentAuthorizations.begin()
    );
    auto lost = RoomRuntime::step(image, cut);
    ASSERT_TRUE(lost);
    EXPECT_EQ(lost->image.actor, before);
    EXPECT_FALSE(lost->replay);
    EXPECT_EQ(lost->image.logicalCutoff, image.logicalCutoff + 1);
    EXPECT_EQ(lost->image.managed->phase, RuntimePhase::Settling);
    EXPECT_EQ(
        lost->image.managed->result->reason,
        RuntimeReason::MemberUnavailable
    );
}

TEST(
    RoomRuntimeTest, InitialDisconnectStillRecordsCompleteFixedCohort
)
{
    auto image = make(true);
    auto cut = cutoff(image);
    cut.roster.clear();
    cut.currentAuthorizations.clear();
    auto result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->replay);
    EXPECT_EQ(result->image.actor.tick, 1u);
    EXPECT_EQ(result->image.actor.players.size(), 2u);
    EXPECT_EQ(result->image.managed->playedTicks, 0u);
    EXPECT_EQ(result->image.managed->waitedTicks, 0u);
    EXPECT_EQ(
        result->image.managed->result->reason,
        RuntimeReason::MemberUnavailable
    );
    EXPECT_TRUE(RoomRuntime::validate(result->image));
}

TEST(
    RoomRuntimeTest,
    SequenceDuplicatesGapsAndConflictingPayloadsUseRealActor
)
{
    auto image = make();
    auto first = step(image, {input(1)});
    auto next = step(first.image, {input(1), input(3), input(1, -1)});
    EXPECT_EQ(next.results[0].input, InputStatus::Conflict);
    EXPECT_EQ(next.results[1].input, InputStatus::Gap);
    EXPECT_EQ(next.results[2].input, InputStatus::Conflict);
    auto retry = step(first.image, {input(1)});
    EXPECT_EQ(retry.results[0].input, InputStatus::Duplicate);
    EXPECT_EQ(retry.image.actor.players[0].x, 1);
}

TEST(
    RoomRuntimeTest,
    ReadyTimeoutAndPermanentPersistenceFailureRetainOneIntent
)
{
    auto image = make(true);
    image.policy.readyTimeoutTicks = 2;
    image = step(image).image;
    image = step(image).image;
    ASSERT_EQ(image.managed->phase, RuntimePhase::Settling);
    EXPECT_EQ(
        image.managed->result->reason,
        RuntimeReason::ReadyTimeout
    );
    EXPECT_EQ(image.counters.settlementIntents, 1u);
    auto failed = step(image, {ack(false)});
    EXPECT_TRUE(failed.image.managed->persistenceBlocked);
    EXPECT_FALSE(failed.releaseMembership);
    auto repeated = step(failed.image, {ack(false), ack(true)});
    EXPECT_FALSE(repeated.results[0].error);
    EXPECT_EQ(
        repeated.results[1].error,
        RuntimeError::BusinessFailure
    );
    EXPECT_EQ(repeated.image.counters.settlementsFailed, 1u);
    EXPECT_EQ(repeated.image.managed->phase, RuntimePhase::Settling);
}

TEST(
    RoomRuntimeTest,
    RetainedResultAuthorizesLogicalCohortAfterRoomRelease
)
{
    auto image = step(settling(), {ack(true)}).image;
    auto cut = cutoff(image);
    cut.roster.clear();
    cut.commands = {
        command(RuntimeCommandKind::Status),
        command(RuntimeCommandKind::Settlement)
    };
    auto result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->results[0].error);
    EXPECT_FALSE(result->results[1].error);
    EXPECT_FALSE(result->replay);
    cut.currentAuthorizations[0].member.sessionId = SessionId{99};
    cut.commands[0].caller = cut.currentAuthorizations[0];
    result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    EXPECT_EQ(
        result->results[0].error,
        RuntimeError::BusinessFailure
    );
    cut = cutoff(image);
    cut.roster.clear();
    cut.nowNanos = *image.managed->expiresAtNanos;
    cut.commands = {command(RuntimeCommandKind::Status)};
    result = RoomRuntime::step(image, cut);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->retire);
    EXPECT_EQ(
        result->results[0].error,
        RuntimeError::BusinessFailure
    );
}

TEST(
    RoomRuntimeTest,
    InvalidImagesCannotCreateAFreshOwnerWithChangedState
)
{
    auto image = playing(20);
    ++image.actor.checksum;
    EXPECT_EQ(
        RoomRuntime::validate(image).error(),
        RuntimeError::InvalidImage
    );
    image = playing(20);
    ++image.managed->room.revision;
    EXPECT_FALSE(RoomRuntime::validate(image));
    image = playing(20);
    ++image.managed->playedTicks;
    EXPECT_FALSE(RoomRuntime::validate(image));
    image = playing(20);
    image.logicalCutoff = 0;
    EXPECT_FALSE(RoomRuntime::validate(image));
}

TEST(RoomRuntimeTest, BoundsRejectWithoutChangingTheCommittedImage)
{
    auto image = make();
    auto cut = cutoff(image);
    cut.commands.resize(image.policy.maxCommands + 1);
    EXPECT_EQ(
        RoomRuntime::step(image, cut).error(),
        RuntimeError::Capacity
    );
    EXPECT_EQ(image.actor.tick, 0u);
    image.logicalCutoff = std::numeric_limits<std::uint64_t>::max();
    EXPECT_EQ(
        RoomRuntime::step(image, cutoff(make())).error(),
        RuntimeError::Capacity
    );
    image = settling();
    cut = cutoff(image);
    cut.commands = {ack(true)};
    cut.nowNanos = std::numeric_limits<std::int64_t>::max();
    EXPECT_EQ(
        RoomRuntime::step(image, cut).error(),
        RuntimeError::Capacity
    );
    EXPECT_EQ(image.managed->phase, RuntimePhase::Settling);
}

TEST(
    RoomRuntimeTest, ManualRosterOverflowRetiresWithoutDroppingMembers
)
{
    auto image = make();
    image.policy.actor.maxMembers = 1;
    auto cut = cutoff(image);
    cut.commands = {command(RuntimeCommandKind::Snapshot)};
    auto proposal = RoomRuntime::step(image, cut);
    ASSERT_TRUE(proposal);
    EXPECT_TRUE(proposal->retire);
    EXPECT_EQ(proposal->results[0].error, RuntimeError::Unavailable);
    EXPECT_EQ(proposal->image.actor, image.actor);
    EXPECT_FALSE(proposal->replay);
    EXPECT_EQ(proposal->image.logicalCutoff, 1u);
}

TEST(RoomRuntimeTest, LegacyPlayingCoverageCountsOnlyNewObservedTicks)
{
    auto image = playing(20);
    image.knownSinceTick = image.actor.tick;
    image.completeCounterHistory = false;
    image.counters = {};
    ASSERT_TRUE(RoomRuntime::validate(image));
    auto proposal = step(image, {input(1)});
    EXPECT_FALSE(proposal.image.completeCounterHistory);
    EXPECT_EQ(proposal.image.knownSinceTick, image.actor.tick);
    EXPECT_EQ(proposal.image.counters.ticks, 1u);
    EXPECT_EQ(proposal.image.counters.battlesStarted, 0u);
    EXPECT_EQ(proposal.image.counters.applied, 1u);
    auto invalid = image;
    invalid.counters.battlesStarted = 1;
    EXPECT_FALSE(RoomRuntime::validate(invalid));
    invalid = image;
    invalid.counters.ticks = 1;
    EXPECT_FALSE(RoomRuntime::validate(invalid));
    invalid = image;
    ++invalid.actor.checksum;
    EXPECT_FALSE(RoomRuntime::validate(invalid));
}

TEST(
    RoomRuntimeTest,
    LegacySettlingCoverageDoesNotInventAnOldIntentOnAck
)
{
    auto image = settling();
    image.knownSinceTick = image.actor.tick;
    image.completeCounterHistory = false;
    image.counters = {};
    ASSERT_TRUE(RoomRuntime::validate(image));
    auto proposal = step(image, {ack(true)});
    EXPECT_TRUE(proposal.releaseMembership);
    EXPECT_EQ(proposal.image.managed->phase, RuntimePhase::Finished);
    EXPECT_EQ(proposal.image.counters.ticks, 0u);
    EXPECT_EQ(proposal.image.counters.settlementIntents, 0u);
    EXPECT_EQ(proposal.image.counters.battlesStarted, 0u);
    EXPECT_EQ(proposal.image.counters.settlementsCommitted, 1u);
    EXPECT_EQ(proposal.image.counters.battlesCompleted, 1u);
    EXPECT_EQ(proposal.image.actor, image.actor);
}

TEST(RoomRuntimeTest, RehydratedOwnersReplayTheSameOrderedCutoffs)
{
    std::mt19937 random(7399);
    for (unsigned game = 0; game != 25; ++game)
    {
        auto image = make();
        ReplayLog log{image.policy.actor, image.actor, {}};
        for (std::uint64_t n = 1; n != 31; ++n)
        {
            auto cut = cutoff(image);
            cut.commands = {
                input(n, static_cast<int>(random() % 3) - 1)
            };
            const auto first = RoomRuntime::step(image, cut);
            const auto second = RoomRuntime::step(image, cut);
            ASSERT_TRUE(first);
            ASSERT_TRUE(second);
            EXPECT_EQ(first->image.actor, second->image.actor);
            ASSERT_TRUE(first->replay);
            log.ticks.push_back(*first->replay);
            image = first->image;
        }
        const auto replay = RoomActor::replay(log);
        ASSERT_TRUE(replay);
        EXPECT_EQ(replay->snapshot, image.actor);
    }
}
} // namespace