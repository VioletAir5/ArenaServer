#include "room/RoomQuery.h"

#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>
namespace
{
using namespace service::room;
using Kind = RuntimeCommandKind;
using Phase = RuntimePhase;
using Error = RuntimeError;
RuntimeAuthorization auth(std::uint64_t player)
{
    return {
        {PlayerId{player}, SessionId{player + 10}, player + 99},
        player + 20,
        player + 1000,
        true
    };
}
RuntimeCommand query(Kind kind)
{
    RuntimeCommand command;
    command.kind = kind;
    command.caller = auth(1);
    command.matchId = 7;
    return command;
}
RuntimeCutoff context(const RuntimeImage& image)
{
    return {
        {auth(1), auth(2)},
        {auth(1), auth(2)},
        {},
        static_cast<std::int64_t>(
            (image.logicalCutoff + 1) * 50'000'000
        ),
        100
    };
}
RuntimeImage advance(
    const RuntimeImage& image,
    std::vector<RuntimeCommand> commands = {}
)
{
    auto cut = context(image);
    cut.commands = std::move(commands);
    auto next = RoomRuntime::step(image, cut);
    if (!next)
        throw std::runtime_error("query fixture step failed");
    return std::move(next->image);
}
RuntimeImage imageFor(std::optional<Phase> phase = {})
{
    RuntimePolicy policy;
    policy.battleDurationTicks = 2;
    auto created = RoomRuntime::create(
        RoomId{phase ? ((std::uint64_t{1} << 63) | 7) : 7},
        9,
        policy,
        phase ? std::optional(
                    RuntimeAllocation{
                        7,
                        {auth(1).member, auth(2).member},
                        {1000, 2000}
                    }
                )
              : std::nullopt,
        100
    );
    if (!created)
        throw std::runtime_error("query fixture create failed");
    auto image = std::move(*created);
    if (!phase || *phase == Phase::Forming)
        return image;
    auto one = query(Kind::Ready), two = one;
    one.ready = two.ready = true;
    one.incarnation = two.incarnation = 9;
    two.caller = auth(2);
    image = advance(image, {one, two});
    if (*phase == Phase::Ready)
        return image;
    image = advance(image);
    if (*phase == Phase::Playing)
        return image;
    image = advance(image);
    if (*phase == Phase::Settling)
        return image;
    RuntimeCommand ack;
    ack.kind = Kind::SettlementAck;
    ack.matchId = 7;
    ack.committed = true;
    return advance(image, {ack});
}
void expect(
    const std::expected<void, Error>& value,
    std::optional<Error> error
)
{
    if (error)
    {
        ASSERT_FALSE(value);
        EXPECT_EQ(value.error(), *error);
    }
    else
        EXPECT_TRUE(value);
}
TEST(
    RoomQuery,
    PhaseMatrixMatchesWorkerCommandRulesWithoutMutatingCommittedImage
)
{
    for (const auto phase :
         {Phase::Forming,
          Phase::Ready,
          Phase::Playing,
          Phase::Settling,
          Phase::Finished})
    {
        const auto image = imageFor(phase);
        const auto original = image;
        for (const auto kind :
             {Kind::Snapshot, Kind::Status, Kind::Settlement})
        {
            SCOPED_TRACE(static_cast<unsigned>(phase));
            SCOPED_TRACE(static_cast<unsigned>(kind));
            auto cut = context(image);
            auto command = query(kind);
            std::optional<Error> expected;
            if (kind == Kind::Settlement && phase != Phase::Finished)
                expected = phase == Phase::Settling
                               ? Error::Unavailable
                               : Error::BusinessFailure;
            else if (
                kind == Kind::Snapshot && phase == Phase::Finished
            )
                expected = Error::BusinessFailure;
            expect(authorizeRoomQuery(image, command, cut), expected);
            cut.commands = {command};
            const auto worker = RoomRuntime::step(image, cut);
            ASSERT_TRUE(worker);
            ASSERT_EQ(worker->results.size(), 1);
            EXPECT_EQ(worker->results.front().error, expected);
            EXPECT_EQ(image.actor, original.actor);
            EXPECT_EQ(image.managed, original.managed);
            EXPECT_EQ(image.logicalCutoff, original.logicalCutoff);
        }
    }
}
TEST(RoomQuery, ManualSnapshotRequiresCurrentOnlineRoomMember)
{
    const auto image = imageFor();
    auto cut = context(image);
    auto command = query(Kind::Snapshot);
    expect(authorizeRoomQuery(image, command, cut), {});
    expect(
        authorizeRoomQuery(image, query(Kind::Status), cut),
        Error::BusinessFailure
    );
    expect(
        authorizeRoomQuery(image, query(Kind::Settlement), cut),
        Error::BusinessFailure
    );
    cut.roster.erase(cut.roster.begin());
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::BusinessFailure
    );
    cut.currentAuthorizations.erase(
        cut.currentAuthorizations.begin()
    );
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
}
TEST(
    RoomQuery,
    OldTransportAndReplacedLogicalSessionCannotReadRetainedResult
)
{
    const auto image = imageFor(Phase::Finished);
    auto command = query(Kind::Settlement);
    auto cut = context(image);
    cut.roster.clear();
    expect(authorizeRoomQuery(image, command, cut), {});
    ++cut.currentAuthorizations.front().sessionEpoch;
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
    command.caller = cut.currentAuthorizations.front();
    expect(authorizeRoomQuery(image, command, cut), {});
    cut.currentAuthorizations.front().connectionHandle++;
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
    cut.currentAuthorizations.front() = auth(1);
    cut.currentAuthorizations.front().member.sessionId = SessionId{
        900
    };
    command.caller = cut.currentAuthorizations.front();
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::BusinessFailure
    );
    command.caller.reset();
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
}
TEST(RoomQuery, ExactRetentionBoundaryAndWrongMatchRemainFenced)
{
    const auto image = imageFor(Phase::Finished);
    auto cut = context(image);
    auto command = query(Kind::Settlement);
    ASSERT_TRUE(image.managed->expiresAtNanos);
    cut.nowNanos = *image.managed->expiresAtNanos - 1;
    expect(authorizeRoomQuery(image, command, cut), {});
    command.matchId = 8;
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::BusinessFailure
    );
    command.matchId = 7;
    cut.nowNanos++;
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::BusinessFailure
    );
    cut.currentAuthorizations.clear();
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
}
TEST(RoomQuery, MalformedAuthorizationAndWrongCommandKindsFailClosed)
{
    const auto image = imageFor();
    auto cut = context(image);
    auto command = query(Kind::Snapshot);
    cut.currentAuthorizations.push_back(
        cut.currentAuthorizations.front()
    );
    expect(authorizeRoomQuery(image, command, cut), Error::Invalid);
    cut = context(image);
    cut.roster.front().member.epoch = 0;
    expect(authorizeRoomQuery(image, command, cut), Error::Invalid);
    cut = context(image);
    cut.currentAuthorizations.front().online = false;
    expect(authorizeRoomQuery(image, command, cut), Error::Invalid);
    cut.currentAuthorizations.front().connectionHandle = 0;
    expect(
        authorizeRoomQuery(image, command, cut),
        Error::Unauthenticated
    );
    cut = context(image);
    cut.nowNanos = -1;
    expect(authorizeRoomQuery(image, command, cut), Error::Capacity);
    for (const auto kind :
         {Kind::Input, Kind::Ready, Kind::SettlementAck})
        expect(
            authorizeRoomQuery(image, query(kind), context(image)),
            Error::Invalid
        );
}
TEST(RoomQuery, RosterCapacityAndManagedCohortAreNotSilentlyTruncated)
{
    auto image = imageFor();
    image.policy.actor.maxMembers = 1;
    auto cut = context(image);
    expect(
        authorizeRoomQuery(image, query(Kind::Snapshot), cut),
        Error::Unavailable
    );
    image = imageFor(Phase::Playing);
    cut = context(image);
    cut.roster.push_back(auth(3));
    expect(
        authorizeRoomQuery(image, query(Kind::Status), cut),
        Error::Invalid
    );
    cut = context(image);
    cut.currentAuthorizations.resize(
        image.policy.maxCommands + image.policy.actor.maxMembers + 1,
        auth(1)
    );
    expect(
        authorizeRoomQuery(image, query(Kind::Status), cut),
        Error::Capacity
    );
}
} // namespace
