#include "room/RoomWire.h"

#include <array>
#include <limits>
#include <random>
#include <stdexcept>

#include <gtest/gtest.h>
#include <openssl/evp.h>

namespace
{
using namespace service::room;
namespace pb = arena::room;
RuntimeImage image()
{
    auto result = RoomRuntime::create(RoomId{7}, 9, RuntimePolicy{});
    if (!result)
        throw std::runtime_error("create fixture failed");
    return *result;
}
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
        throw std::runtime_error("wire fixture failed");
    return std::move(*value);
}
TEST(RoomWire, CompleteImageAndPartialCounterCoverageRoundTrip)
{
    auto initial = image();
    auto cut = cutoff(initial);
    auto stepped = RoomRuntime::step(initial, cut);
    ASSERT_TRUE(stepped);
    auto current = stepped->image;
    current.completeCounterHistory = false;
    current.knownSinceTick = current.actor.tick;
    current.counters = {};
    ASSERT_TRUE(RoomRuntime::validate(current));
    const auto bytes = unwrap(wire::encodeImage(current));
    auto decoded = wire::decodeImage(bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->actor, current.actor);
    EXPECT_EQ(decoded->counters, current.counters);
    EXPECT_FALSE(decoded->completeCounterHistory);
    EXPECT_EQ(decoded->knownSinceTick, current.actor.tick);
    ASSERT_TRUE(RoomRuntime::validate(*decoded));
    auto future = RoomRuntime::step(*decoded, cutoff(*decoded));
    ASSERT_TRUE(future);
    EXPECT_EQ(future->image.actor.tick, current.actor.tick + 1);
    pb::RuntimeImage raw;
    ASSERT_TRUE(raw.ParseFromString(bytes));
    raw.clear_complete_counter_history();
    EXPECT_FALSE(wire::decodeImage(raw.SerializeAsString()));
}
TEST(RoomWire, FullWidthIntegersAndNegativeClockArePreserved)
{
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    auto current = image();
    current.actor.roomId = RoomId{maximum};
    current.actor.incarnation = maximum;
    current.actor.tick = maximum;
    current.actor.checksum = maximum;
    current.logicalCutoff = maximum;
    current.knownSinceTick = maximum;
    current.actor.players = {
        {Member{PlayerId{maximum}, SessionId{maximum}, maximum},
         -123,
         456,
         maximum,
         -1,
         1}
    };
    auto decoded = wire::decodeImage(
        unwrap(wire::encodeImage(current))
    );
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->actor, current.actor);
    EXPECT_EQ(decoded->logicalCutoff, maximum);
    auto cut = cutoff(image());
    cut.nowNanos = std::numeric_limits<std::int64_t>::min();
    cut.wallMillis = maximum;
    auto back = wire::decodeCutoff(unwrap(wire::encodeCutoff(cut)));
    ASSERT_TRUE(back);
    EXPECT_EQ(back->nowNanos, cut.nowNanos);
    EXPECT_EQ(back->wallMillis, maximum);
    EXPECT_TRUE(back->commands.empty());
    cut.commands.push_back(RuntimeCommand{});
    EXPECT_FALSE(wire::encodeCutoff(cut));
}
TEST(RoomWire, ManagedResultAndExplicitOptionalExpiryRoundTrip)
{
    RuntimeAllocation allocation{
        7,
        {auth(1).member, auth(2).member},
        {1000, 1200}
    };
    RuntimePolicy policy;
    policy.battleDurationTicks = 2;
    auto current = RoomRuntime::create(
        RoomId{(std::uint64_t{1} << 63) | 7},
        9,
        policy,
        allocation,
        100
    );
    ASSERT_TRUE(current);
    for (int index = 0; index < 5; ++index)
    {
        auto cut = cutoff(*current);
        cut.roster = {auth(1), auth(2)};
        cut.currentAuthorizations = cut.roster;
        if (index == 0)
        {
            for (auto a : cut.roster)
            {
                RuntimeCommand c;
                c.kind = RuntimeCommandKind::Ready;
                c.caller = a;
                c.incarnation = 9;
                c.ready = true;
                cut.commands.push_back(c);
            }
        }
        auto next = RoomRuntime::step(*current, cut);
        ASSERT_TRUE(next);
        current = std::move(next->image);
    }
    ASSERT_TRUE(current->managed);
    ASSERT_TRUE(current->managed->result);
    current->managed
        ->expiresAtNanos = std::numeric_limits<std::int64_t>::min();
    auto decoded = wire::decodeImage(
        unwrap(wire::encodeImage(*current))
    );
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->managed, current->managed);
    EXPECT_EQ(decoded->counters, current->counters);
}
TEST(
    RoomWire,
    RejectsUnknownVersionUnknownFieldsEnumsAndNoncanonicalBytes
)
{
    auto bytes = unwrap(wire::encodeImage(image()));
    pb::RuntimeImage raw;
    ASSERT_TRUE(raw.ParseFromString(bytes));
    raw.set_version(2);
    auto result = wire::decodeImage(raw.SerializeAsString());
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), wire::CodecError::Version);
    raw.set_version(1);
    raw.GetReflection()->MutableUnknownFields(&raw)->AddVarint(
        100,
        7
    );
    EXPECT_FALSE(wire::decodeImage(raw.SerializeAsString()));
    EXPECT_FALSE(
        wire::decodeImage(bytes + std::string("\x08\x01", 2))
    );
    EXPECT_FALSE(
        wire::decodeImage(std::string(wire::kMaxImageBytes + 1, 'x'))
    );
    RuntimeCommand cmd;
    auto encoded = unwrap(wire::encodeCommand(cmd));
    pb::Command command;
    ASSERT_TRUE(command.ParseFromString(encoded));
    command.set_kind(static_cast<pb::CommandKind>(999));
    EXPECT_FALSE(wire::decodeCommand(command.SerializeAsString()));
    command.set_kind(pb::INPUT);
    command.mutable_caller()->mutable_member()->set_player_id(0);
    EXPECT_FALSE(wire::decodeCommand(command.SerializeAsString()));
    pb::Command unchanged;
    unchanged.set_version(77);
    EXPECT_FALSE(wire::parseMessage("bad", unchanged));
    EXPECT_EQ(unchanged.version(), 77);
}
TEST(
    RoomWire,
    FingerprintUsesExactDomainBigEndianMetadataAndOpaqueBytes
)
{
    const wire::ImageIdentity id{1, 2, 3, 4, 5, 6};
    const std::string opaque("a\0b", 3);
    std::string preimage = "arena-room-image/v2";
    for (auto value :
         {id.roomId,
          id.incarnation,
          id.physicalTick,
          id.checksum,
          id.cursor,
          id.logicalTick})
        for (int shift = 56; shift >= 0; shift -= 8)
            preimage.push_back(static_cast<char>(value >> shift));
    std::array<unsigned char, 32> digest{};
    unsigned size = 0;
    preimage.append(opaque);
    ASSERT_EQ(
        EVP_Digest(
            preimage.data(),
            preimage.size(),
            digest.data(),
            &size,
            EVP_sha256(),
            nullptr
        ),
        1
    );
    constexpr char hex[] = "0123456789abcdef";
    std::string expected;
    for (auto value : digest)
    {
        expected += hex[value >> 4];
        expected += hex[value & 15];
    }
    EXPECT_EQ(unwrap(wire::imageFingerprint(id, opaque)), expected);
    auto changed = id;
    ++changed.logicalTick;
    EXPECT_NE(
        unwrap(wire::imageFingerprint(changed, opaque)),
        expected
    );
    EXPECT_NE(
        unwrap(wire::imageFingerprint(id, opaque + "x")),
        expected
    );
}
TEST(RoomWire, ImageMetadataAndProofAreFencedWithoutCallingKernel)
{
    auto current = image();
    auto packed = unwrap(wire::packImage(current, 90));
    ASSERT_TRUE(wire::unpackImage(packed));
    packed.set_cursor(91);
    EXPECT_FALSE(wire::unpackImage(packed));
    packed = unwrap(wire::packImage(current, 90));
    packed.set_logical_tick(10);
    EXPECT_FALSE(wire::unpackImage(packed));
    // Structural decoding does not claim semantic checksum validity.
    ++current.actor.checksum;
    ASSERT_TRUE(
        wire::decodeImage(unwrap(wire::encodeImage(current)))
    );
    EXPECT_FALSE(RoomRuntime::validate(current));
}
TEST(RoomWire, OneHundredRandomActualTickAndCommandRoundTrips)
{
    std::mt19937 random(34);
    auto current = image();
    std::uint64_t sequence = 0;
    ReplayLog log{current.policy.actor, current.actor, {}};
    for (int tick = 0; tick < 100; ++tick)
    {
        auto cut = cutoff(current);
        RuntimeCommand input;
        input.kind = RuntimeCommandKind::Input;
        input.caller = auth();
        input.incarnation = 9;
        input.sequence = ++sequence;
        input.dx = static_cast<int>(random() % 3) - 1;
        input.dy = static_cast<int>(random() % 3) - 1;
        auto command = wire::decodeCommand(
            unwrap(wire::encodeCommand(input))
        );
        ASSERT_TRUE(command);
        cut.commands.push_back(*command);
        auto next = RoomRuntime::step(current, cut);
        ASSERT_TRUE(next);
        ASSERT_TRUE(next->replay);
        auto decoded = wire::decodeTick(
            unwrap(wire::encodeTick(*next->replay))
        );
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->tick, next->replay->tick);
        EXPECT_EQ(decoded->members, next->replay->members);
        EXPECT_EQ(decoded->inputs, next->replay->inputs);
        EXPECT_EQ(decoded->checksum, next->replay->checksum);
        log.ticks.push_back(*decoded);
        current = next->image;
    }
    auto restored = wire::decodeReplayLog(
        unwrap(wire::encodeReplayLog(log))
    );
    ASSERT_TRUE(restored);
    auto replay = RoomActor::replay(*restored);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->snapshot, current.actor);
}
TEST(RoomWire, ReplayAndImageCollectionsAreBoundedBeforeEmission)
{
    auto current = image();
    current.actor.players.resize(
        65,
        current.actor.players.empty() ? PlayerState{auth().member}
                                      : current.actor.players.front()
    );
    EXPECT_FALSE(wire::encodeImage(current));
    auto initial = image();
    ReplayLog log{initial.policy.actor, initial.actor, {}};
    TickRecord tick{1, {auth().member}, {}, 1};
    tick.inputs.resize(256, Input{auth().member, 1, 1, 1});
    log.ticks.resize(1024, tick);
    auto encoded = wire::encodeReplayLog(log);
    ASSERT_FALSE(encoded);
    EXPECT_EQ(encoded.error(), wire::CodecError::Capacity);
}
TEST(
    RoomWire,
    OutsideRoomCurrentAuthorizationAndFullCommandRosterUnionRoundTrip
)
{
    auto cut = cutoff(image());
    cut.currentAuthorizations.clear();
    for (std::uint64_t id = 1; id <= 320; ++id)
    {
        auto current = auth(id);
        current.member.epoch = 0;
        cut.currentAuthorizations.push_back(current);
    }
    auto encoded = wire::encodeCutoff(cut);
    ASSERT_TRUE(encoded);
    auto decoded = wire::decodeCutoff(*encoded);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->currentAuthorizations.size(), 320);
    EXPECT_EQ(decoded->currentAuthorizations.back().member.epoch, 0);
    cut.currentAuthorizations.push_back(auth(321));
    EXPECT_FALSE(wire::encodeCutoff(cut));
    cut.currentAuthorizations.clear();
    cut.roster.front().member.epoch = 0;
    EXPECT_FALSE(wire::encodeCutoff(cut));
    RuntimeCommand command;
    command.caller = auth();
    command.caller->member.epoch = 0;
    auto caller = wire::decodeCommand(
        unwrap(wire::encodeCommand(command))
    );
    ASSERT_TRUE(caller);
    EXPECT_EQ(caller->caller->member.epoch, 0);
    auto invalid = image();
    invalid.actor.players = {{command.caller->member}};
    EXPECT_FALSE(wire::encodeImage(invalid));
}
} // namespace