/** @file
 * @brief Encodes complete room values without invoking the simulation
 * kernel.
 */
#include "room/RoomWire.h"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>

#include <google/protobuf/io/coded_stream.h>
#include <openssl/evp.h>

namespace service::room::wire
{
namespace
{
namespace pb = arena::room;
constexpr std::size_t maxCommands = 256;

struct Failure
{
    CodecError error;
};
void require(bool value, CodecError error = CodecError::Invalid)
{
    if (!value)
        throw Failure{error};
}
template <class F>
auto guarded(F action)
    -> std::expected<decltype(action()), CodecError>
{
    try
    {
        return action();
    }
    catch (const Failure& failure)
    {
        return std::unexpected(failure.error);
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(CodecError::Capacity);
    }
    catch (...)
    {
        return std::unexpected(CodecError::Invalid);
    }
}
/** @brief Rejects unknown durable fields and undeclared enum numbers
 * recursively. */
bool known(
    const google::protobuf::Message& message, unsigned depth = 0
)
{
    if (depth > 16)
        return false;
    const auto* reflection = message.GetReflection();
    if (reflection->GetUnknownFields(message).field_count())
        return false;
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(message, &fields);
    for (const auto* field : fields)
    {
        const auto count = field->is_repeated()
                               ? reflection->FieldSize(message, field)
                               : 1;
        for (int i = 0; i < count; ++i)
        {
            if (field->cpp_type() ==
                google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
            {
                const auto& child = field->is_repeated()
                                        ? reflection
                                              ->GetRepeatedMessage(
                                                  message,
                                                  field,
                                                  i
                                              )
                                        : reflection->GetMessage(
                                              message,
                                              field
                                          );
                if (!known(child, depth + 1))
                    return false;
            }
            if (field->cpp_type() ==
                google::protobuf::FieldDescriptor::CPPTYPE_ENUM)
            {
                const auto value = field->is_repeated()
                                       ? reflection
                                             ->GetRepeatedEnumValue(
                                                 message,
                                                 field,
                                                 i
                                             )
                                       : reflection->GetEnumValue(
                                             message,
                                             field
                                         );
                if (!field->enum_type()->FindValueByNumber(value))
                    return false;
            }
        }
    }
    return true;
}
void put(const Member& v, pb::Member& p)
{
    require(v.playerId.value() && v.sessionId.value() && v.epoch);
    p.set_player_id(v.playerId.value());
    p.set_session_id(v.sessionId.value());
    p.set_epoch(v.epoch);
}
Member take(const pb::Member& p)
{
    require(p.player_id() && p.session_id() && p.epoch());
    return {
        PlayerId{p.player_id()},
        SessionId{p.session_id()},
        p.epoch()
    };
}
void put(const PlayerState& v, pb::Player& p)
{
    put(v.member, *p.mutable_member());
    p.set_x(v.x);
    p.set_y(v.y);
    p.set_sequence(v.sequence);
    p.set_last_dx(v.lastDx);
    p.set_last_dy(v.lastDy);
}
PlayerState take(const pb::Player& p)
{
    require(p.has_member());
    return {
        take(p.member()),
        p.x(),
        p.y(),
        p.sequence(),
        p.last_dx(),
        p.last_dy()
    };
}
void put(const RuntimeAuthorization& v, pb::Authorization& p)
{
    require(v.member.playerId.value() && v.member.sessionId.value());
    auto& member = *p.mutable_member();
    member.set_player_id(v.member.playerId.value());
    member.set_session_id(v.member.sessionId.value());
    member.set_epoch(v.member.epoch);
    p.set_session_epoch(v.sessionEpoch);
    p.set_connection_handle(v.connectionHandle);
    p.set_online(v.online);
}
RuntimeAuthorization take(const pb::Authorization& p)
{
    require(
        p.has_member() && p.member().player_id() &&
        p.member().session_id()
    );
    return {
        {PlayerId{p.member().player_id()},
         SessionId{p.member().session_id()},
         p.member().epoch()},
        p.session_epoch(),
        p.connection_handle(),
        p.online()
    };
}
void put(const RuntimePolicy& v, pb::Policy& p)
{
    require(
        v.actor.maxMembers && v.actor.maxMembers <= kMaxMembers &&
            v.actor.maxInputsPerTick &&
            v.actor.maxInputsPerTick <= maxCommands &&
            v.maxCommands && v.maxCommands <= maxCommands &&
            v.actor.historyTicks && v.actor.historyTicks <= 65536,
        CodecError::Capacity
    );
    auto& a = *p.mutable_actor();
    a.set_max_members(static_cast<std::uint32_t>(v.actor.maxMembers));
    a.set_max_inputs_per_tick(
        static_cast<std::uint32_t>(v.actor.maxInputsPerTick)
    );
    a.set_history_ticks(
        static_cast<std::uint32_t>(v.actor.historyTicks)
    );
    a.set_position_limit(v.actor.positionLimit);
    p.set_max_commands(static_cast<std::uint32_t>(v.maxCommands));
    p.set_battle_duration_ticks(v.battleDurationTicks);
    p.set_ready_timeout_ticks(v.readyTimeoutTicks);
    p.set_result_retention_nanos(v.resultRetentionNanos);
}
RuntimePolicy take(const pb::Policy& p)
{
    require(p.has_actor());
    const auto& a = p.actor();
    RuntimePolicy v{
        {a.max_members(),
         a.max_inputs_per_tick(),
         a.history_ticks(),
         a.position_limit()},
        p.max_commands(),
        p.battle_duration_ticks(),
        p.ready_timeout_ticks(),
        p.result_retention_nanos()
    };
    pb::Policy checked;
    put(v, checked);
    return v;
}
void put(const Snapshot& v, pb::Snapshot& p)
{
    require(v.roomId.value() && v.incarnation);
    require(v.players.size() <= kMaxMembers, CodecError::Capacity);
    p.set_room_id(v.roomId.value());
    p.set_incarnation(v.incarnation);
    p.set_tick(v.tick);
    p.set_checksum(v.checksum);
    std::uint64_t previous = 0;
    for (const auto& player : v.players)
    {
        require(player.member.playerId.value() > previous);
        previous = player.member.playerId.value();
        put(player, *p.add_players());
    }
}
Snapshot take(const pb::Snapshot& p)
{
    require(p.room_id() && p.incarnation());
    require(
        p.players_size() <= static_cast<int>(kMaxMembers),
        CodecError::Capacity
    );
    Snapshot v{
        RoomId{p.room_id()},
        p.incarnation(),
        p.tick(),
        p.checksum(),
        {}
    };
    for (const auto& player : p.players())
        v.players.push_back(take(player));
    pb::Snapshot checked;
    put(v, checked);
    return v;
}
void put(const RuntimeAllocation& v, pb::Allocation& p)
{
    require(v.matchId);
    p.set_match_id(v.matchId);
    for (const auto& member : v.members)
        put(member, *p.add_members());
    for (const auto mmr : v.initialMmr)
        p.add_initial_mmr(mmr);
}
RuntimeAllocation take(const pb::Allocation& p)
{
    require(
        p.match_id() && p.members_size() == 2 &&
        p.initial_mmr_size() == 2
    );
    return {
        p.match_id(),
        {take(p.members(0)), take(p.members(1))},
        {p.initial_mmr(0), p.initial_mmr(1)}
    };
}
#define SCALAR(member, field) p.set_##field(v.member)
#define TAKE(member, field) v.member = p.field()
void put(const RuntimeSettlement& v, pb::Settlement& p)
{
    require(v.resultId && v.matchId && v.roomId.value());
    require(static_cast<unsigned>(v.reason) <= 2);
    require(v.players.size() <= kMaxMembers, CodecError::Capacity);
    SCALAR(resultId, result_id);
    SCALAR(matchId, match_id);
    p.set_room_id(v.roomId.value());
    SCALAR(finalTick, final_tick);
    SCALAR(finalChecksum, final_checksum);
    p.set_reason(static_cast<pb::Reason>(v.reason));
    SCALAR(winnerPlayerId, winner_player_id);
    for (const auto& player : v.players)
        put(player, *p.add_players());
    SCALAR(createdUnixMs, created_unix_ms);
    SCALAR(startedUnixMs, started_unix_ms);
    SCALAR(finishedUnixMs, finished_unix_ms);
    SCALAR(startedTick, started_tick);
    SCALAR(playedTicks, played_ticks);
}
RuntimeSettlement take(const pb::Settlement& p)
{
    require(p.result_id() && p.match_id() && p.room_id());
    require(
        p.players_size() <= static_cast<int>(kMaxMembers),
        CodecError::Capacity
    );
    RuntimeSettlement v{
        p.result_id(),
        p.match_id(),
        RoomId{p.room_id()},
        p.final_tick(),
        p.final_checksum(),
        static_cast<RuntimeReason>(p.reason()),
        p.winner_player_id(),
        {},
        p.created_unix_ms(),
        p.started_unix_ms(),
        p.finished_unix_ms(),
        p.started_tick(),
        p.played_ticks()
    };
    for (const auto& player : p.players())
        v.players.push_back(take(player));
    return v;
}
void put(const RuntimeManaged& v, pb::Managed& p)
{
    require(
        static_cast<unsigned>(v.room.state) <= 4 &&
        static_cast<unsigned>(v.battle.state) <= 3 &&
        static_cast<unsigned>(v.phase) <= 4
    );
    put(v.allocation, *p.mutable_allocation());
    p.set_room_state(static_cast<std::uint32_t>(v.room.state));
    p.set_room_revision(v.room.revision);
    p.set_battle_state(static_cast<std::uint32_t>(v.battle.state));
    p.set_battle_revision(v.battle.revision);
    p.set_phase(static_cast<pb::Phase>(v.phase));
    for (auto b : v.ready)
        p.add_ready(b);
    for (auto b : v.online)
        p.add_online(b);
    SCALAR(createdUnixMs, created_unix_ms);
    SCALAR(startedUnixMs, started_unix_ms);
    SCALAR(waitedTicks, waited_ticks);
    SCALAR(startedTick, started_tick);
    SCALAR(finishedTick, finished_tick);
    SCALAR(playedTicks, played_ticks);
    if (v.result)
        put(*v.result, *p.mutable_result());
    if (v.expiresAtNanos)
        p.set_expires_at_nanos(*v.expiresAtNanos);
    SCALAR(persistenceBlocked, persistence_blocked);
}
RuntimeManaged take(const pb::Managed& p)
{
    require(
        p.has_allocation() && p.room_state() <= 4 &&
        p.battle_state() <= 3 && p.ready_size() == 2 &&
        p.online_size() == 2
    );
    RuntimeManaged v{
        take(p.allocation()),
        {arena::domain::Room::State::Forming, 0},
        {arena::domain::Battle::State::Prepared, 0}
    };

    v.room = {
        static_cast<arena::domain::Room::State>(p.room_state()),
        p.room_revision()
    };
    v.battle = {
        static_cast<arena::domain::Battle::State>(p.battle_state()),
        p.battle_revision()
    };
    v.phase = static_cast<RuntimePhase>(p.phase());
    v.ready = {p.ready(0), p.ready(1)};
    v.online = {p.online(0), p.online(1)};
    TAKE(createdUnixMs, created_unix_ms);
    TAKE(startedUnixMs, started_unix_ms);
    TAKE(waitedTicks, waited_ticks);
    TAKE(startedTick, started_tick);
    TAKE(finishedTick, finished_tick);
    TAKE(playedTicks, played_ticks);
    if (p.has_result())
        v.result = take(p.result());
    if (p.has_expires_at_nanos())
        v.expiresAtNanos = p.expires_at_nanos();
    TAKE(persistenceBlocked, persistence_blocked);
    return v;
}
void put(const RuntimeCounters& v, pb::Counters& p)
{
    SCALAR(ticks, ticks);
    SCALAR(applied, applied);
    SCALAR(rejected, rejected);
    SCALAR(duplicates, duplicates);
    SCALAR(battlesStarted, battles_started);
    SCALAR(battlesCompleted, battles_completed);
    SCALAR(battlesAborted, battles_aborted);
    SCALAR(settlementIntents, settlement_intents);
    SCALAR(settlementsCommitted, settlements_committed);
    SCALAR(settlementsFailed, settlements_failed);
}
RuntimeCounters take(const pb::Counters& p)
{
    RuntimeCounters v;
    TAKE(ticks, ticks);
    TAKE(applied, applied);
    TAKE(rejected, rejected);
    TAKE(duplicates, duplicates);
    TAKE(battlesStarted, battles_started);
    TAKE(battlesCompleted, battles_completed);
    TAKE(battlesAborted, battles_aborted);
    TAKE(settlementIntents, settlement_intents);
    TAKE(settlementsCommitted, settlements_committed);
    TAKE(settlementsFailed, settlements_failed);
    return v;
}
#undef SCALAR
#undef TAKE
pb::RuntimeImage imageMessage(const RuntimeImage& v)
{
    pb::RuntimeImage p;
    p.set_version(1);
    put(v.policy, *p.mutable_policy());
    put(v.actor, *p.mutable_actor());
    if (v.managed)
        put(*v.managed, *p.mutable_managed());
    put(v.counters, *p.mutable_counters());
    p.set_logical_cutoff(v.logicalCutoff);
    p.set_known_since_tick(v.knownSinceTick);
    p.set_complete_counter_history(v.completeCounterHistory);
    return p;
}
RuntimeImage imageValue(const pb::RuntimeImage& p)
{
    require(p.version() == 1, CodecError::Version);
    require(
        p.has_policy() && p.has_actor() && p.has_counters() &&
        p.has_complete_counter_history()
    );
    RuntimeImage v{
        take(p.policy()),
        take(p.actor()),
        std::nullopt,
        {}
    };
    if (p.has_managed())
        v.managed = take(p.managed());
    v.counters = take(p.counters());
    v.logicalCutoff = p.logical_cutoff();
    v.knownSinceTick = p.known_since_tick();
    v.completeCounterHistory = p.complete_counter_history();
    return v;
}
pb::Command commandMessage(const RuntimeCommand& v)
{
    require(static_cast<unsigned>(v.kind) <= 5);
    pb::Command p;
    p.set_version(1);
    p.set_kind(static_cast<pb::CommandKind>(v.kind));
    if (v.caller)
        put(*v.caller, *p.mutable_caller());
    p.set_incarnation(v.incarnation);
    p.set_sequence(v.sequence);
    p.set_match_id(v.matchId);
    p.set_dx(v.dx);
    p.set_dy(v.dy);
    p.set_ready(v.ready);
    p.set_committed(v.committed);
    return p;
}
RuntimeCommand commandValue(const pb::Command& p)
{
    require(p.version() == 1, CodecError::Version);
    RuntimeCommand v;
    v.kind = static_cast<RuntimeCommandKind>(p.kind());
    if (p.has_caller())
        v.caller = take(p.caller());
    v.incarnation = p.incarnation();
    v.sequence = p.sequence();
    v.matchId = p.match_id();
    v.dx = p.dx();
    v.dy = p.dy();
    v.ready = p.ready();
    v.committed = p.committed();
    return v;
}
/** @brief Compares canonical input tuples without evaluating their
 * outcome. */
auto inputKey(const Input& v)
{
    return std::tuple(
        v.member.playerId.value(),
        v.member.sessionId.value(),
        v.member.epoch,
        v.sequence,
        v.dx,
        v.dy
    );
}
} // namespace

std::expected<void, CodecError> parseMessage(
    std::string_view bytes,
    google::protobuf::Message& output,
    std::size_t maximum
)
{
    const auto parsed = guarded(
        [&]
        {
            require(!bytes.empty());
            require(
                maximum <= kMaxPayloadBytes &&
                    bytes.size() <= maximum,
                CodecError::Capacity
            );
            auto temporary = std::unique_ptr<
                google::protobuf::Message>(output.New());
            google::protobuf::io::CodedInputStream input(
                reinterpret_cast<const std::uint8_t*>(bytes.data()),
                static_cast<int>(bytes.size())
            );
            input.SetTotalBytesLimit(static_cast<int>(maximum));
            input.SetRecursionLimit(16);
            require(
                temporary->ParseFromCodedStream(&input) &&
                input.ConsumedEntireMessage() && known(*temporary)
            );
            require(
                temporary->SerializeAsString() == bytes,
                CodecError::NonCanonical
            );
            output.GetReflection()->Swap(&output, temporary.get());
            return true;
        }
    );
    if (!parsed)
        return std::unexpected(parsed.error());
    return {};
}
std::expected<std::string, CodecError> serializeMessage(
    const google::protobuf::Message& message, std::size_t maximum
)
{
    return guarded(
        [&]
        {
            require(known(message));
            require(
                maximum <= kMaxPayloadBytes &&
                    message.ByteSizeLong() <= maximum,
                CodecError::Capacity
            );
            std::string result;
            require(message.SerializeToString(&result));
            return result;
        }
    );
}
std::expected<std::string, CodecError>
imageFingerprint(const ImageIdentity& id, std::string_view opaque)
{
    return guarded(
        [&]
        {
            require(id.roomId && id.incarnation && !opaque.empty());
            require(
                opaque.size() <= kMaxImageBytes,
                CodecError::Capacity
            );
            auto context = std::
                unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>(
                    EVP_MD_CTX_new(),
                    EVP_MD_CTX_free
                );
            require(bool(context), CodecError::Unavailable);
            require(
                EVP_DigestInit_ex(
                    context.get(),
                    EVP_sha256(),
                    nullptr
                ) == 1,
                CodecError::Unavailable
            );
            constexpr std::string_view domain = "arena-room-image/v2";
            require(
                EVP_DigestUpdate(
                    context.get(),
                    domain.data(),
                    domain.size()
                ) == 1,
                CodecError::Unavailable
            );
            for (const auto value :
                 {id.roomId,
                  id.incarnation,
                  id.physicalTick,
                  id.checksum,
                  id.cursor,
                  id.logicalTick})
            {
                std::array<unsigned char, 8> bytes{};
                for (unsigned i = 0; i < 8; ++i)
                    bytes[7 - i] = static_cast<unsigned char>(
                        value >> (i * 8)
                    );
                require(
                    EVP_DigestUpdate(
                        context.get(),
                        bytes.data(),
                        bytes.size()
                    ) == 1,
                    CodecError::Unavailable
                );
            }
            require(
                EVP_DigestUpdate(
                    context.get(),
                    opaque.data(),
                    opaque.size()
                ) == 1,
                CodecError::Unavailable
            );
            std::array<unsigned char, 32> digest{};
            unsigned length = 0;
            require(
                EVP_DigestFinal_ex(
                    context.get(),
                    digest.data(),
                    &length
                ) == 1 &&
                    length == 32,
                CodecError::Unavailable
            );
            constexpr char hex[] = "0123456789abcdef";
            std::string result;
            result.reserve(64);
            for (auto byte : digest)
            {
                result.push_back(hex[byte >> 4]);
                result.push_back(hex[byte & 15]);
            }
            return result;
        }
    );
}
std::expected<std::string, CodecError>
encodeImage(const RuntimeImage& v)
{
    auto prepared = guarded([&] { return imageMessage(v); });
    if (!prepared)
        return std::unexpected(prepared.error());
    return serializeMessage(*prepared, kMaxImageBytes);
}
std::expected<RuntimeImage, CodecError>
decodeImage(std::string_view bytes)
{
    pb::RuntimeImage p;
    auto parsed = parseMessage(bytes, p, kMaxImageBytes);
    if (!parsed)
        return std::unexpected(parsed.error());
    return guarded([&] { return imageValue(p); });
}
std::expected<std::string, CodecError>
encodeCommand(const RuntimeCommand& v)
{
    auto p = guarded([&] { return commandMessage(v); });
    if (!p)
        return std::unexpected(p.error());
    return serializeMessage(*p, 4096);
}
std::expected<RuntimeCommand, CodecError>
decodeCommand(std::string_view bytes)
{
    pb::Command p;
    auto parsed = parseMessage(bytes, p, 4096);
    if (!parsed)
        return std::unexpected(parsed.error());
    return guarded([&] { return commandValue(p); });
}
std::expected<std::string, CodecError>
encodeCutoff(const RuntimeCutoff& v)
{
    auto p = guarded(
        [&]
        {
            require(v.commands.empty());
            require(
                v.roster.size() <= kMaxMembers &&
                    v.currentAuthorizations.size() <=
                        maxCommands + kMaxMembers,
                CodecError::Capacity
            );
            pb::Cutoff p;
            p.set_version(1);
            for (const auto& a : v.roster)
            {
                require(a.member.epoch);
                put(a, *p.add_roster());
            }
            for (const auto& a : v.currentAuthorizations)
                put(a, *p.add_current_authorizations());
            p.set_now_nanos(v.nowNanos);
            p.set_wall_millis(v.wallMillis);
            return p;
        }
    );
    if (!p)
        return std::unexpected(p.error());
    return serializeMessage(*p, kMaxImageBytes);
}
std::expected<RuntimeCutoff, CodecError>
decodeCutoff(std::string_view bytes)
{
    pb::Cutoff p;
    auto parsed = parseMessage(bytes, p, kMaxImageBytes);
    if (!parsed)
        return std::unexpected(parsed.error());
    return guarded(
        [&]
        {
            require(p.version() == 1, CodecError::Version);
            require(
                p.roster_size() <= static_cast<int>(kMaxMembers) &&
                    p.current_authorizations_size() <=
                        static_cast<int>(maxCommands + kMaxMembers),
                CodecError::Capacity
            );
            RuntimeCutoff v;
            for (const auto& a : p.roster())
            {
                require(a.has_member() && a.member().epoch());
                v.roster.push_back(take(a));
            }
            for (const auto& a : p.current_authorizations())
                v.currentAuthorizations.push_back(take(a));
            v.nowNanos = p.now_nanos();
            v.wallMillis = p.wall_millis();
            return v;
        }
    );
}
std::expected<std::string, CodecError> encodeTick(const TickRecord& v)
{
    auto p = guarded(
        [&]
        {
            require(v.tick);
            require(
                v.members.size() <= kMaxMembers &&
                    v.inputs.size() <= maxCommands,
                CodecError::Capacity
            );
            pb::TickRecord p;
            p.set_version(1);
            p.set_tick(v.tick);
            p.set_checksum(v.checksum);
            std::uint64_t previous = 0;
            for (const auto& m : v.members)
            {
                require(m.playerId.value() > previous);
                previous = m.playerId.value();
                put(m, *p.add_members());
            }
            require(
                std::is_sorted(
                    v.inputs.begin(),
                    v.inputs.end(),
                    [](const auto& a, const auto& b)
                    { return inputKey(a) < inputKey(b); }
                )
            );
            for (const auto& i : v.inputs)
            {
                auto& q = *p.add_inputs();
                put(i.member, *q.mutable_member());
                q.set_sequence(i.sequence);
                q.set_dx(i.dx);
                q.set_dy(i.dy);
            }
            return p;
        }
    );
    if (!p)
        return std::unexpected(p.error());
    return serializeMessage(*p, kMaxImageBytes);
}
std::expected<TickRecord, CodecError>
decodeTick(std::string_view bytes)
{
    pb::TickRecord p;
    auto parsed = parseMessage(bytes, p, kMaxImageBytes);
    if (!parsed)
        return std::unexpected(parsed.error());
    return guarded(
        [&]
        {
            require(p.version() == 1, CodecError::Version);
            require(p.tick());
            require(
                p.members_size() <= static_cast<int>(kMaxMembers) &&
                    p.inputs_size() <=
                        static_cast<int>(maxCommands + kMaxMembers),
                CodecError::Capacity
            );
            TickRecord v{p.tick(), {}, {}, p.checksum()};
            for (const auto& m : p.members())
                v.members.push_back(take(m));
            for (const auto& i : p.inputs())
            {
                require(i.has_member());
                v.inputs.push_back(
                    {take(i.member()), i.sequence(), i.dx(), i.dy()}
                );
            }
            auto checked = encodeTick(v);
            require(
                bool(checked),
                checked ? CodecError::Invalid : checked.error()
            );
            return v;
        }
    );
}
std::expected<pb::Image, CodecError>
packImage(const RuntimeImage& v, std::uint64_t cursor)
{
    try
    {
        auto bytes = encodeImage(v);
        if (!bytes)
            return std::unexpected(bytes.error());
        const ImageIdentity id{
            v.actor.roomId.value(),
            v.actor.incarnation,
            v.actor.tick,
            v.actor.checksum,
            cursor,
            v.logicalCutoff
        };
        auto proof = imageFingerprint(id, *bytes);
        if (!proof)
            return std::unexpected(proof.error());
        pb::Image p;
        p.set_version(1);
        p.set_room_id(id.roomId);
        p.set_incarnation(id.incarnation);
        p.set_physical_tick(id.physicalTick);
        p.set_checksum(id.checksum);
        p.set_cursor(id.cursor);
        p.set_logical_tick(id.logicalTick);
        p.set_opaque(std::move(*bytes));
        p.set_fingerprint(std::move(*proof));
        return p;
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(CodecError::Capacity);
    }
    catch (...)
    {
        return std::unexpected(CodecError::Invalid);
    }
}
std::expected<RuntimeImage, CodecError>
unpackImage(const pb::Image& p)
{
    try
    {
        if (!known(p))
            return std::unexpected(CodecError::Invalid);
        if (p.version() != 1)
            return std::unexpected(CodecError::Version);
        auto value = decodeImage(p.opaque());
        if (!value)
            return std::unexpected(value.error());
        if (p.room_id() != value->actor.roomId.value() ||
            p.incarnation() != value->actor.incarnation ||
            p.physical_tick() != value->actor.tick ||
            p.checksum() != value->actor.checksum ||
            p.logical_tick() != value->logicalCutoff)
            return std::unexpected(CodecError::Invalid);
        auto proof = imageFingerprint(
            {p.room_id(),
             p.incarnation(),
             p.physical_tick(),
             p.checksum(),
             p.cursor(),
             p.logical_tick()},
            p.opaque()
        );
        if (!proof)
            return std::unexpected(proof.error());
        if (*proof != p.fingerprint())
            return std::unexpected(CodecError::Invalid);
        return value;
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(CodecError::Capacity);
    }
    catch (...)
    {
        return std::unexpected(CodecError::Invalid);
    }
}
std::expected<pb::Policy, CodecError>
packPolicy(const RuntimePolicy& v)
{
    return guarded(
        [&]
        {
            pb::Policy p;
            put(v, p);
            return p;
        }
    );
}
std::expected<RuntimePolicy, CodecError>
unpackPolicy(const pb::Policy& p)
{
    return guarded(
        [&]
        {
            require(known(p));
            return take(p);
        }
    );
}
std::expected<pb::Allocation, CodecError>
packAllocation(const RuntimeAllocation& v)
{
    return guarded(
        [&]
        {
            pb::Allocation p;
            put(v, p);
            return p;
        }
    );
}
std::expected<RuntimeAllocation, CodecError>
unpackAllocation(const pb::Allocation& p)
{
    return guarded(
        [&]
        {
            require(known(p));
            return take(p);
        }
    );
}
std::expected<std::string, CodecError>
encodeReplayLog(const ReplayLog& v)
{
    auto prepared = guarded(
        [&]
        {
            require(v.ticks.size() <= 65536, CodecError::Capacity);
            pb::ReplayLog p;
            p.set_version(1);
            auto* c = p.mutable_config();
            require(
                v.config.maxMembers <= kMaxMembers &&
                    v.config.maxInputsPerTick <= maxCommands &&
                    v.config.historyTicks <= 65536,
                CodecError::Capacity
            );
            c->set_max_members(
                static_cast<std::uint32_t>(v.config.maxMembers)
            );
            c->set_max_inputs_per_tick(
                static_cast<std::uint32_t>(v.config.maxInputsPerTick)
            );
            c->set_history_ticks(
                static_cast<std::uint32_t>(v.config.historyTicks)
            );
            c->set_position_limit(v.config.positionLimit);
            put(v.checkpoint, *p.mutable_checkpoint());
            for (const auto& tick : v.ticks)
            {
                auto bytes = encodeTick(tick);
                require(
                    bool(bytes),
                    bytes ? CodecError::Invalid : bytes.error()
                );
                auto parsed = parseMessage(
                    *bytes,
                    *p.add_ticks(),
                    kMaxImageBytes
                );
                require(
                    bool(parsed),
                    parsed ? CodecError::Invalid : parsed.error()
                );
                require(
                    p.ByteSizeLong() <= kMaxImageBytes,
                    CodecError::Capacity
                );
            }
            return p;
        }
    );
    if (!prepared)
        return std::unexpected(prepared.error());
    return serializeMessage(*prepared, kMaxImageBytes);
}
std::expected<ReplayLog, CodecError>
decodeReplayLog(std::string_view bytes)
{
    pb::ReplayLog p;
    auto parsed = parseMessage(bytes, p, kMaxImageBytes);
    if (!parsed)
        return std::unexpected(parsed.error());
    return guarded(
        [&]
        {
            require(p.version() == 1, CodecError::Version);
            require(p.has_config() && p.has_checkpoint());
            require(p.ticks_size() <= 65536, CodecError::Capacity);
            ReplayLog v{
                {p.config().max_members(),
                 p.config().max_inputs_per_tick(),
                 p.config().history_ticks(),
                 p.config().position_limit()},
                take(p.checkpoint()),
                {}
            };
            for (const auto& tick : p.ticks())
            {
                auto encoded = serializeMessage(tick, kMaxImageBytes);
                require(
                    bool(encoded),
                    encoded ? CodecError::Invalid : encoded.error()
                );
                auto t = decodeTick(*encoded);
                require(bool(t), t ? CodecError::Invalid : t.error());
                v.ticks.push_back(std::move(*t));
            }
            auto checked = encodeReplayLog(v);
            require(
                bool(checked),
                checked ? CodecError::Invalid : checked.error()
            );
            return v;
        }
    );
}
} // namespace service::room::wire