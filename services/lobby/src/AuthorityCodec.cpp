/** @file
 * @brief Maps bounded version-one durable protobuf state to owned
 * business values.
 */
#include "lobby/AuthorityCodec.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <new>
#include <ratio>
#include <tuple>
#include <type_traits>
#include <utility>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "lobby_state.pb.h"

namespace service::lobby
{
namespace
{
namespace wire = arena::lobby_state;
namespace match = service::match;
namespace room = service::room;
using Clock = std::chrono::steady_clock;
static_assert(
    std::ratio_equal_v<Clock::period, std::nano> &&
        std::is_signed_v<Clock::rep> &&
        std::numeric_limits<Clock::rep>::digits == 63,
    "Lobby durable clocks require Linux signed 64-bit monotonic "
    "nanoseconds"
);
static_assert(
    std::is_same_v<Clock, match::Clock> &&
    std::is_same_v<Clock, arena::runtime::OwnerClock>
);

struct Rejected
{
    CodecError error;
};
void require(bool valid)
{
    if (!valid)
        throw Rejected{CodecError::Invalid};
}
void capacity(bool valid)
{
    if (!valid)
        throw Rejected{CodecError::Capacity};
}
struct Budget
{
    std::size_t items{0}, messages{0};
    void collection(std::size_t count)
    {
        capacity(count <= authorityCodecMaxItems - items);
        items += count;
    }
};
std::size_t sizeValue(std::uint64_t value)
{
    require(value <= std::numeric_limits<std::size_t>::max());
    return static_cast<std::size_t>(value);
}
std::uint64_t identity(std::uint64_t value)
{
    require(value != 0);
    return value;
}
Clock::time_point clockValue(const wire::ClockValue& value)
{
    return Clock::time_point{Clock::duration{value.nanoseconds()}};
}
void clockValue(Clock::time_point value, wire::ClockValue* encoded)
{
    encoded->set_nanoseconds(value.time_since_epoch().count());
}
std::chrono::milliseconds milliseconds(std::int64_t value)
{
    using Rep = std::chrono::milliseconds::rep;
    static_assert(
        std::is_signed_v<Rep> &&
        std::numeric_limits<Rep>::digits == 63
    );
    return std::chrono::milliseconds{value};
}
std::optional<Clock::time_point>
optionalClock(bool present, const wire::ClockValue& value)
{
    return present ? std::optional(clockValue(value)) : std::nullopt;
}
std::optional<std::uint64_t>
optionalId(bool present, const wire::UInt64Slot& value)
{
    return present ? std::optional(identity(value.value()))
                   : std::nullopt;
}
template <class Id>
std::optional<Id>
strongOptional(bool present, const wire::UInt64Slot& value)
{
    return present ? std::optional(Id{identity(value.value())})
                   : std::nullopt;
}
template <class WireEnum, class Enum>
WireEnum toEnum(Enum value, int maximum)
{
    const auto number = static_cast<int>(value);
    require(number >= 0 && number <= maximum);
    return static_cast<WireEnum>(number);
}
template <class Enum, class WireEnum>
Enum fromEnum(WireEnum value, int maximum)
{
    const auto number = static_cast<int>(value);
    require(number >= 0 && number <= maximum);
    return static_cast<Enum>(number);
}

/** @brief Bounds structure and validates wire types before protobuf
 * allocates objects. */
struct Reader
{
    std::string_view bytes;
    std::size_t offset{0};
    bool empty() const
    {
        return offset == bytes.size();
    }
    std::uint64_t varint()
    {
        std::uint64_t result = 0;
        for (unsigned i = 0; i < 10; ++i)
        {
            require(offset < bytes.size());
            const auto byte = static_cast<unsigned char>(
                bytes[offset++]
            );
            require(i != 9 || byte <= 1);
            result |= static_cast<std::uint64_t>(byte & 127)
                      << (7 * i);
            if (!(byte & 128))
            {
                require(i == 0 || byte != 0);
                return result;
            }
        }
        throw Rejected{CodecError::Invalid};
    }
    std::string_view length()
    {
        const auto count = varint();
        require(count <= bytes.size() - offset);
        const auto result = bytes.substr(
            offset,
            static_cast<std::size_t>(count)
        );
        offset += static_cast<std::size_t>(count);
        return result;
    }
    void fixed(std::size_t count)
    {
        require(count <= bytes.size() - offset);
        offset += count;
    }
};
void scalar(
    Reader& reader,
    const google::protobuf::FieldDescriptor& field,
    unsigned wireType
)
{
    using Field = google::protobuf::FieldDescriptor;
    switch (field.type())
    {
    case Field::TYPE_UINT64:
    case Field::TYPE_SINT64:
        require(wireType == 0);
        reader.varint();
        return;
    case Field::TYPE_UINT32:
    case Field::TYPE_SINT32:
        require(wireType == 0);
        require(reader.varint() <= UINT32_MAX);
        return;
    case Field::TYPE_BOOL:
        require(wireType == 0);
        require(reader.varint() <= 1);
        return;
    case Field::TYPE_ENUM:
    {
        require(wireType == 0);
        const auto value = reader.varint();
        require(
            value <= INT32_MAX &&
            field.enum_type()->FindValueByNumber(
                static_cast<int>(value)
            )
        );
        return;
    }
    case Field::TYPE_FIXED64:
        require(wireType == 1);
        reader.fixed(8);
        return;
    default:
        throw Rejected{CodecError::Invalid};
    }
}
void preflight(
    std::string_view bytes,
    const google::protobuf::Descriptor& descriptor,
    Budget& budget,
    unsigned depth = 0
)
{
    capacity(depth <= 16 && budget.messages < authorityCodecMaxItems);
    ++budget.messages;
    require(descriptor.field_count() <= 64);
    std::array<bool, 64> seen{};
    Reader reader{bytes};
    while (!reader.empty())
    {
        const auto tag = reader.varint();
        require(tag <= UINT32_MAX && (tag >> 3) != 0);
        const auto* field = descriptor.FindFieldByNumber(
            static_cast<int>(tag >> 3)
        );
        require(field != nullptr); // Unknown durable fields require
                                   // an explicit schema migration.
        const auto type = static_cast<unsigned>(tag & 7);
        if (!field->is_repeated())
        {
            require(!seen[field->index()]);
            seen[field->index()] = true;
        }
        if (field->type() ==
            google::protobuf::FieldDescriptor::TYPE_MESSAGE)
        {
            require(type == 2);
            if (field->is_repeated())
                budget.collection(1);
            preflight(
                reader.length(),
                *field->message_type(),
                budget,
                depth + 1
            );
        }
        else if (
            field->is_repeated() && field->is_packable() && type == 2
        )
        {
            Reader packed{reader.length()};
            while (!packed.empty())
            {
                budget.collection(1);
                scalar(packed, *field, 0);
            }
        }
        else
        {
            if (field->is_repeated())
                budget.collection(1);
            scalar(reader, *field, type);
        }
    }
}
template <class Message> Message parse(std::string_view bytes)
{
    capacity(bytes.size() <= authorityCodecMaxBytes);
    require(!bytes.empty());
    Budget budget;
    preflight(bytes, *Message::descriptor(), budget);
    Message message;
    require(message.ParseFromArray(
        bytes.data(),
        static_cast<int>(bytes.size())
    ));
    return message;
}
template <class Message> std::string serialize(const Message& message)
{
    const auto size = message.ByteSizeLong();
    capacity(size <= authorityCodecMaxBytes);
    std::string result;
    require(message.SerializeToString(&result));
    Budget budget;
    preflight(result, *Message::descriptor(), budget);
    return result;
}
void version(std::uint32_t value)
{
    if (value != 1)
        throw Rejected{CodecError::UnsupportedVersion};
}

void encode(const SessionBinding& v, wire::SessionBinding* w, Budget&)
{
    w->set_player_id(v.playerId.value());
    w->set_connection_id(v.connectionId);
    w->set_epoch(v.epoch);
}
SessionBinding decode(const wire::SessionBinding& w, Budget&)
{
    return {
        arena::domain::PlayerId{identity(w.player_id())},
        identity(w.connection_id()),
        identity(w.epoch())
    };
}
void encode(const OnlineSnapshot& v, wire::OnlineSnapshot* w, Budget&)
{
    w->set_player_id(v.playerId.value());
    w->set_session_id(v.sessionId.value());
    w->set_connection_id(v.connectionId);
    w->set_epoch(v.epoch);
    w->set_presence(toEnum<wire::Presence>(v.presence, 1));
    if (v.roomId)
        w->mutable_room_id()->set_value(v.roomId->value());
    if (v.graceDeadline)
        clockValue(*v.graceDeadline, w->mutable_grace_deadline());
    w->set_room_epoch(v.roomEpoch);
}
OnlineSnapshot decode(const wire::OnlineSnapshot& w, Budget&)
{
    return {
        arena::domain::PlayerId{identity(w.player_id())},
        arena::domain::SessionId{identity(w.session_id())},
        w.connection_id(),
        identity(w.epoch()),
        fromEnum<Presence>(w.presence(), 1),
        strongOptional<arena::domain::RoomId>(
            w.has_room_id(),
            w.room_id()
        ),
        optionalClock(w.has_grace_deadline(), w.grace_deadline()),
        w.room_epoch()
    };
}
void encode(
    const ManagedRoom& v, wire::ManagedRoom* w, Budget& budget
)
{
    w->set_match_id(v.matchId);
    w->set_room_id(v.roomId.value());
    budget.collection(v.members.size());
    for (const auto& member : v.members)
    {
        auto* m = w->add_members();
        m->set_player_id(member.playerId.value());
        m->set_session_id(member.sessionId.value());
        m->set_room_epoch(member.roomEpoch);
        m->set_initial_mmr(member.initialMmr);
    }
}
ManagedRoom decode(const wire::ManagedRoom& w, Budget& budget)
{
    require(w.members_size() == 2);
    budget.collection(2);
    const auto member = [](const wire::MatchRoomMember& m)
    {
        return MatchRoomMember{
            arena::domain::PlayerId{identity(m.player_id())},
            arena::domain::SessionId{identity(m.session_id())},
            identity(m.room_epoch()),
            m.initial_mmr()
        };
    };
    return {
        identity(w.match_id()),
        arena::domain::RoomId{identity(w.room_id())},
        {member(w.members(0)), member(w.members(1))}
    };
}
void encode(
    const RegistryCheckpoint& v,
    wire::RegistryCheckpoint* w,
    Budget& budget
)
{
    w->set_epoch_watermark(v.epochWatermark);
    w->set_session_watermark(v.sessionWatermark);
    w->set_stopped(v.stopped);
    budget.collection(v.entries.size());
    budget.collection(v.rooms.size());
    for (const auto& entry : v.entries)
    {
        auto* e = w->add_entries();
        encode(entry.snapshot, e->mutable_snapshot(), budget);
        encode(entry.binding, e->mutable_binding(), budget);
        e->set_expires_at(entry.expiresAt);
        e->set_lease_valid(entry.leaseValid);
    }
    for (const auto& room : v.rooms)
    {
        auto* r = w->add_rooms();
        r->set_room_id(room.roomId.value());
        budget.collection(room.players.size());
        for (const auto player : room.players)
            r->add_players(player.value());
        if (room.managed)
            encode(*room.managed, r->mutable_managed(), budget);
    }
}
RegistryCheckpoint
decode(const wire::RegistryCheckpoint& w, Budget& budget)
{
    RegistryCheckpoint v;
    v.epochWatermark = w.epoch_watermark();
    v.sessionWatermark = w.session_watermark();
    v.stopped = w.stopped();
    budget.collection(w.entries_size());
    budget.collection(w.rooms_size());
    for (const auto& e : w.entries())
    {
        require(e.has_snapshot() && e.has_binding());
        v.entries.push_back(
            {decode(e.snapshot(), budget),
             decode(e.binding(), budget),
             e.expires_at(),
             e.lease_valid()}
        );
    }
    for (const auto& r : w.rooms())
    {
        RegistryRoomCheckpoint value{
            arena::domain::RoomId{identity(r.room_id())},
            {},
            {}
        };
        budget.collection(r.players_size());
        for (const auto player : r.players())
            value.players.emplace_back(identity(player));
        if (r.has_managed())
            value.managed = decode(r.managed(), budget);
        v.rooms.push_back(std::move(value));
    }
    return v;
}

void encode(
    const match::QueueCheckpointPolicy& v,
    wire::QueuePolicy* w,
    Budget&
)
{
    w->set_initial_window(v.initialWindow);
    w->set_expansion_step(v.expansionStep);
    w->set_maximum_window(v.maximumWindow);
    w->set_expansion_interval_ms(v.expansionInterval.count());
    w->set_timeout_ms(v.timeout.count());
    w->set_retention_ms(v.retention.count());
    w->set_max_tickets(v.maxTickets);
    w->set_max_queued(v.maxQueued);
    w->set_comparisons_per_tick(v.comparisonsPerTick);
}
match::QueueCheckpointPolicy
decode(const wire::QueuePolicy& w, Budget&)
{
    return {
        w.initial_window(),
        w.expansion_step(),
        w.maximum_window(),
        milliseconds(w.expansion_interval_ms()),
        milliseconds(w.timeout_ms()),
        milliseconds(w.retention_ms()),
        sizeValue(w.max_tickets()),
        sizeValue(w.max_queued()),
        sizeValue(w.comparisons_per_tick())
    };
}
void encode(
    const match::QueueEntryCheckpoint& v, wire::QueueEntry* w, Budget&
)
{
    w->set_ticket_id(v.ticketId.value());
    w->set_player_id(v.playerId.value());
    w->set_session_id(v.sessionId.value());
    w->set_join_key(v.joinKey);
    w->set_mmr(v.mmr);
    w->mutable_lifecycle()->set_state(
        toEnum<wire::TicketState>(v.lifecycle.state, 5)
    );
    w->mutable_lifecycle()->set_revision(v.lifecycle.revision);
    clockValue(v.admitted, w->mutable_admitted());
    if (v.finished)
        clockValue(*v.finished, w->mutable_finished());
    if (v.reclaimFrom)
        clockValue(*v.reclaimFrom, w->mutable_reclaim_from());
    w->set_match_id(v.matchId);
    if (v.opponent)
        w->mutable_opponent()->set_value(v.opponent->value());
    if (v.opponentTicket)
        w->mutable_opponent_ticket()->set_value(
            v.opponentTicket->value()
        );
    w->set_active(v.active);
    if (v.roomId)
        w->mutable_room_id()->set_value(v.roomId->value());
}
match::QueueEntryCheckpoint decode(const wire::QueueEntry& w, Budget&)
{
    require(w.has_lifecycle() && w.has_admitted());
    return {
        match::TicketId{identity(w.ticket_id())},
        match::PlayerId{identity(w.player_id())},
        match::SessionId{identity(w.session_id())},
        identity(w.join_key()),
        w.mmr(),
        {fromEnum<match::TicketState>(w.lifecycle().state(), 5),
         w.lifecycle().revision()},
        clockValue(w.admitted()),
        optionalClock(w.has_finished(), w.finished()),
        optionalClock(w.has_reclaim_from(), w.reclaim_from()),
        w.match_id(),
        strongOptional<match::PlayerId>(
            w.has_opponent(),
            w.opponent()
        ),
        strongOptional<match::TicketId>(
            w.has_opponent_ticket(),
            w.opponent_ticket()
        ),
        w.active(),
        strongOptional<arena::domain::RoomId>(
            w.has_room_id(),
            w.room_id()
        )
    };
}
void encode(
    const match::QueueSearchCheckpoint& v,
    wire::QueueSearch* w,
    Budget&
)
{
    if (v.anchor)
        w->mutable_anchor()->set_value(*v.anchor);
    if (v.bestCandidate)
        w->mutable_best_candidate()->set_value(*v.bestCandidate);
    w->set_best_gap(v.bestGap);
    w->set_anchor_after(v.anchorAfter);
    w->set_candidate_after(v.candidateAfter);
    w->set_ceiling(v.ceiling);
    clockValue(v.time, w->mutable_time());
}
match::QueueSearchCheckpoint
decode(const wire::QueueSearch& w, Budget&)
{
    require(w.has_time());
    return {
        optionalId(w.has_anchor(), w.anchor()),
        optionalId(w.has_best_candidate(), w.best_candidate()),
        w.best_gap(),
        w.anchor_after(),
        w.candidate_after(),
        w.ceiling(),
        clockValue(w.time())
    };
}
void encode(
    const match::QueueMetrics& v,
    wire::QueueMetrics* w,
    Budget& budget
)
{
    w->set_joined(v.joined);
    w->set_replayed(v.replayed);
    w->set_cancelled(v.cancelled);
    w->set_expired(v.expired);
    w->set_matched_pairs(v.matchedPairs);
    w->set_rejected(v.rejected);
    w->set_released_pairs(v.releasedPairs);
    w->set_queued(v.queued);
    w->set_assigned_players(v.assignedPlayers);
    w->set_retained(v.retained);
    budget.collection(24);
    for (const auto x : v.matchedWait)
        w->add_matched_wait(x);
    for (const auto x : v.mmrGap)
        w->add_mmr_gap(x);
    for (const auto x : v.abandonedWait)
        w->add_abandoned_wait(x);
    w->set_total_matched_wait_ms(v.totalMatchedWaitMs);
    w->set_total_mmr_gap(v.totalMmrGap);
}
match::QueueMetrics
decode(const wire::QueueMetrics& w, Budget& budget)
{
    require(
        w.matched_wait_size() == 8 && w.mmr_gap_size() == 8 &&
        w.abandoned_wait_size() == 8
    );
    budget.collection(24);
    match::QueueMetrics v;
    v.joined = w.joined();
    v.replayed = w.replayed();
    v.cancelled = w.cancelled();
    v.expired = w.expired();
    v.matchedPairs = w.matched_pairs();
    v.rejected = w.rejected();
    v.releasedPairs = w.released_pairs();
    v.queued = sizeValue(w.queued());
    v.assignedPlayers = sizeValue(w.assigned_players());
    v.retained = sizeValue(w.retained());
    for (std::size_t i = 0; i < 8; ++i)
    {
        v.matchedWait[i] = w.matched_wait(i);
        v.mmrGap[i] = w.mmr_gap(i);
        v.abandonedWait[i] = w.abandoned_wait(i);
    }
    v.totalMatchedWaitMs = w.total_matched_wait_ms();
    v.totalMmrGap = w.total_mmr_gap();
    return v;
}
void encode(
    const match::QueueCheckpoint& v,
    wire::QueueCheckpoint* w,
    Budget& budget
)
{
    version(v.version);
    w->set_version(v.version);
    encode(v.policy, w->mutable_policy(), budget);
    budget.collection(v.entries.size());
    for (const auto& e : v.entries)
        encode(e, w->add_entries(), budget);
    if (v.lastNow)
        clockValue(*v.lastNow, w->mutable_last_now());
    w->set_last_ticket_id(v.lastTicketId);
    w->set_last_durable_id(v.lastDurableId);
    w->set_external_ids(v.externalIds);
    encode(v.search, w->mutable_search(), budget);
    encode(v.metrics, w->mutable_metrics(), budget);
}
match::QueueCheckpoint
decode(const wire::QueueCheckpoint& w, Budget& budget)
{
    version(w.version());
    require(w.has_policy() && w.has_search() && w.has_metrics());
    match::QueueCheckpoint v;
    v.version = w.version();
    v.policy = decode(w.policy(), budget);
    budget.collection(w.entries_size());
    for (const auto& e : w.entries())
        v.entries.push_back(decode(e, budget));
    v.lastNow = optionalClock(w.has_last_now(), w.last_now());
    v.lastTicketId = w.last_ticket_id();
    v.lastDurableId = w.last_durable_id();
    v.externalIds = w.external_ids();
    v.search = decode(w.search(), budget);
    v.metrics = decode(w.metrics(), budget);
    return v;
}

void encode(const room::ActorConfig& v, wire::ActorConfig* w, Budget&)
{
    w->set_max_members(v.maxMembers);
    w->set_max_inputs_per_tick(v.maxInputsPerTick);
    w->set_history_ticks(v.historyTicks);
    w->set_position_limit(v.positionLimit);
}
room::ActorConfig decode(const wire::ActorConfig& w, Budget&)
{
    return {
        sizeValue(w.max_members()),
        sizeValue(w.max_inputs_per_tick()),
        sizeValue(w.history_ticks()),
        w.position_limit()
    };
}
void encode(const room::Member& v, wire::Member* w, Budget&)
{
    w->set_player_id(v.playerId.value());
    w->set_session_id(v.sessionId.value());
    w->set_epoch(v.epoch);
}
room::Member decode(const wire::Member& w, Budget&)
{
    // A rejected input may intentionally contain epoch zero; roster
    // validation remains stricter than attempt validation.
    return {
        room::PlayerId{identity(w.player_id())},
        room::SessionId{identity(w.session_id())},
        w.epoch()
    };
}
void encode(const room::Input& v, wire::Input* w, Budget& budget)
{
    encode(v.member, w->mutable_member(), budget);
    w->set_sequence(v.sequence);
    w->set_dx(v.dx);
    w->set_dy(v.dy);
}
room::Input decode(const wire::Input& w, Budget& budget)
{
    require(w.has_member());
    return {decode(w.member(), budget), w.sequence(), w.dx(), w.dy()};
}
void encode(
    const room::PlayerState& v, wire::PlayerState* w, Budget& budget
)
{
    encode(v.member, w->mutable_member(), budget);
    w->set_x(v.x);
    w->set_y(v.y);
    w->set_sequence(v.sequence);
    w->set_last_dx(v.lastDx);
    w->set_last_dy(v.lastDy);
}
room::PlayerState decode(const wire::PlayerState& w, Budget& budget)
{
    require(w.has_member());
    return {
        decode(w.member(), budget),
        w.x(),
        w.y(),
        w.sequence(),
        w.last_dx(),
        w.last_dy()
    };
}
void encode(
    const room::Snapshot& v, wire::ActorSnapshot* w, Budget& budget
)
{
    w->set_room_id(v.roomId.value());
    w->set_incarnation(v.incarnation);
    w->set_tick(v.tick);
    w->set_checksum(v.checksum);
    budget.collection(v.players.size());
    for (const auto& p : v.players)
        encode(p, w->add_players(), budget);
}
room::Snapshot decode(const wire::ActorSnapshot& w, Budget& budget)
{
    room::Snapshot v{
        room::RoomId{identity(w.room_id())},
        identity(w.incarnation()),
        w.tick(),
        w.checksum(),
        {}
    };
    budget.collection(w.players_size());
    for (const auto& p : w.players())
        v.players.push_back(decode(p, budget));
    return v;
}
bool inputLess(const room::Input& a, const room::Input& b)
{
    return std::tuple{
               a.member.playerId.value(),
               a.member.sessionId.value(),
               a.member.epoch,
               a.sequence,
               a.dx,
               a.dy
           } < std::tuple{
                   b.member.playerId.value(),
                   b.member.sessionId.value(),
                   b.member.epoch,
                   b.sequence,
                   b.dx,
                   b.dy
               };
}
void encode(
    const room::TickRecord& v, wire::TickRecord* w, Budget& budget
)
{
    w->set_tick(v.tick);
    w->set_checksum(v.checksum);
    budget.collection(v.members.size());
    budget.collection(v.inputs.size());
    for (const auto& m : v.members)
        encode(m, w->add_members(), budget);
    for (const auto& i : v.inputs)
        encode(i, w->add_inputs(), budget);
}
room::TickRecord decode(const wire::TickRecord& w, Budget& budget)
{
    require(w.tick() != 0);
    room::TickRecord v{w.tick(), {}, {}, w.checksum()};
    budget.collection(w.members_size());
    budget.collection(w.inputs_size());
    for (const auto& m : w.members())
    {
        auto member = decode(m, budget);
        require(member.epoch != 0);
        require(
            v.members.empty() || v.members.back().playerId.value() <
                                     member.playerId.value()
        );
        v.members.push_back(member);
    }
    for (const auto& i : w.inputs())
    {
        auto input = decode(i, budget);
        require(
            v.inputs.empty() || !inputLess(input, v.inputs.back())
        );
        v.inputs.push_back(input);
    }
    return v;
}
void encode(const RoomPolicy& v, wire::RoomPolicy* w, Budget& budget)
{
    w->set_max_rooms(v.maxRooms);
    w->set_max_pending(v.maxPending);
    w->set_max_pending_per_room(v.maxPendingPerRoom);
    w->set_max_catch_up(v.maxCatchUp);
    encode(v.actor, w->mutable_actor(), budget);
    w->set_battle_duration_ticks(v.battleDurationTicks);
    w->set_ready_timeout_ticks(v.readyTimeoutTicks);
    w->set_result_retention_ms(v.resultRetention.count());
    w->set_persistence_admission_attempts(
        v.persistenceAdmissionAttempts
    );
    w->set_persistence_retry_base_ms(v.persistenceRetryBase.count());
    w->set_persistence_retry_max_ms(v.persistenceRetryMax.count());
    w->set_external_ticks(v.externalTicks);
    w->set_external_settlement(v.externalSettlement);
    w->set_callback_settlement(v.callbackSettlement);
    w->set_archives(v.archives);
}
RoomPolicy decode(const wire::RoomPolicy& w, Budget& budget)
{
    require(w.has_actor());
    return {
        sizeValue(w.max_rooms()),
        sizeValue(w.max_pending()),
        sizeValue(w.max_pending_per_room()),
        sizeValue(w.max_catch_up()),
        decode(w.actor(), budget),
        w.battle_duration_ticks(),
        w.ready_timeout_ticks(),
        milliseconds(w.result_retention_ms()),
        sizeValue(w.persistence_admission_attempts()),
        milliseconds(w.persistence_retry_base_ms()),
        milliseconds(w.persistence_retry_max_ms()),
        w.external_ticks(),
        w.external_settlement(),
        w.callback_settlement(),
        w.archives()
    };
}
void encode(
    const RoomSettlement& v, wire::RoomSettlement* w, Budget& budget
)
{
    w->set_result_id(v.resultId);
    w->set_match_id(v.matchId);
    w->set_room_id(v.roomId.value());
    w->set_final_tick(v.finalTick);
    w->set_final_checksum(v.finalChecksum);
    w->set_reason(toEnum<wire::SettlementReason>(v.reason, 2));
    w->set_winner_player_id(v.winnerPlayerId);
    budget.collection(v.players.size());
    for (const auto& p : v.players)
        encode(p, w->add_players(), budget);
    w->set_created_unix_ms(v.createdUnixMs);
    w->set_started_unix_ms(v.startedUnixMs);
    w->set_finished_unix_ms(v.finishedUnixMs);
    w->set_started_tick(v.startedTick);
    w->set_played_ticks(v.playedTicks);
}
RoomSettlement decode(const wire::RoomSettlement& w, Budget& budget)
{
    RoomSettlement v{
        identity(w.result_id()),
        identity(w.match_id()),
        arena::domain::RoomId{identity(w.room_id())},
        w.final_tick(),
        w.final_checksum(),
        fromEnum<SettlementReason>(w.reason(), 2),
        w.winner_player_id(),
        {}
    };
    budget.collection(w.players_size());
    for (const auto& p : w.players())
        v.players.push_back(decode(p, budget));
    v.createdUnixMs = w.created_unix_ms();
    v.startedUnixMs = w.started_unix_ms();
    v.finishedUnixMs = w.finished_unix_ms();
    v.startedTick = w.started_tick();
    v.playedTicks = w.played_ticks();
    return v;
}
void encode(
    const ManagedMatchCheckpoint& v,
    wire::ManagedMatch* w,
    Budget& budget
)
{
    encode(v.allocation, w->mutable_allocation(), budget);
    w->mutable_room()->set_state(
        toEnum<wire::RoomState>(v.room.state, 4)
    );
    w->mutable_room()->set_revision(v.room.revision);
    w->mutable_battle()->set_state(
        toEnum<wire::BattleState>(v.battle.state, 3)
    );
    w->mutable_battle()->set_revision(v.battle.revision);
    w->set_phase(toEnum<wire::RoomPhase>(v.phase, 4));
    budget.collection(4);
    for (const auto r : v.ready)
        w->add_ready(r);
    for (const auto p : v.presence)
        w->add_presence(toEnum<wire::Presence>(p, 1));
    w->set_created_unix_ms(v.createdUnixMs);
    w->set_started_unix_ms(v.startedUnixMs);
    w->set_waited_ticks(v.waitedTicks);
    w->set_started_tick(v.startedTick);
    w->set_finished_tick(v.finishedTick);
    w->set_played_ticks(v.playedTicks);
    if (v.result)
        encode(*v.result, w->mutable_result(), budget);
    if (v.expires)
        clockValue(*v.expires, w->mutable_expires());
    if (v.retryAt)
        clockValue(*v.retryAt, w->mutable_retry_at());
    w->set_admission_attempts(v.admissionAttempts);
    w->set_persistence_accepted(v.persistenceAccepted);
    w->set_persistence_blocked(v.persistenceBlocked);
    require(v.persistenceOutcome >= 0 && v.persistenceOutcome <= 2);
    w->set_persistence_outcome(
        static_cast<std::uint32_t>(v.persistenceOutcome)
    );
}
ManagedMatchCheckpoint
decode(const wire::ManagedMatch& w, Budget& budget)
{
    require(
        w.has_allocation() && w.has_room() && w.has_battle() &&
        w.ready_size() == 2 && w.presence_size() == 2 &&
        w.persistence_outcome() <= 2
    );
    budget.collection(4);
    return {
        decode(w.allocation(), budget),
        {fromEnum<arena::domain::Room::State>(w.room().state(), 4),
         w.room().revision()},
        {fromEnum<arena::domain::Battle::State>(
             w.battle().state(),
             3
         ),
         w.battle().revision()},
        fromEnum<RoomPhase>(w.phase(), 4),
        {w.ready(0), w.ready(1)},
        {fromEnum<Presence>(w.presence(0), 1),
         fromEnum<Presence>(w.presence(1), 1)},
        w.created_unix_ms(),
        w.started_unix_ms(),
        w.waited_ticks(),
        w.started_tick(),
        w.finished_tick(),
        w.played_ticks(),
        w.has_result() ? std::optional(decode(w.result(), budget))
                       : std::nullopt,
        optionalClock(w.has_expires(), w.expires()),
        optionalClock(w.has_retry_at(), w.retry_at()),
        sizeValue(w.admission_attempts()),
        w.persistence_accepted(),
        w.persistence_blocked(),
        static_cast<int>(w.persistence_outcome())
    };
}
void encode(const RoomMetrics& v, wire::RoomMetrics* w, Budget&)
{
    w->set_actors(v.actors);
    w->set_ticks(v.ticks);
    w->set_applied(v.applied);
    w->set_rejected(v.rejected);
    w->set_duplicates(v.duplicates);
    w->set_skipped_wall_slots(v.skippedWallSlots);
    w->set_managed_matches(v.managedMatches);
    w->set_retained_results(v.retainedResults);
    w->set_battles_started(v.battlesStarted);
    w->set_battles_completed(v.battlesCompleted);
    w->set_battles_aborted(v.battlesAborted);
    w->set_persistence_submitted(v.persistenceSubmitted);
    w->set_persistence_rejected(v.persistenceRejected);
    w->set_persistence_failed(v.persistenceFailed);
    w->set_persistence_committed(v.persistenceCommitted);
    w->set_pending_settlements(v.pendingSettlements);
    w->set_blocked_settlements(v.blockedSettlements);
}
RoomMetrics decode(const wire::RoomMetrics& w, Budget&)
{
    RoomMetrics v;
    v.actors = sizeValue(w.actors());
    v.ticks = w.ticks();
    v.applied = w.applied();
    v.rejected = w.rejected();
    v.duplicates = w.duplicates();
    v.skippedWallSlots = w.skipped_wall_slots();
    v.managedMatches = sizeValue(w.managed_matches());
    v.retainedResults = sizeValue(w.retained_results());
    v.battlesStarted = w.battles_started();
    v.battlesCompleted = w.battles_completed();
    v.battlesAborted = w.battles_aborted();
    v.persistenceSubmitted = w.persistence_submitted();
    v.persistenceRejected = w.persistence_rejected();
    v.persistenceFailed = w.persistence_failed();
    v.persistenceCommitted = w.persistence_committed();
    v.pendingSettlements = sizeValue(w.pending_settlements());
    v.blockedSettlements = sizeValue(w.blocked_settlements());
    return v;
}
void encode(
    const RoomCheckpoint& v, wire::RoomCheckpoint* w, Budget& budget
)
{
    encode(v.policy, w->mutable_policy(), budget);
    budget.collection(v.actors.size());
    budget.collection(v.matches.size());
    for (const auto& a : v.actors)
        encode(a, w->add_actors(), budget);
    for (const auto& m : v.matches)
        encode(m, w->add_matches(), budget);
    encode(v.metrics, w->mutable_metrics(), budget);
    clockValue(v.deadline, w->mutable_deadline());
    w->set_running(v.running);
    w->set_started(v.started);
    w->set_accepting(v.accepting);
    w->set_incarnation_high_water(v.incarnationHighWater);
    w->set_counter_version(v.counterVersion);
    budget.collection(v.roomCounters.size());
    for (const auto& item : v.roomCounters)
    {
        auto* value = w->add_room_counters();
        value->set_room_id(item.roomId.value());
        value->set_known_since_tick(item.knownSinceTick);
        value->set_complete_history(item.completeHistory);
        encode(item.counters, value->mutable_counters(), budget);
    }
    if (v.counterVersion)
        encode(
            v.unattributedCounters,
            w->mutable_unattributed_counters(),
            budget
        );
}
RoomCheckpoint decode(const wire::RoomCheckpoint& w, Budget& budget)
{
    require(w.has_policy() && w.has_metrics() && w.has_deadline());
    RoomCheckpoint v{
        decode(w.policy(), budget),
        {},
        {},
        decode(w.metrics(), budget),
        clockValue(w.deadline()),
        w.running(),
        w.started(),
        w.accepting(),
        w.incarnation_high_water()
    };
    budget.collection(w.actors_size());
    budget.collection(w.matches_size());
    for (const auto& a : w.actors())
        v.actors.push_back(decode(a, budget));
    for (const auto& m : w.matches())
        v.matches.push_back(decode(m, budget));
    require(w.counter_version() <= 1);
    v.counterVersion = w.counter_version();
    budget.collection(w.room_counters_size());
    if (v.counterVersion)
    {
        require(w.has_unattributed_counters());
        v.unattributedCounters = decode(
            w.unattributed_counters(),
            budget
        );
    }
    else
        require(
            !w.room_counters_size() && !w.has_unattributed_counters()
        );
    for (const auto& item : w.room_counters())
    {
        require(item.has_counters());
        v.roomCounters.push_back(
            {arena::domain::RoomId{identity(item.room_id())},
             item.known_since_tick(),
             item.complete_history(),
             decode(item.counters(), budget)}
        );
    }
    return v;
}
wire::CoreCheckpoint encodeCore(const CoreCheckpoint& v)
{
    wire::CoreCheckpoint w;
    Budget budget;
    w.set_version(2);
    w.set_distributed_rooms(v.distributedRooms);
    if (v.distributedRooms)
        require(
            v.rooms.actors.empty() && v.rooms.matches.empty() &&
            v.rooms.roomCounters.empty()
        );
    encode(v.registry, w.mutable_registry(), budget);
    encode(v.queue, w.mutable_queue(), budget);
    encode(v.rooms, w.mutable_rooms(), budget);
    return w;
}
CoreCheckpoint decodeCore(const wire::CoreCheckpoint& w)
{
    if (w.version() != 1 && w.version() != 2)
        throw Rejected{CodecError::UnsupportedVersion};
    require(w.has_registry() && w.has_queue() && w.has_rooms());
    require(
        w.version() == 1 ? !w.has_distributed_rooms()
                         : w.has_distributed_rooms()
    );
    Budget budget;
    CoreCheckpoint value{
        decode(w.registry(), budget),
        decode(w.queue(), budget),
        decode(w.rooms(), budget),
        w.version() == 2 && w.distributed_rooms()
    };
    if (value.distributedRooms)
        require(
            value.rooms.actors.empty() &&
            value.rooms.matches.empty() &&
            value.rooms.roomCounters.empty()
        );
    return value;
}
wire::TickEnvelope encodeTick(const room::TickRecord& v)
{
    wire::TickEnvelope w;
    Budget budget;
    w.set_version(1);
    encode(v, w.mutable_record(), budget);
    return w;
}
room::TickRecord decodeTick(const wire::TickEnvelope& w)
{
    version(w.version());
    require(w.has_record());
    Budget budget;
    return decode(w.record(), budget);
}
wire::ReplayLog encodeReplay(const room::ReplayLog& v)
{
    wire::ReplayLog w;
    Budget budget;
    w.set_version(1);
    encode(v.config, w.mutable_config(), budget);
    encode(v.checkpoint, w.mutable_checkpoint(), budget);
    budget.collection(v.ticks.size());
    for (const auto& tick : v.ticks)
        encode(tick, w.add_ticks(), budget);
    return w;
}
room::ReplayLog decodeReplay(const wire::ReplayLog& w)
{
    version(w.version());
    require(w.has_config() && w.has_checkpoint());
    Budget budget;
    room::ReplayLog v{
        decode(w.config(), budget),
        decode(w.checkpoint(), budget),
        {}
    };
    budget.collection(w.ticks_size());
    std::uint64_t previous = v.checkpoint.tick;
    for (const auto& tick : w.ticks())
    {
        require(
            previous != UINT64_MAX && tick.tick() == previous + 1
        );
        v.ticks.push_back(decode(tick, budget));
        previous = tick.tick();
    }
    return v;
}
template <class Operation>
auto attempt(Operation operation)
    -> std::expected<decltype(operation()), CodecError>
{
    try
    {
        return operation();
    }
    catch (const Rejected& error)
    {
        return std::unexpected(error.error);
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
} // namespace

std::expected<std::string, CodecError>
encodeCoreCheckpoint(const CoreCheckpoint& value)
{
    return attempt(
        [&]
        {
            auto message = encodeCore(value);
            auto bytes = serialize(message);
            static_cast<void>(decodeCore(message));
            return bytes;
        }
    );
}
std::expected<CoreCheckpoint, CodecError>
decodeCoreCheckpoint(std::string_view bytes)
{
    return attempt(
        [&] { return decodeCore(parse<wire::CoreCheckpoint>(bytes)); }
    );
}
std::expected<std::string, CodecError>
encodeTickRecord(const room::TickRecord& value)
{
    return attempt(
        [&]
        {
            auto message = encodeTick(value);
            auto bytes = serialize(message);
            static_cast<void>(decodeTick(message));
            return bytes;
        }
    );
}
std::expected<room::TickRecord, CodecError>
decodeTickRecord(std::string_view bytes)
{
    return attempt(
        [&] { return decodeTick(parse<wire::TickEnvelope>(bytes)); }
    );
}
std::expected<std::string, CodecError>
encodeReplayLog(const room::ReplayLog& value)
{
    return attempt(
        [&]
        {
            auto message = encodeReplay(value);
            auto bytes = serialize(message);
            static_cast<void>(decodeReplay(message));
            return bytes;
        }
    );
}
std::expected<room::ReplayLog, CodecError>
decodeReplayLog(std::string_view bytes)
{
    return attempt(
        [&] { return decodeReplay(parse<wire::ReplayLog>(bytes)); }
    );
}
} // namespace service::lobby
