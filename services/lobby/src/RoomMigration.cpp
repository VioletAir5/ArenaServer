/** @file
 * @brief Encodes and plans bounded per-room coordinator transactions.
 */
#include "lobby/RoomMigration.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <google/protobuf/message.h>
#include <openssl/sha.h>

#include "lobby_state.pb.h"
#include "room/RoomWire.h"

namespace service::lobby
{
namespace
{
namespace wire = arena::lobby_state;
using Error = RoomDirectoryError;
constexpr std::size_t wireLimit = 4 * 1024 * 1024;
constexpr std::size_t itemLimit = 65536;
constexpr std::string_view prefix = "room-directory/";
constexpr std::string_view roomPrefix = "room-directory/room/";
constexpr std::string_view replayPrefix = "room-directory/replay/";
constexpr std::string_view metaKey = "room-directory/meta";
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

bool label(std::string_view value)
{
    return !value.empty() && value.size() <= 128 &&
           std::all_of(
               value.begin(),
               value.end(),
               [](unsigned char c) { return c >= 33 && c <= 126; }
           );
}
bool validImageProof(std::string_view value)
{
    return value.size() == 64 &&
           std::all_of(
               value.begin(),
               value.end(),
               [](char c)
               {
                   return (c >= '0' && c <= '9') ||
                          (c >= 'a' && c <= 'f');
               }
           );
}
bool identity(RoomIdentity id)
{
    return id.room && id.incarnation;
}
std::string suffix(RoomIdentity id)
{
    return std::to_string(id.room) + "/" +
           std::to_string(id.incarnation);
}
std::string roomKey(RoomIdentity id)
{
    return std::string(roomPrefix) + suffix(id);
}
std::string replayKey(RoomIdentity id, std::uint64_t first)
{
    return std::string(replayPrefix) + suffix(id) + "/" +
           std::to_string(first);
}
bool known(
    const google::protobuf::Message& value,
    std::size_t& items,
    int depth = 0
)
{
    if (depth > 32 || ++items > itemLimit)
        return false;
    const auto* r = value.GetReflection();
    if (r->GetUnknownFields(value).field_count())
        return false;
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    r->ListFields(value, &fields);
    for (const auto* f : fields)
        if (f->cpp_type() ==
            google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            if (f->is_repeated())
                for (int i = 0; i < r->FieldSize(value, f); ++i)
                {
                    if (!known(
                            r->GetRepeatedMessage(value, f, i),
                            items,
                            depth + 1
                        ))
                        return false;
                }
            else if (!known(
                         r->GetMessage(value, f),
                         items,
                         depth + 1
                     ))
                return false;
        }
    return true;
}
template <class T> bool parse(std::string_view bytes, T& message)
{
    if (bytes.empty() || bytes.size() > wireLimit ||
        !message.ParseFromArray(
            bytes.data(),
            static_cast<int>(bytes.size())
        ))
        return false;
    std::size_t items = 0;
    // Canonical re-encoding rejects duplicated scalars and alternate
    // encodings.
    return known(message, items) &&
           message.SerializeAsString() == bytes;
}
void encodeImage(
    const RoomWorkerImage& image, wire::DirectoryImage* out
)
{
    out->set_tick(image.tick);
    out->set_checksum(image.checksum);
    out->set_cursor(image.cursor);
    out->set_logical_tick(image.logicalTick);
    out->set_checkpoint(image.checkpoint);
}
RoomWorkerImage decodeImage(const wire::DirectoryImage& image)
{
    return {
        image.tick(),
        image.checksum(),
        image.cursor(),
        image.checkpoint(),
        image.logical_tick()
    };
}
void encodeOperation(
    const RoomOperation& op, wire::DirectoryOperation* out
)
{
    out->set_id(op.id);
    out->set_kind(static_cast<unsigned>(op.kind));
    out->set_tick(op.tick);
    out->set_payload(op.payload);
}
RoomOperation decodeOperation(const wire::DirectoryOperation& op)
{
    return {
        op.id(),
        static_cast<RoomIngressKind>(op.kind()),
        op.tick(),
        op.payload()
    };
}
std::string operationFingerprint(const RoomOperation& operation)
{
    wire::DirectoryOperation out;
    encodeOperation(operation, &out);
    const auto bytes = out.SerializeAsString();
    std::string hash(SHA256_DIGEST_LENGTH, '\0');
    if (!SHA256(
            reinterpret_cast<const unsigned char*>(bytes.data()),
            bytes.size(),
            reinterpret_cast<unsigned char*>(hash.data())
        ))
        throw std::runtime_error("operation fingerprint unavailable");
    return hash;
}
std::string encodeRoom(const RoomRouteRecord& room)
{
    wire::DirectoryRoom out;
    out.set_version(2);
    out.set_room_id(room.identity.room);
    out.set_incarnation(room.identity.incarnation);
    out.set_owner(room.owner);
    out.set_owner_epoch(room.ownerEpoch);
    encodeImage(room.image, out.mutable_image());
    out.set_next_sequence(room.nextSequence);
    out.set_admitted_tick(room.admittedTick);
    out.set_initial_tick(room.initialTick);
    out.set_lifecycle(static_cast<unsigned>(room.lifecycle));
    out.set_migration_watermark(room.migrationWatermark);
    for (const auto& input : room.inbox)
    {
        auto* row = out.add_inbox();
        row->set_sequence(input.sequence);
        encodeOperation(input.operation, row->mutable_operation());
    }
    for (const auto& [id, receipt] : room.receipts)
    {
        auto* row = out.add_receipts();
        row->set_id(id);
        row->set_sequence(receipt.sequence);
        row->set_fingerprint(receipt.fingerprint);
        row->set_expires_at(receipt.expiresAt);
        row->set_applied(receipt.applied);
        row->set_response(receipt.response);
    }
    if (room.handoff)
    {
        const auto& h = *room.handoff;
        auto* row = out.mutable_handoff();
        row->set_id(h.id);
        row->set_source(h.source);
        row->set_target(h.target);
        row->set_source_epoch(h.sourceEpoch);
        row->set_target_epoch(h.targetEpoch);
        row->set_abort_epoch(h.abortEpoch);
        row->set_cutoff(h.cutoff);
        row->set_deadline(h.deadline);
        row->set_phase(static_cast<unsigned>(h.phase));
        row->set_frozen_fingerprint(h.frozenFingerprint);
    }
    return out.SerializeAsString();
}
bool decodeRoom(std::string_view bytes, RoomRouteRecord& room)
{
    wire::DirectoryRoom in;
    if (!parse(bytes, in) || in.version() != 2 || !in.has_image())
        return false;
    room.identity = {in.room_id(), in.incarnation()};
    room.owner = in.owner();
    room.ownerEpoch = in.owner_epoch();
    room.image = decodeImage(in.image());
    room.nextSequence = in.next_sequence();
    room.admittedTick = in.admitted_tick();
    room.initialTick = in.initial_tick();
    if (in.lifecycle() >
        static_cast<unsigned>(RoomRouteLifecycle::Retired))
        return false;
    room.lifecycle = static_cast<RoomRouteLifecycle>(in.lifecycle());
    room.migrationWatermark = in.migration_watermark();
    for (const auto& row : in.inbox())
    {
        if (!row.has_operation() ||
            row.operation().kind() >
                static_cast<unsigned>(RoomIngressKind::Membership))
            return false;
        room.inbox.push_back(
            {row.sequence(), decodeOperation(row.operation())}
        );
    }
    for (const auto& row : in.receipts())
        if (!room.receipts
                 .emplace(
                     row.id(),
                     RoomReceipt{
                         row.sequence(),
                         row.fingerprint(),
                         row.expires_at(),
                         row.applied(),
                         row.response()
                     }
                 )
                 .second)
            return false;
    if (in.has_handoff())
    {
        const auto& h = in.handoff();
        if (h.phase() >
            static_cast<unsigned>(RoomMigrationPhase::Aborted))
            return false;
        room.handoff = RoomHandoff{
            h.id(),
            h.source(),
            h.target(),
            h.source_epoch(),
            h.target_epoch(),
            h.abort_epoch(),
            h.cutoff(),
            h.deadline(),
            static_cast<RoomMigrationPhase>(h.phase()),
            h.frozen_fingerprint()
        };
    }
    return encodeRoom(room) == bytes;
}
bool validImage(
    const RoomWorkerImage& image, const RoomDirectoryLimits& limits
)
{
    return !image.checkpoint.empty() &&
           image.checkpoint.size() <= limits.maxImageBytes;
}
bool validOperation(
    const RoomOperation& op, const RoomDirectoryLimits& limits
)
{
    if (!label(op.id) ||
        static_cast<unsigned>(op.kind) >
            static_cast<unsigned>(RoomIngressKind::Membership) ||
        op.payload.size() > limits.maxOperationBytes)
        return false;
    return op.kind == RoomIngressKind::Tick
               ? op.tick != 0
               : op.tick == 0 && !op.payload.empty();
}
bool terminal(RoomMigrationPhase phase)
{
    return phase == RoomMigrationPhase::Committed ||
           phase == RoomMigrationPhase::Aborted;
}
bool validRoom(
    const RoomRouteRecord& room, const RoomDirectoryLimits& limits
)
{
    if (!identity(room.identity) || !label(room.owner) ||
        !room.ownerEpoch || !validImage(room.image, limits) ||
        !room.nextSequence ||
        room.image.cursor >= room.nextSequence ||
        room.initialTick > room.image.tick ||
        room.image.logicalTick > room.admittedTick ||
        room.inbox.size() > limits.maxInbox ||
        room.receipts.size() > limits.maxReceipts ||
        static_cast<unsigned>(room.lifecycle) >
            static_cast<unsigned>(RoomRouteLifecycle::Retired) ||
        (room.lifecycle == RoomRouteLifecycle::Retired &&
         !room.inbox.empty()) ||
        room.nextSequence - room.image.cursor - 1 !=
            room.inbox.size())
        return false;
    std::set<std::uint64_t> sequences;
    for (const auto& [id, r] : room.receipts)
        if (!label(id) || !r.sequence ||
            r.sequence >= room.nextSequence ||
            !sequences.insert(r.sequence).second ||
            r.fingerprint.size() != SHA256_DIGEST_LENGTH ||
            r.expiresAt < 0 ||
            r.response.size() > limits.maxOperationBytes ||
            r.applied != (r.sequence <= room.image.cursor) ||
            (!r.applied && !r.response.empty()))
            return false;
    auto cursor = room.image.cursor;
    auto tick = room.image.logicalTick;
    for (const auto& input : room.inbox)
    {
        if (input.sequence != ++cursor ||
            !validOperation(input.operation, limits))
            return false;
        const auto receipt = room.receipts.find(input.operation.id);
        if (receipt == room.receipts.end() ||
            receipt->second.sequence != input.sequence ||
            receipt->second.fingerprint !=
                operationFingerprint(input.operation))
            return false;
        if (input.operation.kind == RoomIngressKind::Tick)
        {
            if (tick == maximum || input.operation.tick != ++tick)
                return false;
        }
    }
    if (tick != room.admittedTick)
        return false;
    if (!room.handoff)
        return room.migrationWatermark == 0;
    const auto& h = *room.handoff;
    if (room.lifecycle != RoomRouteLifecycle::Active &&
        !terminal(h.phase))
        return false;
    if (!h.id || h.id != room.migrationWatermark ||
        !label(h.source) || !label(h.target) ||
        h.source == h.target || !h.sourceEpoch ||
        h.sourceEpoch > maximum - 2 ||
        h.targetEpoch != h.sourceEpoch + 1 ||
        h.abortEpoch != h.sourceEpoch + 2 ||
        h.cutoff >= room.nextSequence || h.deadline <= 0)
        return false;
    if (h.phase == RoomMigrationPhase::Committed)
        return room.owner == h.target &&
               room.ownerEpoch == h.targetEpoch &&
               room.image.cursor >= h.cutoff &&
               validImageProof(h.frozenFingerprint);
    if (h.phase == RoomMigrationPhase::Aborted)
        return room.owner == h.source &&
               room.ownerEpoch == h.abortEpoch &&
               (h.frozenFingerprint.empty() ||
                validImageProof(h.frozenFingerprint));
    if (room.owner != h.source || room.ownerEpoch != h.sourceEpoch)
        return false;
    if (h.phase == RoomMigrationPhase::Preparing)
        return !h.cutoff && h.frozenFingerprint.empty();
    if (h.phase == RoomMigrationPhase::Freezing)
        return h.cutoff >= room.image.cursor &&
               h.frozenFingerprint.empty();
    return room.image.cursor == h.cutoff &&
           h.frozenFingerprint ==
               roomImageFingerprint(room.identity, room.image);
}
std::string
encodeReplay(RoomIdentity id, const RoomReplayExtent& extent)
{
    wire::DirectoryReplay out;
    out.set_version(2);
    out.set_room_id(id.room);
    out.set_incarnation(id.incarnation);
    out.set_first_tick(extent.firstTick);
    out.set_last_tick(extent.lastTick);
    out.set_payload(extent.bytes);
    return out.SerializeAsString();
}
std::string metadata(std::string_view boot)
{
    wire::DirectoryMeta out;
    out.set_version(2);
    out.set_boot_id(std::string(boot));
    return out.SerializeAsString();
}
} // namespace

std::uint64_t
RoomMigration::Candidate::expectedRevision() const noexcept
{
    return m_expected;
}
const std::vector<AtomicJournal::Mutation>&
RoomMigration::Candidate::mutations() const noexcept
{
    return m_mutations;
}
const RoomDirectory&
RoomMigration::Candidate::planned() const noexcept
{
    return *m_planned;
}
RoomMigration::RoomMigration(
    RoomDirectoryLimits limits, std::string boot
)
    : m_limits(limits)
    , m_boot(std::move(boot))
{
    if (!label(m_boot) || !limits.maxRooms ||
        limits.maxRooms > itemLimit || !limits.maxInbox ||
        !limits.maxReceipts || limits.maxInbox > limits.maxReceipts ||
        limits.maxReceipts > itemLimit || !limits.maxOperationBytes ||
        limits.maxOperationBytes > wireLimit ||
        !limits.maxImageBytes ||
        limits.maxImageBytes > service::room::wire::kMaxImageBytes ||
        !limits.maxReplayBytes || limits.maxReplayBytes > wireLimit ||
        !limits.maxCommands || limits.maxCommands > itemLimit ||
        limits.receiptRetentionNanos <= 0 ||
        limits.receiptRetentionNanos > 3'600'000'000'000LL)
        throw std::invalid_argument(
            "invalid bounded room directory configuration"
        );
    // Root + image + optional handoff, two messages per pending
    // input, and one per receipt must fit the strict recovery node
    // budget.
    if (limits.maxReceipts > itemLimit - 3 ||
        limits.maxInbox > (itemLimit - 3 - limits.maxReceipts) / 2)
        throw std::invalid_argument(
            "room directory exceeds wire item budget"
        );
}
std::expected<
    std::shared_ptr<const RoomDirectory>,
    RoomMigration::Error>
RoomMigration::load(const AtomicJournal& store) const
{
    try
    {
        auto out = std::shared_ptr<RoomDirectory>(new RoomDirectory);
        if (store.stats().poisoned)
            return std::unexpected(Error::NotDurable);
        out->m_boot = m_boot;
        out->m_limits = m_limits;
        out->m_durable = store.snapshot();
        out->m_revision = out->m_durable->revision;
        out->m_controlRevision = out->m_revision;
        const auto meta = out->m_durable->values.find(
            std::string(metaKey)
        );
        bool any = false;
        std::map<
            RoomIdentity,
            std::vector<std::pair<std::uint64_t, std::uint64_t>>>
            extents;
        std::size_t replayRecords = 0;
        for (const auto& [key, bytes] : out->m_durable->values)
        {
            if (!key.starts_with(prefix))
                continue;
            any = true;
            if (key == metaKey)
                continue;
            if (key.starts_with(roomPrefix))
            {
                RoomRouteRecord record;
                if (!decodeRoom(bytes, record) ||
                    !validRoom(record, m_limits) ||
                    key != roomKey(record.identity))
                    return std::unexpected(Error::Invalid);
                if (out->m_rooms.size() >= m_limits.maxRooms)
                    return std::unexpected(Error::Capacity);
                if (record.lifecycle != RoomRouteLifecycle::Retired)
                    for (auto existing = out->m_rooms.lower_bound(
                             RoomIdentity{record.identity.room, 0}
                         );
                         existing != out->m_rooms.end() &&
                         existing->first.room == record.identity.room;
                         ++existing)
                        if (existing->second->lifecycle !=
                            RoomRouteLifecycle::Retired)
                            return std::unexpected(Error::Invalid);
                auto saved = std::make_shared<const RoomRouteRecord>(
                    std::move(record)
                );
                if (!out->m_rooms.emplace(saved->identity, saved)
                         .second)
                    return std::unexpected(Error::Invalid);
            }
            else if (key.starts_with(replayPrefix))
            {
                wire::DirectoryReplay r;
                if (!parse(bytes, r) || r.version() != 2 ||
                    !identity({r.room_id(), r.incarnation()}) ||
                    !r.first_tick() ||
                    r.last_tick() < r.first_tick() ||
                    r.payload().empty() ||
                    r.payload().size() > m_limits.maxReplayBytes ||
                    key != replayKey(
                               {r.room_id(), r.incarnation()},
                               r.first_tick()
                           ))
                    return std::unexpected(Error::Invalid);
                if (++replayRecords > itemLimit)
                    return std::unexpected(Error::Capacity);
                extents[{r.room_id(), r.incarnation()}].push_back(
                    {r.first_tick(), r.last_tick()}
                );
            }
            else
                return std::unexpected(Error::Invalid);
        }
        if (any)
        {
            wire::DirectoryMeta m;
            if (meta == out->m_durable->values.end() ||
                !parse(meta->second, m) || m.version() != 2 ||
                !label(m.boot_id()))
                return std::unexpected(Error::Invalid);
            if (m.boot_id() != m_boot)
                return std::unexpected(Error::BootMismatch);
        }
        for (const auto& [id, spans] : extents)
            if (!out->m_rooms.contains(id))
                return std::unexpected(Error::Invalid);
        for (const auto& [id, room] : out->m_rooms)
        {
            auto& spans = extents[id];
            std::sort(spans.begin(), spans.end());
            auto end = room->initialTick;
            for (const auto& [first, last] : spans)
            {
                if (end == maximum || first != end + 1)
                    return std::unexpected(Error::Invalid);
                end = last;
            }
            if (end != room->image.tick)
                return std::unexpected(Error::Invalid);
        }
        out->m_replayRecords = replayRecords;
        return out;
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(Error::Capacity);
    }
    catch (...)
    {
        return std::unexpected(Error::Invalid);
    }
}

std::expected<RoomMigration::Candidate, RoomMigration::Error>
RoomMigration::plan(
    const RoomDirectory& base,
    const std::vector<RoomDirectoryCommand>& commands,
    std::int64_t now
) const
{
    if (base.m_boot != m_boot || base.m_limits != m_limits ||
        !base.m_durable || now < 0)
        return std::unexpected(Error::Invalid);
    if (commands.size() > m_limits.maxCommands ||
        base.size() > m_limits.maxRooms)
        return std::unexpected(Error::Capacity);
    try
    {
        auto next = std::shared_ptr<RoomDirectory>(
            new RoomDirectory(base)
        );
        std::map<std::string, std::optional<std::string>> writes;
        const auto reject =
            [](Error error) -> std::expected<void, Error>
        { return std::unexpected(error); };
        for (const auto& command : commands)
        {
            const auto id = std::visit(
                [](const auto& c) { return c.room; },
                command
            );
            if (!identity(id))
                return std::unexpected(Error::Invalid);
            const auto found = next->m_rooms.find(id);
            std::shared_ptr<RoomRouteRecord> room;
            if (found != next->m_rooms.end())
                room = std::make_shared<RoomRouteRecord>(
                    *found->second
                );
            const auto outcome = std::visit(
                [&](const auto& c) -> std::expected<void, Error>
                {
                    using T = std::decay_t<decltype(c)>;
                    if constexpr (std::is_same_v<T, RoomCreate>)
                    {
                        if (!label(c.owner) ||
                            !validImage(c.image, m_limits) ||
                            c.image.cursor)
                            return reject(Error::Invalid);
                        if (room)
                            return room->lifecycle ==
                                               RoomRouteLifecycle::
                                                   Active &&
                                           room->owner == c.owner &&
                                           room->ownerEpoch == 1 &&
                                           room->image == c.image &&
                                           room->inbox.empty() &&
                                           !room->handoff
                                       ? std::expected<void, Error>{}
                                       : reject(Error::Conflict);
                        for (auto existing =
                                 next->m_rooms.lower_bound(
                                     RoomIdentity{id.room, 0}
                                 );
                             existing != next->m_rooms.end() &&
                             existing->first.room == id.room;
                             ++existing)
                            if (existing->second->lifecycle !=
                                RoomRouteLifecycle::Retired)
                                return reject(Error::Conflict);
                        if (next->size() >= m_limits.maxRooms)
                            return reject(Error::Capacity);
                        room = std::make_shared<RoomRouteRecord>();
                        room->identity = id;
                        room->owner = c.owner;
                        room->image = c.image;
                        room->initialTick = c.image.tick;
                        room->admittedTick = c.image.logicalTick;
                        return {};
                    }
                    else
                    {
                        if (!room)
                            return reject(Error::NotFound);
                        if constexpr (std::is_same_v<T, RoomAccept>)
                        {
                            if (c.epoch != room->ownerEpoch)
                                return reject(Error::StaleEpoch);
                            if (room->lifecycle ==
                                    RoomRouteLifecycle::Retired ||
                                (room->lifecycle ==
                                     RoomRouteLifecycle::Closing &&
                                 c.operation.kind !=
                                     RoomIngressKind::Tick))
                                return reject(Error::Phase);
                            if (!validOperation(
                                    c.operation,
                                    m_limits
                                ))
                                return reject(Error::Invalid);
                            const auto
                                fingerprint = operationFingerprint(
                                    c.operation
                                );
                            if (const auto
                                    receipt = room->receipts.find(
                                        c.operation.id
                                    );
                                receipt != room->receipts.end())
                                return receipt->second.fingerprint ==
                                               fingerprint
                                           ? std::expected<
                                                 void,
                                                 Error>{}
                                           : reject(Error::Conflict);
                            if (room->inbox.size() >=
                                    m_limits.maxInbox ||
                                room->receipts.size() >=
                                    m_limits.maxReceipts)
                                return reject(Error::Capacity);
                            if (room->nextSequence == maximum)
                                return reject(Error::Exhausted);
                            if (now >
                                std::numeric_limits<
                                    std::int64_t>::max() -
                                    m_limits.receiptRetentionNanos)
                                return reject(Error::Exhausted);
                            if (c.operation.kind ==
                                RoomIngressKind::Tick)
                            {
                                if (room->admittedTick == maximum ||
                                    c.operation.tick !=
                                        room->admittedTick + 1)
                                    return reject(Error::Invalid);
                                room->admittedTick = c.operation.tick;
                            }
                            const auto
                                sequence = room->nextSequence++;
                            room->inbox.push_back(
                                {sequence, c.operation}
                            );
                            room->receipts.emplace(
                                c.operation.id,
                                RoomReceipt{
                                    sequence,
                                    fingerprint,
                                    now + m_limits
                                              .receiptRetentionNanos,
                                    false,
                                    {}
                                }
                            );
                            return {};
                        }
                        else if constexpr (
                            std::is_same_v<T, RoomApply>
                        )
                        {
                            if (c.epoch != room->ownerEpoch ||
                                c.owner != room->owner)
                                return reject(Error::StaleEpoch);
                            if (room->lifecycle ==
                                RoomRouteLifecycle::Retired)
                                return reject(Error::Phase);
                            if (!validImage(c.image, m_limits))
                                return reject(Error::Invalid);
                            if (c.image == room->image)
                            {
                                // A lost ACK may repeat the current
                                // proposal, but cannot change
                                // results.
                                for (const auto& reply : c.replies)
                                {
                                    const auto r = std::find_if(
                                        room->receipts.begin(),
                                        room->receipts.end(),
                                        [&](const auto& pair)
                                        {
                                            return pair.second
                                                       .sequence ==
                                                   reply.sequence;
                                        }
                                    );
                                    if (r == room->receipts.end() ||
                                        !r->second.applied ||
                                        r->second.response !=
                                            reply.response)
                                        return reject(
                                            Error::Conflict
                                        );
                                }
                                if (c.replay)
                                {
                                    const auto key = replayKey(
                                        id,
                                        c.replay->firstTick
                                    );
                                    const auto prior = writes.find(
                                        key
                                    );
                                    const auto
                                        durable = base.m_durable
                                                      ->values.find(
                                                          key
                                                      );
                                    const auto bytes = encodeReplay(
                                        id,
                                        *c.replay
                                    );
                                    if (prior != writes.end())
                                    {
                                        if (!prior->second ||
                                            *prior->second != bytes)
                                            return reject(
                                                Error::Conflict
                                            );
                                    }
                                    else if (
                                        durable ==
                                            base.m_durable->values
                                                .end() ||
                                        durable->second != bytes
                                    )
                                        return reject(
                                            Error::Conflict
                                        );
                                }
                                return {};
                            }
                            if (room->handoff &&
                                (room->handoff->phase ==
                                     RoomMigrationPhase::Frozen ||
                                 room->handoff->phase ==
                                     RoomMigrationPhase::Transferred))
                                return reject(Error::Phase);
                            if (c.image.cursor <=
                                    room->image.cursor ||
                                c.image.cursor >= room->nextSequence)
                                return reject(Error::Conflict);
                            if (room->handoff &&
                                room->handoff->phase ==
                                    RoomMigrationPhase::Freezing &&
                                c.image.cursor >
                                    room->handoff->cutoff)
                                return reject(Error::Phase);
                            const auto count = c.image.cursor -
                                               room->image.cursor;
                            if (count > room->inbox.size() ||
                                c.replies.size() != count)
                                return reject(Error::Invalid);
                            auto logical = room->image.logicalTick;
                            for (std::size_t i = 0; i < count; ++i)
                            {
                                const auto& input = room->inbox[i];
                                const auto& reply = c.replies[i];
                                if (reply.sequence !=
                                        input.sequence ||
                                    reply.response.size() >
                                        m_limits.maxOperationBytes)
                                    return reject(Error::Invalid);
                                if (input.operation.kind ==
                                    RoomIngressKind::Tick)
                                    ++logical;
                                auto& receipt = room->receipts.at(
                                    input.operation.id
                                );
                                receipt.applied = true;
                                receipt.response = reply.response;
                            }
                            if (c.image.logicalTick != logical ||
                                c.image.tick < room->image.tick ||
                                c.image.tick - room->image.tick >
                                    logical - room->image.logicalTick)
                                return reject(Error::Invalid);
                            if (c.image.tick != room->image.tick)
                            {
                                if (!c.replay ||
                                    c.replay->bytes.empty() ||
                                    c.replay->bytes.size() >
                                        m_limits.maxReplayBytes ||
                                    c.replay->firstTick !=
                                        room->image.tick + 1 ||
                                    c.replay->lastTick !=
                                        c.image.tick)
                                    return reject(Error::Invalid);
                                const auto key = replayKey(
                                    id,
                                    c.replay->firstTick
                                );
                                if (writes.contains(key) ||
                                    base.m_durable->values.contains(
                                        key
                                    ))
                                    return reject(Error::Conflict);
                                writes.emplace(
                                    key,
                                    encodeReplay(id, *c.replay)
                                );
                            }
                            else if (c.replay)
                                return reject(Error::Invalid);
                            room->inbox.erase(
                                room->inbox.begin(),
                                room->inbox.begin() +
                                    static_cast<std::ptrdiff_t>(count)
                            );
                            room->image = c.image;
                            return {};
                        }
                        else if constexpr (
                            std::is_same_v<T, RoomPrepare>
                        )
                        {
                            if (room->lifecycle !=
                                RoomRouteLifecycle::Active)
                                return reject(Error::Phase);
                            if (!c.migration || !label(c.target))
                                return reject(Error::Invalid);
                            if (c.migration ==
                                    room->migrationWatermark &&
                                room->handoff)
                            {
                                const auto& h = *room->handoff;
                                return c.target == h.target &&
                                               c.epoch ==
                                                   h.sourceEpoch &&
                                               c.deadline ==
                                                   h.deadline
                                           ? std::expected<
                                                 void,
                                                 Error>{}
                                           : reject(Error::Conflict);
                            }
                            if (c.epoch != room->ownerEpoch)
                                return reject(Error::StaleEpoch);
                            if (c.target == room->owner)
                                return reject(Error::Invalid);
                            if (c.migration <=
                                    room->migrationWatermark ||
                                (room->handoff &&
                                 !terminal(room->handoff->phase)))
                                return reject(Error::Conflict);
                            if (room->ownerEpoch > maximum - 2)
                                return reject(Error::Exhausted);
                            if (c.deadline <= now ||
                                c.deadline - now > 10'000'000'000LL)
                                return reject(Error::Invalid);
                            room->migrationWatermark = c.migration;
                            room->handoff = RoomHandoff{
                                c.migration,
                                room->owner,
                                c.target,
                                room->ownerEpoch,
                                room->ownerEpoch + 1,
                                room->ownerEpoch + 2,
                                0,
                                c.deadline,
                                RoomMigrationPhase::Preparing,
                                {}
                            };
                            return {};
                        }
                        else if constexpr (
                            std::is_same_v<T, RoomRetire>
                        )
                        {
                            if (c.owner != room->owner ||
                                c.epoch != room->ownerEpoch)
                                return reject(Error::StaleEpoch);
                            if (c.cursor != room->image.cursor ||
                                c.fingerprint != roomImageFingerprint(
                                                     id,
                                                     room->image
                                                 ))
                                return reject(Error::Conflict);
                            if (room->handoff &&
                                !terminal(room->handoff->phase))
                                return reject(Error::Phase);
                            room->lifecycle =
                                room->inbox.empty()
                                    ? RoomRouteLifecycle::Retired
                                    : RoomRouteLifecycle::Closing;
                            return {};
                        }
                        else if constexpr (
                            std::is_same_v<T, RoomPruneReceipts>
                        )
                        {
                            std::erase_if(
                                room->receipts,
                                [&](const auto& row)
                                {
                                    return row.second.applied &&
                                           row.second.expiresAt <=
                                               now;
                                }
                            );
                            return {};
                        }
                        else
                        {
                            // Closing and retired records can only
                            // retain a terminal handoff, whose exact
                            // retries stay no-ops.
                            if (!room->handoff ||
                                c.migration != room->handoff->id)
                                return reject(Error::Conflict);
                            auto& h = *room->handoff;
                            if constexpr (
                                std::is_same_v<T, RoomFreeze>
                            )
                            {
                                if (terminal(h.phase))
                                    return {};
                                if (h.phase ==
                                        RoomMigrationPhase::Frozen ||
                                    h.phase == RoomMigrationPhase::
                                                   Transferred)
                                    return {};
                                if (h.phase == RoomMigrationPhase::
                                                   Freezing &&
                                    !c.recoverCommittedImage)
                                    return {};
                                h.cutoff = c.recoverCommittedImage
                                               ? room->image.cursor
                                               : room->nextSequence -
                                                     1;
                                h.phase = c.recoverCommittedImage
                                              ? RoomMigrationPhase::
                                                    Frozen
                                              : RoomMigrationPhase::
                                                    Freezing;
                                if (c.recoverCommittedImage)
                                    h.frozenFingerprint =
                                        roomImageFingerprint(
                                            id,
                                            room->image
                                        );
                                return {};
                            }
                            else if constexpr (
                                std::is_same_v<T, RoomFrozen>
                            )
                            {
                                if (h.phase ==
                                        RoomMigrationPhase::Frozen ||
                                    h.phase == RoomMigrationPhase::
                                                   Transferred ||
                                    h.phase ==
                                        RoomMigrationPhase::Committed)
                                    return c.fingerprint ==
                                                   h.frozenFingerprint
                                               ? std::expected<
                                                     void,
                                                     Error>{}
                                               : reject(
                                                     Error::Conflict
                                                 );
                                if (c.fingerprint !=
                                    roomImageFingerprint(
                                        id,
                                        room->image
                                    ))
                                    return reject(Error::Conflict);
                                if (h.phase != RoomMigrationPhase::
                                                   Freezing ||
                                    room->image.cursor != h.cutoff)
                                    return reject(Error::Phase);
                                h.frozenFingerprint = c.fingerprint;
                                h.phase = RoomMigrationPhase::Frozen;
                                return {};
                            }
                            else if constexpr (
                                std::is_same_v<T, RoomTransferred>
                            )
                            {
                                if (c.target != h.target ||
                                    c.epoch != h.targetEpoch ||
                                    c.fingerprint !=
                                        h.frozenFingerprint)
                                    return reject(Error::Conflict);
                                if (h.phase == RoomMigrationPhase::
                                                   Transferred ||
                                    h.phase ==
                                        RoomMigrationPhase::Committed)
                                    return {};
                                if (h.phase !=
                                    RoomMigrationPhase::Frozen)
                                    return reject(Error::Phase);
                                h.phase = RoomMigrationPhase::
                                    Transferred;
                                return {};
                            }
                            else if constexpr (
                                std::is_same_v<T, RoomCommit>
                            )
                            {
                                if (h.phase ==
                                    RoomMigrationPhase::Committed)
                                    return {};
                                if (h.phase !=
                                    RoomMigrationPhase::Transferred)
                                    return reject(Error::Phase);
                                room->owner = h.target;
                                room->ownerEpoch = h.targetEpoch;
                                h.phase = RoomMigrationPhase::
                                    Committed;
                                return {};
                            }
                            else if constexpr (
                                std::is_same_v<T, RoomAbort>
                            )
                            {
                                if (h.phase ==
                                    RoomMigrationPhase::Aborted)
                                    return {};
                                if (h.phase ==
                                    RoomMigrationPhase::Committed)
                                    return reject(Error::Phase);
                                // A staged target owns sourceEpoch+1;
                                // rollback must fence it too.
                                room->owner = h.source;
                                room->ownerEpoch = h.abortEpoch;
                                h.phase = RoomMigrationPhase::Aborted;
                                return {};
                            }
                        }
                    }
                    return reject(Error::Invalid);
                },
                command
            );
            if (!outcome)
                return std::unexpected(outcome.error());
            if (!room || !validRoom(*room, m_limits))
                return std::unexpected(Error::Invalid);
            const auto bytes = encodeRoom(*room);
            if (bytes.size() > wireLimit)
                return std::unexpected(Error::Capacity);
            const auto old = base.m_durable->values.find(roomKey(id));
            if (old == base.m_durable->values.end() ||
                old->second != bytes)
                writes[roomKey(id)] = bytes;
            else
                writes.erase(roomKey(id));
            next->m_rooms[id] = std::move(room);
        }
        Candidate candidate;
        candidate.m_expected = base.revision();
        if (!writes.empty())
        {
            if (base.revision() == maximum)
                return std::unexpected(Error::Exhausted);
            next->m_revision = base.revision() + 1;
            const auto oldMeta = base.m_durable->values.find(
                std::string(metaKey)
            );
            const auto bytes = metadata(m_boot);
            if (oldMeta == base.m_durable->values.end())
                writes[std::string(metaKey)] = bytes;
            else if (oldMeta->second != bytes)
                return std::unexpected(Error::BootMismatch);
        }
        // Only final control facts invalidate scheduler cache
        // disposal. Tick images, inboxes, receipts and unrelated
        // authority CASes do not.
        for (const auto& command : commands)
        {
            const auto id = std::visit(
                [](const auto& value) { return value.room; },
                command
            );
            const auto* before = base.find(id);
            const auto* after = next->find(id);
            if (!before || !after ||
                before->identity != after->identity ||
                before->owner != after->owner ||
                before->ownerEpoch != after->ownerEpoch ||
                before->lifecycle != after->lifecycle ||
                before->handoff != after->handoff ||
                before->migrationWatermark !=
                    after->migrationWatermark)
            {
                next->m_controlRevision = next->m_revision;
                break;
            }
        }
        auto replayRecords = base.m_replayRecords;
        if (replayRecords > itemLimit)
            return std::unexpected(Error::Capacity);
        for (const auto& [key, value] : writes)
            if (key.starts_with(replayPrefix))
            {
                const bool existed = base.m_durable->values.contains(
                    key
                );
                if (value && !existed)
                {
                    if (replayRecords == itemLimit)
                        return std::unexpected(Error::Capacity);
                    ++replayRecords;
                }
                else if (!value && existed)
                {
                    if (!replayRecords)
                        return std::unexpected(Error::Invalid);
                    --replayRecords;
                }
            }
        next->m_replayRecords = replayRecords;
        std::size_t total = 0;
        for (auto& [key, value] : writes)
        {
            const auto size = key.size() +
                              (value ? value->size() : 0) + 32;
            if (size > wireLimit || total > wireLimit - size)
                return std::unexpected(Error::Capacity);
            total += size;
            candidate.m_mutations.push_back(
                {std::move(key), std::move(value)}
            );
        }
        candidate.m_planned = std::move(next);
        return candidate;
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(Error::Capacity);
    }
    catch (...)
    {
        return std::unexpected(Error::Invalid);
    }
}

std::expected<
    std::shared_ptr<const RoomDirectory>,
    RoomMigration::Error>
RoomMigration::publish(
    Candidate&& candidate,
    const AtomicJournal& store,
    std::uint64_t acknowledged
) const noexcept
{
    if (store.stats().poisoned)
        return std::unexpected(Error::NotDurable);
    if (!candidate.m_planned ||
        candidate.m_planned->m_boot != m_boot ||
        candidate.m_planned->m_limits != m_limits)
        return std::unexpected(Error::Invalid);
    const auto durable = store.snapshot();
    const auto expected = candidate.m_expected;
    const bool same = acknowledged == expected;
    const bool next = expected != maximum &&
                      acknowledged == expected + 1;
    if (durable->revision != acknowledged ||
        (candidate.m_mutations.empty() ? !(same || next) : !next))
        return std::unexpected(Error::NotDurable);
    // Journal proves this exact durable transition, including the
    // unchanged owned namespace. Verification scales with the
    // admitted batch, not history.
    if (!candidate.m_planned->m_durable ||
        !durable->verifiesNamespaceTransition(
            *candidate.m_planned->m_durable,
            prefix,
            candidate.m_mutations
        ))
        return std::unexpected(Error::NotDurable);
    // The directory, map nodes and immutable records were allocated
    // before WAL admission. After ACK this is only a noexcept
    // shared-pointer handoff.
    auto published = std::move(candidate.m_planned);
    published->m_revision = acknowledged;
    published->m_durable = durable;
    return std::shared_ptr<const RoomDirectory>(std::move(published));
}
} // namespace service::lobby