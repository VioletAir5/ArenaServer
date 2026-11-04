/** @file
 * @brief Verifies bounded segment identities, continuity and full
 * replay checksums.
 */
#include "lobby/ReplayArchive.h"

#include <algorithm>
#include <charconv>
#include <map>
#include <new>
#include <string_view>
#include <tuple>
#include <utility>

#include "lobby/AuthorityCodec.h"
#include "lobby_state.pb.h"
#include "room/RoomWire.h"

namespace service::lobby
{
namespace
{
using Code = ReplayArchiveErrorCode;
struct Key
{
    std::uint64_t room, incarnation, first;
    std::string_view bytes;
    bool directory{false};
};
struct Failure
{
    ReplayArchiveError error;
};
[[noreturn]] void
fail(Code code, const char* reason, const Key* key = nullptr)
{
    throw Failure{
        {code,
         reason,
         key ? std::optional(key->room) : std::nullopt,
         key ? std::optional(key->incarnation) : std::nullopt,
         key ? std::optional(key->first) : std::nullopt}
    };
}
std::uint64_t integer(std::string_view value)
{
    std::uint64_t result = 0;
    const auto [last, error] = std::from_chars(
        value.data(),
        value.data() + value.size(),
        result
    );
    if (value.empty() || (value.size() > 1 && value.front() == '0') ||
        error != std::errc{} || last != value.data() + value.size() ||
        !result)
        fail(Code::Invalid, "noncanonical replay segment identity");
    return result;
}
Key parse(
    std::string_view key,
    std::string_view bytes,
    bool directory = false
)
{
    key.remove_prefix(
        directory ? std::string_view("room-directory/replay/").size()
                  : 7
    );
    const auto first = key.find('/');
    if (first == std::string_view::npos)
        fail(Code::Invalid, "malformed replay segment key");
    const auto room = integer(key.substr(0, first));
    key.remove_prefix(first + 1);
    const auto second = key.find('/');
    if (second == std::string_view::npos)
        fail(Code::Invalid, "malformed replay segment key");
    const auto incarnation = integer(key.substr(0, second));
    return {
        room,
        incarnation,
        integer(key.substr(second + 1)),
        bytes,
        directory
    };
}
bool sameConfig(
    const service::room::ActorConfig& left,
    const service::room::ActorConfig& right
)
{
    return left.maxMembers == right.maxMembers &&
           left.maxInputsPerTick == right.maxInputsPerTick &&
           left.historyTicks == right.historyTicks &&
           left.positionLimit == right.positionLimit;
}
void charge(
    std::uint64_t& current,
    std::uint64_t amount,
    std::uint64_t maximum,
    const Key& key
)
{
    if (current > maximum || amount > maximum - current)
        fail(
            Code::Capacity,
            "replay archive aggregate limit exceeded",
            &key
        );
    current += amount;
}

using Identity = std::pair<std::uint64_t, std::uint64_t>;
struct Anchor
{
    service::room::ActorConfig config;
    service::room::Snapshot snapshot;
};
struct ReadResult
{
    std::vector<ReplayArchive> archives;
    std::vector<ReplayArchiveTail> tails;
};
bool known(
    const google::protobuf::Message& value,
    std::size_t& items,
    int depth = 0
)
{
    if (depth > 32 || ++items > authorityCodecMaxItems)
        return false;
    const auto* reflection = value.GetReflection();
    if (reflection->GetUnknownFields(value).field_count())
        return false;
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(value, &fields);
    for (const auto* field : fields)
        if (field->cpp_type() ==
            google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            if (field->is_repeated())
            {
                for (int i = 0;
                     i < reflection->FieldSize(value, field);
                     ++i)
                    if (!known(
                            reflection
                                ->GetRepeatedMessage(value, field, i),
                            items,
                            depth + 1
                        ))
                        return false;
            }
            else if (!known(
                         reflection->GetMessage(value, field),
                         items,
                         depth + 1
                     ))
                return false;
        }
    return true;
}
template <class Message>
bool parseDirectory(std::string_view bytes, Message& value)
{
    if (bytes.empty() || bytes.size() > authorityCodecMaxBytes ||
        !value.ParseFromArray(
            bytes.data(),
            static_cast<int>(bytes.size())
        ))
        return false;
    std::size_t items = 0;
    return known(value, items) && value.SerializeAsString() == bytes;
}
void policy(
    const service::room::ActorConfig& config,
    const service::room::Snapshot& checkpoint,
    const service::room::ReplayFileLimits& limits,
    const Key& key
)
{
    if (!config.maxMembers || !config.maxInputsPerTick ||
        !config.historyTicks || config.positionLimit <= 0 ||
        config.maxMembers > limits.maxMembers ||
        config.maxInputsPerTick > limits.maxInputsPerTick ||
        config.historyTicks > limits.maxHistoryTicks ||
        checkpoint.players.size() > limits.maxMembers)
        fail(
            Code::Capacity,
            "replay actor policy exceeds archive limits",
            &key
        );
}
// The structural instantiation cannot invoke a room kernel.
template <bool Physics>
ReadResult readArchives(
    const AtomicJournal::Snapshot& snapshot,
    service::room::ReplayFileLimits limits,
    std::size_t maxRecords
)
{
    if (!maxRecords || maxRecords > 1000000)
        fail(
            Code::Invalid,
            "unsupported replay assembly record limit"
        );
    if (snapshot.values.size() > maxRecords)
        fail(
            Code::Capacity,
            "journal key count exceeds replay assembly limit"
        );
    std::vector<Key> keys;
    keys.reserve(snapshot.values.size());
    std::map<std::uint64_t, std::uint64_t> incarnationOwners;
    std::map<Identity, Anchor> anchors;
    bool directoryPresent = false, directoryMeta = false;
    std::size_t directoryExtents = 0;
    const auto claim = [&](std::uint64_t room,
                           std::uint64_t incarnation,
                           const Key* key)
    {
        const auto [owner, inserted] = incarnationOwners.emplace(
            incarnation,
            room
        );
        if (!inserted && owner->second != room)
            fail(
                Code::Invalid,
                "actor incarnation reused by different rooms",
                key
            );
    };
    for (const auto& [name, bytes] : snapshot.values)
    {
        const bool directoryReplay = name.starts_with(
            "room-directory/replay/"
        );
        if (name.starts_with("replay/") || directoryReplay)
        {
            auto parsed = parse(name, bytes, directoryReplay);
            if (bytes.size() > authorityCodecMaxBytes)
                fail(
                    Code::Capacity,
                    "replay segment exceeds codec byte limit",
                    &parsed
                );
            claim(parsed.room, parsed.incarnation, &parsed);
            if (directoryReplay &&
                ++directoryExtents > authorityCodecMaxItems)
                fail(
                    Code::Capacity,
                    "directory replay count exceeds recovery limit",
                    &parsed
                );
            directoryPresent = directoryPresent || directoryReplay;
            keys.push_back(parsed);
        }
        else if (name.starts_with("room-directory/room/"))
        {
            arena::lobby_state::DirectoryRoom record;
            if (!parseDirectory(bytes, record) ||
                record.version() != 2 || !record.room_id() ||
                !record.incarnation() || !record.has_image() ||
                record.lifecycle() > 2 ||
                name != "room-directory/room/" +
                            std::to_string(record.room_id()) + "/" +
                            std::to_string(record.incarnation()))
                fail(
                    Code::Invalid,
                    "invalid directory archive anchor"
                );
            const auto& image = record.image();
            const auto decoded = service::room::wire::decodeImage(
                image.checkpoint()
            );
            if (!decoded ||
                decoded->actor.roomId.value() != record.room_id() ||
                decoded->actor.incarnation != record.incarnation() ||
                decoded->actor.tick != image.tick() ||
                decoded->actor.checksum != image.checksum() ||
                decoded->logicalCutoff != image.logical_tick())
                fail(
                    Code::Invalid,
                    "directory image disagrees with worker metadata"
                );
            claim(record.room_id(), record.incarnation(), nullptr);
            if (!anchors
                     .emplace(
                         Identity{
                             record.room_id(),
                             record.incarnation()
                         },
                         Anchor{decoded->policy.actor, decoded->actor}
                     )
                     .second)
                fail(
                    Code::Invalid,
                    "duplicate directory archive anchor"
                );
            directoryPresent = true;
        }
        else if (name == "room-directory/meta")
        {
            arena::lobby_state::DirectoryMeta meta;
            if (!parseDirectory(bytes, meta) || meta.version() != 2 ||
                meta.boot_id().empty())
                fail(
                    Code::Invalid,
                    "invalid directory archive metadata"
                );
            directoryPresent = directoryMeta = true;
        }
        else if (name.starts_with("room-directory/"))
            fail(Code::Invalid, "unknown directory archive record");
    }
    if (directoryPresent && !directoryMeta)
        fail(Code::Invalid, "directory archive metadata is missing");
    std::sort(
        keys.begin(),
        keys.end(),
        [](const auto& left, const auto& right)
        {
            return std::tuple{
                       left.room,
                       left.incarnation,
                       left.first,
                       left.directory
                   } < std::tuple{
                           right.room,
                           right.incarnation,
                           right.first,
                           right.directory
                       };
        }
    );
    ReadResult out;
    out.archives.reserve(anchors.size());
    out.tails.reserve(anchors.size());
    std::map<Identity, std::size_t> groups;
    std::uint64_t totalBytes = 0, totalTicks = 0;
    bool directoryHistory = false;
    for (const auto& key : keys)
    {
        std::optional<service::room::ReplayLog> segment;
        if (key.directory)
        {
            arena::lobby_state::DirectoryReplay wrapper;
            if (!parseDirectory(key.bytes, wrapper) ||
                wrapper.version() != 2 ||
                wrapper.room_id() != key.room ||
                wrapper.incarnation() != key.incarnation ||
                wrapper.first_tick() != key.first ||
                wrapper.last_tick() < key.first ||
                !anchors.contains(
                    Identity{key.room, key.incarnation}
                ))
                fail(
                    Code::Invalid,
                    "invalid directory replay wrapper",
                    &key
                );
            auto decoded = service::room::wire::decodeReplayLog(
                wrapper.payload()
            );
            if (!decoded)
                fail(
                    decoded.error() ==
                            service::room::wire::CodecError::Capacity
                        ? Code::Capacity
                        : Code::Invalid,
                    "worker replay segment decoding failed",
                    &key
                );
            if (decoded->ticks.empty() ||
                decoded->ticks.back().tick != wrapper.last_tick())
                fail(
                    Code::Invalid,
                    "directory replay extent disagrees with segment",
                    &key
                );
            segment = std::move(*decoded);
        }
        else
        {
            auto decoded = decodeReplayLog(key.bytes);
            if (!decoded)
                fail(
                    decoded.error() == CodecError::Capacity
                        ? Code::Capacity
                        : Code::Invalid,
                    "replay segment decoding failed",
                    &key
                );
            segment = std::move(*decoded);
        }
        if (segment->ticks.empty() ||
            segment->checkpoint.roomId.value() != key.room ||
            segment->checkpoint.incarnation != key.incarnation ||
            segment->ticks.front().tick != key.first)
            fail(
                Code::Invalid,
                "replay key disagrees with segment identity",
                &key
            );
        policy(segment->config, segment->checkpoint, limits, key);
        const Identity id{key.room, key.incarnation};
        const bool starts = !groups.contains(id);
        if (starts)
        {
            if (segment->checkpoint.tick != 0)
                fail(
                    Code::Invalid,
                    "replay initial segment is missing",
                    &key
                );
            const auto header = std::uint64_t{104} +
                                std::uint64_t{48} *
                                    segment->checkpoint.players
                                        .size();
            charge(totalBytes, header, limits.maxFileBytes, key);
            groups.emplace(id, out.archives.size());
            out.archives.push_back(
                {{segment->config, segment->checkpoint, {}},
                 segment->checkpoint,
                 header}
            );
            out.tails.push_back(
                {key.room,
                 key.incarnation,
                 key.first,
                 segment->checkpoint.tick,
                 segment->checkpoint.checksum,
                 segment->config}
            );
            directoryHistory = false;
        }
        const auto index = groups.at(id);
        auto& archive = out.archives[index];
        auto& tail = out.tails[index];
        if (directoryHistory && !key.directory)
            fail(
                Code::Invalid,
                "legacy replay cannot follow worker directory "
                "history",
                &key
            );
        directoryHistory = directoryHistory || key.directory;
        if (!sameConfig(archive.log.config, segment->config) ||
            tail.lastTick != segment->checkpoint.tick ||
            tail.checksum != segment->checkpoint.checksum ||
            (Physics && archive.finalSnapshot != segment->checkpoint))
            fail(
                Code::Invalid,
                "replay segments overlap, have a gap or change "
                "checkpoint fields",
                &key
            );
        charge(
            totalTicks,
            segment->ticks.size(),
            limits.maxTicks,
            key
        );
        auto tickIndex = segment->checkpoint.tick;
        for (const auto& tick : segment->ticks)
        {
            if (tickIndex == UINT64_MAX || tick.tick != ++tickIndex)
                fail(
                    Code::Invalid,
                    "replay physical ticks are not consecutive",
                    &key
                );
            if (tick.members.size() > limits.maxMembers ||
                tick.inputs.size() > limits.maxInputsPerTick)
                fail(
                    Code::Capacity,
                    "replay cutoff exceeds archive limits",
                    &key
                );
            const auto count = std::uint64_t{32} +
                               std::uint64_t{24} *
                                   tick.members.size() +
                               std::uint64_t{40} * tick.inputs.size();
            charge(totalBytes, count, limits.maxFileBytes, key);
            if (count > UINT64_MAX - archive.fileBytes)
                fail(
                    Code::Capacity,
                    "replay archive byte count overflows",
                    &key
                );
            archive.fileBytes += count;
        }
        tail.lastTick = segment->ticks.back().tick;
        tail.checksum = segment->ticks.back().checksum;
        if constexpr (Physics)
        {
            auto replay = service::room::RoomActor::replay(*segment);
            if (!replay)
                throw Failure{
                    {replay.error().code ==
                             service::room::ReplayError::
                                 ChecksumMismatch
                         ? Code::Desync
                         : Code::Invalid,
                     "replay segment validation failed",
                     key.room,
                     key.incarnation,
                     replay.error().tick}
                };
            archive.finalSnapshot = std::move(replay->snapshot);
        }
        if constexpr (Physics)
        {
            archive.log.ticks.reserve(
                archive.log.ticks.size() + segment->ticks.size()
            );
            for (auto& tick : segment->ticks)
                archive.log.ticks.push_back(std::move(tick));
        }
    }
    for (const auto& [id, anchor] : anchors)
    {
        const Key
            key{id.first, id.second, anchor.snapshot.tick, {}, true};
        const auto group = groups.find(id);
        if (group == groups.end())
        {
            if (anchor.snapshot.tick)
                fail(
                    Code::Invalid,
                    "directory image has no complete replay history",
                    &key
                );
            policy(anchor.config, anchor.snapshot, limits, key);
            const auto bytes = std::uint64_t{104} +
                               std::uint64_t{48} *
                                   anchor.snapshot.players.size();
            charge(totalBytes, bytes, limits.maxFileBytes, key);
            ReplayArchive archive{
                {anchor.config, anchor.snapshot, {}},
                anchor.snapshot,
                bytes
            };
            if constexpr (Physics)
            {
                auto replay = service::room::RoomActor::replay(
                    archive.log
                );
                if (!replay)
                    fail(
                        Code::Invalid,
                        "empty directory actor checkpoint is invalid",
                        &key
                    );
            }
            out.archives.push_back(std::move(archive));
            out.tails.push_back(
                {id.first,
                 id.second,
                 0,
                 0,
                 anchor.snapshot.checksum,
                 anchor.config}
            );
        }
        else
        {
            const auto& tail = out.tails[group->second];
            const auto& archive = out.archives[group->second];
            if (!sameConfig(tail.config, anchor.config) ||
                tail.lastTick != anchor.snapshot.tick ||
                tail.checksum != anchor.snapshot.checksum ||
                (Physics && archive.finalSnapshot != anchor.snapshot))
                fail(
                    Code::Invalid,
                    "directory image disagrees with complete replay "
                    "tail",
                    &key
                );
        }
    }
    std::sort(
        out.archives.begin(),
        out.archives.end(),
        [](const auto& left, const auto& right)
        {
            return std::tuple{
                       left.log.checkpoint.roomId.value(),
                       left.log.checkpoint.incarnation
                   } < std::tuple{
                           right.log.checkpoint.roomId.value(),
                           right.log.checkpoint.incarnation
                       };
        }
    );
    std::sort(
        out.tails.begin(),
        out.tails.end(),
        [](const auto& left, const auto& right)
        {
            return std::tuple{left.roomId, left.incarnation} <
                   std::tuple{right.roomId, right.incarnation};
        }
    );
    return out;
}
ReplayArchiveError allocationFailure()
{
    return {
        Code::Capacity,
        "replay assembly allocation failed",
        {},
        {},
        {}
    };
}
} // namespace

std::expected<std::vector<ReplayArchiveTail>, ReplayArchiveError>
inspectReplayArchives(
    const AtomicJournal::Snapshot& snapshot,
    service::room::ReplayFileLimits limits,
    std::size_t maxRecords
)
try
{
    return readArchives<false>(snapshot, limits, maxRecords).tails;
}
catch (const Failure& failure)
{
    return std::unexpected(failure.error);
}
catch (const std::bad_alloc&)
{
    return std::unexpected(allocationFailure());
}

std::expected<std::vector<ReplayArchive>, ReplayArchiveError>
assembleReplayArchives(
    const AtomicJournal::Snapshot& snapshot,
    service::room::ReplayFileLimits limits,
    std::size_t maxRecords
)
try
{
    return readArchives<true>(snapshot, limits, maxRecords).archives;
}
catch (const Failure& failure)
{
    return std::unexpected(failure.error);
}
catch (const std::bad_alloc&)
{
    return std::unexpected(allocationFailure());
}
} // namespace service::lobby
