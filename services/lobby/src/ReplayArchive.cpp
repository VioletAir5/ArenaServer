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

namespace service::lobby
{
namespace
{
using Code = ReplayArchiveErrorCode;
struct Key
{
    std::uint64_t room, incarnation, first;
    std::string_view bytes;
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
Key parse(std::string_view key, std::string_view bytes)
{
    key.remove_prefix(7);
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
        bytes
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
} // namespace

std::expected<std::vector<ReplayArchive>, ReplayArchiveError>
assembleReplayArchives(
    const DurableStateStore::Snapshot& snapshot,
    service::room::ReplayFileLimits limits,
    std::size_t maxRecords
)
try
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
    for (const auto& [key, bytes] : snapshot.values)
    {
        if (!key.starts_with("replay/"))
            continue;
        auto parsed = parse(key, bytes);
        if (bytes.size() > authorityCodecMaxBytes)
            fail(
                Code::Capacity,
                "replay segment exceeds codec byte limit",
                &parsed
            );
        const auto [owner, inserted] = incarnationOwners.emplace(
            parsed.incarnation,
            parsed.room
        );
        if (!inserted && owner->second != parsed.room)
            fail(
                Code::Invalid,
                "actor incarnation reused by different rooms",
                &parsed
            );
        keys.push_back(parsed);
    }
    std::sort(
        keys.begin(),
        keys.end(),
        [](const auto& left, const auto& right)
        {
            return std::tuple{
                       left.room,
                       left.incarnation,
                       left.first
                   } < std::tuple{
                           right.room,
                           right.incarnation,
                           right.first
                       };
        }
    );
    std::vector<ReplayArchive> archives;
    archives.reserve(keys.size());
    std::uint64_t totalBytes = 0, totalTicks = 0;
    for (const auto& key : keys)
    {
        auto segment = decodeReplayLog(key.bytes);
        if (!segment)
            fail(
                segment.error() == CodecError::Capacity
                    ? Code::Capacity
                    : Code::Invalid,
                "replay segment decoding failed",
                &key
            );
        if (segment->ticks.empty() ||
            segment->checkpoint.roomId.value() != key.room ||
            segment->checkpoint.incarnation != key.incarnation ||
            segment->ticks.front().tick != key.first)
            fail(
                Code::Invalid,
                "replay key disagrees with segment identity",
                &key
            );
        const auto& config = segment->config;
        if (config.maxMembers > limits.maxMembers ||
            config.maxInputsPerTick > limits.maxInputsPerTick ||
            config.historyTicks > limits.maxHistoryTicks ||
            segment->checkpoint.players.size() > limits.maxMembers)
            fail(
                Code::Capacity,
                "replay actor policy exceeds archive limits",
                &key
            );
        const bool
            starts = archives.empty() ||
                     archives.back().log.checkpoint.roomId.value() !=
                         key.room ||
                     archives.back().log.checkpoint.incarnation !=
                         key.incarnation;
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
            archives.push_back(
                {{segment->config, segment->checkpoint, {}},
                 segment->checkpoint,
                 header}
            );
        }
        auto& archive = archives.back();
        if (!sameConfig(archive.log.config, segment->config) ||
            archive.finalSnapshot != segment->checkpoint)
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
        for (const auto& tick : segment->ticks)
        {
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
        auto replay = service::room::RoomActor::replay(*segment);
        if (!replay)
        {
            auto error = ReplayArchiveError{
                replay.error().code ==
                        service::room::ReplayError::ChecksumMismatch
                    ? Code::Desync
                    : Code::Invalid,
                "replay segment validation failed",
                key.room,
                key.incarnation,
                replay.error().tick
            };
            throw Failure{std::move(error)};
        }
        archive.log.ticks.reserve(
            archive.log.ticks.size() + segment->ticks.size()
        );
        for (auto& tick : segment->ticks)
            archive.log.ticks.push_back(std::move(tick));
        archive.finalSnapshot = std::move(replay->snapshot);
    }
    return archives;
}
catch (const Failure& failure)
{
    return std::unexpected(failure.error);
}
catch (const std::bad_alloc&)
{
    return std::unexpected(
        ReplayArchiveError{
            Code::Capacity,
            "replay assembly allocation failed",
            {},
            {},
            {}
        }
    );
}
} // namespace service::lobby
