/** @file
 * @brief Implements fixed-width little-endian room replay archives.
 */
#include "room/ReplayFile.h"

#include <algorithm>
#include <array>
#include <bit>
#include <fstream>
#include <limits>
#include <new>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace service::room
{
namespace
{
constexpr std::array<char, 8> magic{
    'A', 'R', 'E', 'P', 'L', 'A', 'Y', '\0'
};
constexpr std::uint32_t tickTag = UINT32_C(0x4b434954);
constexpr std::uint32_t endTag = UINT32_C(0x21444e45);
constexpr std::uint64_t footerBytes = 24;
using Bytes = std::vector<char>;

struct Failure
{
    ReplayFileError error;
};

/** @brief Unwinds bounded codec helpers into a public expected error.
 */
[[noreturn]] void fail(
    ReplayFileErrorCode code,
    const char* reason,
    std::optional<std::uint64_t> tick = {}
)
{
    throw Failure{{code, reason, tick}};
}

/** @brief Appends integer bytes without serializing native padding.
 */
void u32(Bytes& bytes, std::uint32_t value)
{
    for (unsigned index = 0; index < 4; ++index)
    {
        bytes.push_back(static_cast<char>(value & 255));
        value >>= 8;
    }
}

void u64(Bytes& bytes, std::uint64_t value)
{
    for (unsigned index = 0; index < 8; ++index)
    {
        bytes.push_back(static_cast<char>(value & 255));
        value >>= 8;
    }
}

void member(Bytes& bytes, const Member& value)
{
    u64(bytes, value.playerId.value());
    u64(bytes, value.sessionId.value());
    u64(bytes, value.epoch);
}

void input(Bytes& bytes, const Input& value)
{
    member(bytes, value.member);
    u64(bytes, value.sequence);
    u32(bytes, static_cast<std::uint32_t>(value.dx));
    u32(bytes, static_cast<std::uint32_t>(value.dy));
}

void player(Bytes& bytes, const PlayerState& value)
{
    member(bytes, value.member);
    u32(bytes, static_cast<std::uint32_t>(value.x));
    u32(bytes, static_cast<std::uint32_t>(value.y));
    u64(bytes, value.sequence);
    u32(bytes, static_cast<std::uint32_t>(value.lastDx));
    u32(bytes, static_cast<std::uint32_t>(value.lastDy));
}

/** @brief Checks policy sizes before converting or allocating them.
 */
void validateConfig(
    const ActorConfig& config, const ReplayFileLimits& limits
)
{
    if (!config.maxMembers || !config.maxInputsPerTick ||
        !config.historyTicks || config.positionLimit <= 0)
        fail(
            ReplayFileErrorCode::Invalid,
            "invalid actor configuration"
        );
    if (config.maxMembers > limits.maxMembers ||
        config.maxInputsPerTick > limits.maxInputsPerTick ||
        config.historyTicks > limits.maxHistoryTicks)
        fail(
            ReplayFileErrorCode::Limit,
            "actor configuration exceeds file limits"
        );
}

/** @brief Canonically orders attempts including intentionally invalid
 * controls. */
bool inputLess(const Input& left, const Input& right)
{
    return std::tuple{
               left.member.playerId.value(),
               left.member.sessionId.value(),
               left.member.epoch,
               left.sequence,
               left.dx,
               left.dy
           } < std::tuple{
                   right.member.playerId.value(),
                   right.member.sessionId.value(),
                   right.member.epoch,
                   right.sequence,
                   right.dx,
                   right.dy
               };
}

/** @brief Bounds each record without dropping rejected input
 * attempts. */
void validateRecord(
    const TickRecord& record, const ActorConfig& config
)
{
    if (record.members.size() > config.maxMembers ||
        record.inputs.size() > config.maxInputsPerTick)
        fail(
            ReplayFileErrorCode::Limit,
            "tick exceeds actor limits",
            record.tick
        );
    for (std::size_t index = 0; index < record.members.size();
         ++index)
        if (!record.members[index].epoch ||
            (index && record.members[index - 1].playerId.value() >=
                          record.members[index].playerId.value()))
            fail(
                ReplayFileErrorCode::Invalid,
                "noncanonical tick roster",
                record.tick
            );
    if (!std::is_sorted(
            record.inputs.begin(),
            record.inputs.end(),
            inputLess
        ))
        fail(
            ReplayFileErrorCode::Invalid,
            "noncanonical tick inputs",
            record.tick
        );
}

/** @brief Encodes a validated initial checkpoint, preserving its
 * stored hash. */
Bytes header(
    const ActorConfig& config,
    const Snapshot& checkpoint,
    const ReplayFileLimits& limits
)
{
    validateConfig(config, limits);
    try
    {
        // Shape validation deliberately does not replace a recorded
        // checksum.
        static_cast<void>(RoomActor::checksum(checkpoint, config));
    }
    catch (const std::invalid_argument&)
    {
        fail(
            ReplayFileErrorCode::Invalid,
            "invalid initial checkpoint",
            checkpoint.tick
        );
    }
    if (checkpoint.players.size() > UINT32_MAX)
        fail(
            ReplayFileErrorCode::Limit,
            "checkpoint count exceeds format",
            checkpoint.tick
        );
    const auto size = UINT64_C(80) +
                      UINT64_C(48) * checkpoint.players.size();
    if (size > limits.maxFileBytes ||
        footerBytes > limits.maxFileBytes - size)
        fail(
            ReplayFileErrorCode::Limit,
            "header exceeds file byte limit"
        );
    Bytes bytes;
    bytes.reserve(static_cast<std::size_t>(size));
    bytes.insert(bytes.end(), magic.begin(), magic.end());
    u32(bytes, ReplayWriter::formatVersion);
    u32(bytes,
        static_cast<std::uint32_t>(RoomActor::checksumVersion));
    u64(bytes, config.maxMembers);
    u64(bytes, config.maxInputsPerTick);
    u64(bytes, config.historyTicks);
    u32(bytes, static_cast<std::uint32_t>(config.positionLimit));
    u64(bytes, checkpoint.roomId.value());
    u64(bytes, checkpoint.incarnation);
    u64(bytes, checkpoint.tick);
    u64(bytes, checkpoint.checksum);
    u32(bytes, static_cast<std::uint32_t>(checkpoint.players.size()));
    for (const auto& value : checkpoint.players)
        player(bytes, value);
    return bytes;
}

/** @brief Reads exact bytes inside a fixed observed file-size budget.
 */
class Reader
{
public:
    Reader(std::ifstream& stream, std::uint64_t size)
        : m_stream(stream)
        , m_remaining(size)
    {
    }
    void bytes(char* output, std::uint64_t count)
    {
        if (count > m_remaining)
            fail(
                ReplayFileErrorCode::Invalid,
                "truncated replay file",
                tick
            );
        m_stream.read(output, static_cast<std::streamsize>(count));
        if (!m_stream)
            fail(
                m_stream.bad() ? ReplayFileErrorCode::Io
                               : ReplayFileErrorCode::Invalid,
                m_stream.bad() ? "replay read failed"
                               : "truncated replay file",
                tick
            );
        m_remaining -= count;
    }
    std::uint32_t u32()
    {
        std::array<char, 4> value{};
        bytes(value.data(), value.size());
        std::uint32_t result = 0;
        for (unsigned index = 0; index < value.size(); ++index)
            result |= static_cast<std::uint32_t>(
                          static_cast<unsigned char>(value[index])
                      )
                      << (8 * index);
        return result;
    }
    std::uint64_t u64()
    {
        std::array<char, 8> value{};
        bytes(value.data(), value.size());
        std::uint64_t result = 0;
        for (unsigned index = 0; index < value.size(); ++index)
            result |= static_cast<std::uint64_t>(
                          static_cast<unsigned char>(value[index])
                      )
                      << (8 * index);
        return result;
    }
    std::int32_t i32()
    {
        return std::bit_cast<std::int32_t>(u32());
    }
    Member member()
    {
        const auto player = u64();
        const auto session = u64();
        const auto epoch = u64();
        if (!player || !session)
            fail(
                ReplayFileErrorCode::Invalid,
                "zero member identity",
                tick
            );
        return {PlayerId(player), SessionId(session), epoch};
    }
    Input input()
    {
        auto identity = member();
        const auto sequence = u64();
        const auto dx = i32();
        const auto dy = i32();
        return {std::move(identity), sequence, dx, dy};
    }
    PlayerState player()
    {
        auto identity = member();
        const auto x = i32();
        const auto y = i32();
        const auto sequence = u64();
        const auto dx = i32();
        const auto dy = i32();
        return {std::move(identity), x, y, sequence, dx, dy};
    }
    std::uint64_t remaining() const noexcept
    {
        return m_remaining;
    }
    /// Current absolute record, used for structural diagnostics.
    std::optional<std::uint64_t> tick;

private:
    /// Borrowed stream; its lifetime encloses this decoder.
    std::ifstream& m_stream;
    /// Observed bytes left, checked before allocations/read calls.
    std::uint64_t m_remaining;
};

/** @brief Converts a serialized size only after checking both limits.
 */
std::size_t sizeValue(std::uint64_t value, std::uint64_t maximum)
{
    if (value > maximum ||
        value > std::numeric_limits<std::size_t>::max())
        fail(
            ReplayFileErrorCode::Limit,
            "encoded configuration exceeds file limits"
        );
    return static_cast<std::size_t>(value);
}

/** @brief Decodes no record allocation until count and frame length
 * agree. */
ReplayLog decode(
    std::ifstream& stream,
    std::uint64_t size,
    const ReplayFileLimits& limits
)
{
    Reader read(stream, size);
    std::array<char, 8> signature{};
    read.bytes(signature.data(), signature.size());
    if (signature != magic)
        fail(ReplayFileErrorCode::Invalid, "invalid replay magic");
    if (read.u32() != ReplayWriter::formatVersion)
        fail(
            ReplayFileErrorCode::Invalid,
            "unsupported replay format version"
        );
    if (read.u32() != RoomActor::checksumVersion)
        fail(
            ReplayFileErrorCode::Invalid,
            "unsupported replay checksum version"
        );
    const auto members = sizeValue(read.u64(), limits.maxMembers);
    const auto inputs = sizeValue(
        read.u64(),
        limits.maxInputsPerTick
    );
    const auto history = sizeValue(
        read.u64(),
        limits.maxHistoryTicks
    );
    const auto position = read.i32();
    ActorConfig config{members, inputs, history, position};
    validateConfig(config, limits);
    const auto room = read.u64();
    const auto incarnation = read.u64();
    const auto checkpointTick = read.u64();
    const auto checksum = read.u64();
    const auto playerCount = read.u32();
    if (!room)
        fail(ReplayFileErrorCode::Invalid, "zero room identity");
    if (playerCount > config.maxMembers)
        fail(
            ReplayFileErrorCode::Limit,
            "checkpoint exceeds member limit",
            checkpointTick
        );
    if (UINT64_C(48) * playerCount > read.remaining())
        fail(
            ReplayFileErrorCode::Invalid,
            "truncated checkpoint",
            checkpointTick
        );
    ReplayLog log{
        config,
        {RoomId(room), incarnation, checkpointTick, checksum, {}},
        {}
    };
    log.checkpoint.players.reserve(playerCount);
    for (std::uint32_t index = 0; index < playerCount; ++index)
        log.checkpoint.players.push_back(read.player());
    std::uint64_t lastTick = checkpointTick;
    for (;;)
    {
        read.tick.reset();
        if (!read.remaining())
            fail(
                ReplayFileErrorCode::Invalid,
                "missing replay completion footer"
            );
        const auto tag = read.u32();
        const auto length = read.u32();
        if (tag == endTag)
        {
            if (length != 16)
                fail(
                    ReplayFileErrorCode::Invalid,
                    "invalid footer length"
                );
            const auto records = read.u64();
            const auto finalTick = read.u64();
            if (records != log.ticks.size() || finalTick != lastTick)
                fail(
                    ReplayFileErrorCode::Invalid,
                    "inconsistent completion footer"
                );
            if (read.remaining() ||
                stream.peek() != std::char_traits<char>::eof())
                fail(
                    ReplayFileErrorCode::Invalid,
                    "trailing replay bytes"
                );
            if (stream.bad())
                fail(ReplayFileErrorCode::Io, "replay read failed");
            return log;
        }
        if (tag != tickTag)
            fail(
                ReplayFileErrorCode::Invalid,
                "unknown replay record tag"
            );
        if (log.ticks.size() >= limits.maxTicks)
            fail(
                ReplayFileErrorCode::Limit,
                "replay tick limit exceeded"
            );
        if (length < 24 || length > read.remaining())
            fail(
                ReplayFileErrorCode::Invalid,
                "invalid or truncated tick length"
            );
        const auto tick = read.u64();
        read.tick = tick;
        const auto expected = read.u64();
        const auto memberCount = read.u32();
        const auto inputCount = read.u32();
        if (memberCount > config.maxMembers ||
            inputCount > config.maxInputsPerTick)
            fail(
                ReplayFileErrorCode::Limit,
                "tick counts exceed actor limits",
                tick
            );
        const auto needed = UINT64_C(24) +
                            UINT64_C(24) * memberCount +
                            UINT64_C(40) * inputCount;
        if (needed != length)
            fail(
                ReplayFileErrorCode::Invalid,
                "tick length disagrees with counts",
                tick
            );
        TickRecord record{tick, {}, {}, expected};
        record.members.reserve(memberCount);
        record.inputs.reserve(inputCount);
        for (std::uint32_t index = 0; index < memberCount; ++index)
            record.members.push_back(read.member());
        for (std::uint32_t index = 0; index < inputCount; ++index)
            record.inputs.push_back(read.input());
        log.ticks.push_back(std::move(record));
        lastTick = tick;
    }
}
} // namespace

struct ReplayWriter::State
{
    /// Owned output stream, never shared with the actor owner.
    std::ofstream file;
    /// Immutable format and allocation policies.
    ActorConfig config;
    ReplayFileLimits limits;
    /// Last fully committed file tick and aggregate output counts.
    std::uint64_t lastTick;
    std::uint64_t bytes{0}, ticks{0};
    /// Completion is explicit; errors permanently poison the writer.
    bool finished{false};
    std::optional<ReplayFileError> failure;

    State(
        const std::filesystem::path& path,
        ActorConfig config,
        ReplayFileLimits limits,
        std::uint64_t tick
    )
        : file(path, std::ios::binary | std::ios::trunc)
        , config(config)
        , limits(limits)
        , lastTick(tick)
    {
    }
    void write(const Bytes& data)
    {
        file.write(
            data.data(),
            static_cast<std::streamsize>(data.size())
        );
        if (!file)
            fail(
                ReplayFileErrorCode::Io,
                "replay write failed",
                lastTick
            );
        bytes += data.size();
    }
};

ReplayWriter::ReplayWriter(std::unique_ptr<State> state)
    : m_state(std::move(state))
{
}

ReplayWriter::~ReplayWriter() noexcept = default;

std::expected<std::unique_ptr<ReplayWriter>, ReplayFileError>
ReplayWriter::create(
    const std::filesystem::path& path,
    const ActorConfig& config,
    const Snapshot& checkpoint,
    ReplayFileLimits limits
)
{
    try
    {
        const auto initial = header(config, checkpoint, limits);
        auto state = std::make_unique<State>(
            path,
            config,
            limits,
            checkpoint.tick
        );
        if (!state->file)
            fail(
                ReplayFileErrorCode::Io,
                "cannot open replay output"
            );
        state->write(initial);
        return std::unique_ptr<ReplayWriter>(
            new ReplayWriter(std::move(state))
        );
    }
    catch (const Failure& failure)
    {
        return std::unexpected(failure.error);
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(
            ReplayFileError{
                ReplayFileErrorCode::Limit,
                "replay allocation failed",
                {}
            }
        );
    }
    catch (const std::length_error&)
    {
        return std::unexpected(
            ReplayFileError{
                ReplayFileErrorCode::Limit,
                "replay allocation too large",
                {}
            }
        );
    }
}

std::expected<void, ReplayFileError>
ReplayWriter::append(const TickRecord& record)
{
    auto& state = *m_state;
    if (state.failure)
        return std::unexpected(*state.failure);
    if (state.finished)
        return std::unexpected(
            ReplayFileError{
                ReplayFileErrorCode::Invalid,
                "replay already finished",
                record.tick
            }
        );
    try
    {
        if (state.lastTick == UINT64_MAX ||
            record.tick != state.lastTick + 1)
            fail(
                ReplayFileErrorCode::Invalid,
                "nonconsecutive replay tick",
                record.tick
            );
        if (state.ticks >= state.limits.maxTicks)
            fail(
                ReplayFileErrorCode::Limit,
                "replay tick limit exceeded",
                record.tick
            );
        validateRecord(record, state.config);
        const auto payload = UINT64_C(24) +
                             UINT64_C(24) * record.members.size() +
                             UINT64_C(40) * record.inputs.size();
        const auto bytes = payload + 8;
        if (payload > UINT32_MAX ||
            bytes > state.limits.maxFileBytes - state.bytes ||
            footerBytes >
                state.limits.maxFileBytes - state.bytes - bytes)
            fail(
                ReplayFileErrorCode::Limit,
                "replay file byte limit exceeded",
                record.tick
            );
        Bytes encoded;
        encoded.reserve(static_cast<std::size_t>(bytes));
        u32(encoded, tickTag);
        u32(encoded, static_cast<std::uint32_t>(payload));
        u64(encoded, record.tick);
        u64(encoded, record.checksum);
        u32(encoded,
            static_cast<std::uint32_t>(record.members.size()));
        u32(encoded,
            static_cast<std::uint32_t>(record.inputs.size()));
        for (const auto& value : record.members)
            member(encoded, value);
        for (const auto& value : record.inputs)
            input(encoded, value);
        state.write(encoded);
        state.lastTick = record.tick;
        ++state.ticks;
        return {};
    }
    catch (const Failure& failure)
    {
        state.failure = failure.error;
    }
    catch (const std::bad_alloc&)
    {
        state.failure = ReplayFileError{
            ReplayFileErrorCode::Limit,
            "replay allocation failed",
            record.tick
        };
    }
    catch (const std::length_error&)
    {
        state.failure = ReplayFileError{
            ReplayFileErrorCode::Limit,
            "replay allocation too large",
            record.tick
        };
    }
    return std::unexpected(*state.failure);
}

std::expected<void, ReplayFileError> ReplayWriter::finish()
{
    auto& state = *m_state;
    if (state.failure)
        return std::unexpected(*state.failure);
    if (state.finished)
        return {};
    try
    {
        Bytes footer;
        footer.reserve(footerBytes);
        u32(footer, endTag);
        u32(footer, 16);
        u64(footer, state.ticks);
        u64(footer, state.lastTick);
        state.write(footer);
        state.file.flush();
        if (!state.file)
            fail(
                ReplayFileErrorCode::Io,
                "replay flush failed",
                state.lastTick
            );
        state.file.close();
        if (!state.file)
            fail(
                ReplayFileErrorCode::Io,
                "replay close failed",
                state.lastTick
            );
        state.finished = true;
        return {};
    }
    catch (const Failure& failure)
    {
        state.failure = failure.error;
    }
    catch (const std::bad_alloc&)
    {
        state.failure = ReplayFileError{
            ReplayFileErrorCode::Limit,
            "replay allocation failed",
            state.lastTick
        };
    }
    return std::unexpected(*state.failure);
}

std::uint64_t ReplayWriter::bytesWritten() const noexcept
{
    return m_state->bytes;
}

std::uint64_t ReplayWriter::ticksWritten() const noexcept
{
    return m_state->ticks;
}

std::expected<ReplayLog, ReplayFileError> readReplayFile(
    const std::filesystem::path& path, ReplayFileLimits limits
)
{
    try
    {
        std::error_code error;
        const auto size = std::filesystem::file_size(path, error);
        if (error)
            fail(ReplayFileErrorCode::Io, "cannot stat replay input");
        if (size > limits.maxFileBytes)
            fail(
                ReplayFileErrorCode::Limit,
                "replay file byte limit exceeded"
            );
        std::ifstream file(path, std::ios::binary);
        if (!file)
            fail(ReplayFileErrorCode::Io, "cannot open replay input");
        return decode(file, size, limits);
    }
    catch (const Failure& failure)
    {
        return std::unexpected(failure.error);
    }
    catch (const std::bad_alloc&)
    {
        return std::unexpected(
            ReplayFileError{
                ReplayFileErrorCode::Limit,
                "replay allocation failed",
                {}
            }
        );
    }
    catch (const std::length_error&)
    {
        return std::unexpected(
            ReplayFileError{
                ReplayFileErrorCode::Limit,
                "replay allocation too large",
                {}
            }
        );
    }
}

std::expected<void, ReplayFileError> writeReplayFile(
    const std::filesystem::path& path,
    const ReplayLog& log,
    ReplayFileLimits limits
)
{
    auto writer = ReplayWriter::create(
        path,
        log.config,
        log.checkpoint,
        limits
    );
    if (!writer)
        return std::unexpected(writer.error());
    for (const auto& tick : log.ticks)
        if (auto result = (*writer)->append(tick); !result)
            return result;
    return (*writer)->finish();
}
} // namespace service::room
