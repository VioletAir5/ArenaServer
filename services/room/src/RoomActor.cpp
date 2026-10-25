/** @file
 * @brief Implements canonical integer room steps and bounded
 * self-contained replay.
 */
#include "room/RoomActor.h"

#include <algorithm>
#include <deque>
#include <exception>
#include <limits>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

namespace service::room
{
namespace
{
/** @brief Rejects policies that cannot bound a useful simulation and
 * journal. */
void validateConfig(const ActorConfig& config)
{
    if (!config.maxMembers || !config.maxInputsPerTick ||
        !config.historyTicks || config.positionLimit <= 0)
        throw std::invalid_argument(
            "invalid room actor configuration"
        );
}

/** @brief Orders roster values independently of registry container
 * iteration. */
bool memberLess(const Member& left, const Member& right) noexcept
{
    return left.playerId.value() < right.playerId.value();
}

/** @brief Orders all attempts, including malformed and rejected
 * controls. */
bool inputLess(const Input& left, const Input& right) noexcept
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

/** @brief Identifies attempts sharing one membership's sequence
 * number. */
bool sameSequence(const Input& left, const Input& right) noexcept
{
    return left.member == right.member &&
           left.sequence == right.sequence;
}

/** @brief Checks the authoritative roster without accepting duplicate
 * players. */
bool validRoster(
    const std::vector<Member>& members, const ActorConfig& config
) noexcept
{
    if (members.size() > config.maxMembers)
        return false;
    for (std::size_t index = 0; index < members.size(); ++index)
        if (!members[index].epoch ||
            (index &&
             !memberLess(members[index - 1], members[index])))
            return false;
    return true;
}

/** @brief Checks signed controls before any sequence or position
 * mutation. */
bool validControls(const Input& input) noexcept
{
    return input.sequence && input.member.epoch && input.dx >= -1 &&
           input.dx <= 1 && input.dy >= -1 && input.dy <= 1;
}

/** @brief Validates every checkpoint field that influences later
 * simulation. */
bool validSnapshot(
    const Snapshot& snapshot, const ActorConfig& config
) noexcept
{
    if (!snapshot.incarnation ||
        snapshot.players.size() > config.maxMembers)
        return false;
    for (std::size_t index = 0; index < snapshot.players.size();
         ++index)
    {
        const auto& player = snapshot.players[index];
        if (!player.member.epoch ||
            (index && !memberLess(
                          snapshot.players[index - 1].member,
                          player.member
                      )) ||
            player.x < -config.positionLimit ||
            player.x > config.positionLimit ||
            player.y < -config.positionLimit ||
            player.y > config.positionLimit || player.lastDx < -1 ||
            player.lastDx > 1 || player.lastDy < -1 ||
            player.lastDy > 1 ||
            (!player.sequence && (player.x || player.y ||
                                  player.lastDx || player.lastDy)))
            return false;
    }
    return true;
}

/** @brief Feeds fixed-width little-endian integers into version-one
 * FNV-1a64. */
class StateHash
{
public:
    /** @brief Adds an unsigned integer without host byte-order
     * dependence. */
    void u64(std::uint64_t value) noexcept
    {
        for (unsigned index = 0; index < 8; ++index)
        {
            byte(static_cast<std::uint8_t>(value));
            value >>= 8;
        }
    }
    /** @brief Adds a 32-bit control or position using its unsigned
     * conversion. */
    void u32(std::uint32_t value) noexcept
    {
        for (unsigned index = 0; index < 4; ++index)
        {
            byte(static_cast<std::uint8_t>(value));
            value >>= 8;
        }
    }
    /** @brief Returns the completed noncryptographic state checksum.
     */
    std::uint64_t value() const noexcept
    {
        return m_value;
    }

private:
    /** @brief Uses defined unsigned multiplication for FNV
     * wraparound. */
    void byte(std::uint8_t value) noexcept
    {
        m_value ^= value;
        m_value *= UINT64_C(1099511628211);
    }
    /// Version-one FNV-1a64 offset basis.
    std::uint64_t m_value{UINT64_C(14695981039346656037)};
};

/** @brief Hashes already canonical, validated state without
 * allocating memory. */
std::uint64_t stateChecksum(
    const Snapshot& snapshot, const ActorConfig& config
) noexcept
{
    StateHash hash;
    hash.u64(RoomActor::checksumVersion);
    hash.u64(snapshot.roomId.value());
    hash.u64(snapshot.incarnation);
    hash.u64(snapshot.tick);
    hash.u32(static_cast<std::uint32_t>(config.positionLimit));
    hash.u64(static_cast<std::uint64_t>(snapshot.players.size()));
    for (const auto& player : snapshot.players)
    {
        hash.u64(player.member.playerId.value());
        hash.u64(player.member.sessionId.value());
        hash.u64(player.member.epoch);
        hash.u32(static_cast<std::uint32_t>(player.x));
        hash.u32(static_cast<std::uint32_t>(player.y));
        hash.u64(player.sequence);
        hash.u32(static_cast<std::uint32_t>(player.lastDx));
        hash.u32(static_cast<std::uint32_t>(player.lastDy));
    }
    return hash.value();
}

/** @brief Applies a single bounded grid step using a wider signed
 * intermediate. */
std::int32_t moved(
    std::int32_t position, std::int32_t delta, std::int32_t limit
) noexcept
{
    return static_cast<std::int32_t>(std::clamp(
        static_cast<std::int64_t>(position) + delta,
        -static_cast<std::int64_t>(limit),
        static_cast<std::int64_t>(limit)
    ));
}
} // namespace

struct RoomActor::State
{
    struct JournalEntry
    {
        /// Full cutoff used to reproduce this committed tick.
        TickRecord record;
        /// State after the record, used when rolling the checkpoint
        /// forward.
        Snapshot after;
    };

    /** @brief Constructs the room identity and empty simulation on
     * their fixed owner. */
    State(
        RoomId roomId, std::uint64_t incarnation, ActorConfig limits
    )
        : roomId(roomId)
        , config(limits)
        , current{roomId, incarnation, 0, 0, {}}
        , checkpoint(current)
    {
        current.checksum = stateChecksum(current, config);
        checkpoint.checksum = current.checksum;
    }

    /** @brief Rejects foreign state access before reading any mutable
     * member. */
    void requireOwner() const
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "room actor requires its construction owner"
            );
    }

    /** @brief Reconciles the entire roster, preserving only exact
     * memberships. */
    std::vector<PlayerState>
    reconcile(const std::vector<Member>& members) const
    {
        std::vector<PlayerState> players;
        players.reserve(members.size());
        auto previous = current.players.begin();
        for (const auto& member : members)
        {
            while (previous != current.players.end() &&
                   memberLess(previous->member, member))
                ++previous;
            if (previous != current.players.end() &&
                previous->member == member)
                players.push_back(*previous);
            else
                players.push_back({member});
        }
        return players;
    }

    /** @brief Resolves a matching logical membership in canonical
     * working state. */
    static PlayerState* find(
        std::vector<PlayerState>& players, const Member& member
    ) noexcept
    {
        const auto found = std::lower_bound(
            players.begin(),
            players.end(),
            member,
            [](const PlayerState& player, const Member& sought)
            { return memberLess(player.member, sought); }
        );
        return found != players.end() && found->member == member
                   ? &*found
                   : nullptr;
    }

    /** @brief Validates and applies one nonconflicting sequence
     * attempt. */
    InputStatus apply(
        std::vector<PlayerState>& players, const Input& input
    ) const noexcept
    {
        if (!validControls(input))
            return InputStatus::Invalid;
        auto* player = find(players, input.member);
        if (!player)
            return InputStatus::NotMember;
        if (input.sequence == player->sequence)
            return input.dx == player->lastDx &&
                           input.dy == player->lastDy
                       ? InputStatus::Duplicate
                       : InputStatus::Conflict;
        if (input.sequence < player->sequence)
            return InputStatus::Stale;
        if (player->sequence == UINT64_MAX ||
            input.sequence != player->sequence + 1)
            return InputStatus::Gap;
        player->x = moved(player->x, input.dx, config.positionLimit);
        player->y = moved(player->y, input.dy, config.positionLimit);
        player->sequence = input.sequence;
        player->lastDx = input.dx;
        player->lastDy = input.dy;
        return InputStatus::Applied;
    }

    /// Checked before mutable state; no lock is needed for owner-only
    /// simulation.
    const std::thread::id owner{std::this_thread::get_id()};
    /// Lifecycle remains Forming until a future battle protocol
    /// explicitly drives it.
    const RoomId roomId;
    /// Immutable limits bound membership, cutoff attempts and
    /// retained journal ticks.
    const ActorConfig config;
    /// Last fully committed simulation state.
    Snapshot current;
    /// State immediately before the oldest retained tick.
    Snapshot checkpoint;
    /// Includes after-state copies so rollover never needs to replay
    /// discarded work.
    std::deque<JournalEntry> journal;
};

RoomActor::RoomActor(
    RoomId room, std::uint64_t incarnation, ActorConfig config
)
{
    validateConfig(config);
    if (!incarnation)
        throw std::invalid_argument(
            "room incarnation must be nonzero"
        );
    m_state = std::make_unique<State>(room, incarnation, config);
}

RoomActor::~RoomActor() noexcept
{
    if (std::this_thread::get_id() != m_state->owner)
        std::terminate();
}

TickResult RoomActor::step(
    std::vector<Member> members, std::vector<Input> inputs
)
{
    auto& state = *m_state;
    state.requireOwner();
    if (members.size() > state.config.maxMembers ||
        inputs.size() > state.config.maxInputsPerTick)
        throw std::invalid_argument(
            "room tick exceeds configured bounds"
        );
    std::sort(members.begin(), members.end(), memberLess);
    if (!validRoster(members, state.config))
        throw std::invalid_argument("invalid room roster");
    if (state.current.tick == UINT64_MAX)
        throw std::overflow_error("room tick identity exhausted");

    Snapshot working{
        state.roomId,
        state.current.incarnation,
        state.current.tick + 1,
        0,
        state.reconcile(members)
    };
    std::vector<std::size_t> order(inputs.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(
        order.begin(),
        order.end(),
        [&inputs](std::size_t left, std::size_t right)
        {
            if (inputLess(inputs[left], inputs[right]))
                return true;
            if (inputLess(inputs[right], inputs[left]))
                return false;
            return left < right;
        }
    );
    std::vector<InputStatus> results(
        inputs.size(),
        InputStatus::Invalid
    );
    TickRecord record{working.tick, std::move(members), {}, 0};
    record.inputs.reserve(inputs.size());
    for (const auto index : order)
        record.inputs.push_back(inputs[index]);

    for (std::size_t first = 0; first < order.size();)
    {
        auto end = first + 1;
        const auto& initial = inputs[order[first]];
        bool conflict = false;
        while (end < order.size() &&
               sameSequence(initial, inputs[order[end]]))
        {
            const auto& attempt = inputs[order[end]];
            conflict = conflict || initial.dx != attempt.dx ||
                       initial.dy != attempt.dy;
            ++end;
        }
        for (auto index = first; index < end; ++index)
            results[order[index]] = conflict
                                        ? InputStatus::Conflict
                                        : state.apply(
                                              working.players,
                                              inputs[order[index]]
                                          );
        first = end;
    }

    working.checksum = stateChecksum(working, state.config);
    record.checksum = working.checksum;
    static_assert(
        std::is_nothrow_move_constructible_v<State::JournalEntry>
    );
    static_assert(std::is_nothrow_move_assignable_v<Snapshot>);
    // Prepare every externally visible copy and rollover allocation
    // before the journal insertion. The following state swaps and
    // deque eviction cannot throw.
    TickResult result{working, std::move(results), record};
    State::JournalEntry entry{std::move(record), working};
    std::optional<Snapshot> checkpoint;
    if (state.journal.size() == state.config.historyTicks)
        checkpoint.emplace(state.journal.front().after);
    state.journal.push_back(std::move(entry));
    state.current = std::move(working);
    if (checkpoint)
    {
        state.checkpoint = std::move(*checkpoint);
        state.journal.pop_front();
    }
    return result;
}

Snapshot RoomActor::snapshot() const
{
    m_state->requireOwner();
    return m_state->current;
}

ReplayLog RoomActor::replayLog() const
{
    m_state->requireOwner();
    ReplayLog result{m_state->config, m_state->checkpoint, {}};
    result.ticks.reserve(m_state->journal.size());
    for (const auto& entry : m_state->journal)
        result.ticks.push_back(entry.record);
    return result;
}

std::expected<ReplayResult, ReplayFailure>
RoomActor::replay(const ReplayLog& log)
{
    const auto invalid = [](std::uint64_t tick, const char* reason)
    {
        return std::unexpected(
            ReplayFailure{
                ReplayError::InvalidLog,
                tick,
                {},
                {},
                reason
            }
        );
    };
    const auto mismatch = [](std::uint64_t tick,
                             std::uint64_t expected,
                             std::uint64_t actual,
                             const char* reason)
    {
        return std::unexpected(
            ReplayFailure{
                ReplayError::ChecksumMismatch,
                tick,
                expected,
                actual,
                reason
            }
        );
    };
    try
    {
        validateConfig(log.config);
    }
    catch (const std::invalid_argument&)
    {
        return invalid(
            log.checkpoint.tick,
            "invalid actor configuration"
        );
    }
    if (!validSnapshot(log.checkpoint, log.config))
        return invalid(
            log.checkpoint.tick,
            "invalid canonical checkpoint state"
        );
    const auto checkpointHash = stateChecksum(
        log.checkpoint,
        log.config
    );
    if (checkpointHash != log.checkpoint.checksum)
        return mismatch(
            log.checkpoint.tick,
            log.checkpoint.checksum,
            checkpointHash,
            "checkpoint checksum mismatch"
        );

    // Retention affects neither controls nor checksums. Full archives
    // may span many rolling journals, so replay only retains its
    // current working cutoff.
    auto workingConfig = log.config;
    workingConfig.historyTicks = 1;
    RoomActor actor(
        log.checkpoint.roomId,
        log.checkpoint.incarnation,
        workingConfig
    );
    actor.m_state->current = log.checkpoint;
    actor.m_state->checkpoint = log.checkpoint;
    ReplayResult result{log.checkpoint, {}};
    result.checksums.reserve(log.ticks.size());
    auto previous = log.checkpoint.tick;
    for (const auto& record : log.ticks)
    {
        if (previous == UINT64_MAX)
            return invalid(previous, "tick counter overflow");
        const auto expectedTick = previous + 1;
        if (record.tick != expectedTick)
            return invalid(
                expectedTick,
                "non-contiguous tick sequence"
            );
        if (record.members.size() > log.config.maxMembers)
            return invalid(
                expectedTick,
                "roster exceeds member limit"
            );
        if (!validRoster(record.members, log.config))
            return invalid(expectedTick, "invalid canonical roster");
        if (record.inputs.size() > log.config.maxInputsPerTick)
            return invalid(
                expectedTick,
                "input batch exceeds tick limit"
            );
        if (!std::is_sorted(
                record.inputs.begin(),
                record.inputs.end(),
                inputLess
            ))
            return invalid(expectedTick, "non-canonical input order");

        // Check each cutoff immediately: a later malformed record
        // must not hide an earlier state divergence in an otherwise
        // valid prefix.
        auto tick = actor.step(record.members, record.inputs);
        if (tick.snapshot.checksum != record.checksum)
            return mismatch(
                expectedTick,
                record.checksum,
                tick.snapshot.checksum,
                "tick checksum mismatch"
            );
        result.checksums.push_back(tick.snapshot.checksum);
        result.snapshot = std::move(tick.snapshot);
        previous = expectedTick;
    }
    return result;
}
std::uint64_t RoomActor::checksum(
    const Snapshot& snapshot, const ActorConfig& config
)
{
    validateConfig(config);
    if (!validSnapshot(snapshot, config))
        throw std::invalid_argument(
            "invalid canonical room snapshot"
        );
    return stateChecksum(snapshot, config);
}
} // namespace service::room
