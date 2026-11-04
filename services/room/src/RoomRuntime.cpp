/** @file
 * @brief Executes deterministic room candidates without external
 * effects.
 */
#include "room/RoomRuntime.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <tuple>

#include "room/RoomQuery.h"

namespace service::room
{
namespace
{
using Error = RuntimeError;
using Phase = RuntimePhase;
using Reason = RuntimeReason;
using Room = arena::domain::Room;
using Battle = arena::domain::Battle;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

bool policyValid(const RuntimePolicy& p)
{
    return p.actor.maxMembers && p.actor.maxMembers <= 65536 &&
           p.actor.maxInputsPerTick &&
           p.actor.maxInputsPerTick <= 65536 &&
           p.actor.historyTicks && p.actor.historyTicks <= 65536 &&
           p.actor.positionLimit > 0 && p.maxCommands &&
           p.maxCommands <= p.actor.maxInputsPerTick &&
           p.battleDurationTicks && p.readyTimeoutTicks &&
           p.battleDurationTicks < maximum - p.readyTimeoutTicks &&
           p.resultRetentionNanos > 0 &&
           p.resultRetentionNanos <= 3'600'000'000'000LL;
}
bool allocationValid(const RuntimeAllocation& a, RoomId id)
{
    const auto& x = a.members[0];
    const auto& y = a.members[1];
    return a.matchId && a.matchId < (std::uint64_t{1} << 63) &&
           id.value() == ((std::uint64_t{1} << 63) | a.matchId) &&
           x.playerId.value() < y.playerId.value() &&
           x.sessionId != y.sessionId && x.epoch && y.epoch &&
           x.epoch != maximum && y.epoch != maximum &&
           a.initialMmr[0] <= 10000 && a.initialMmr[1] <= 10000;
}
void increment(std::uint64_t& value)
{
    if (value == maximum)
        throw std::overflow_error("room counter exhausted");
    ++value;
}
template <class Entity>
void transition(Entity& entity, typename Entity::Event event)
{
    const auto before = entity.snapshot();
    if (!before || !entity.apply(event, before->revision))
        throw std::invalid_argument(
            "invalid room lifecycle transition"
        );
}
bool sameTransport(
    const RuntimeAuthorization& a, const RuntimeAuthorization& b
)
{
    return a.member.playerId == b.member.playerId &&
           a.member.sessionId == b.member.sessionId &&
           a.sessionEpoch && a.sessionEpoch == b.sessionEpoch &&
           a.connectionHandle &&
           a.connectionHandle == b.connectionHandle && b.online;
}
const RuntimeAuthorization* findPlayer(
    const std::vector<RuntimeAuthorization>& values, PlayerId player
)
{
    const auto found = std::find_if(
        values.begin(),
        values.end(),
        [&](const auto& v) { return v.member.playerId == player; }
    );
    return found == values.end() ? nullptr : &*found;
}
bool uniqueAuthorizations(
    const std::vector<RuntimeAuthorization>& values
)
{
    for (std::size_t i = 0; i != values.size(); ++i)
    {
        const auto& v = values[i];
        if (!v.sessionEpoch || (v.online ? !v.connectionHandle
                                         : v.connectionHandle != 0))
            return false;
        for (std::size_t j = 0; j != i; ++j)
            if (values[j].member.playerId == v.member.playerId ||
                values[j].member.sessionId == v.member.sessionId)
                return false;
    }
    return true;
}
bool participant(
    const RuntimeManaged& managed, const RuntimeAuthorization& caller
)
{
    return std::any_of(
        managed.allocation.members.begin(),
        managed.allocation.members.end(),
        [&](const auto& m)
        {
            return m.playerId == caller.member.playerId &&
                   m.sessionId == caller.member.sessionId;
        }
    );
}
bool terminal(Phase phase)
{
    return phase == Phase::Settling || phase == Phase::Finished;
}
bool managedValid(const RuntimeImage& image)
{
    const auto& m = *image.managed;
    const auto& actor = image.actor;
    const auto& p = image.policy;
    if (!allocationValid(m.allocation, actor.roomId) ||
        p.actor.maxMembers < 2 || !m.createdUnixMs ||
        (m.startedUnixMs && m.startedUnixMs < m.createdUnixMs) ||
        m.waitedTicks > p.readyTimeoutTicks ||
        m.playedTicks > p.battleDurationTicks ||
        m.waitedTicks > maximum - m.playedTicks ||
        terminal(m.phase) != m.result.has_value() ||
        (m.phase == Phase::Finished) !=
            m.expiresAtNanos.has_value() ||
        (m.expiresAtNanos && *m.expiresAtNanos < 0))
        return false;
    if (actor.players.empty())
    {
        if (actor.tick || m.phase != Phase::Forming ||
            m.waitedTicks || m.playedTicks || m.ready[0] ||
            m.ready[1])
            return false;
    }
    else
    {
        if (actor.players.size() != 2)
            return false;
        for (std::size_t i = 0; i != 2; ++i)
            if (actor.players[i].member != m.allocation.members[i])
                return false;
    }
    const auto total = m.waitedTicks + m.playedTicks;
    const bool initialAbort = terminal(m.phase) && m.result &&
                              m.result->reason ==
                                  Reason::MemberUnavailable &&
                              !total && actor.tick == 1;
    if (actor.tick != total && !initialAbort)
        return false;
    if (m.playedTicks)
    {
        if (!m.startedUnixMs || m.startedTick != m.waitedTicks + 1 ||
            m.startedTick > actor.tick ||
            m.playedTicks != actor.tick - m.startedTick + 1)
            return false;
    }
    else if (m.startedTick || m.startedUnixMs)
        return false;
    if ((!terminal(m.phase) &&
         (m.finishedTick || m.persistenceBlocked)) ||
        (actor.tick <= maximum - 3 &&
         m.room.revision > actor.tick + 3))
        return false;
    switch (m.phase)
    {
    case Phase::Forming:
    case Phase::Ready:
        if (m.room.state != (m.phase == Phase::Forming
                                 ? Room::State::Forming
                                 : Room::State::Ready) ||
            m.battle.state != Battle::State::Prepared ||
            m.playedTicks || m.waitedTicks >= p.readyTimeoutTicks ||
            ((m.ready[0] && m.ready[1]) != (m.phase == Phase::Ready)))
            return false;
        break;
    case Phase::Playing:
        if (m.room.state != Room::State::Running ||
            m.battle.state != Battle::State::Running || !m.ready[0] ||
            !m.ready[1] || !m.playedTicks ||
            m.playedTicks >= p.battleDurationTicks ||
            m.waitedTicks >= p.readyTimeoutTicks)
            return false;
        break;
    case Phase::Settling:
    case Phase::Finished:
        if (m.room.state != (m.phase == Phase::Settling
                                 ? Room::State::Closing
                                 : Room::State::Closed) ||
            (m.phase == Phase::Finished && m.persistenceBlocked))
            return false;
        break;
    default:
        return false;
    }
    if (terminal(m.phase))
    {
        const auto& result = *m.result;
        if (actor.players.size() != 2 ||
            result.resultId != m.allocation.matchId ||
            result.matchId != m.allocation.matchId ||
            result.roomId != actor.roomId ||
            result.finalTick != actor.tick ||
            result.finalChecksum != actor.checksum ||
            result.players != actor.players ||
            result.winnerPlayerId ||
            result.createdUnixMs != m.createdUnixMs ||
            result.startedUnixMs != m.startedUnixMs ||
            result.finishedUnixMs <
                std::max(m.createdUnixMs, m.startedUnixMs) ||
            result.startedTick != m.startedTick ||
            result.playedTicks != m.playedTicks ||
            m.finishedTick != actor.tick)
            return false;
        switch (result.reason)
        {
        case Reason::Completed:
            if (m.playedTicks != p.battleDurationTicks)
                return false;
            break;
        case Reason::ReadyTimeout:
            if (m.playedTicks || m.waitedTicks != p.readyTimeoutTicks)
                return false;
            break;
        case Reason::MemberUnavailable:
            break;
        default:
            return false;
        }
        const auto expected = m.phase == Phase::Finished
                                  ? (result.reason ==
                                             Reason::Completed
                                         ? Battle::State::Finished
                                         : Battle::State::Aborted)
                                  : (m.playedTicks
                                         ? Battle::State::Running
                                         : Battle::State::Prepared);
        if (m.battle.state != expected)
            return false;
    }
    const auto expectedRevision = m.battle.state ==
                                          Battle::State::Prepared
                                      ? 0u
                                  : m.battle.state ==
                                          Battle::State::Running
                                      ? 1u
                                  : m.battle.state ==
                                          Battle::State::Finished
                                      ? 2u
                                  : m.playedTicks ? 2u
                                                  : 1u;
    return m.battle.revision == expectedRevision;
}
} // namespace

bool RuntimeManaged::operator==(const RuntimeManaged& other) const
{
    const auto values = [](const RuntimeManaged& m)
    {
        return std::tie(
            m.allocation,
            m.room.state,
            m.room.revision,
            m.battle.state,
            m.battle.revision,
            m.phase,
            m.ready,
            m.online,
            m.createdUnixMs,
            m.startedUnixMs,
            m.waitedTicks,
            m.startedTick,
            m.finishedTick,
            m.playedTicks,
            m.result,
            m.expiresAtNanos,
            m.persistenceBlocked
        );
    };
    return values(*this) == values(other);
}

std::expected<RuntimeImage, RuntimeError> RoomRuntime::create(
    RoomId id,
    std::uint64_t incarnation,
    RuntimePolicy policy,
    std::optional<RuntimeAllocation> allocation,
    std::uint64_t created
)
try
{
    if (!policyValid(policy) || !incarnation ||
        (allocation && (!allocationValid(*allocation, id) ||
                        policy.actor.maxMembers < 2)))
        return std::unexpected(Error::Invalid);
    RoomActor actor(id, incarnation, policy.actor);
    RuntimeImage image{policy, actor.snapshot(), {}, {}};
    if (allocation)
        image.managed.emplace(
            RuntimeManaged{
                *allocation,
                {Room::State::Forming, 0},
                {Battle::State::Prepared, 0}
            }
        );
    if (image.managed)
        image.managed->createdUnixMs = std::max<std::uint64_t>(
            1,
            created
        );
    return image;
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
catch (...)
{
    return std::unexpected(Error::Invalid);
}

std::expected<void, RuntimeError>
RoomRuntime::validate(const RuntimeImage& image)
try
{
    if (!policyValid(image.policy) ||
        image.actor.tick > image.logicalCutoff ||
        !RoomActor::replay(
            ReplayLog{image.policy.actor, image.actor, {}}
        ))
        return std::unexpected(Error::InvalidImage);
    const auto& c = image.counters;
    if (image.knownSinceTick > image.actor.tick ||
        (image.completeCounterHistory && image.knownSinceTick) ||
        c.ticks != image.actor.tick - image.knownSinceTick ||
        c.battlesCompleted > 1 || c.battlesAborted > 1 ||
        c.battlesCompleted + c.battlesAborted > 1 ||
        c.battlesStarted > 1 || c.settlementIntents > 1 ||
        c.settlementsCommitted > 1 || c.settlementsFailed > 1 ||
        c.applied > maximum - c.duplicates ||
        c.applied + c.duplicates > maximum - c.rejected)
        return std::unexpected(Error::InvalidImage);
    const auto
        capacity = c.ticks > maximum /
                                 image.policy.actor.maxInputsPerTick
                       ? maximum
                       : c.ticks *
                             image.policy.actor.maxInputsPerTick;
    if (c.applied + c.duplicates + c.rejected > capacity)
        return std::unexpected(Error::InvalidImage);
    if (image.managed)
    {
        if (!managedValid(image))
            return std::unexpected(Error::InvalidImage);
        const auto& m = *image.managed;
        Room room(image.actor.roomId, m.room);
        Battle battle(
            arena::domain::BattleId{m.allocation.matchId},
            image.actor.roomId,
            m.battle
        );
        const bool finished = m.phase == Phase::Finished;
        const auto fact = [&](std::uint64_t observed, bool occurred)
        {
            const auto expected = std::uint64_t(occurred);
            return image.completeCounterHistory
                       ? observed == expected
                       : observed <= expected;
        };
        if (!fact(
                c.battlesStarted,
                m.playedTicks != 0 &&
                    (image.completeCounterHistory ||
                     m.startedTick > image.knownSinceTick)
            ) ||
            !fact(c.settlementIntents, terminal(m.phase)) ||
            !fact(c.settlementsCommitted, finished) ||
            !fact(c.settlementsFailed, m.persistenceBlocked) ||
            !fact(
                c.battlesCompleted,
                finished && m.result->reason == Reason::Completed
            ) ||
            !fact(
                c.battlesAborted,
                finished && m.result->reason != Reason::Completed
            ))
            return std::unexpected(Error::InvalidImage);
    }
    else if (
        c.battlesStarted || c.battlesCompleted || c.battlesAborted ||
        c.settlementIntents || c.settlementsCommitted ||
        c.settlementsFailed
    )
        return std::unexpected(Error::InvalidImage);
    return {};
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
catch (...)
{
    return std::unexpected(Error::InvalidImage);
}

std::expected<RuntimeProposal, RuntimeError> RoomRuntime::step(
    const RuntimeImage& base, const RuntimeCutoff& cutoff
)
try
{
    if (auto valid = validate(base); !valid)
        return std::unexpected(valid.error());
    if (cutoff.nowNanos < 0 ||
        cutoff.commands.size() > base.policy.maxCommands ||
        cutoff.roster.size() > 65536 ||
        cutoff.currentAuthorizations.size() >
            base.policy.maxCommands + base.policy.actor.maxMembers)
        return std::unexpected(Error::Capacity);
    if (!uniqueAuthorizations(cutoff.roster) ||
        !uniqueAuthorizations(cutoff.currentAuthorizations))
        return std::unexpected(Error::Invalid);
    for (const auto& member : cutoff.roster)
        if (!member.member.epoch || member.member.epoch == maximum)
            return std::unexpected(Error::Invalid);
    if (base.logicalCutoff == maximum)
        return std::unexpected(Error::Capacity);
    RuntimeProposal out{
        base,
        std::vector<RuntimeCommandResult>(cutoff.commands.size()),
        {},
        false,
        false
    };
    auto& image = out.image;
    ++image.logicalCutoff;
    auto& managed = image.managed;
    std::optional<Room> room;
    std::optional<Battle> battle;
    if (managed)
    {
        room.emplace(image.actor.roomId, managed->room);
        battle.emplace(
            arena::domain::BattleId{managed->allocation.matchId},
            image.actor.roomId,
            managed->battle
        );
        if (managed->expiresAtNanos &&
            cutoff.nowNanos >= *managed->expiresAtNanos)
            out.retire = true;
        for (const auto& current : cutoff.roster)
            if (std::find(
                    managed->allocation.members.begin(),
                    managed->allocation.members.end(),
                    current.member
                ) == managed->allocation.members.end())
                return std::unexpected(Error::Invalid);
    }
    const auto currentCaller = [&](const RuntimeCommand& command)
    {
        if (!command.caller)
            return false;
        const auto* current = findPlayer(
            cutoff.currentAuthorizations,
            command.caller->member.playerId
        );
        return current && sameTransport(*command.caller, *current);
    };
    const auto authorizedRoster = [&](const RuntimeCommand& command)
    {
        if (!command.caller)
            return false;
        const auto* current = findPlayer(
            cutoff.roster,
            command.caller->member.playerId
        );
        return current && sameTransport(*command.caller, *current) &&
               ((command.kind != RuntimeCommandKind::Ready &&
                 command.kind != RuntimeCommandKind::Input) ||
                command.caller->member.epoch ==
                    current->member.epoch);
    };
    std::vector<bool> authorized(cutoff.commands.size(), false);
    for (std::size_t i = 0; i != cutoff.commands.size(); ++i)
    {
        const auto& command = cutoff.commands[i];
        auto& reply = out.results[i];
        if (command.kind == RuntimeCommandKind::SettlementAck)
        {
            if (command.caller || !managed ||
                command.matchId != managed->allocation.matchId ||
                out.retire || !terminal(managed->phase))
                reply.error = Error::BusinessFailure;
            else if (managed->phase == Phase::Finished)
            {
                if (!command.committed)
                    reply.error = Error::BusinessFailure;
            }
            else if (managed->persistenceBlocked)
            {
                if (command.committed)
                    reply.error = Error::BusinessFailure;
            }
            else if (!command.committed)
            {
                managed->persistenceBlocked = true;
                increment(image.counters.settlementsFailed);
            }
            else
            {
                if (cutoff.nowNanos >
                    std::numeric_limits<std::int64_t>::max() -
                        image.policy.resultRetentionNanos)
                    return std::unexpected(Error::Capacity);
                transition(
                    *battle,
                    managed->result->reason == Reason::Completed
                        ? Battle::Event::Finish
                        : Battle::Event::Abort
                );
                transition(*room, Room::Event::CloseCompleted);
                managed->phase = Phase::Finished;
                managed->expiresAtNanos = cutoff.nowNanos +
                                          image.policy
                                              .resultRetentionNanos;
                increment(image.counters.settlementsCommitted);
                increment(
                    managed->result->reason == Reason::Completed
                        ? image.counters.battlesCompleted
                        : image.counters.battlesAborted
                );
                out.releaseMembership = true;
            }
            continue;
        }
        if (static_cast<unsigned>(command.kind) >
            static_cast<unsigned>(RuntimeCommandKind::SettlementAck))
        {
            reply.error = Error::Invalid;
            continue;
        }
        if (command.kind == RuntimeCommandKind::Snapshot ||
            command.kind == RuntimeCommandKind::Status ||
            command.kind == RuntimeCommandKind::Settlement)
        {
            const auto query = authorizeRoomQuery(
                image,
                command,
                cutoff
            );
            if (!query)
                reply.error = query.error();
            else
                authorized[i] = true;
            continue;
        }
        if (!currentCaller(command))
        {
            reply.error = Error::Unauthenticated;
            continue;
        }
        if (out.retire)
        {
            reply.error = Error::BusinessFailure;
            continue;
        }
        if (managed && terminal(managed->phase))
        {
            reply.error = !participant(*managed, *command.caller)
                              ? Error::BusinessFailure
                              : (managed->phase == Phase::Settling
                                     ? Error::Unavailable
                                     : Error::BusinessFailure);
            continue;
        }
        if (!managed && command.kind == RuntimeCommandKind::Ready)
        {
            reply.error = Error::BusinessFailure;
            continue;
        }
        if (!authorizedRoster(command))
        {
            reply.error = Error::BusinessFailure;
            continue;
        }
        if ((command.kind == RuntimeCommandKind::Input ||
             command.kind == RuntimeCommandKind::Ready) &&
            command.incarnation != image.actor.incarnation)
        {
            reply.error = Error::BusinessFailure;
            continue;
        }
        authorized[i] = true;
        if (command.kind == RuntimeCommandKind::Ready)
        {
            if (managed->phase == Phase::Playing && !command.ready)
            {
                authorized[i] = false;
                reply.error = Error::BusinessFailure;
            }
            else if (managed->phase != Phase::Playing)
                for (std::size_t j = 0; j != 2; ++j)
                    if (managed->allocation.members[j].playerId ==
                        command.caller->member.playerId)
                        managed->ready[j] = command.ready;
        }
    }
    if (!managed &&
        cutoff.roster.size() > image.policy.actor.maxMembers)
    {
        // Match the existing manual-room behavior: retire the smaller
        // actor rather than silently dropping authoritative roster
        // members.
        out.retire = true;
        for (std::size_t i = 0; i != authorized.size(); ++i)
            if (authorized[i])
                out.results[i].error = Error::Unavailable;
        return out;
    }
    if (!managed || !terminal(managed->phase))
    {
        std::vector<Member> members;
        bool complete = true;
        const auto entering = base.managed ? base.managed->phase
                                           : Phase::Forming;
        if (managed)
        {
            members.assign(
                managed->allocation.members.begin(),
                managed->allocation.members.end()
            );
            for (std::size_t j = 0; j != 2; ++j)
            {
                const auto found = std::find_if(
                    cutoff.roster.begin(),
                    cutoff.roster.end(),
                    [&](const auto& current)
                    { return current.member == members[j]; }
                );
                complete = complete && found != cutoff.roster.end();
                managed->online[j] = found != cutoff.roster.end() &&
                                     found->online;
            }
            if (complete && managed->phase != Phase::Playing)
            {
                const bool ready = managed->ready[0] &&
                                   managed->ready[1];
                if (ready && managed->phase == Phase::Forming)
                {
                    transition(*room, Room::Event::MembersReady);
                    managed->phase = Phase::Ready;
                }
                else if (!ready && managed->phase == Phase::Ready)
                {
                    transition(*room, Room::Event::ReadinessLost);
                    managed->phase = Phase::Forming;
                }
                if (entering == Phase::Ready &&
                    managed->phase == Phase::Ready)
                {
                    transition(*room, Room::Event::StartBattle);
                    transition(*battle, Battle::Event::Start);
                    managed->phase = Phase::Playing;
                    managed->startedUnixMs = std::max(
                        managed->createdUnixMs,
                        cutoff.wallMillis
                    );
                    increment(image.counters.battlesStarted);
                }
            }
        }
        else
            for (const auto& member : cutoff.roster)
                members.push_back(member.member);
        const auto settle = [&](Reason reason)
        {
            auto& m = *managed;
            m.result.emplace(
                RuntimeSettlement{
                    m.allocation.matchId,
                    m.allocation.matchId,
                    image.actor.roomId,
                    image.actor.tick,
                    image.actor.checksum,
                    reason,
                    0,
                    image.actor.players,
                    m.createdUnixMs,
                    m.startedUnixMs,
                    std::max(
                        {m.createdUnixMs,
                         m.startedUnixMs,
                         cutoff.wallMillis}
                    ),
                    m.startedTick,
                    m.playedTicks
                }
            );
            transition(*room, Room::Event::RequestClose);
            m.finishedTick = image.actor.tick;
            m.phase = Phase::Settling;
            increment(image.counters.settlementIntents);
        };
        const bool initialAbort = managed && !complete &&
                                  image.actor.players.empty();
        if (!managed && members.empty())
            out.retire = true;
        else if (!managed || complete || initialAbort)
        {
            std::vector<Input> inputs;
            std::vector<std::size_t> indexes;
            for (std::size_t j = 0; j != cutoff.commands.size(); ++j)
                if (authorized[j] && cutoff.commands[j].kind ==
                                         RuntimeCommandKind::Input)
                {
                    if (managed && managed->phase != Phase::Playing)
                    {
                        authorized[j] = false;
                        out.results[j].error = Error::BusinessFailure;
                    }
                    else
                    {
                        const auto& c = cutoff.commands[j];
                        inputs.push_back(
                            {c.caller->member, c.sequence, c.dx, c.dy}
                        );
                        indexes.push_back(j);
                    }
                }
            auto actor = RoomActor::restore(
                image.actor,
                image.policy.actor
            );
            if (!actor)
                return std::unexpected(Error::InvalidImage);
            auto tick = (*actor)->step(
                std::move(members),
                initialAbort ? std::vector<Input>{}
                             : std::move(inputs)
            );
            image.actor = tick.snapshot;
            out.replay = std::move(tick.record);
            increment(image.counters.ticks);
            for (std::size_t j = 0; j != tick.results.size(); ++j)
            {
                out.results[indexes[j]].input = tick.results[j];
                if (tick.results[j] == InputStatus::Applied)
                    increment(image.counters.applied);
                else if (tick.results[j] == InputStatus::Duplicate)
                    increment(image.counters.duplicates);
                else
                    increment(image.counters.rejected);
            }
            if (managed && complete)
            {
                if (managed->phase == Phase::Playing)
                {
                    if (!managed->playedTicks)
                        managed->startedTick = image.actor.tick;
                    increment(managed->playedTicks);
                    if (managed->playedTicks ==
                        image.policy.battleDurationTicks)
                        settle(Reason::Completed);
                }
                else if (
                    ++managed->waitedTicks ==
                    image.policy.readyTimeoutTicks
                )
                    settle(Reason::ReadyTimeout);
            }
        }
        if (managed && !complete)
            settle(Reason::MemberUnavailable);
    }
    if (managed)
    {
        managed->room = *room->snapshot();
        managed->battle = *battle->snapshot();
    }
    for (std::size_t i = 0; i != cutoff.commands.size(); ++i)
        if (cutoff.commands[i].kind == RuntimeCommandKind::Input &&
            !out.results[i].error && !out.results[i].input)
            out.results[i].error = Error::BusinessFailure;
    if (auto valid = validate(image); !valid)
        return std::unexpected(valid.error());
    return out;
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
catch (...)
{
    return std::unexpected(Error::InvalidImage);
}
} // namespace service::room