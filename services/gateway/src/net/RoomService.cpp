/** @file
 * @brief Applies bounded room mailboxes on one absolute-deadline tick
 * owner.
 */
#include "net/RoomService.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "arena/domain/Battle.h"
#include "arena/domain/Room.h"
#include "runtime/OwnerScheduler.h"

namespace service::gateway
{
namespace
{
/** @brief Orders strong room identities without adding ordering to
 * domain IDs. */
struct RoomLess
{
    bool operator()(
        arena::domain::RoomId left, arena::domain::RoomId right
    ) const noexcept
    {
        return left.value() < right.value();
    }
};
/** @brief Allocates a process-local actor identity without ever
 * wrapping to zero. */
std::uint64_t nextIncarnation(std::uint64_t highWater)
{
    static std::atomic<std::uint64_t> next{0};
    auto previous = next.load(std::memory_order_relaxed);
    do
    {
        const auto floor = std::max(previous, highWater);
        if (floor == UINT64_MAX)
            throw std::overflow_error("room incarnation exhausted");
        if (next.compare_exchange_weak(
                previous,
                floor + 1,
                std::memory_order_relaxed
            ))
            return floor + 1;
    } while (true);
}

/** @brief Captures numeric policies while excluding clocks and
 * handles. */
RoomPolicy policy(const RoomServiceConfig& c)
{
    return {
        c.maxRooms,
        c.maxPending,
        c.maxPendingPerRoom,
        c.maxCatchUp,
        c.actor,
        c.battleDurationTicks,
        c.readyTimeoutTicks,
        c.resultRetention,
        c.persistenceAdmissionAttempts,
        c.persistenceRetryBase,
        c.persistenceRetryMax,
        c.externalTicks,
        c.externalSettlement,
        static_cast<bool>(c.persistSettlement),
        static_cast<bool>(c.replays)
    };
}

/** @brief Uses stable diagnostic names independent of protobuf enum
 * numbering. */
std::string_view phaseName(RoomPhase phase) noexcept
{
    switch (phase)
    {
    case RoomPhase::Forming:
        return "Forming";
    case RoomPhase::Ready:
        return "Ready";
    case RoomPhase::Playing:
        return "Playing";
    case RoomPhase::Settling:
        return "Settling";
    case RoomPhase::Finished:
        return "Finished";
    }
    return "Unknown";
}
std::string_view reasonName(SettlementReason reason) noexcept
{
    switch (reason)
    {
    case SettlementReason::Completed:
        return "Completed";
    case SettlementReason::MemberUnavailable:
        return "MemberUnavailable";
    case SettlementReason::ReadyTimeout:
        return "ReadyTimeout";
    }
    return "Unknown";
}

/** @brief Prevents monotonic deadline overflow before changing timer
 * state. */
arena::runtime::OwnerClock::time_point after(
    arena::runtime::OwnerClock::time_point time,
    arena::runtime::OwnerClock::duration duration
)
{
    if (time >
        arena::runtime::OwnerClock::time_point::max() - duration)
        throw std::overflow_error("room timer deadline exhausted");
    return time + duration;
}
} // namespace

bool RoomPolicy::operator==(const RoomPolicy& other) const noexcept
{
    const auto values = [](const RoomPolicy& p)
    {
        return std::tie(
            p.maxRooms,
            p.maxPending,
            p.maxPendingPerRoom,
            p.maxCatchUp,
            p.actor.maxMembers,
            p.actor.maxInputsPerTick,
            p.actor.historyTicks,
            p.actor.positionLimit,
            p.battleDurationTicks,
            p.readyTimeoutTicks,
            p.resultRetention,
            p.persistenceAdmissionAttempts,
            p.persistenceRetryBase,
            p.persistenceRetryMax,
            p.externalTicks,
            p.externalSettlement,
            p.callbackSettlement,
            p.archives
        );
    };
    return values(*this) == values(other);
}

namespace
{
/** @brief Rejects cross-layer contradictions without reading clocks
 * or rosters. */
bool validMatch(
    const ManagedMatchCheckpoint& m,
    const service::room::Snapshot& actor,
    const RoomPolicy& p,
    const RegistryCheckpoint& registry
)
{
    using Room = arena::domain::Room;
    using Battle = arena::domain::Battle;
    const bool durable = p.callbackSettlement || p.externalSettlement;
    const bool terminal = m.phase == RoomPhase::Settling ||
                          m.phase == RoomPhase::Finished;
    constexpr auto managedBit = std::uint64_t{1} << 63;
    if (!m.allocation.matchId ||
        (m.allocation.matchId & managedBit) ||
        m.allocation.roomId.value() !=
            (managedBit | m.allocation.matchId) ||
        actor.roomId != m.allocation.roomId || !m.createdUnixMs ||
        (m.startedUnixMs && m.startedUnixMs < m.createdUnixMs) ||
        m.waitedTicks > p.readyTimeoutTicks ||
        m.playedTicks > p.battleDurationTicks ||
        m.admissionAttempts > p.persistenceAdmissionAttempts ||
        m.persistenceOutcome < 0 || m.persistenceOutcome > 2)
        return false;
    const auto& a = m.allocation.members[0];
    const auto& b = m.allocation.members[1];
    if (a.playerId.value() >= b.playerId.value() ||
        a.sessionId == b.sessionId || !a.roomEpoch || !b.roomEpoch ||
        a.roomEpoch == UINT64_MAX || b.roomEpoch == UINT64_MAX ||
        a.initialMmr > 10000 || b.initialMmr > 10000)
        return false;
    for (const auto presence : m.presence)
        if (presence != Presence::Online &&
            presence != Presence::Detached)
            return false;
    const auto room = std::find_if(
        registry.rooms.begin(),
        registry.rooms.end(),
        [&](const auto& r) { return r.roomId == m.allocation.roomId; }
    );
    if (m.phase != RoomPhase::Finished)
    {
        // A managed allocation owns its historical cohort even if
        // grace maintenance has already removed one or both retained
        // entries.
        if (room == registry.rooms.end() || !room->managed ||
            room->managed->matchId != m.allocation.matchId ||
            room->managed->members != m.allocation.members)
            return false;
    }
    else if (room != registry.rooms.end())
        return false; // Finished releases this reserved allocation
                      // atomically.
    if (actor.players.empty())
    {
        if (actor.tick || m.phase != RoomPhase::Forming ||
            m.waitedTicks || m.playedTicks || m.ready[0] ||
            m.ready[1])
            return false;
    }
    else
    {
        if (actor.players.size() != m.allocation.members.size())
            return false;
        for (std::size_t i = 0; i != actor.players.size(); ++i)
        {
            const auto& expected = m.allocation.members[i];
            if (actor.players[i].member != service::room::Member{
                                               expected.playerId,
                                               expected.sessionId,
                                               expected.roomEpoch
                                           })
                return false;
        }
    }
    if (m.waitedTicks > UINT64_MAX - m.playedTicks)
        return false;
    const auto totalTicks = m.waitedTicks + m.playedTicks;
    const bool
        initialAbort = terminal && m.result &&
                       m.result->reason ==
                           SettlementReason::MemberUnavailable &&
                       !totalTicks && actor.tick == 1;
    if (actor.tick != totalTicks && !initialAbort)
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
    if ((!terminal && m.room.revision > actor.tick) ||
        (actor.tick <= UINT64_MAX - 3 &&
         m.room.revision > actor.tick + 3))
        return false;
    if (terminal != m.result.has_value() ||
        (m.phase == RoomPhase::Finished) != m.expires.has_value())
        return false;
    if (!terminal && (m.finishedTick || m.retryAt ||
                      m.admissionAttempts || m.persistenceAccepted ||
                      m.persistenceBlocked || m.persistenceOutcome))
        return false;
    switch (m.phase)
    {
    case RoomPhase::Forming:
    case RoomPhase::Ready:
        if (m.room.state != (m.phase == RoomPhase::Forming
                                 ? Room::State::Forming
                                 : Room::State::Ready) ||
            m.battle.state != Battle::State::Prepared ||
            m.playedTicks || m.waitedTicks >= p.readyTimeoutTicks ||
            ((m.ready[0] && m.ready[1]) !=
             (m.phase == RoomPhase::Ready)))
            return false;
        break;
    case RoomPhase::Playing:
        if (m.room.state != Room::State::Running ||
            m.battle.state != Battle::State::Running || !m.ready[0] ||
            !m.ready[1] || !m.playedTicks ||
            m.playedTicks >= p.battleDurationTicks ||
            m.waitedTicks >= p.readyTimeoutTicks)
            return false;
        break;
    case RoomPhase::Settling:
    case RoomPhase::Finished:
        if (m.room.state != (m.phase == RoomPhase::Settling
                                 ? Room::State::Closing
                                 : Room::State::Closed))
            return false;
        break;
    default:
        return false;
    }
    if (terminal)
    {
        const auto& r = *m.result;
        if (actor.players.size() != 2 ||
            r.resultId != m.allocation.matchId ||
            r.matchId != m.allocation.matchId ||
            r.roomId != actor.roomId || r.finalTick != actor.tick ||
            r.finalChecksum != actor.checksum ||
            r.players != actor.players || r.winnerPlayerId ||
            r.createdUnixMs != m.createdUnixMs ||
            r.startedUnixMs != m.startedUnixMs ||
            r.finishedUnixMs <
                std::max(m.createdUnixMs, m.startedUnixMs) ||
            r.startedTick != m.startedTick ||
            r.playedTicks != m.playedTicks ||
            m.finishedTick != actor.tick)
            return false;
        switch (r.reason)
        {
        case SettlementReason::Completed:
            if (m.playedTicks != p.battleDurationTicks)
                return false;
            break;
        case SettlementReason::ReadyTimeout:
            if (m.playedTicks || m.waitedTicks != p.readyTimeoutTicks)
                return false;
            break;
        case SettlementReason::MemberUnavailable:
            break;
        default:
            return false;
        }
        const bool battleEnded = !durable ||
                                 m.phase == RoomPhase::Finished;
        const auto expectedBattle =
            battleEnded ? (r.reason == SettlementReason::Completed
                               ? Battle::State::Finished
                               : Battle::State::Aborted)
                        : (m.playedTicks ? Battle::State::Running
                                         : Battle::State::Prepared);
        if (m.battle.state != expectedBattle)
            return false;
        if (durable)
        {
            if (!m.admissionAttempts ||
                (m.persistenceAccepted && m.retryAt) ||
                (m.persistenceOutcome && !m.persistenceAccepted) ||
                (m.phase == RoomPhase::Finished &&
                 (!m.persistenceAccepted || m.persistenceBlocked)))
                return false;
            if (p.externalSettlement)
            {
                if (m.admissionAttempts != 1 ||
                    !m.persistenceAccepted || m.persistenceOutcome ||
                    m.retryAt)
                    return false;
            }
            else if (
                (m.phase == RoomPhase::Settling &&
                 m.persistenceAccepted && !m.persistenceOutcome) ||
                (m.phase == RoomPhase::Finished &&
                 m.persistenceOutcome != 1) ||
                (m.persistenceBlocked && m.persistenceAccepted &&
                 m.persistenceOutcome != 2) ||
                (!m.persistenceAccepted && !m.persistenceBlocked &&
                 !m.retryAt) ||
                (!m.persistenceAccepted && m.persistenceBlocked &&
                 m.admissionAttempts !=
                     p.persistenceAdmissionAttempts)
            )
                return false;
        }
        else if (
            m.admissionAttempts || m.persistenceAccepted ||
            m.persistenceBlocked || m.persistenceOutcome || m.retryAt
        )
            return false;
    }
    const auto
        expectedBattleRevision = m.battle.state ==
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
    return m.battle.revision == expectedBattleRevision;
}
} // namespace

struct RoomService::State : std::enable_shared_from_this<State>
{
    struct Command
    {
        /** @brief Copies the immutable authorization tuple before
         * mailbox publication. */
        Command(
            SessionBinding current,
            arena::domain::SessionId logical,
            std::optional<arena::domain::RoomId> target = {}
        )
            : binding(current)
            , session(logical)
            , room(target)
        {
        }
        /// Exact authenticated transport captured on its connection
        /// owner.
        SessionBinding binding;
        /// Logical session and roster room captured with the request.
        arena::domain::SessionId session;
        std::optional<arena::domain::RoomId> room;
        /// Actor and membership fences supplied from an earlier room
        /// snapshot.
        std::uint64_t incarnation{0}, membershipEpoch{0}, sequence{0};
        /// Integer controls; the actor validates their range without
        /// mutating state.
        std::int32_t dx{0}, dy{0};
        /// Exactly one completion is set; neither retains connection
        /// state.
        SnapshotCompletion snapshotComplete;
        StatusCompletion statusComplete;
        SettlementCompletion settlementComplete;
        std::uint64_t matchId{0};
        bool isReady{false}, readyValue{false};
        InputCompletion inputComplete;
        /// Owner-produced replies are delivered after releasing all
        /// actor references.
        std::optional<SnapshotResult> snapshotResult;
        std::optional<InputResult> inputResult;
        std::optional<StatusResult> statusResult;
        std::optional<SettlementResult> settlementResult;
        ErrorCode error{arena::protocol::ERR_INTERNAL};
        bool authorized{false};
    };

    struct PersistenceAttempt
    {
        /// Callback threads publish one outcome; only owner reads
        /// room state.
        std::atomic<int> outcome{0};
    };

    struct ManagedMatch
    {
        explicit ManagedMatch(
            ManagedRoom allocation, std::uint64_t created
        )
            : allocation(std::move(allocation))
            , createdUnixMs(created)
            , room(this->allocation.roomId)
            , battle(
                  arena::domain::BattleId{this->allocation.matchId},
                  this->allocation.roomId
              )
        {
        }
        explicit ManagedMatch(const ManagedMatchCheckpoint& saved)
            : allocation(saved.allocation)
            , createdUnixMs(saved.createdUnixMs)
            , startedUnixMs(saved.startedUnixMs)
            , room(allocation.roomId, saved.room)
            , battle(
                  arena::domain::BattleId{allocation.matchId},
                  allocation.roomId,
                  saved.battle
              )
            , phase(saved.phase)
            , ready(saved.ready)
            , presence(saved.presence)
            , waitedTicks(saved.waitedTicks)
            , startedTick(saved.startedTick)
            , finishedTick(saved.finishedTick)
            , playedTicks(saved.playedTicks)
            , result(saved.result)
            , expires(saved.expires)
            , retryAt(saved.retryAt)
            , admissionAttempts(saved.admissionAttempts)
            , persistenceAccepted(saved.persistenceAccepted)
            , persistenceBlocked(saved.persistenceBlocked)
        {
            if (saved.persistenceAccepted &&
                !saved.persistenceOutcome)
                return; // External intents deliberately have no
                        // callback loan.
            if (saved.persistenceOutcome)
            {
                persistence = std::make_shared<PersistenceAttempt>();
                persistence->outcome.store(saved.persistenceOutcome);
            }
        }
        ManagedRoom allocation;
        std::uint64_t createdUnixMs, startedUnixMs{0};
        arena::domain::Room room;
        arena::domain::Battle battle;
        RoomPhase phase{RoomPhase::Forming};
        std::array<bool, 2> ready{false, false};
        std::array<Presence, 2> presence{
            Presence::Online, Presence::Online
        };
        std::uint64_t waitedTicks{0}, startedTick{0}, finishedTick{0},
            playedTicks{0};
        std::optional<RoomSettlement> result;
        std::optional<arena::runtime::OwnerClock::time_point> expires;
        std::shared_ptr<PersistenceAttempt> persistence;
        std::optional<arena::runtime::OwnerClock::time_point> retryAt;
        std::size_t admissionAttempts{0};
        bool persistenceAccepted{false}, persistenceBlocked{false};
    };

    /** @brief Applies trusted owner events through the existing
     * domain FSM. */
    template <class Entity>
    static void
    transition(Entity& entity, typename Entity::Event event)
    {
        const auto current = entity.snapshot();
        if (!current || !entity.apply(event, current->revision))
            throw std::logic_error(
                "invalid managed room lifecycle transition"
            );
    }

    /** @brief Fixes ownership and validates finite mailbox and actor
     * policies. */
    State(
        std::shared_ptr<OnlineRegistry> authority,
        RoomServiceConfig limits
    )
        : online(std::move(authority))
        , config(std::move(limits))
    {
        if (!online || !config.now || !config.wallNow ||
            (config.externalSettlement && config.persistSettlement) ||
            !config.maxRooms ||
            !config.persistenceAdmissionAttempts ||
            config.persistenceAdmissionAttempts > 32 ||
            config.persistenceRetryBase <=
                std::chrono::milliseconds::zero() ||
            config.persistenceRetryMax <
                config.persistenceRetryBase ||
            config.persistenceRetryMax > std::chrono::minutes(1) ||
            !config.maxPending || !config.maxPendingPerRoom ||
            !config.maxCatchUp || !config.actor.maxMembers ||
            !config.actor.maxInputsPerTick ||
            !config.actor.historyTicks ||
            config.actor.positionLimit <= 0 ||
            config.maxPendingPerRoom >
                config.actor.maxInputsPerTick ||
            !config.battleDurationTicks ||
            !config.readyTimeoutTicks ||
            config.resultRetention <=
                std::chrono::milliseconds::zero() ||
            config.resultRetention > std::chrono::hours(1))
            throw std::invalid_argument(
                "invalid room service configuration"
            );
    }

    bool durableSettlement() const noexcept
    {
        return config.externalSettlement ||
               static_cast<bool>(config.persistSettlement);
    }

    /** @brief Reserves only newly created actors, never checkpoint
     * clones. */
    std::uint64_t allocateIncarnation()
    {
        const auto value = config.incarnationSource
                               ? config.incarnationSource()
                               : nextIncarnation(
                                     incarnationHighWater
                                 );
        if (!value || value <= incarnationHighWater)
            throw std::invalid_argument(
                "non-increasing room incarnation source"
            );
        incarnationHighWater = value;
        return value;
    }

    /** @brief Rejects foreign access before reading owner-affine
     * runtime state. */
    void requireOwner() const
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "room service requires management owner"
            );
    }

    /** @brief Admits copied values under one bound without posting
     * per-input work. */
    bool submit(Command command)
    {
        std::lock_guard lock(mutex);
        if (!accepting || pending.size() >= config.maxPending)
            return false;
        try
        {
            if (command.room)
            {
                auto [count, inserted] = pendingRooms.try_emplace(
                    *command.room,
                    0
                );
                (void)inserted;
                if (count->second >= config.maxPendingPerRoom)
                    return false;
                try
                {
                    pending.push_back(std::move(command));
                }
                catch (...)
                {
                    if (!count->second)
                        pendingRooms.erase(count);
                    throw;
                }
                ++count->second;
            }
            else
                pending.push_back(std::move(command));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    /** @brief Isolates completion exceptions and settles each
     * accepted command once. */
    static bool deliver(Command& command) noexcept
    {
        try
        {
            if (command.snapshotComplete)
                command.snapshotComplete(
                    command.snapshotResult
                        ? std::move(*command.snapshotResult)
                        : SnapshotResult(
                              std::unexpected(command.error)
                          )
                );
            else if (command.statusComplete)
                command.statusComplete(
                    command.statusResult
                        ? std::move(*command.statusResult)
                        : StatusResult(std::unexpected(command.error))
                );
            else if (command.settlementComplete)
                command.settlementComplete(
                    command.settlementResult
                        ? std::move(*command.settlementResult)
                        : SettlementResult(
                              std::unexpected(command.error)
                          )
                );
            else
                command.inputComplete(
                    command.inputResult
                        ? std::move(*command.inputResult)
                        : InputResult(std::unexpected(command.error))
                );
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    /** @brief Copies a single cutoff roster and rejects stale
     * transport generations. */
    bool authorize(
        Command& command, const std::vector<OnlineSnapshot>& roster
    )
    {
        const auto member = std::find_if(
            roster.begin(),
            roster.end(),
            [&command](const auto& value)
            { return value.playerId == command.binding.playerId; }
        );
        if (member != roster.end() &&
            member->sessionId == command.session &&
            member->connectionId == command.binding.connectionId &&
            member->epoch == command.binding.epoch &&
            member->presence == Presence::Online)
        {
            if ((command.inputComplete || command.isReady) &&
                member->roomEpoch != command.membershipEpoch)
            {
                command.error = arena::protocol::ERR_BUSINESS_FAILURE;
                return false;
            }
            return true;
        }
        const auto current = online->snapshot(
            command.binding.playerId
        );
        command.error = current &&
                                current->sessionId ==
                                    command.session &&
                                current->connectionId ==
                                    command.binding.connectionId &&
                                current->epoch ==
                                    command.binding.epoch &&
                                current->presence == Presence::Online
                            ? arena::protocol::ERR_BUSINESS_FAILURE
                            : arena::protocol::ERR_UNAUTHENTICATED;
        return false;
    }

    /** @brief Seals accepted archive jobs before actor retirement. */
    void finishRecording(arena::domain::RoomId id) noexcept
    {
        const auto found = recordings.find(id);
        if (found != recordings.end())
        {
            found->second->finish();
            recordings.erase(found);
        }
    }

    /** @brief Checks a current transport before looking up retained
     * logical results. */
    std::optional<OnlineSnapshot> current(Command& command)
    {
        auto value = online->snapshot(command.binding.playerId);
        if (value && value->sessionId == command.session &&
            value->connectionId == command.binding.connectionId &&
            value->epoch == command.binding.epoch &&
            value->presence == Presence::Online)
            return value;
        command.error = arena::protocol::ERR_UNAUTHENTICATED;
        return {};
    }

    static bool
    participant(const ManagedMatch& match, const Command& command)
    {
        return std::any_of(
            match.allocation.members.begin(),
            match.allocation.members.end(),
            [&](const auto& member)
            {
                return member.playerId == command.binding.playerId &&
                       member.sessionId == command.session;
            }
        );
    }

    RoomStatus
    view(arena::domain::RoomId id, const ManagedMatch& match) const
    {
        RoomStatus result{
            actors.at(id)->snapshot(),
            match.allocation.matchId,
            match.phase,
            {},
            match.startedTick,
            match.finishedTick,
            match.playedTicks,
            config.battleDurationTicks,
            durableSettlement() && match.phase != RoomPhase::Finished
                ? std::nullopt
                : match.result
        };
        for (std::size_t i = 0; i != match.allocation.members.size();
             ++i)
        {
            const auto& member = match.allocation.members[i];
            result.members.push_back(
                {{member.playerId,
                  member.sessionId,
                  member.roomEpoch},
                 match.ready[i],
                 match.presence[i]}
            );
        }
        return result;
    }

    /** @brief Routes recovery queries on owner; producer threads
     * retain only values. */
    void resolve(Command& command)
    {
        if (command.room)
            return;
        const auto auth = current(command);
        if (!running || !auth)
            return;
        if (command.settlementComplete)
        {
            for (const auto& [id, match] : matches)
            {
                (void)id;
                if (match->allocation.matchId == command.matchId &&
                    participant(*match, command) && match->result)
                {
                    if (durableSettlement() &&
                        match->phase != RoomPhase::Finished)
                        command
                            .error = arena::protocol::ERR_UNAVAILABLE;
                    else
                        command.settlementResult.emplace(
                            *match->result
                        );
                    return;
                }
            }
            command.error = arena::protocol::ERR_BUSINESS_FAILURE;
            return;
        }
        if (auth->roomId)
        {
            const auto found = matches.find(*auth->roomId);
            if (found != matches.end() &&
                participant(*found->second, command))
            {
                command.room = *auth->roomId;
                return;
            }
        }
        const ManagedMatch* latest = nullptr;
        std::optional<arena::domain::RoomId> latestId;
        for (const auto& [id, match] : matches)
            if (match->result && participant(*match, command) &&
                (!latest || match->allocation.matchId >
                                latest->allocation.matchId))
            {
                latest = match.get();
                latestId = id;
            }
        if (latest)
            command.statusResult.emplace(view(*latestId, *latest));
        else
            command.error = arena::protocol::ERR_BUSINESS_FAILURE;
    }

    /** @brief Emits correlatable lifecycle values without allowing
     * logging to affect play. */
    void logPhase(
        arena::domain::RoomId id, const ManagedMatch& match
    ) noexcept
    {
        try
        {
            const auto state = actors.at(id)->snapshot();
            spdlog::info(
                "event=room_phase match_id={} room_id={} phase={} "
                "tick={} played_ticks={} checksum={}",
                match.allocation.matchId,
                id.value(),
                phaseName(match.phase),
                state.tick,
                match.playedTicks,
                state.checksum
            );
        }
        catch (...)
        {
        }
    }

    /** @brief Reads owner wall time without allowing zero to mean
     * Started. */
    std::uint64_t wallMillis() const
    {
        const auto value = std::chrono::duration_cast<
                               std::chrono::milliseconds>(
                               config.wallNow().time_since_epoch()
        )
                               .count();
        return value > 0 ? static_cast<std::uint64_t>(value) : 1;
    }

    /** @brief Coalesces foreign acknowledgements onto the management
     * owner. */
    void wakePersistence() noexcept
    {
        std::lock_guard lock(mutex);
        if (!accepting || !persistenceWake || persistenceWakePending)
            return;
        persistenceWakePending = true;
        try
        {
            // defer never invokes inline. Holding the admission mutex
            // prevents a post from racing owner-executor teardown in
            // stop().
            persistenceWake(
                [weak = weak_from_this()]
                {
                    if (auto self = weak.lock())
                    {
                        try
                        {
                            self->requireOwner();
                            {
                                std::lock_guard lock(self->mutex);
                                self->persistenceWakePending = false;
                            }
                            if (self->running)
                                self->drainPersistence();
                        }
                        catch (...)
                        {
                            // The regular owner tick can still
                            // consume the slot.
                        }
                    }
                }
            );
        }
        catch (...)
        {
            persistenceWakePending = false;
        }
    }

    /** @brief Reports a durable boundary without retaining room
     * references. */
    void logPersistence(
        const ManagedMatch& match, const char* event
    ) noexcept
    {
        try
        {
            spdlog::info(
                "event={} match_id={} room_id={} admissions={}",
                event,
                match.allocation.matchId,
                match.allocation.roomId.value(),
                match.admissionAttempts
            );
        }
        catch (...)
        {
        }
    }

    /** @brief Retries only rejected handoff; an accepted adapter owns
     * its budget. */
    void progressPersistence(arena::domain::RoomId id)
    {
        requireOwner();
        auto found = matches.find(id);
        if (!running || found == matches.end())
            return;
        auto& match = *found->second;
        if (match.phase != RoomPhase::Settling ||
            match.persistenceBlocked)
            return;
        if (match.persistenceAccepted)
        {
            const auto outcome = match.persistence->outcome.load(
                std::memory_order_acquire
            );
            if (outcome == 1)
                finish(id, match);
            else if (outcome == 2)
            {
                match.persistenceBlocked = true;
                ++counts.persistenceFailed;
                logPersistence(match, "room_persistence_failed");
            }
            return;
        }
        const auto now = config.now();
        if (match.retryAt && now < *match.retryAt)
            return;
        auto attempt = std::make_shared<PersistenceAttempt>();
        const auto allocation = match.allocation;
        const auto result = *match.result;
        match.persistence = attempt;
        ++match.admissionAttempts;
        bool accepted = false;
        try
        {
            accepted = config.persistSettlement(
                allocation,
                result,
                [weak = weak_from_this(), attempt](bool committed)
                {
                    int expected = 0;
                    if (!attempt->outcome.compare_exchange_strong(
                            expected,
                            committed ? 1 : 2,
                            std::memory_order_release
                        ))
                        return;
                    if (auto self = weak.lock())
                        self->wakePersistence();
                }
            );
        }
        catch (...)
        {
        }
        // External admission may synchronously stop the service.
        // Never touch the previous ManagedMatch reference after that
        // callback.
        found = matches.find(id);
        if (!running || found == matches.end() ||
            found->second->persistence != attempt)
            return;
        auto& current = *found->second;
        if (accepted)
        {
            current.persistenceAccepted = true;
            current.retryAt.reset();
            ++counts.persistenceSubmitted;
            logPersistence(current, "room_persistence_submitted");
            return;
        }
        current.persistence.reset();
        ++counts.persistenceRejected;
        if (current.admissionAttempts >=
            config.persistenceAdmissionAttempts)
        {
            current.persistenceBlocked = true;
            ++counts.persistenceFailed;
            logPersistence(
                current,
                "room_persistence_admission_exhausted"
            );
            return;
        }
        auto delay = config.persistenceRetryBase;
        for (std::size_t i = 1; i < current.admissionAttempts &&
                                delay < config.persistenceRetryMax;
             ++i)
            delay = std::min(config.persistenceRetryMax, delay * 2);
        current.retryAt = after(now, delay);
        logPersistence(current, "room_persistence_rejected");
    }

    /** @brief Consumes immutable acknowledgement slots only on the
     * owner. */
    void drainPersistence()
    {
        requireOwner();
        if (processing)
            return;
        processing = true;
        struct Guard
        {
            bool& flag;
            ~Guard()
            {
                flag = false;
            }
        } guard{processing};
        std::vector<arena::domain::RoomId> ids;
        for (const auto& [id, match] : matches)
            if (match->phase == RoomPhase::Settling)
                ids.push_back(id);
        for (const auto id : ids)
        {
            if (!running)
                break;
            try
            {
                progressPersistence(id);
            }
            catch (...)
            {
            }
        }
    }

    /** @brief Prepares a result once before retiring registry
     * membership. */
    void settle(
        arena::domain::RoomId id,
        ManagedMatch& match,
        SettlementReason reason
    )
    {
        if (match.result)
            return;
        const auto snapshot = actors.at(id)->snapshot();
        RoomSettlement result{
            match.allocation.matchId,
            match.allocation.matchId,
            id,
            snapshot.tick,
            snapshot.checksum,
            reason,
            0,
            snapshot.players
        };
        result.createdUnixMs = match.createdUnixMs;
        result.startedUnixMs = match.startedUnixMs;
        result.finishedUnixMs = std::max(
            {match.createdUnixMs, match.startedUnixMs, wallMillis()}
        );
        result.startedTick = match.startedTick;
        result.playedTicks = match.playedTicks;
        if (!durableSettlement() && match.phase == RoomPhase::Playing)
        {
            transition(
                match.battle,
                reason == SettlementReason::Completed
                    ? arena::domain::Battle::Event::Finish
                    : arena::domain::Battle::Event::Abort
            );
            if (reason == SettlementReason::Completed)
                transition(
                    match.room,
                    arena::domain::Room::Event::BattleEnded
                );
        }
        else if (!durableSettlement())
            transition(
                match.battle,
                arena::domain::Battle::Event::Abort
            );
        transition(
            match.room,
            arena::domain::Room::Event::RequestClose
        );
        match.result.emplace(std::move(result));
        match.finishedTick = snapshot.tick;
        match.phase = RoomPhase::Settling;
        if (!durableSettlement())
        {
            if (reason == SettlementReason::Completed)
                ++counts.battlesCompleted;
            else
                ++counts.battlesAborted;
        }
        logPhase(id, match);
        if (!running)
            return;
        if (config.externalSettlement)
        {
            // The external owner journals this one transferred
            // intent; no callback or automatic retry is allowed to
            // reopen its budget.
            match.admissionAttempts = 1;
            match.persistenceAccepted = true;
            ++counts.persistenceSubmitted;
        }
        else if (config.persistSettlement)
            progressPersistence(id);
        if (!running)
            return;
        try
        {
            spdlog::info(
                "event=room_result result_id={} match_id={} "
                "room_id={} reason={} final_tick={} checksum={}",
                match.result->resultId,
                match.allocation.matchId,
                id.value(),
                reasonName(reason),
                snapshot.tick,
                snapshot.checksum
            );
        }
        catch (...)
        {
        }
    }

    /** @brief Freezes the completed simulation before releasing both
     * memberships. */
    void finish(arena::domain::RoomId id, ManagedMatch& match)
    {
        // No callback retains this reference. Registry release has no
        // network side effect.
        const auto expiry = after(
            config.now(),
            config.resultRetention
        );
        if (durableSettlement())
            transition(
                match.battle,
                match.result->reason == SettlementReason::Completed
                    ? arena::domain::Battle::Event::Finish
                    : arena::domain::Battle::Event::Abort
            );
        online->finishMatch(id, match.allocation.matchId);
        if (!running)
            return;
        transition(
            match.room,
            arena::domain::Room::Event::CloseCompleted
        );
        finishRecording(id);
        match.phase = RoomPhase::Finished;
        match.expires = expiry;
        if (durableSettlement())
        {
            ++counts.persistenceCommitted;
            if (match.result->reason == SettlementReason::Completed)
                ++counts.battlesCompleted;
            else
                ++counts.battlesAborted;
        }
        logPhase(id, match);
    }

    /** @brief Shares archive/metrics behavior between manual and
     * managed actors. */
    void committed(
        arena::domain::RoomId id, service::room::TickResult& result
    )
    {
        if (const auto recording = recordings.find(id);
            recording != recordings.end())
            recording->second->append(std::move(result.record));
        ++counts.ticks;
        for (const auto status : result.results)
        {
            if (status == service::room::InputStatus::Applied)
                ++counts.applied;
            else if (status == service::room::InputStatus::Duplicate)
                ++counts.duplicates;
            else
                ++counts.rejected;
        }
        try
        {
            spdlog::info(
                "event=room_tick room_id={} incarnation={} tick={} "
                "checksum={}",
                id.value(),
                result.snapshot.incarnation,
                result.snapshot.tick,
                result.snapshot.checksum
            );
        }
        catch (...)
        {
        }
    }

    /** @brief Runs managed lifecycle and kernel steps without
     * client-driven finish. */
    void stepManaged(
        arena::domain::RoomId id,
        ManagedMatch& match,
        const std::vector<OnlineSnapshot>& roster,
        const std::vector<std::size_t>& commands,
        std::vector<Command>& batch
    )
    {
        if (match.phase == RoomPhase::Settling)
        {
            if (config.persistSettlement)
                progressPersistence(id);
            else if (!config.externalSettlement)
                finish(id, match);
            if (!running)
                return;
            if (match.phase == RoomPhase::Settling)
            {
                for (const auto index : commands)
                {
                    auto& command = batch[index];
                    const auto auth = current(command);
                    if (!running)
                        return;
                    if (auth && participant(match, command) &&
                        command.statusComplete && !command.isReady)
                        command.statusResult.emplace(view(id, match));
                    else if (
                        auth && participant(match, command) &&
                        command.snapshotComplete
                    )
                        command.snapshotResult.emplace(
                            actors.at(id)->snapshot()
                        );
                    else if (auth)
                        command
                            .error = arena::protocol::ERR_UNAVAILABLE;
                }
                return;
            }
        }
        if (!running)
            return;
        if (match.phase == RoomPhase::Finished)
        {
            for (const auto index : commands)
            {
                auto& command = batch[index];
                const auto auth = current(command);
                if (!running)
                    return;
                if (auth && command.statusComplete &&
                    !command.isReady && participant(match, command))
                    command.statusResult.emplace(view(id, match));
                else if (auth)
                    command.error = arena::protocol::
                        ERR_BUSINESS_FAILURE;
            }
            return;
        }
        bool complete = true;
        std::vector<service::room::Member> members;
        for (std::size_t i = 0; i != match.allocation.members.size();
             ++i)
        {
            const auto& expected = match.allocation.members[i];
            members.push_back(
                {expected.playerId,
                 expected.sessionId,
                 expected.roomEpoch}
            );
            const auto found = std::find_if(
                roster.begin(),
                roster.end(),
                [&](const auto& actual)
                {
                    return actual.playerId == expected.playerId &&
                           actual.sessionId == expected.sessionId &&
                           actual.roomEpoch == expected.roomEpoch;
                }
            );
            if (found == roster.end())
            {
                complete = false;
                match.presence[i] = Presence::Detached;
            }
            else
                match.presence[i] = found->presence;
        }
        const auto enteringPhase = match.phase;
        const auto
            incarnation = actors.at(id)->snapshot().incarnation;
        for (const auto index : commands)
        {
            auto& command = batch[index];
            command.authorized = authorize(command, roster);
            if (!running)
                return;
            if (!command.authorized)
                continue;
            if ((command.isReady || command.inputComplete) &&
                command.incarnation != incarnation)
            {
                command.authorized = false;
                command.error = arena::protocol::ERR_BUSINESS_FAILURE;
                continue;
            }
            if (command.isReady)
            {
                if (match.phase == RoomPhase::Playing &&
                    !command.readyValue)
                {
                    command.authorized = false;
                    command.error = arena::protocol::
                        ERR_BUSINESS_FAILURE;
                    continue;
                }
                if (match.phase != RoomPhase::Playing)
                    for (std::size_t i = 0;
                         i != match.allocation.members.size();
                         ++i)
                        if (match.allocation.members[i].playerId ==
                            command.binding.playerId)
                            match.ready[i] = command.readyValue;
            }
        }
        if (complete && match.phase != RoomPhase::Playing)
        {
            const bool allReady = match.ready[0] && match.ready[1];
            if (allReady && match.phase == RoomPhase::Forming)
            {
                transition(
                    match.room,
                    arena::domain::Room::Event::MembersReady
                );
                match.phase = RoomPhase::Ready;
                logPhase(id, match);
                if (!running)
                    return;
            }
            else if (!allReady && match.phase == RoomPhase::Ready)
            {
                transition(
                    match.room,
                    arena::domain::Room::Event::ReadinessLost
                );
                match.phase = RoomPhase::Forming;
                logPhase(id, match);
                if (!running)
                    return;
            }
            if (enteringPhase == RoomPhase::Ready &&
                match.phase == RoomPhase::Ready)
            {
                transition(
                    match.room,
                    arena::domain::Room::Event::StartBattle
                );
                transition(
                    match.battle,
                    arena::domain::Battle::Event::Start
                );
                match.phase = RoomPhase::Playing;
                match.startedUnixMs = std::max(
                    match.createdUnixMs,
                    wallMillis()
                );
                ++counts.battlesStarted;
                logPhase(id, match);
                if (!running)
                    return;
            }
        }
        if (!complete)
        {
            // Even an allocation disconnected before its first cutoff
            // has a complete fixed cohort in its result and
            // reproducible archive.
            if (actors.at(id)->snapshot().players.empty())
            {
                auto initial = actors.at(id)->step(
                    std::move(members),
                    {}
                );
                committed(id, initial);
                if (!running)
                    return;
            }
            settle(id, match, SettlementReason::MemberUnavailable);
        }
        else if (match.playedTicks >= config.battleDurationTicks)
            settle(id, match, SettlementReason::Completed);
        else
        {
            std::vector<service::room::Input> inputs;
            std::vector<std::size_t> inputCommands;
            for (const auto index : commands)
            {
                auto& command = batch[index];
                if (!command.authorized || !command.inputComplete)
                    continue;
                if (match.phase != RoomPhase::Playing)
                {
                    command.authorized = false;
                    command.error = arena::protocol::
                        ERR_BUSINESS_FAILURE;
                    continue;
                }
                inputs.push_back(
                    {{command.binding.playerId,
                      command.session,
                      command.membershipEpoch},
                     command.sequence,
                     command.dx,
                     command.dy}
                );
                inputCommands.push_back(index);
            }
            auto result = actors.at(id)->step(
                std::move(members),
                std::move(inputs)
            );
            committed(id, result);
            if (!running)
                return;
            if (match.phase == RoomPhase::Playing)
            {
                if (!match.playedTicks)
                    match.startedTick = result.snapshot.tick;
                ++match.playedTicks;
                if (match.playedTicks == config.battleDurationTicks)
                    settle(id, match, SettlementReason::Completed);
            }
            else if (++match.waitedTicks == config.readyTimeoutTicks)
                settle(id, match, SettlementReason::ReadyTimeout);
            if (!running)
                return;
            for (std::size_t i = 0; i != inputCommands.size(); ++i)
                batch[inputCommands[i]].inputResult.emplace(
                    RoomInputReply{result.snapshot, result.results[i]}
                );
        }
        if (!running)
            return;
        for (const auto index : commands)
        {
            auto& command = batch[index];
            if (!command.authorized)
                continue;
            if (command.snapshotComplete)
                command.snapshotResult.emplace(
                    actors.at(id)->snapshot()
                );
            if (command.statusComplete)
                command.statusResult.emplace(view(id, match));
            if (command.inputComplete && !command.inputResult)
                command.error = arena::protocol::ERR_BUSINESS_FAILURE;
        }
    }

    /** @brief Creates one actor and lifecycle together, rolling back
     * local failure. */
    bool openMatch(const ManagedRoom& allocation)
    {
        requireOwner();
        if (processing)
            return false;
        processing = true;
        struct Guard
        {
            bool& flag;
            ~Guard()
            {
                flag = false;
            }
        } guard{processing};
        if (!running || !allocation.matchId ||
            actors.size() >= config.maxRooms ||
            config.actor.maxMembers < 2 ||
            actors.contains(allocation.roomId) ||
            matches.contains(allocation.roomId))
            return false;
        const auto authority = online->managedRoom(allocation.roomId);
        if (!running || !authority ||
            authority->matchId != allocation.matchId ||
            authority->members != allocation.members)
            return false;
        for (const auto& [id, match] : matches)
        {
            (void)id;
            if (match->allocation.matchId == allocation.matchId)
                return false;
        }
        auto actor = std::make_unique<service::room::RoomActor>(
            allocation.roomId,
            allocateIncarnation(),
            config.actor
        );
        auto match = std::make_unique<ManagedMatch>(
            allocation,
            wallMillis()
        );
        actors.emplace(allocation.roomId, std::move(actor));
        try
        {
            matches.emplace(allocation.roomId, std::move(match));
        }
        catch (...)
        {
            actors.erase(allocation.roomId);
            throw;
        }
        try
        {
            if (config.replays)
            {
                auto recording = config.replays->begin(
                    config.actor,
                    actors.at(allocation.roomId)->snapshot()
                );
                if (recording)
                    recordings.emplace(
                        allocation.roomId,
                        std::move(recording)
                    );
            }
        }
        catch (...)
        {
        }
        logPhase(allocation.roomId, *matches.at(allocation.roomId));
        return running;
    }

    /** @brief Reconciles and steps one room; callbacks run after all
     * rooms. */
    void stepRoom(
        arena::domain::RoomId id,
        const std::vector<std::size_t>& commands,
        std::vector<Command>& batch
    )
    {
        const auto roster = online->roomMembers(id);
        // Registry expiry invokes kick callbacks unlocked. A callback
        // on this owner may stop the service, so do not recreate
        // actors after retirement.
        if (!running)
            return;
        if (const auto managed = matches.find(id);
            managed != matches.end())
        {
            stepManaged(
                id,
                *managed->second,
                roster,
                commands,
                batch
            );
            return;
        }
        if (online->managedRoom(id))
        {
            for (const auto index : commands)
                batch[index].error = arena::protocol::ERR_UNAVAILABLE;
            return;
        }
        for (const auto index : commands)
        {
            if (batch[index].statusComplete ||
                batch[index].settlementComplete)
            {
                batch[index]
                    .error = arena::protocol::ERR_BUSINESS_FAILURE;
                continue;
            }
            batch[index].authorized = authorize(batch[index], roster);
            if (!running)
                return;
        }
        auto found = actors.find(id);
        if (roster.empty())
        {
            finishRecording(id);
            if (found != actors.end())
                actors.erase(found);
            return;
        }
        if (roster.size() > config.actor.maxMembers)
        {
            // A smaller simulation policy cannot silently drop
            // authoritative members.
            finishRecording(id);
            if (found != actors.end())
                actors.erase(found);
            for (const auto index : commands)
                if (batch[index].authorized)
                    batch[index]
                        .error = arena::protocol::ERR_UNAVAILABLE;
            return;
        }
        if (found == actors.end())
        {
            const bool requested = std::any_of(
                commands.begin(),
                commands.end(),
                [&batch](auto index)
                {
                    return batch[index].authorized &&
                           bool(batch[index].snapshotComplete);
                }
            );
            if (requested && actors.size() < config.maxRooms)
            {
                found = actors
                            .emplace(
                                id,
                                std::make_unique<
                                    service::room::RoomActor>(
                                    id,
                                    allocateIncarnation(),
                                    config.actor
                                )
                            )
                            .first;
                if (config.replays)
                {
                    auto recording = config.replays->begin(
                        config.actor,
                        found->second->snapshot()
                    );
                    if (recording)
                        recordings.emplace(id, std::move(recording));
                }
            }
            else
            {
                for (const auto index : commands)
                    if (batch[index].authorized)
                        batch[index].error =
                            batch[index].snapshotComplete
                                ? arena::protocol::ERR_UNAVAILABLE
                                : arena::protocol::
                                      ERR_BUSINESS_FAILURE;
                return;
            }
        }
        std::vector<service::room::Member> members;
        members.reserve(roster.size());
        for (const auto& member : roster)
            members.push_back(
                {member.playerId, member.sessionId, member.roomEpoch}
            );
        const auto incarnation = found->second->snapshot()
                                     .incarnation;
        std::vector<service::room::Input> inputs;
        std::vector<std::size_t> inputCommands;
        inputs.reserve(commands.size());
        inputCommands.reserve(commands.size());
        for (const auto index : commands)
        {
            auto& command = batch[index];
            if (!command.authorized || !command.inputComplete)
                continue;
            if (command.incarnation != incarnation)
            {
                command.authorized = false;
                command.error = arena::protocol::ERR_BUSINESS_FAILURE;
                continue;
            }
            inputs.push_back(
                {{command.binding.playerId,
                  command.session,
                  command.membershipEpoch},
                 command.sequence,
                 command.dx,
                 command.dy}
            );
            inputCommands.push_back(index);
        }
        auto result = found->second->step(
            std::move(members),
            std::move(inputs)
        );
        committed(id, result);
        // Reply copies may fail after commit. A retry uses the same
        // sequence to discover Duplicate; an allocation failure never
        // rolls the simulation back.
        for (std::size_t index = 0; index < inputCommands.size();
             ++index)
            batch[inputCommands[index]].inputResult.emplace(
                RoomInputReply{result.snapshot, result.results[index]}
            );
        for (const auto index : commands)
            if (batch[index].authorized &&
                batch[index].snapshotComplete)
                batch[index].snapshotResult.emplace(result.snapshot);
    }

    /** @brief Detaches the cutoff mailbox, then mutates actors
     * exclusively on owner. */
    bool step()
    {
        if (processing)
            return false;
        processing = true;
        struct ProcessingGuard
        {
            bool& flag;
            ~ProcessingGuard()
            {
                flag = false;
            }
        } guard{processing};
        bool succeeded = true;
        std::vector<Command> batch;
        {
            std::lock_guard lock(mutex);
            pending.swap(batch);
            pendingRooms.clear();
        }
        try
        {
            const auto now = config.now();
            for (auto it = matches.begin(); it != matches.end();)
                if (it->second->expires &&
                    now >= *it->second->expires)
                {
                    actors.erase(it->first);
                    it = matches.erase(it);
                }
                else
                    ++it;
            for (auto& command : batch)
            {
                if (!running)
                    break;
                resolve(command);
            }
            std::map<
                arena::domain::RoomId,
                std::vector<std::size_t>,
                RoomLess>
                rooms;
            for (const auto& [id, actor] : actors)
            {
                (void)actor;
                rooms.try_emplace(id);
            }
            for (std::size_t index = 0; index < batch.size(); ++index)
                if (batch[index].room)
                {
                    auto& group = rooms[*batch[index].room];
                    if (group.size() < config.maxPendingPerRoom)
                        group.push_back(index);
                    else
                    {
                        batch[index].room.reset();
                        batch[index]
                            .error = arena::protocol::ERR_UNAVAILABLE;
                    }
                }
            for (const auto& [id, commands] : rooms)
            {
                if (!running)
                    break;
                try
                {
                    stepRoom(id, commands, batch);
                }
                catch (...)
                {
                    // Timer mode isolates rooms. Explicit candidates
                    // report failure so their owner can discard all
                    // partial effects.
                    succeeded = false;
                }
            }
        }
        catch (...)
        {
            succeeded = false;
        }
        for (auto& command : batch)
        {
            if (!running && !command.snapshotResult &&
                !command.inputResult && !command.statusResult &&
                !command.settlementResult)
                command.error = arena::protocol::ERR_UNAVAILABLE;
            succeeded = deliver(command) && succeeded;
        }
        return succeeded && running;
    }

    /** @brief Arms only the next absolute owner deadline without
     * retaining the service. */
    void arm()
    {
        timer = scheduler.at(
            deadline,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock())
                    self->tick();
            }
        );
    }

    /** @brief Runs bounded catch-up while keeping logical tick
     * identities contiguous. */
    void tick()
    {
        requireOwner();
        if (!running)
            return;
        timer.reset();
        try
        {
            auto now = config.now();
            std::size_t advanced = 0;
            while (running && deadline <= now &&
                   advanced < config.maxCatchUp)
            {
                deadline = after(deadline, tickInterval);
                step();
                ++advanced;
                now = config.now();
            }
            if (running && deadline <= now)
            {
                const auto overdue = now - deadline;
                const auto slots = overdue / tickInterval + 1;
                counts.skippedWallSlots += static_cast<std::uint64_t>(
                    slots
                );
                deadline = after(
                    now,
                    tickInterval - overdue % tickInterval
                );
            }
            if (running)
                arm();
        }
        catch (...)
        {
            stop();
        }
    }

    /** @brief Retires admission before clearing owner objects and
     * cancelling the executor. */
    void stop() noexcept
    {
        if (std::this_thread::get_id() != owner)
            std::terminate();
        std::vector<Command> abandoned;
        {
            std::lock_guard lock(mutex);
            accepting = false;
            persistenceWake = {};
            persistenceWakePending = false;
            running = false;
            started = true;
            abandoned.swap(pending);
            pendingRooms.clear();
        }
        timer.reset();
        scheduler = {};
        for (auto& [id, recording] : recordings)
        {
            (void)id;
            recording->finish();
        }
        recordings.clear();
        actors.clear();
        for (const auto& [id, match] : matches)
            try
            {
                // Unacknowledged durable results retain registry
                // fences until registry shutdown; stop must not
                // impersonate a commit.
                if (!durableSettlement() ||
                    match->phase == RoomPhase::Finished)
                    online->finishMatch(
                        id,
                        match->allocation.matchId
                    );
            }
            catch (...)
            {
            }
        matches.clear();
        for (auto& command : abandoned)
        {
            command.error = arena::protocol::ERR_UNAVAILABLE;
            deliver(command);
        }
    }

    /// Fixed before publication; every actor and timer lives and dies
    /// on this thread.
    const std::thread::id owner{std::this_thread::get_id()};
    /// Synchronized value authority, never an owner-affine connection
    /// or session.
    const std::shared_ptr<OnlineRegistry> online;
    /// Immutable limits and monotonic clock, read by producers only
    /// for admission limits.
    const RoomServiceConfig config;
    /// Protects admission and pending commands against producers and
    /// retirement.
    std::mutex mutex;
    /// Bounded commands retained until the next owner cutoff; no
    /// per-command owner post.
    std::vector<Command> pending;
    std::map<arena::domain::RoomId, std::size_t, RoomLess>
        pendingRooms;
    bool accepting{false};
    /// Protected by mutex. At most one owner wake is queued for all
    /// acknowledgements.
    std::function<void(arena::runtime::OwnerScheduler::Action)>
        persistenceWake;
    bool persistenceWakePending{false};
    /// The following state is owner-only; callbacks are invoked after
    /// leaving its references.
    std::map<
        arena::domain::RoomId,
        std::unique_ptr<service::room::RoomActor>,
        RoomLess>
        actors;
    /// Owner-only admission endpoints, never disk streams or actors
    /// shared with the writer thread.
    std::map<
        arena::domain::RoomId,
        std::unique_ptr<service::room::ReplayRecording>,
        RoomLess>
        recordings;
    /// Domain lifecycles and retained immutable results share the
    /// management owner.
    std::map<
        arena::domain::RoomId,
        std::unique_ptr<ManagedMatch>,
        RoomLess>
        matches;
    RoomMetrics counts;
    arena::runtime::OwnerScheduler scheduler;
    arena::runtime::OwnerTimer timer;
    arena::runtime::OwnerClock::time_point deadline{};
    bool running{false}, started{false};
    /// Checkpoints reject reentrant owner mutation, including reply
    /// delivery.
    bool processing{false};
    /// A timer-mode restore attaches one scheduler without resetting
    /// its cut.
    bool restoredUnbound{false};
    /// Retired IDs remain reserved even when no actors survive.
    std::uint64_t incarnationHighWater{0};
};

RoomService::RoomService(
    std::shared_ptr<OnlineRegistry> online, RoomServiceConfig config
)
    : m_state(
          std::make_shared<State>(
              std::move(online), std::move(config)
          )
      )
{
}
RoomService::~RoomService() noexcept
{
    stop();
}
void RoomService::start(arena::runtime::OwnerScheduler scheduler)
{
    auto& state = *m_state;
    state.requireOwner();
    if ((state.started && !state.restoredUnbound) ||
        (!state.config.externalTicks &&
         (!scheduler.at || !scheduler.defer)))
        throw std::logic_error(
            "room start requires a fresh service and owner scheduler"
        );
    const bool restored = state.restoredUnbound;
    state.restoredUnbound = false;
    if (restored && !state.running)
        throw std::logic_error("stopped room service cannot restart");
    state.started = true;
    state.scheduler = std::move(scheduler);
    state.running = true;
    try
    {
        if (!restored)
            state.deadline = after(state.config.now(), tickInterval);
        if (!state.config.externalTicks)
            state.arm();
    }
    catch (...)
    {
        state.stop();
        throw;
    }
    std::lock_guard lock(state.mutex);
    if (!state.config.externalTicks)
        state.persistenceWake = state.scheduler.defer;
    state.accepting = true;
}

std::expected<RoomCheckpoint, RoomRecoveryError>
RoomService::checkpoint() const
{
    auto& state = *m_state;
    state.requireOwner();
    std::lock_guard lock(state.mutex);
    if (state.processing || !state.pending.empty() ||
        !state.recordings.empty() || state.persistenceWakePending)
        return std::unexpected(RoomRecoveryError::Busy);
    RoomCheckpoint saved{
        policy(state.config),
        {},
        {},
        metrics(),
        state.deadline,
        state.running,
        state.started,
        state.accepting,
        state.incarnationHighWater
    };
    saved.actors.reserve(state.actors.size());
    saved.matches.reserve(state.matches.size());
    for (const auto& [id, actor] : state.actors)
    {
        (void)id;
        saved.actors.push_back(actor->snapshot());
    }
    for (const auto& [id, value] : state.matches)
    {
        (void)id;
        const auto& m = *value;
        const auto outcome = m.persistence
                                 ? m.persistence->outcome.load(
                                       std::memory_order_acquire
                                   )
                                 : 0;
        if (!state.config.externalSettlement &&
            m.phase == RoomPhase::Settling && m.persistenceAccepted &&
            !outcome)
            return std::unexpected(RoomRecoveryError::Busy);
        const auto room = m.room.snapshot();
        const auto battle = m.battle.snapshot();
        if (!room || !battle)
            return std::unexpected(RoomRecoveryError::Invalid);
        saved.matches.push_back(
            {m.allocation,
             *room,
             *battle,
             m.phase,
             m.ready,
             m.presence,
             m.createdUnixMs,
             m.startedUnixMs,
             m.waitedTicks,
             m.startedTick,
             m.finishedTick,
             m.playedTicks,
             m.result,
             m.expires,
             m.retryAt,
             m.admissionAttempts,
             m.persistenceAccepted,
             m.persistenceBlocked,
             outcome}
        );
    }
    return saved;
}

std::expected<std::unique_ptr<RoomService>, RoomRecoveryError>
RoomService::restore(
    std::shared_ptr<OnlineRegistry> online,
    const RoomCheckpoint& saved,
    RoomServiceConfig bindings
)
{
    if (!online || !(saved.policy == policy(bindings)))
        return std::unexpected(RoomRecoveryError::Invalid);
    // A published service is constructed only after all owned values
    // validate. Destruction of a failed staging map must never
    // release registry members.
    try
    {
        auto restoredService = std::make_unique<RoomService>(
            online,
            std::move(bindings)
        );
        auto& state = *restoredService->m_state;
        if (saved.actors.size() > state.config.maxRooms ||
            saved.matches.size() > saved.actors.size() ||
            saved.running != saved.accepting ||
            (saved.running && !saved.started) ||
            (!saved.running &&
             (!saved.actors.empty() || !saved.matches.empty())) ||
            (!saved.started &&
             saved.deadline !=
                 arena::runtime::OwnerClock::time_point{}))
            return std::unexpected(RoomRecoveryError::Invalid);
        decltype(state.actors) actors;
        decltype(state.matches) matches;
        std::set<std::uint64_t> incarnations, matchIds;
        std::uint64_t previousRoom = 0;
        for (const auto& value : saved.actors)
        {
            if (value.roomId.value() <= previousRoom ||
                !value.incarnation ||
                value.incarnation > saved.incarnationHighWater ||
                !incarnations.insert(value.incarnation).second ||
                saved.metrics.ticks < value.tick)
                return std::unexpected(RoomRecoveryError::Invalid);
            previousRoom = value.roomId.value();
            auto actor = service::room::RoomActor::restore(
                value,
                state.config.actor
            );
            if (!actor)
                return std::unexpected(RoomRecoveryError::Invalid);
            actors.emplace(value.roomId, std::move(*actor));
        }
        const auto registry = online->checkpoint();
        RoomMetrics gauges;
        std::uint64_t started = 0, completed = 0, aborted = 0,
                      submitted = 0, committed = 0, failed = 0;
        previousRoom = 0;
        for (const auto& value : saved.matches)
        {
            if (saved.policy.callbackSettlement &&
                value.phase == RoomPhase::Settling &&
                value.persistenceAccepted &&
                !value.persistenceOutcome)
                return std::unexpected(RoomRecoveryError::Busy);
            const auto id = value.allocation.roomId;
            const auto actor = actors.find(id);
            if (id.value() <= previousRoom || actor == actors.end() ||
                !matchIds.insert(value.allocation.matchId).second ||
                !validMatch(
                    value,
                    actor->second->snapshot(),
                    saved.policy,
                    registry
                ))
                return std::unexpected(RoomRecoveryError::Invalid);
            previousRoom = id.value();
            matches.emplace(
                id,
                std::make_unique<State::ManagedMatch>(value)
            );
            if (value.phase == RoomPhase::Finished)
                ++gauges.retainedResults;
            else
                ++gauges.managedMatches;
            if (value.playedTicks)
                ++started;
            if (value.persistenceAccepted)
                ++submitted;
            if (value.persistenceBlocked)
                ++failed;
            if (state.durableSettlement() &&
                value.phase == RoomPhase::Settling)
            {
                ++gauges.pendingSettlements;
                if (value.persistenceBlocked)
                    ++gauges.blockedSettlements;
            }
            if (value.result && (!state.durableSettlement() ||
                                 value.phase == RoomPhase::Finished))
            {
                if (value.result->reason ==
                    SettlementReason::Completed)
                    ++completed;
                else
                    ++aborted;
                if (state.durableSettlement())
                    ++committed;
            }
        }
        for (const auto& [id, actor] : actors)
        {
            (void)actor;
            if ((id.value() & (std::uint64_t{1} << 63)) &&
                !matches.contains(id))
                return std::unexpected(RoomRecoveryError::Invalid);
        }
        for (const auto& value : registry.rooms)
            if (value.managed && actors.contains(value.roomId) &&
                !matches.contains(value.roomId))
                return std::unexpected(RoomRecoveryError::Invalid);
        const auto& counters = saved.metrics;
        if (counters.actors != actors.size() ||
            counters.managedMatches != gauges.managedMatches ||
            counters.retainedResults != gauges.retainedResults ||
            counters.pendingSettlements !=
                gauges.pendingSettlements ||
            counters.blockedSettlements !=
                gauges.blockedSettlements ||
            counters.battlesStarted < started ||
            counters.battlesCompleted < completed ||
            counters.battlesAborted < aborted ||
            counters.persistenceSubmitted < submitted ||
            counters.persistenceCommitted < committed ||
            counters.persistenceFailed < failed)
            return std::unexpected(RoomRecoveryError::Invalid);
        // No operation after publication can fail or invoke a runtime
        // binding.
        state.counts = counters;
        state.counts.actors = state.counts.managedMatches =
            state.counts.retainedResults = 0;
        state.counts.pendingSettlements = state.counts
                                              .blockedSettlements = 0;
        state.deadline = saved.deadline;
        state.incarnationHighWater = saved.incarnationHighWater;
        state.running = saved.running;
        state.started = saved.started;
        state.accepting = saved.accepting;
        state.restoredUnbound = saved.running &&
                                !state.config.externalTicks;
        state.actors = std::move(actors);
        state.matches = std::move(matches);
        return restoredService;
    }
    catch (...)
    {
        return std::unexpected(RoomRecoveryError::Invalid);
    }
}

bool RoomService::advance()
{
    auto& state = *m_state;
    state.requireOwner();
    if (!state.config.externalTicks || !state.running ||
        state.processing)
        return false;
    try
    {
        state.deadline = after(state.deadline, tickInterval);
        return state.step();
    }
    catch (...)
    {
        return false;
    }
}

bool RoomService::acknowledgeSettlement(
    std::uint64_t matchId, bool committed
)
{
    auto& state = *m_state;
    state.requireOwner();
    if (!state.config.externalSettlement || !state.running ||
        state.processing || !matchId)
        return false;
    for (auto& [id, value] : state.matches)
    {
        auto& match = *value;
        if (match.allocation.matchId != matchId)
            continue;
        if (match.phase == RoomPhase::Finished)
            return committed;
        if (match.phase != RoomPhase::Settling ||
            !match.persistenceAccepted)
            return false;
        if (match.persistenceBlocked)
            return !committed;
        state.processing = true;
        struct Guard
        {
            bool& flag;
            ~Guard()
            {
                flag = false;
            }
        } guard{state.processing};
        if (!committed)
        {
            match.persistenceBlocked = true;
            ++state.counts.persistenceFailed;
            state.logPersistence(match, "room_persistence_failed");
            return state.running;
        }
        try
        {
            state.finish(id, match);
            return state.running;
        }
        catch (...)
        {
            return false; // Candidate owner discards state and
                          // captured effects.
        }
    }
    return false;
}

void RoomService::stop() noexcept
{
    m_state->stop();
}
bool RoomService::openMatch(const ManagedRoom& allocation)
{
    return m_state->openMatch(allocation);
}
bool RoomService::ready(
    SessionBinding binding,
    arena::domain::SessionId session,
    arena::domain::RoomId room,
    std::uint64_t incarnation,
    std::uint64_t membershipEpoch,
    bool value,
    StatusCompletion complete
)
{
    if (!complete)
        return false;
    State::Command command{binding, session, room};
    command.incarnation = incarnation;
    command.membershipEpoch = membershipEpoch;
    command.isReady = true;
    command.readyValue = value;
    command.statusComplete = std::move(complete);
    return m_state->submit(std::move(command));
}
bool RoomService::status(
    SessionBinding binding,
    arena::domain::SessionId session,
    StatusCompletion complete
)
{
    if (!complete)
        return false;
    State::Command command{binding, session};
    command.statusComplete = std::move(complete);
    return m_state->submit(std::move(command));
}
bool RoomService::settlement(
    SessionBinding binding,
    arena::domain::SessionId session,
    std::uint64_t matchId,
    SettlementCompletion complete
)
{
    if (!complete)
        return false;
    State::Command command{binding, session};
    command.matchId = matchId;
    command.settlementComplete = std::move(complete);
    return m_state->submit(std::move(command));
}
bool RoomService::snapshot(
    SessionBinding binding,
    arena::domain::SessionId session,
    arena::domain::RoomId room,
    SnapshotCompletion complete
)
{
    if (!complete)
        return false;
    State::Command command{binding, session, room};
    command.snapshotComplete = std::move(complete);
    return m_state->submit(std::move(command));
}
bool RoomService::input(
    SessionBinding binding,
    arena::domain::SessionId session,
    arena::domain::RoomId room,
    std::uint64_t incarnation,
    std::uint64_t membershipEpoch,
    std::uint64_t sequence,
    std::int32_t dx,
    std::int32_t dy,
    InputCompletion complete
)
{
    if (!complete)
        return false;
    State::Command command{binding, session, room};
    command.incarnation = incarnation;
    command.membershipEpoch = membershipEpoch;
    command.sequence = sequence;
    command.dx = dx;
    command.dy = dy;
    command.inputComplete = std::move(complete);
    return m_state->submit(std::move(command));
}
RoomMetrics RoomService::metrics() const
{
    m_state->requireOwner();
    auto result = m_state->counts;
    result.actors = m_state->actors.size();
    for (const auto& [id, match] : m_state->matches)
    {
        (void)id;
        if (match->phase == RoomPhase::Finished)
            ++result.retainedResults;
        else
            ++result.managedMatches;
        if (m_state->durableSettlement() &&
            match->phase == RoomPhase::Settling)
        {
            ++result.pendingSettlements;
            if (match->persistenceBlocked)
                ++result.blockedSettlements;
        }
    }
    return result;
}
std::int32_t RoomService::positionLimit() const noexcept
{
    return m_state->config.actor.positionLimit;
}
std::optional<service::room::ReplayLog>
RoomService::replayLog(arena::domain::RoomId room) const
{
    m_state->requireOwner();
    const auto found = m_state->actors.find(room);
    if (found == m_state->actors.end())
        return std::nullopt;
    return found->second->replayLog();
}
} // namespace service::gateway
