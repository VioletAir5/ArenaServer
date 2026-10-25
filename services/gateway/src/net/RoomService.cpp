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
#include <thread>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "arena/domain/Battle.h"
#include "arena/domain/Room.h"

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
std::uint64_t nextIncarnation()
{
    static std::atomic<std::uint64_t> next{0};
    auto previous = next.load(std::memory_order_relaxed);
    do
    {
        if (previous == UINT64_MAX)
            throw std::overflow_error("room incarnation exhausted");
    } while (!next.compare_exchange_weak(
        previous,
        previous + 1,
        std::memory_order_relaxed
    ));
    return previous + 1;
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
ConnectionClock::time_point after(
    ConnectionClock::time_point time,
    ConnectionClock::duration duration
)
{
    if (time > ConnectionClock::time_point::max() - duration)
        throw std::overflow_error("room timer deadline exhausted");
    return time + duration;
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

    struct ManagedMatch
    {
        explicit ManagedMatch(ManagedRoom allocation)
            : allocation(std::move(allocation))
            , room(this->allocation.roomId)
            , battle(
                  arena::domain::BattleId{this->allocation.matchId},
                  this->allocation.roomId
              )
        {
        }
        ManagedRoom allocation;
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
        std::optional<ConnectionClock::time_point> expires;
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
        if (!online || !config.now || !config.maxRooms ||
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
    static void deliver(Command& command) noexcept
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
        }
        catch (...)
        {
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
            match.result
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
                    command.settlementResult.emplace(*match->result);
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
        if (match.phase == RoomPhase::Playing)
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
        else
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
        if (reason == SettlementReason::Completed)
            ++counts.battlesCompleted;
        else
            ++counts.battlesAborted;
        logPhase(id, match);
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
            finish(id, match);
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
            nextIncarnation(),
            config.actor
        );
        auto match = std::make_unique<ManagedMatch>(allocation);
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
                                    nextIncarnation(),
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
    void step()
    {
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
                    // A core step commits atomically. Other rooms can
                    // keep ticking.
                }
            }
        }
        catch (...)
        {
        }
        for (auto& command : batch)
        {
            if (!running && !command.snapshotResult &&
                !command.inputResult && !command.statusResult &&
                !command.settlementResult)
                command.error = arena::protocol::ERR_UNAVAILABLE;
            deliver(command);
        }
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
                online->finishMatch(id, match->allocation.matchId);
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
    ConnectionScheduler scheduler;
    ConnectionTimer timer;
    ConnectionClock::time_point deadline{};
    bool running{false}, started{false};
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
void RoomService::start(ConnectionScheduler scheduler)
{
    auto& state = *m_state;
    state.requireOwner();
    if (state.started || !scheduler.at || !scheduler.defer)
        throw std::logic_error(
            "room start requires a fresh service and owner scheduler"
        );
    state.started = true;
    state.scheduler = std::move(scheduler);
    state.running = true;
    try
    {
        state.deadline = after(state.config.now(), tickInterval);
        state.arm();
    }
    catch (...)
    {
        state.stop();
        throw;
    }
    std::lock_guard lock(state.mutex);
    state.accepting = true;
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
