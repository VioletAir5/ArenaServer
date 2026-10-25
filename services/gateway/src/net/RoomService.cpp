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
            arena::domain::RoomId target
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
        arena::domain::RoomId room;
        /// Actor and membership fences supplied from an earlier room
        /// snapshot.
        std::uint64_t incarnation{0}, membershipEpoch{0}, sequence{0};
        /// Integer controls; the actor validates their range without
        /// mutating state.
        std::int32_t dx{0}, dy{0};
        /// Exactly one completion is set; neither retains connection
        /// state.
        SnapshotCompletion snapshotComplete;
        InputCompletion inputComplete;
        /// Owner-produced replies are delivered after releasing all
        /// actor references.
        std::optional<SnapshotResult> snapshotResult;
        std::optional<InputResult> inputResult;
        ErrorCode error{arena::protocol::ERR_INTERNAL};
        bool authorized{false};
    };

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
            config.maxPendingPerRoom > config.actor.maxInputsPerTick)
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
            auto [count, inserted] = pendingRooms.try_emplace(
                command.room,
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
            if (command.inputComplete &&
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
        for (const auto index : commands)
        {
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
        // The canonical cutoff is prepared before actor commit; only
        // copied values enter the bounded disk-worker queue.
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
                incarnation,
                result.snapshot.tick,
                result.snapshot.checksum
            );
        }
        catch (...)
        {
        }
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
                rooms[batch[index].room].push_back(index);
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
                !command.inputResult)
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
