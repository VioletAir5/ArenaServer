/** @file
 * @brief Marshals matchmaking commands to the management owner and
 * fences sessions.
 */
#include "net/MatchmakingService.h"

#include <map>
#include <mutex>
#include <thread>
#include <tuple>
#include <utility>

#include <spdlog/spdlog.h>

namespace service::gateway
{
namespace
{
/** @brief Maps queue failures to stable public application errors. */
ErrorCode queueError(service::match::QueueError error)
{
    using service::match::QueueError;
    switch (error)
    {
    case QueueError::InvalidRequest:
        return arena::protocol::ERR_INVALID_REQUEST;
    case QueueError::Capacity:
        return arena::protocol::ERR_UNAVAILABLE;
    case QueueError::Conflict:
    case QueueError::NotFound:
    case QueueError::AlreadyAssigned:
        return arena::protocol::ERR_BUSINESS_FAILURE;
    }
    return arena::protocol::ERR_INTERNAL;
}

/** @brief Isolates client completion failures from upkeep and other
 * requests. */
void deliver(
    MatchmakingService::Completion complete,
    MatchmakingService::Result result
) noexcept
{
    try
    {
        complete(std::move(result));
    }
    catch (...)
    {
    }
}
} // namespace

struct MatchmakingService::State : std::enable_shared_from_this<State>
{
    using Operation = std::function<
        Result(service::match::MatchQueue&)>;
    struct Command
    {
        /// Immutable authorization tuple copied on the connection
        /// owner.
        SessionBinding binding;
        /// Logical identity preserved by authorized JWT transport
        /// recovery.
        arena::domain::SessionId session;
        /// Executes only on the management owner, after authorization
        /// is rechecked.
        Operation operation;
        /// Dispatcher-owned completion safe to invoke from this
        /// owner.
        Completion complete;
        /// Only new joins require the caller to be outside a gateway
        /// roster.
        bool requiresFreeRoom{false};
    };

    State(
        std::shared_ptr<OnlineRegistry> authority,
        RatingProvider provider,
        service::match::QueueConfig limits,
        std::chrono::milliseconds period,
        std::size_t pendingLimit
    )
        : online(std::move(authority))
        , ratings(std::move(provider))
        , interval(period)
        , maxPending(pendingLimit)
    {
        if (!online || !ratings ||
            interval <= std::chrono::milliseconds::zero() ||
            !maxPending)
            throw std::invalid_argument(
                "invalid matchmaking dependencies"
            );
        // Eligibility reads synchronized value snapshots, never
        // ConnectionSession or another owner's mutable domain
        // objects.
        queue = std::make_unique<service::match::MatchQueue>(
            limits,
            [this](auto player, auto session)
            {
                const auto current = online->snapshot(player);
                if (!current || current->sessionId != session ||
                    current->roomId)
                    return service::match::Availability::Gone;
                return current->presence == Presence::Online
                           ? service::match::Availability::Ready
                           : service::match::Availability::Paused;
            }
        );
    }

    /** @brief Rejects foreign access before reading owner-affine
     * state. */
    void requireOwner() const
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "matchmaking requires management owner"
            );
    }

    /** @brief Keeps enqueue and executor retirement behind the same
     * lock. */
    bool submit(Command command)
    {
        if (!command.complete)
            return false;
        std::lock_guard lock(mutex);
        if (!accepting || pending.size() >= maxPending ||
            nextCommand == UINT64_MAX)
            return false;
        const auto id = ++nextCommand;
        try
        {
            pending.emplace(id, std::move(command));
            scheduler.defer(
                [weak = weak_from_this(), id]
                {
                    if (auto self = weak.lock())
                        self->execute(id);
                }
            );
            return true;
        }
        catch (...)
        {
            pending.erase(id);
            return false;
        }
    }

    /** @brief Removes a command exactly once, then invokes callbacks
     * unlocked. */
    void execute(std::uint64_t id)
    {
        requireOwner();
        std::optional<Command> command;
        {
            std::lock_guard lock(mutex);
            const auto found = pending.find(id);
            if (found == pending.end())
                return;
            command.emplace(std::move(found->second));
            pending.erase(found);
        }
        Result result = std::unexpected(
            arena::protocol::ERR_UNAUTHENTICATED
        );
        try
        {
            const auto current = online->snapshot(
                command->binding.playerId
            );
            // The deferred request must still belong to this exact
            // transport; existing tickets use logical SessionId so
            // JWT recovery can reuse them.
            if (current && current->sessionId == command->session &&
                current->connectionId ==
                    command->binding.connectionId &&
                current->epoch == command->binding.epoch &&
                current->presence == Presence::Online)
            {
                if (current->roomId && command->requiresFreeRoom)
                    result = std::unexpected(
                        arena::protocol::ERR_BUSINESS_FAILURE
                    );
                else
                    result = command->operation(*queue);
            }
        }
        catch (...)
        {
            result = std::unexpected(arena::protocol::ERR_INTERNAL);
        }
        deliver(std::move(command->complete), std::move(result));
    }

    /** @brief Arms an owner timer without extending this service's
     * lifetime. */
    void arm()
    {
        timer = scheduler.at(
            service::match::Clock::now() + interval,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock())
                    self->tick();
            }
        );
    }

    /** @brief Advances matching and expiry even when no client sends
     * traffic. */
    void tick()
    {
        requireOwner();
        if (!running)
            return;
        timer.reset();
        try
        {
            queue->advance(service::match::Clock::now());
            const auto metrics = queue->metrics();
            const auto stamp = std::tuple{
                metrics.joined,
                metrics.cancelled,
                metrics.expired,
                metrics.matchedPairs
            };
            if (stamp != lastLog)
            {
                lastLog = stamp;
                try
                {
                    spdlog::info(
                        "event=match_tick queued={} assigned={} "
                        "retained={} pairs={} expired={} "
                        "cancelled={} wait_ms_sum={} mmr_gap_sum={}",
                        metrics.queued,
                        metrics.assignedPlayers,
                        metrics.retained,
                        metrics.matchedPairs,
                        metrics.expired,
                        metrics.cancelled,
                        metrics.totalMatchedWaitMs,
                        metrics.totalMmrGap
                    );
                }
                catch (...)
                {
                }
            }
            if (running)
                arm();
        }
        catch (...)
        {
            stop();
        }
    }

    /** @brief Retires the mailbox before destroying timers or domain
     * Tickets. */
    void stop() noexcept
    {
        if (std::this_thread::get_id() != owner)
            std::terminate();
        std::map<std::uint64_t, Command> abandoned;
        {
            std::lock_guard lock(mutex);
            accepting = false;
            running = false;
            abandoned.swap(pending);
            scheduler.defer = {};
        }
        timer.reset();
        scheduler = {};
        if (queue)
        {
            finalMetrics = queue->metrics();
            queue.reset();
            finalMetrics.queued = finalMetrics.assignedPlayers =
                finalMetrics.retained = 0;
        }
        for (auto& [id, command] : abandoned)
        {
            (void)id;
            deliver(
                std::move(command.complete),
                std::unexpected(arena::protocol::ERR_UNAVAILABLE)
            );
        }
    }

    /// Owner thread is fixed before publication to I/O workers.
    const std::thread::id owner{std::this_thread::get_id()};
    /// Synchronized value authority shared with connection workers.
    std::shared_ptr<OnlineRegistry> online;
    /// Trusted, nonblocking admission rating lookup on the management
    /// owner.
    RatingProvider ratings;
    /// Monotonic maintenance period independent of incoming traffic.
    const std::chrono::milliseconds interval;
    /// Hard limit for commands retained by the mailbox.
    const std::size_t maxPending;
    /// Only admission fields and pending commands require a mutex.
    std::mutex mutex;
    /// Admitted commands removed once by execute or stop.
    std::map<std::uint64_t, Command> pending;
    /// Nonwrapping local generation for deferred command callbacks.
    std::uint64_t nextCommand{0};
    /// Published under mutex before any foreign enqueue may use
    /// scheduler.defer.
    bool accepting{false};
    /// Queue, timer and cumulative snapshots are mutated only by the
    /// owner.
    std::unique_ptr<service::match::MatchQueue> queue;
    /// Cleared only after admission closes under the mailbox mutex.
    ConnectionScheduler scheduler;
    /// Owner-only cancellation token, reset before executor
    /// retirement.
    ConnectionTimer timer;
    /// Cumulative statistics preserved after queue reclamation at
    /// stop.
    service::match::QueueMetrics finalMetrics;
    /// Suppresses periodic logs when lifecycle counters have not
    /// changed.
    std::tuple<
        std::uint64_t,
        std::uint64_t,
        std::uint64_t,
        std::uint64_t>
        lastLog{};
    /// Owner-only fence for already-ready periodic callbacks.
    bool running{false};
    /// Prevents restarting a retired service with old callback
    /// generations.
    bool started{false};
};

MatchmakingService::MatchmakingService(
    std::shared_ptr<OnlineRegistry> online,
    RatingProvider ratings,
    service::match::QueueConfig config,
    std::chrono::milliseconds interval,
    std::size_t maxPending
)
    : m_state(
          std::make_shared<State>(
              std::move(online),
              std::move(ratings),
              config,
              interval,
              maxPending
          )
      )
{
}
MatchmakingService::~MatchmakingService() noexcept
{
    stop();
}
void MatchmakingService::start(ConnectionScheduler scheduler)
{
    auto& state = *m_state;
    state.requireOwner();
    if (state.started || !scheduler.defer || !scheduler.at ||
        !state.queue)
        throw std::logic_error(
            "matchmaking start requires a fresh service and scheduler"
        );
    state.started = true;
    state.scheduler = std::move(scheduler);
    state.running = true;
    try
    {
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
void MatchmakingService::stop() noexcept
{
    m_state->stop();
}
bool MatchmakingService::join(
    SessionBinding binding,
    arena::domain::SessionId session,
    std::uint64_t key,
    Completion complete
)
{
    return m_state->submit(
        {binding,
         session,
         [state = m_state.get(),
          player = binding.playerId,
          session,
          key](auto& queue) -> Result
         {
             if (!key)
                 return std::unexpected(
                     arena::protocol::ERR_INVALID_REQUEST
                 );
             if (auto retained = queue.replay(
                     player,
                     session,
                     key,
                     service::match::Clock::now()
                 ))
                 return *retained;
             const auto rating = state->ratings(player);
             if (!rating)
                 return std::unexpected(
                     arena::protocol::ERR_UNAVAILABLE
                 );
             auto result = queue.join(
                 player,
                 session,
                 key,
                 *rating,
                 service::match::Clock::now()
             );
             if (!result)
                 return std::unexpected(queueError(result.error()));
             return *result;
         },
         std::move(complete),
         true}
    );
}
bool MatchmakingService::cancel(
    SessionBinding binding,
    arena::domain::SessionId session,
    arena::domain::TicketId ticket,
    Completion complete
)
{
    return m_state->submit(
        {binding,
         session,
         [player = binding.playerId, session, ticket](auto& queue)
             -> Result
         {
             auto result = queue.cancel(
                 player,
                 session,
                 ticket,
                 service::match::Clock::now()
             );
             if (!result)
                 return std::unexpected(queueError(result.error()));
             return *result;
         },
         std::move(complete)}
    );
}
bool MatchmakingService::status(
    SessionBinding binding,
    arena::domain::SessionId session,
    arena::domain::TicketId ticket,
    Completion complete
)
{
    return m_state->submit(
        {binding,
         session,
         [player = binding.playerId, session, ticket](auto& queue)
             -> Result
         {
             auto result = queue.status(
                 player,
                 session,
                 ticket,
                 service::match::Clock::now()
             );
             if (!result)
                 return std::unexpected(queueError(result.error()));
             return *result;
         },
         std::move(complete)}
    );
}
bool MatchmakingService::release(std::uint64_t matchId)
{
    m_state->requireOwner();
    return m_state->queue && m_state->queue->release(
                                 matchId,
                                 service::match::Clock::now()
                             );
}
service::match::QueueMetrics MatchmakingService::metrics() const
{
    m_state->requireOwner();
    return m_state->queue ? m_state->queue->metrics()
                          : m_state->finalMetrics;
}
} // namespace service::gateway
