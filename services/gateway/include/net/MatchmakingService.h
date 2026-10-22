/** @file
 * @brief Adapts authenticated requests to an owner-affine matchmaking
 * queue.
 */
#pragma once

#include "net/ConnectionFeature.h"
#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "match/MatchQueue.h"

namespace service::gateway
{
/** @brief Runs matching on the management owner through a bounded
 * mailbox. Construct/start/stop/destroy on that owner; requests may
 * arrive on any thread. The scheduler must outlive stop(). No
 * transport or connection session is retained.
 */
class MatchmakingService final
{
public:
    using Result = std::
        expected<service::match::TicketSnapshot, ErrorCode>;
    using Completion = std::function<void(Result)>;
    using RatingProvider = std::function<
        std::optional<std::uint32_t>(arena::domain::PlayerId)>;
    /** @brief Installs shared online authority and a trusted rating
     * adapter.
     * @param[in] ratings Fast, nonblocking owner callback returning a
     * server rating, or empty if unavailable. Existing ticket retries
     * do not call it again.
     * @param[in] interval Positive maintenance and matching interval.
     * @param[in] maxPending Maximum admitted commands waiting on the
     * owner.
     */
    MatchmakingService(
        std::shared_ptr<OnlineRegistry>,
        RatingProvider,
        service::match::QueueConfig = {},
        std::chrono::milliseconds interval =
            std::chrono::milliseconds(100),
        std::size_t maxPending = 1024
    );
    ~MatchmakingService() noexcept;
    MatchmakingService(const MatchmakingService&) = delete;
    MatchmakingService& operator=(const MatchmakingService&) = delete;
    /** @brief Starts periodic matching before listeners begin
     * processing requests. */
    void start(ConnectionScheduler);
    /** @brief Closes admission, cancels upkeep and rejects pending
     * commands. */
    void stop() noexcept;
    /** @brief Admits an idempotent join; false means completion will
     * not be called. */
    bool join(
        SessionBinding,
        arena::domain::SessionId,
        std::uint64_t key,
        Completion
    );
    /** @brief Admits cancellation for a nonzero caller-owned ticket
     * identity. */
    bool cancel(
        SessionBinding,
        arena::domain::SessionId,
        arena::domain::TicketId,
        Completion
    );
    /** @brief Admits a caller-owned ticket query, including terminal
     * history. */
    bool status(
        SessionBinding,
        arena::domain::SessionId,
        arena::domain::TicketId,
        Completion
    );
    /** @brief Releases an assignment on the management owner after
     * allocation finishes. */
    bool release(std::uint64_t matchId);
    /** @brief Copies management-owner metrics; call while the service
     * is alive. */
    service::match::QueueMetrics metrics() const;

private:
    struct State;
    /// Management owner retains queue state until all listeners have
    /// retired.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
