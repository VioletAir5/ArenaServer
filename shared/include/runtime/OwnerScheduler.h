/** @file
 * @brief Schedules callbacks and cancellable timers on a designated
 * owner.
 */
#pragma once

#include <chrono>
#include <functional>

#include <boost/asio/any_io_executor.hpp>

namespace arena::runtime
{
using OwnerClock = std::chrono::steady_clock;
// Destroy/reset on the owner to cancel. The scheduler outlives
// tokens.
/** @brief Cancels an owner-thread timer when its token is reset or
 * destroyed. */
class OwnerTimer
{
public:
    explicit OwnerTimer(std::function<void()> cancel = {});
    ~OwnerTimer();
    OwnerTimer(OwnerTimer&&) noexcept;
    OwnerTimer& operator=(OwnerTimer&&) noexcept;
    OwnerTimer(const OwnerTimer&) = delete;
    /** @brief Cancels the held timer and clears its action. */
    void reset() noexcept;

private:
    /// Cancellation action for the scheduled timer.
    std::function<void()> m_cancel;
};
/** @brief Provides owner-thread callback and timer scheduling to
 * services. */
struct OwnerScheduler
{
    using Action = std::function<void()>;
    // Thread-safe; always enqueue on owner, never invoke inline. If
    // enqueue throws it must not retain the action. The service
    // closes admission before releasing its owner executor.
    /// Defers an action to the designated owner.
    std::function<void(Action)> defer;
    /// Schedules an action for a monotonic deadline.
    std::function<OwnerTimer(OwnerClock::time_point, Action)> at;
};
/** @brief Adapts an executor to owner scheduling without owning a
 * thread.
 * @param[in] executor Serialized executor whose lifetime exceeds all
 * tokens.
 * @return Thread-safe deferred posting and owner-only timer
 * operations.
 */
OwnerScheduler ownerScheduler(boost::asio::any_io_executor executor);
} // namespace arena::runtime
