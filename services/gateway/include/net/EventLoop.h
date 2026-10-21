/** @file
 * @brief Runs channel callbacks, timers, and queued work on one
 * thread.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "net/TimerQueue.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{
/** @brief Runs channel callbacks, timers, and queued work on one
 * thread. */
class EventLoop
{
public:
    using Functor = std::function<void()>;

    /** @brief Creates the loop on the calling thread. */
    EventLoop();
    /** @brief Releases owned resources. */
    ~EventLoop() = default;

    // The loop and channel operations belong to the construction
    // thread.
    void loop();
    /** @brief Requests that the event loop stop. */
    void quit();
    /** @brief Reports whether the caller owns this event loop. */
    bool isInLoopThread() const noexcept;

    /** @brief Runs a callback now on the owner, or queues it from
     * another thread.
     * @param[in] cb Callback to run on the loop thread.
     */
    void runInLoop(Functor cb);
    // Thread-safe. The queue is currently unbounded.
    void queueInLoop(Functor cb);
    /** @brief Returns the highest observed callback queue length. */
    std::size_t peakPendingFunctorCount() const;

    // Timer operations are confined to the construction thread.
    TimerId runAfter(Timer::Duration delay, Functor cb);
    /** @brief Schedules a repeating callback on this loop.
     * @param[in] interval Delay between firings.
     * @param[in] cb Callback invoked on the owner thread.
     * @return Identifier that can be passed to cancelTimer().
     */
    TimerId runEvery(Timer::Duration interval, Functor cb);
    /** @brief Cancels timer. */
    bool cancelTimer(TimerId id);
    /** @brief Returns the number of scheduled timers. */
    std::size_t pendingTimerCount() const;

    /** @brief Adds channel. */
    void addChannel(Channel*);
    /** @brief Updates channel. */
    void updateChannel(Channel*);
    // Also invalidate an event already copied into the current batch.
    // Remove before destroying a Channel, on the owner thread.
    void removeChannel(Channel*);

private:
    /** @brief Consumes the event-loop wakeup notification. */
    void handleWakeup();
    /** @brief Wakes the event loop from a blocking poll. */
    void wakeup();
    /** @brief Runs callbacks queued for the owner thread. */
    void doPendingFunctors();

    /// Thread that owns this event loop.
    const std::thread::id m_threadId;
    /// Event descriptor used to wake the polling loop.
    FileDescriptor m_wakeupFd;
    /// Channel that consumes event-loop wakeups.
    std::unique_ptr<Channel> m_wakeupChannel;
    /// Guards state shared with callers on other threads.
    mutable std::mutex m_mutex;
    /// Cross-thread request to stop the loop.
    std::atomic<bool> m_quit{false};
    /// Poller for registered channels.
    EpollPoller m_poller;
    // TimerQueue teardown removes its Channel, so this batch storage
    // must outlive m_timerQueue (members destruct in reverse order).
    std::vector<Channel*> m_activeChannels;
    /// Timers owned by this event loop.
    std::unique_ptr<TimerQueue> m_timerQueue;
    /// Callbacks awaiting owner-thread execution.
    std::vector<Functor> m_pendingFunctors;
    /// Highest observed pending callback count.
    std::size_t m_peakPendingFunctors{0};
};
} // namespace service::gateway
