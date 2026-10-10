/** @file
 * @brief Schedules timers using a Linux timer descriptor.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "net/Channel.h"
#include "net/Timer.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{
class EventLoop;

using TimerId = std::uint64_t;

// All methods are called on the owning EventLoop thread.
/** @brief Schedules timers using a Linux timer descriptor. */
class TimerQueue
{
public:
    /** @brief Constructs the timer queue. */
    explicit TimerQueue(EventLoop* loop);
    /** @brief Releases owned resources. */
    ~TimerQueue();

    /** @brief Schedules a one-shot timer after the delay. */
    TimerId addAfter(Timer::Duration delay, Timer::Callback callback);
    TimerId
    addEvery(Timer::Duration interval, Timer::Callback callback);
    bool cancel(TimerId id);
    std::size_t size() const noexcept;

private:
    static bool earlier(const Timer& lhs, const Timer& rhs) noexcept;
    void swapEntries(std::size_t lhs, std::size_t rhs);
    void siftUp(std::size_t index);
    void siftDown(std::size_t index);
    void insert(Timer timer);
    /** @brief Removes the timer at a heap index. */
    Timer removeAt(std::size_t index);
    void arm();
    /** @brief Consumes timer readiness and runs expired callbacks. */
    void handleRead();

    /// Event loop that owns this object and its callbacks.
    EventLoop* m_loop;
    /// Stores the timer fd.
    FileDescriptor m_timerFd;
    /// Owner-thread channel for timer descriptor readiness.
    Channel m_channel;
    /// Stores the heap.
    std::vector<Timer> m_heap;
    /// Stores the indices.
    std::unordered_map<TimerId, std::size_t> m_indices;
    /// Stores the next id.
    TimerId m_nextId{1};
    /// Stores the running id.
    TimerId m_runningId{0};
    /// Stores the cancel running.
    bool m_cancelRunning{false};
};
} // namespace service::gateway