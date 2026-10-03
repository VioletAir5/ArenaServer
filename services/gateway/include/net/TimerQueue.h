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
class TimerQueue
{
public:
    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

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
    Timer removeAt(std::size_t index);
    void arm();
    void handleRead();

    EventLoop* m_loop;
    FileDescriptor m_timerFd;
    Channel m_channel;
    std::vector<Timer> m_heap;
    std::unordered_map<TimerId, std::size_t> m_indices;
    TimerId m_nextId{1};
    TimerId m_runningId{0};
    bool m_cancelRunning{false};
};
} // namespace service::gateway