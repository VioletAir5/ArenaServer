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
class EventLoop
{
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop() = default;

    // The loop and channel operations belong to the construction
    // thread.
    void loop();
    void quit();
    bool isInLoopThread() const noexcept;

    void runInLoop(Functor cb);
    // Thread-safe. The queue is currently unbounded.
    void queueInLoop(Functor cb);
    std::size_t peakPendingFunctorCount() const;

    // Timer operations are confined to the construction thread.
    TimerId runAfter(Timer::Duration delay, Functor cb);
    TimerId runEvery(Timer::Duration interval, Functor cb);
    bool cancelTimer(TimerId id);
    std::size_t pendingTimerCount() const;

    void addChannel(Channel*);
    void updateChannel(Channel*);
    // Also invalidate an event already copied into the current batch.
    // Remove before destroying a Channel, on the owner thread.
    void removeChannel(Channel*);

private:
    void handleWakeup();
    void wakeup();
    void doPendingFunctors();

    const std::thread::id m_threadId;
    FileDescriptor m_wakeupFd;
    std::unique_ptr<Channel> m_wakeupChannel;
    mutable std::mutex m_mutex;
    std::atomic<bool> m_quit{false};
    EpollPoller m_poller;
    // TimerQueue teardown removes its Channel, so this batch storage
    // must outlive m_timerQueue (members destruct in reverse order).
    std::vector<Channel*> m_activeChannels;
    std::unique_ptr<TimerQueue> m_timerQueue;
    std::vector<Functor> m_pendingFunctors;
    std::size_t m_peakPendingFunctors{0};
};
} // namespace service::gateway
