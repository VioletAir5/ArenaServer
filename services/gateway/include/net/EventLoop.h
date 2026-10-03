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
    using Functor = std::move_only_function<void()>;

    EventLoop();
    ~EventLoop() = default;

    // The loop and channel operations belong to the construction
    // thread.
    void loop();
    void quit();

    // A foreign caller delegates the task to the owner thread. The task
    // may own move-only resources such as an accepted Socket.
    void runInLoop(Functor cb);
    // Thread-safe. The queue is currently unbounded.
    void queueInLoop(Functor cb);
    std::size_t peakPendingFunctorCount() const;
    void assertInLoopThread() const;

    // Timer operations are confined to the construction thread.
    TimerId runAfter(Timer::Duration delay, Timer::Callback cb);
    TimerId runEvery(Timer::Duration interval, Timer::Callback cb);
    bool cancelTimer(TimerId id);
    std::size_t pendingTimerCount() const;

    void addChannel(Channel*);
    void updateChannel(Channel*);
    void removeChannel(Channel*);

private:
    void handleWakeup();
    void wakeup();
    void doPendingFunctors();
    bool isInLoopThread() const noexcept;

    const std::thread::id m_threadId;
    FileDescriptor m_wakeupFd;
    std::unique_ptr<Channel> m_wakeupChannel;
    mutable std::mutex m_mutex;
    std::atomic<bool> m_quit{false};
    EpollPoller m_poller;
    std::unique_ptr<TimerQueue> m_timerQueue;
    std::vector<Channel*> m_activeChannels;
    std::vector<Functor> m_pendingFunctors;
    std::size_t m_peakPendingFunctors{0};
};
} // namespace service::gateway
