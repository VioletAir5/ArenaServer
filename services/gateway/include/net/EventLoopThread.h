#pragma once

#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>

#include "net/EventLoop.h"

namespace service::gateway
{
class EventLoopThread
{
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // Control calls and destruction must be serialized by the caller.
    // Starts once; repeated calls while running return the same loop.
    // The returned non-owning pointer expires when the worker exits,
    // including quit() or a callback exception. Finish all producers
    // before requesting exit. Cross-thread work must be queued.
    EventLoop* startLoop();

    // Call outside the worker: request quit and join, then rethrow any
    // worker exception once. No restart after exit. Destruction also
    // joins, but does not propagate worker exceptions.
    void stop();

private:
    void threadFunc();
    void stopThread();

    EventLoop* m_loop{nullptr};
    bool m_startComplete{false};
    std::exception_ptr m_exception;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    // The thread must be joined before synchronization members die.
    std::unique_ptr<std::jthread> m_thread;
};
} // namespace service::gateway
