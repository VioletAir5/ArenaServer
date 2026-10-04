#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"

namespace service::gateway
{
class EventLoopThreadPool
{
public:
    EventLoopThreadPool(EventLoop* baseLoop, std::size_t threadCount);
    ~EventLoopThreadPool() noexcept;

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool&
    operator=(const EventLoopThreadPool&) = delete;

    // All pool operations, including destruction, belong to
    // baseLoop's thread. baseLoop must outlive the pool. Workers only
    // use their selected loop, never the pool's management API.
    // Idempotent while started; after stop(), start() creates new
    // workers. A failed start rolls back all workers and leaves the
    // pool stopped.
    void start();
    // Finish producers and tear down worker-owned channels on their
    // owner loops before stop(). Requests all workers to quit, joins
    // all, then rethrows the first worker error. Repeated stop() is
    // harmless. This is not a pending-task drain: await task completion
    // before requesting shutdown.
    void stop();

    // Non-owning pointers: queue work through
    // EventLoop::queueInLoop(). A borrow expires on worker exit,
    // including an uncaught callback exception. Do not independently
    // quit a worker while using it. Getters reject use before
    // start/after stop and report exited workers. With zero workers,
    // both getters fall back to baseLoop.
    EventLoop* getNextLoop();
    std::vector<EventLoop*> getAllLoops() const;

    // Indicates a started session, not worker health.
    bool started() const;

private:
    void requireBaseThread() const;

    EventLoop* m_baseLoop;
    std::size_t m_threadCount;
    std::size_t m_next{0};
    bool m_started{false};
    std::vector<std::unique_ptr<EventLoopThread>> m_threads;
};
} // namespace service::gateway
