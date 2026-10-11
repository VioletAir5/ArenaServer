# services/gateway/include/net/EventLoopThreadPool.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；70行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Distributes work across event-loop threads.
 */
#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"

namespace service::gateway
{
/** @brief Distributes work across event-loop threads. */
class EventLoopThreadPool
{
public:
    /** @brief Constructs the event loop thread pool. */
    EventLoopThreadPool(EventLoop* baseLoop, std::size_t threadCount);
    /** @brief Releases owned resources. */
    ~EventLoopThreadPool() noexcept;

    /** @brief Copy construction is disabled. */
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
    /** @brief Returns all loops. */
    std::vector<EventLoop*> getAllLoops() const;

    // Indicates a started session, not worker health.
    bool started() const;

private:
    void requireBaseThread() const;

    /// Stores the base loop.
    EventLoop* m_baseLoop;
    /// Stores the thread count.
    std::size_t m_threadCount;
    /// Stores the next.
    std::size_t m_next{0};
    /// Stores the started.
    bool m_started{false};
    /// Stores the threads.
    std::vector<std::unique_ptr<EventLoopThread>> m_threads;
};
} // namespace service::gateway
~~~
