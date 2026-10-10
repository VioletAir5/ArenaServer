/** @file
 * @brief Owns a thread running one event loop.
 */
#pragma once

#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>

#include "net/EventLoop.h"

namespace service::gateway
{
/** @brief Owns a thread running one event loop. */
class EventLoopThread
{
public:
    /** @brief Constructs the event loop thread. */
    EventLoopThread() = default;
    /** @brief Releases owned resources. */
    ~EventLoopThread() noexcept;

    /** @brief Copy construction is disabled. */
    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // startLoop() and stop() may overlap; stop calls serialize joins.
    // Destruction requires exclusive ownership and must not run on
    // the worker thread (that contract violation terminates).
    // A concurrent stop may make startup fail or expire its result.
    // Starts once; repeated calls while running return the same loop.
    // The returned non-owning pointer expires when the worker exits,
    // including quit() or a callback exception. Finish all producers
    // before requesting exit. Cross-thread work must be queued.
    EventLoop* startLoop();

    // Thread-safe, including from the worker. Wait for startup, request
    // quit while the published loop is alive, but do not join.
    // Pool shutdown uses this to wake every worker before joining any.
    void requestStop();

    // Call outside the worker: request quit and join, then rethrow any
    // worker exception once. No restart after exit. Destruction also
    // joins, but does not propagate worker exceptions.
    void stop();

private:
    // Only the test peer may arrange a paused startup for race tests.
    friend class EventLoopThreadTestPeer;

    /** @brief Reports whether in worker thread. */
    bool isInWorkerThread();
    void threadFunc();
    /** @brief Stops thread. */
    void stopThread();

    /// Event loop that owns this object and its callbacks.
    EventLoop* m_loop{nullptr};
    /// Stores the start complete.
    bool m_startComplete{false};
    /// Stores the exception.
    std::exception_ptr m_exception;
    // Cached identity avoids reading a jthread handle during join().
    std::thread::id m_workerId;
    /// Guards state shared with callers on other threads.
    std::mutex m_mutex;
    // Only stopping callers use this lock; the worker never takes it.
    std::mutex m_stopMutex;
    /// Stores the cv.
    std::condition_variable m_cv;
    // The thread must be joined before synchronization members die.
    std::unique_ptr<std::jthread> m_thread;
};
} // namespace service::gateway
