/** @file
 * @brief Executes queued work on worker threads.
 */
#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "concurrent/BlockingQueue.h"

namespace service
{

using Task = std::function<void()>;

/** @brief Executes queued work on worker threads. */
class ThreadPool
{
private:
    /** @brief Tracks worker startup and shutdown. */
    enum class State
    {
        Created,
        Running,
        Stopped
    };

public:
    /** @brief Constructs the thread pool. */
    explicit ThreadPool(size_t, size_t);

    /** @brief Joins workers and releases the pool. */
    virtual ~ThreadPool();

    /** @brief Copy construction is disabled. */
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void start();

    void shutdown();

protected:
    virtual void onWorkerStart() noexcept
    {
    }
    virtual void onWorkerStop() noexcept
    {
    }

    virtual void beforeExecute(Task&) noexcept
    {
    }
    virtual void afterExecute() noexcept
    {
    }

private:
    void workerLoop();

private:
    /// Configured number of worker threads.
    size_t _threadCount;

    /// Bounded task queue shared with workers.
    BlockingQueue<Task> _tasks;
    /// Worker threads owned by this pool.
    std::vector<std::thread> _workers;

    /// Guards lifecycle transitions.
    std::mutex _stateMutex;
    /// Current lifecycle state observed across threads.
    std::atomic<State> _state{State::Created};

    // =======================
    // implement
    // =======================
public:
    template <class F, class... Args>
    auto submit(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>
    {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            [func = std::forward<F>(f),
             arguments = std::make_tuple(std::forward<Args>(args)...)]() mutable
            {
                return std::apply(
                    [&func](auto&&... unpacked) -> ReturnType
                    { return std::invoke(func, std::forward<decltype(unpacked)>(unpacked)...); },
                    std::move(arguments));
            });

        std::future<ReturnType> future = task->get_future();

        if (_state.load() != State::Running)
        {
            throw std::runtime_error("thread pool is not accepting tasks");
        }

        if (!_tasks.push([task] { (*task)(); }))
        {
            throw std::runtime_error("thread pool is shutting down");
        }

        return future;
    }
};

} // namespace service
