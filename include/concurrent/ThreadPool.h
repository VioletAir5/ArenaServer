#pragma once

#include "concurrent/BlockingQueue.h"

#include<functional>
#include<mutex>
#include<thread>
#include<vector>
#include<future>
#include<type_traits>
#include<tuple>
#include<stdexcept>
#include <utility>
#include <memory>
#include<atomic>

namespace service
{

using Task = std::function<void()>;

class ThreadPool
{
private:
    enum class State
    {
        Created,
        Running,
        Stopped
    };

public:
    explicit ThreadPool(size_t, size_t);

    virtual ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void start();

    void shutdown();

protected:
    virtual void onWorkerStart() noexcept {}
    virtual void onWorkerStop() noexcept {}

    virtual void beforeExecute(Task&) noexcept {}
    virtual void afterExecute() noexcept {}

private:
    void workerLoop();

private:
    size_t _threadCount;

    BlockingQueue<Task> _tasks;
    std::vector<std::thread> _workers;

    std::mutex _stateMutex;
    std::atomic<State> _state{State::Created};

// =======================
// implement
// =======================
public:
    template<class F, class... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>
    {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            [func = std::forward<F>(f),
            arguments = std::make_tuple(std::forward<Args>(args)...)] () mutable
            {
                return std::apply(
                    [&func](auto&&... unpacked) -> ReturnType
                    {
                        return std::invoke(
                            func,
                            std::forward<decltype(unpacked)>(unpacked)...
                        );
                    },
                    std::move(arguments)
                );
            }
        );

        std::future<ReturnType> future = task->get_future();

        if (_state.load() != State::Running) {
            throw std::runtime_error("thread pool is not accepting tasks");
        }
        
        if (!_tasks.push([task] {
            (*task)();
        })) 
        {
            throw std::runtime_error("thread pool is shutting down");
        }

        return future;
    }
};

}