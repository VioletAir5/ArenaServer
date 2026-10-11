# shared/src/concurrent/ThreadPool.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；107行。

[对应分析](../../../../01-shared.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Executes queued work on worker threads.
 */
#include <concurrent/ThreadPool.h>

namespace service
{

ThreadPool::ThreadPool(size_t threadCount, size_t queueSize)
    : _threadCount(threadCount)
    , _tasks(queueSize)
{
    if (threadCount == 0)
    {
        throw std::invalid_argument("threadCount must be greater than 0");
    }

    _workers.reserve(threadCount);
}

ThreadPool::~ThreadPool()
{
    shutdown();
}

void ThreadPool::start()
{

    std::lock_guard<std::mutex> lock(_stateMutex);

    State s = _state.load();
    switch (s)
    {
    case State::Created:
        try
        {
            for (size_t i = 0; i < _threadCount; i++)
            {
                _workers.emplace_back([this] { workerLoop(); });
            }
            _state.store(State::Running);
            break;
        }
        catch (...)
        {
            _tasks.close();
            for (auto& work : _workers)
            {
                if (work.joinable())
                    work.join();
            }

            _state = State::Stopped;

            throw;
        }
    case State::Running:
        throw std::runtime_error("can't restart thread pool");
    case State::Stopped:
        throw std::runtime_error("thread pool has stopped");
    }
}

void ThreadPool::shutdown()
{
    std::lock_guard<std::mutex> lock(_stateMutex);
    State s = _state.load();
    switch (s)
    {
    case State::Created:
        _state.store(State::Stopped);
        break;
    case State::Running:
        _state.store(State::Stopped);
        _tasks.close();
        for (auto& worker : _workers)
        {
            if (worker.joinable())
                worker.join();
        }
        break;
    case State::Stopped:
        // log
        break;
    }
}

void ThreadPool::workerLoop()
{
    onWorkerStart();

    std::optional<Task> task;

    while (task = _tasks.pop())
    {
        Task& current = *task;
        beforeExecute(current);

        current();

        afterExecute();
    }

    onWorkerStop();
}

} // namespace service
~~~
