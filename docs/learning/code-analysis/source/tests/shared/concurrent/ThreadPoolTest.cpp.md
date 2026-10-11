# tests/shared/concurrent/ThreadPoolTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；700行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include <gtest/gtest.h>

#include "concurrent/ThreadPool.h"

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{

class TestThreadPool : public service::ThreadPool
{
public:
    using ThreadPool::ThreadPool;

    std::atomic<int> workerStarts{0};
    std::atomic<int> workerStops{0};
    std::atomic<int> beforeCount{0};
    std::atomic<int> afterCount{0};

protected:
    void onWorkerStart() noexcept override
    {
        workerStarts.fetch_add(
            1,
            std::memory_order_relaxed
        );
    }

    void onWorkerStop() noexcept override
    {
        workerStops.fetch_add(
            1,
            std::memory_order_relaxed
        );
    }

    void beforeExecute(service::Task&) noexcept override
    {
        beforeCount.fetch_add(
            1,
            std::memory_order_relaxed
        );
    }

    void afterExecute() noexcept override
    {
        afterCount.fetch_add(
            1,
            std::memory_order_relaxed
        );
    }
};

} // namespace

namespace service::test
{

// ============================================================
// Lifecycle
// ============================================================

TEST(ThreadPoolTest, StartAndShutdown)
{
    ThreadPool pool(4, 100);

    EXPECT_NO_THROW(pool.start());
    EXPECT_NO_THROW(pool.shutdown());
}


TEST(ThreadPoolTest, CannotStartTwice)
{
    ThreadPool pool(4, 100);

    pool.start();

    EXPECT_THROW(
        pool.start(),
        std::runtime_error
    );

    pool.shutdown();
}


TEST(ThreadPoolTest, ShutdownBeforeStartIsSafe)
{
    ThreadPool pool(4, 100);

    EXPECT_NO_THROW(
        pool.shutdown()
    );
}


TEST(ThreadPoolTest, CannotStartAfterShutdown)
{
    ThreadPool pool(4, 100);

    pool.shutdown();

    EXPECT_THROW(
        pool.start(),
        std::runtime_error
    );
}


TEST(ThreadPoolTest, MultipleShutdownIsSafe)
{
    ThreadPool pool(4, 100);

    pool.start();

    EXPECT_NO_THROW(pool.shutdown());
    EXPECT_NO_THROW(pool.shutdown());
    EXPECT_NO_THROW(pool.shutdown());
}


// ============================================================
// Submit
// ============================================================

TEST(ThreadPoolTest, RejectSubmitBeforeStart)
{
    ThreadPool pool(4, 100);

    EXPECT_THROW(
        pool.submit([] {
            return 42;
        }),
        std::runtime_error
    );
}


TEST(ThreadPoolTest, RejectSubmitAfterShutdown)
{
    ThreadPool pool(4, 100);

    pool.start();
    pool.shutdown();

    EXPECT_THROW(
        pool.submit([] {
            return 42;
        }),
        std::runtime_error
    );
}


TEST(ThreadPoolTest, SubmitReturnsFutureResult)
{
    ThreadPool pool(4, 100);

    pool.start();

    auto future = pool.submit([] {
        return 42;
    });

    EXPECT_EQ(
        future.get(),
        42
    );

    pool.shutdown();
}


TEST(ThreadPoolTest, SubmitVoidTask)
{
    ThreadPool pool(4, 100);

    pool.start();

    std::atomic<int> value{0};

    auto future = pool.submit([&value] {
        value.store(
            123,
            std::memory_order_relaxed
        );
    });

    future.get();

    EXPECT_EQ(
        value.load(std::memory_order_relaxed),
        123
    );

    pool.shutdown();
}


TEST(ThreadPoolTest, SubmitTaskWithArguments)
{
    ThreadPool pool(4, 100);

    pool.start();

    auto future = pool.submit(
        [](int a, int b) {
            return a + b;
        },
        10,
        20
    );

    EXPECT_EQ(
        future.get(),
        30
    );

    pool.shutdown();
}


TEST(ThreadPoolTest, SubmitMoveOnlyArgument)
{
    ThreadPool pool(4, 100);

    pool.start();

    auto ptr = std::make_unique<int>(42);

    auto future = pool.submit(
        [](std::unique_ptr<int> value) {
            return *value;
        },
        std::move(ptr)
    );

    EXPECT_EQ(future.get(), 42);
    EXPECT_EQ(ptr, nullptr);

    pool.shutdown();
}


// ============================================================
// Future / exception
// ============================================================

TEST(ThreadPoolTest, TaskExceptionPropagatesThroughFuture)
{
    ThreadPool pool(4, 100);

    pool.start();

    auto future = pool.submit([]() -> int {
        throw std::runtime_error("task failed");
    });

    EXPECT_THROW(
        future.get(),
        std::runtime_error
    );

    pool.shutdown();
}


TEST(ThreadPoolTest, ExceptionDoesNotKillWorkerThread)
{
    ThreadPool pool(1, 100);

    pool.start();

    auto badFuture = pool.submit([]() -> int {
        throw std::runtime_error("task failed");
    });

    EXPECT_THROW(
        badFuture.get(),
        std::runtime_error
    );

    auto goodFuture = pool.submit([] {
        return 123;
    });

    EXPECT_EQ(
        goodFuture.get(),
        123
    );

    pool.shutdown();
}


// ============================================================
// Multiple tasks
// ============================================================

TEST(ThreadPoolTest, MultipleTasksReturnCorrectResults)
{
    ThreadPool pool(4, 100);

    pool.start();

    constexpr int taskCount = 1000;

    std::vector<std::future<int>> futures;
    futures.reserve(taskCount);

    for (int i = 0; i < taskCount; ++i)
    {
        futures.emplace_back(
            pool.submit([i] {
                return i * 2;
            })
        );
    }

    for (int i = 0; i < taskCount; ++i)
    {
        EXPECT_EQ(
            futures[i].get(),
            i * 2
        );
    }

    pool.shutdown();
}


// ============================================================
// Graceful shutdown
// ============================================================

TEST(ThreadPoolTest, GracefulShutdownCompletesQueuedTasks)
{
    constexpr int taskCount = 500;

    ThreadPool pool(4, taskCount);

    std::atomic<int> completed{0};

    pool.start();

    for (int i = 0; i < taskCount; ++i)
    {
        pool.submit([&completed] {
            std::this_thread::sleep_for(100us);

            completed.fetch_add(
                1,
                std::memory_order_relaxed
            );
        });
    }

    pool.shutdown();

    EXPECT_EQ(
        completed.load(std::memory_order_relaxed),
        taskCount
    );
}


TEST(ThreadPoolTest, ShutdownWaitsForRunningTask)
{
    ThreadPool pool(1, 100);

    std::promise<void> taskStarted;
    std::promise<void> allowTaskFinish;

    auto startedFuture =
        taskStarted.get_future();

    auto finishFuture =
        allowTaskFinish.get_future();

    pool.start();

    pool.submit([&] {
        taskStarted.set_value();

        finishFuture.wait();
    });

    startedFuture.wait();

    std::atomic<bool> shutdownFinished{false};

    std::thread shutdownThread([&] {
        pool.shutdown();

        shutdownFinished.store(
            true,
            std::memory_order_release
        );
    });

    std::this_thread::sleep_for(10ms);

    // worker 仍然被 task 阻塞，因此 shutdown 不应该已经结束
    EXPECT_FALSE(
        shutdownFinished.load(
            std::memory_order_acquire
        )
    );

    allowTaskFinish.set_value();

    shutdownThread.join();

    EXPECT_TRUE(
        shutdownFinished.load(
            std::memory_order_acquire
        )
    );
}


// ============================================================
// Concurrent submit
// ============================================================

TEST(ThreadPoolTest, ConcurrentSubmit)
{
    constexpr int producerCount = 4;
    constexpr int tasksPerProducer = 1000;
    constexpr int taskCount =
        producerCount * tasksPerProducer;

    ThreadPool pool(4, taskCount);

    std::atomic<int> completed{0};

    std::vector<std::thread> producers;
    producers.reserve(producerCount);

    pool.start();

    for (int i = 0; i < producerCount; ++i)
    {
        producers.emplace_back([&] {
            for (int j = 0;
                 j < tasksPerProducer;
                 ++j)
            {
                pool.submit([&completed] {
                    completed.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                });
            }
        });
    }

    for (auto& producer : producers)
    {
        producer.join();
    }

    pool.shutdown();

    EXPECT_EQ(
        completed.load(
            std::memory_order_relaxed
        ),
        taskCount
    );
}


// ============================================================
// submit() vs shutdown()
// ============================================================

TEST(ThreadPoolTest, ConcurrentSubmitAndShutdownIsSafe)
{
    ThreadPool pool(4, 1024);

    std::atomic<bool> stopProducer{false};
    std::atomic<int> accepted{0};
    std::atomic<int> executed{0};
    std::atomic<int> rejected{0};

    pool.start();

    std::thread producer([&] {
        while (!stopProducer.load(
            std::memory_order_relaxed))
        {
            try
            {
                pool.submit([&executed] {
                    executed.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                });

                accepted.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
            catch (const std::runtime_error&)
            {
                rejected.fetch_add(
                    1,
                    std::memory_order_relaxed
                );

                break;
            }
        }
    });

    std::this_thread::sleep_for(10ms);

    pool.shutdown();

    stopProducer.store(
        true,
        std::memory_order_relaxed
    );

    producer.join();

    // 所有成功进入 queue 的任务都应该在 graceful shutdown 中执行
    EXPECT_EQ(
        executed.load(std::memory_order_relaxed),
        accepted.load(std::memory_order_relaxed)
    );

    // shutdown 后一定不能继续 submit
    EXPECT_THROW(
        pool.submit([] {}),
        std::runtime_error
    );
}


// ============================================================
// Concurrent shutdown()
// ============================================================

TEST(ThreadPoolTest, ConcurrentShutdownIsSafe)
{
    ThreadPool pool(4, 1000);

    constexpr int taskCount = 500;
    constexpr int shutdownThreadCount = 8;

    std::atomic<int> completed{0};

    pool.start();

    for (int i = 0; i < taskCount; ++i)
    {
        pool.submit([&completed] {
            completed.fetch_add(
                1,
                std::memory_order_relaxed
            );
        });
    }

    std::vector<std::thread> shutdownThreads;
    shutdownThreads.reserve(shutdownThreadCount);

    for (int i = 0;
         i < shutdownThreadCount;
         ++i)
    {
        shutdownThreads.emplace_back([&] {
            EXPECT_NO_THROW(
                pool.shutdown()
            );
        });
    }

    for (auto& thread : shutdownThreads)
    {
        thread.join();
    }

    EXPECT_EQ(
        completed.load(std::memory_order_relaxed),
        taskCount
    );
}


// ============================================================
// Destructor
// ============================================================

TEST(ThreadPoolTest, DestructorPerformsGracefulShutdown)
{
    constexpr int taskCount = 100;

    std::atomic<int> completed{0};

    {
        ThreadPool pool(4, taskCount);

        pool.start();

        for (int i = 0; i < taskCount; ++i)
        {
            pool.submit([&completed] {
                std::this_thread::sleep_for(100us);

                completed.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            });
        }
    }

    EXPECT_EQ(
        completed.load(std::memory_order_relaxed),
        taskCount
    );
}


// ============================================================
// Worker hooks
// ============================================================

TEST(ThreadPoolTest, WorkerHooksAreCalled)
{
    constexpr int workerCount = 4;
    constexpr int taskCount = 100;

    TestThreadPool pool(
        workerCount,
        taskCount
    );

    pool.start();

    std::vector<std::future<void>> futures;
    futures.reserve(taskCount);

    for (int i = 0; i < taskCount; ++i)
    {
        futures.emplace_back(
            pool.submit([] {})
        );
    }

    for (auto& future : futures)
    {
        future.get();
    }

    pool.shutdown();

    EXPECT_EQ(
        pool.workerStarts.load(
            std::memory_order_relaxed
        ),
        workerCount
    );

    EXPECT_EQ(
        pool.workerStops.load(
            std::memory_order_relaxed
        ),
        workerCount
    );

    EXPECT_EQ(
        pool.beforeCount.load(
            std::memory_order_relaxed
        ),
        taskCount
    );

    EXPECT_EQ(
        pool.afterCount.load(
            std::memory_order_relaxed
        ),
        taskCount
    );
}

} // namespace service::test
~~~
