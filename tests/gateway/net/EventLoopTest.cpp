#include "net/EventLoop.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using service::gateway::EventLoop;

TEST(EventLoopTest, RunInLoopKeepsCallbacksOnTheOwnerThread)
{
    EventLoop loop;
    const std::thread::id owner = std::this_thread::get_id();
    bool ranInline = false;
    loop.runInLoop([&] { ranInline = true; });
    EXPECT_TRUE(ranInline);

    std::atomic<bool> ranFromOtherThread{false};
    std::atomic<bool> ranOnOwner{false};
    std::thread producer(
        [&]
        {
            loop.runInLoop(
                [&]
                {
                    ranOnOwner.store(
                        std::this_thread::get_id() == owner
                    );
                    ranFromOtherThread.store(true);
                    loop.quit();
                }
            );
        }
    );
    producer.join();
    EXPECT_FALSE(ranFromOtherThread.load());

    loop.loop();
    EXPECT_TRUE(ranFromOtherThread.load());
    EXPECT_TRUE(ranOnOwner.load());
}

TEST(EventLoopTest, FourThreadsDeliverFourHundredThousandTasksAndStop)
{
    constexpr std::size_t producerCount = 4;
    constexpr std::size_t tasksPerProducer = 100000;
    constexpr std::size_t totalTasks = producerCount *
                                       tasksPerProducer;

    EventLoop loop;
    const std::thread::id owner = std::this_thread::get_id();
    std::promise<void> startPromise;
    const std::shared_future<void> start = startPromise.get_future()
                                               .share();
    std::atomic<std::size_t> completed{0};
    std::vector<unsigned char> executions(totalTasks, 0);
    std::atomic<bool> wrongThread{false};
    std::mutex completionMutex;
    std::condition_variable completion;
    bool timedOut = false;

    std::vector<std::thread> producers;
    producers.reserve(producerCount);
    for (std::size_t producer = 0; producer < producerCount;
         ++producer)
    {
        producers.emplace_back(
            [&, producer]
            {
                start.wait();
                for (std::size_t task = 0; task < tasksPerProducer;
                     ++task)
                {
                    const std::size_t id = producer *
                                               tasksPerProducer +
                                           task;
                    loop.queueInLoop(
                        [&, id]
                        {
                            ++executions[id];
                            if (std::this_thread::get_id() != owner)
                            {
                                wrongThread.store(true);
                            }
                            if (completed.fetch_add(1) + 1 ==
                                totalTasks)
                            {
                                std::lock_guard<std::mutex> lock(
                                    completionMutex
                                );
                                completion.notify_one();
                            }
                        }
                    );
                }
            }
        );
    }

    std::thread stopper(
        [&]
        {
            std::unique_lock<std::mutex> lock(completionMutex);
            timedOut = !completion.wait_for(
                lock,
                std::chrono::seconds(30),
                [&] { return completed.load() == totalTasks; }
            );
            loop.quit();
        }
    );

    // Start producers only after the owner has entered the loop.
    loop.queueInLoop([&] { startPromise.set_value(); });
    loop.loop();

    for (auto& producer : producers)
    {
        producer.join();
    }
    stopper.join();

    const std::size_t peak = loop.peakPendingFunctorCount();
    std::cout << "EventLoop peak pending tasks: " << peak << '\n';
    EXPECT_FALSE(timedOut);
    EXPECT_FALSE(wrongThread.load());
    EXPECT_EQ(completed.load(), totalTasks);
    EXPECT_TRUE(std::all_of(
        executions.begin(),
        executions.end(),
        [](unsigned char count) { return count == 1; }
    ));
    EXPECT_GE(peak, 1U);
    EXPECT_LE(peak, totalTasks);
}

TEST(EventLoopTest, QuitFromAnotherThreadWakesIdlePoll)
{
    EventLoop loop;
    std::promise<void> startedPromise;
    auto started = startedPromise.get_future();
    loop.queueInLoop([&] { startedPromise.set_value(); });

    std::thread stopper(
        [&]
        {
            started.wait();
            loop.quit();
        }
    );

    loop.loop();
    stopper.join();
}
