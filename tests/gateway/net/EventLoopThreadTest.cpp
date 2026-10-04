#include <gtest/gtest.h>

#include <sys/resource.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <system_error>

#include "net/EventLoopThread.h"

using service::gateway::EventLoopThread;

TEST(EventLoopThreadTest, UnstartedObjectCanBeDestroyed)
{
    EventLoopThread thread;
}

TEST(EventLoopThreadTest, RepeatedStartupAndDestructionWakeIdlePoll)
{
    for (int attempt = 0; attempt < 100; ++attempt)
    {
        EventLoopThread thread;
        thread.startLoop();
        thread.startLoop();
    }
}

TEST(EventLoopThreadTest, ReturnedLoopRunsTasksOnWorkerAndStopJoins)
{
    EventLoopThread thread;
    auto* loop = thread.startLoop();
    ASSERT_NE(loop, nullptr);
    EXPECT_EQ(thread.startLoop(), loop);
    EXPECT_FALSE(loop->isInLoopThread());
    std::promise<bool> ranOnOwner;
    auto result = ranOnOwner.get_future();
    loop->queueInLoop([&]
    {
        ranOnOwner.set_value(loop->isInLoopThread());
    });
    ASSERT_EQ(result.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    EXPECT_TRUE(result.get());
    thread.stop();
    EXPECT_NO_THROW(thread.stop());
    EXPECT_THROW(thread.startLoop(), std::logic_error);
}

TEST(EventLoopThreadTest, LoopCanExitBeforeWrapperDestruction)
{
    for (int attempt = 0; attempt < 200; ++attempt)
    {
        std::promise<void> quitRequested;
        auto done = quitRequested.get_future();
        EventLoopThread thread;
        auto* loop = thread.startLoop();
        loop->queueInLoop([&]
        {
            loop->quit();
            quitRequested.set_value();
        });
        ASSERT_EQ(done.wait_for(std::chrono::seconds(3)),
                  std::future_status::ready);
        // Destructor races the worker's loop exit but must never
        // call quit through a pointer whose stack owner has gone.
    }
}

TEST(EventLoopThreadTest, CallbackExceptionIsReportedByStop)
{
    EventLoopThread thread;
    auto* loop = thread.startLoop();
    std::promise<void> entered;
    auto started = entered.get_future();
    loop->queueInLoop([&]
    {
        entered.set_value();
        throw std::runtime_error("worker callback failed");
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    EXPECT_THROW(thread.stop(), std::runtime_error);
}

TEST(EventLoopThreadTest, DestructorHandlesWorkerException)
{
    // Completion objects must outlive the worker's callback.
    std::promise<void> entered;
    auto started = entered.get_future();
    EventLoopThread thread;
    auto* loop = thread.startLoop();
    loop->queueInLoop([&]
    {
        entered.set_value();
        throw std::runtime_error("worker callback failed");
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
}

TEST(EventLoopThreadTest, StopWaitsForCurrentCallback)
{
    EventLoopThread thread;
    auto* loop = thread.startLoop();
    std::promise<void> entered;
    auto started = entered.get_future();
    std::promise<void> release;
    auto released = release.get_future();
    bool returned = false;
    loop->queueInLoop([&]
    {
        entered.set_value();
        released.wait();
        returned = true;
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    std::jthread releaser([&]
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        release.set_value();
    });
    thread.stop();
    EXPECT_TRUE(returned);
}

TEST(EventLoopThreadTest, WorkerRejectsJoiningItself)
{
    std::promise<bool> rejected;
    auto result = rejected.get_future();
    EventLoopThread thread;
    auto* loop = thread.startLoop();
    loop->queueInLoop([&]
    {
        try
        {
            thread.stop();
            rejected.set_value(false);
        }
        catch (const std::logic_error&)
        {
            rejected.set_value(true);
        }
    });
    ASSERT_EQ(result.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    EXPECT_TRUE(result.get());
    EXPECT_NO_THROW(thread.stop());
}

TEST(EventLoopThreadDeathTest, ConstructionFailureReachesStartingThread)
{
    EXPECT_EXIT(
        {
            rlimit limit{};
            if (::getrlimit(RLIMIT_NOFILE, &limit) != 0)
                ::_exit(3);
            limit.rlim_cur = 0;
            if (::setrlimit(RLIMIT_NOFILE, &limit) != 0)
                ::_exit(3);
            EventLoopThread thread;
            try
            {
                thread.startLoop();
            }
            catch (const std::system_error& error)
            {
                ::_exit(error.code().value() == EMFILE ? 0 : 4);
            }
            ::_exit(5);
        },
        testing::ExitedWithCode(0),
        "");
}
