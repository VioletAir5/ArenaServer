#include <gtest/gtest.h>

#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <system_error>
#include <type_traits>
#include <vector>

#include "net/EventLoopThread.h"

using service::gateway::EventLoopThread;

namespace service::gateway
{
class EventLoopThreadTestPeer
{
public:
    static void startPaused(
        EventLoopThread& thread, std::shared_future<void> release
    )
    {
        std::lock_guard<std::mutex> lock(thread.m_mutex);
        thread.m_thread = std::make_unique<std::jthread>(
            [&thread, release]()
            {
                release.wait();
                thread.threadFunc();
            }
        );
        thread.m_workerId = thread.m_thread->get_id();
    }

    static bool waitForStop(EventLoopThread& thread)
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (!thread.m_stopMutex.try_lock())
            {
                return true;
            }
            thread.m_stopMutex.unlock();
            std::this_thread::yield();
        }
        return false;
    }

    // Rescue a regression that joined before requesting quit, so the
    // test can report a failure rather than hanging during cleanup.
    static void quitPublishedLoop(EventLoopThread& thread)
    {
        std::lock_guard<std::mutex> lock(thread.m_mutex);
        if (thread.m_loop != nullptr)
        {
            thread.m_loop->quit();
        }
    }
};
} // namespace service::gateway

using service::gateway::EventLoopThreadTestPeer;

static_assert(std::is_nothrow_destructible_v<EventLoopThread>);

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

TEST(EventLoopThreadTest, StopWaitsForStartupBeforeJoining)
{
    std::promise<void> stopped;
    auto done = stopped.get_future();
    EventLoopThread thread;
    std::promise<void> release;
    EventLoopThreadTestPeer::startPaused(
        thread, release.get_future().share()
    );
    std::promise<void> started;
    auto startup = started.get_future();
    std::jthread starter([&]()
    {
        try
        {
            // Only observe startup; stop may invalidate the returned borrow.
            thread.startLoop();
            started.set_value();
        }
        catch (const std::logic_error&)
        {
            // The stop may finish before the starting waiter gets its lock.
            started.set_value();
        }
        catch (...)
        {
            started.set_exception(std::current_exception());
        }
    });
    std::jthread stopper([&]()
    {
        try
        {
            thread.stop();
            stopped.set_value();
        }
        catch (...)
        {
            stopped.set_exception(std::current_exception());
        }
    });

    EXPECT_TRUE(EventLoopThreadTestPeer::waitForStop(thread));
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release.set_value();
    const bool finished =
        done.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
    if (!finished)
    {
        EventLoopThreadTestPeer::quitPublishedLoop(thread);
    }
    stopper.join();
    starter.join();
    EXPECT_TRUE(finished);
    EXPECT_NO_THROW(done.get());
    EXPECT_NO_THROW(startup.get());
    EXPECT_THROW(thread.startLoop(), std::logic_error);
}

TEST(EventLoopThreadTest, ConcurrentStopsJoinOnlyOnce)
{
    for (int attempt = 0; attempt < 100; ++attempt)
    {
        EventLoopThread thread;
        thread.startLoop();
        std::barrier ready(5);
        std::atomic<int> failures{0};
        std::vector<std::jthread> stoppers;
        for (int caller = 0; caller < 4; ++caller)
        {
            stoppers.emplace_back([&]()
            {
                ready.arrive_and_wait();
                try
                {
                    thread.stop();
                }
                catch (...)
                {
                    failures.fetch_add(1);
                }
            });
        }
        ready.arrive_and_wait();
        stoppers.clear();
        EXPECT_EQ(failures.load(), 0);
        EXPECT_THROW(thread.startLoop(), std::logic_error);
    }
}

TEST(EventLoopThreadDeathTest, WorkerDestructionTerminatesWithoutThrowing)
{
    EXPECT_EXIT(
        {
            ::alarm(5);
            std::set_terminate([]()
            {
                // The old noexcept destructor terminated while handling
                // a thrown logic_error. Self-destruction must terminate
                // directly, without introducing an active exception.
                ::_exit(std::current_exception() ? 4 : 0);
            });
            auto* thread = new EventLoopThread;
            auto* loop = thread->startLoop();
            loop->queueInLoop([thread]() { delete thread; });
            for (;;)
            {
                ::pause();
            }
        },
        testing::ExitedWithCode(0),
        "");
}

TEST(EventLoopThreadDeathTest, WorkerStopRejectsWhileAnotherCallerJoins)
{
    EXPECT_EXIT(
        {
            ::alarm(5);
            std::promise<void> release;
            auto released = release.get_future();
            std::promise<void> entered;
            auto started = entered.get_future();
            bool rejected = false;
            EventLoopThread thread;
            auto* loop = thread.startLoop();
            loop->queueInLoop([&]()
            {
                entered.set_value();
                released.wait();
                try
                {
                    thread.stop();
                }
                catch (const std::logic_error&)
                {
                    rejected = true;
                }
            });
            started.wait();
            std::jthread stopper([&]() { thread.stop(); });
            if (!EventLoopThreadTestPeer::waitForStop(thread))
            {
                ::_exit(3);
            }
            release.set_value();
            stopper.join();
            ::_exit(rejected ? 0 : 4);
        },
        testing::ExitedWithCode(0),
        "");
}
