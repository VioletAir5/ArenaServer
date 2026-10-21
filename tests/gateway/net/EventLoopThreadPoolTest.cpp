#include "net/EventLoopThreadPool.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <exception>
#include <filesystem>
#include <future>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "net/EventLoop.h"

using service::gateway::EventLoop;
using service::gateway::EventLoopThreadPool;
using namespace std::chrono_literals;

namespace
{
// Negative disables injection. Fail exactly the selected timerfd
// creation.
std::atomic<int> timerFdFailureCountdown{-1};

std::size_t openFdCount()
{
    std::size_t count = 0;
    for ([[maybe_unused]] const auto& entry :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        ++count;
    }
    return count;
}

struct ExitSignal
{
    std::promise<void>* exited;
    ~ExitSignal()
    {
        exited->set_value();
    }
};

// Keep the signal exclusively in the loop's timer queue, so
// destroying that queue proves that this worker has exited its
// dispatch loop.
void signalOnLoopDestruction(
    EventLoop* loop,
    std::promise<void>& ready,
    std::promise<void>& exited
)
{
    loop->queueInLoop(
        [loop, &ready, &exited]()
        {
            auto signal = std::make_shared<ExitSignal>();
            signal->exited = &exited;
            loop->runAfter(1h, [signal]() {});
            ready.set_value();
        }
    );
}
} // namespace

extern "C" int __real_timerfd_create(int clockId, int flags);
extern "C" int __wrap_timerfd_create(int clockId, int flags)
{
    int remaining = timerFdFailureCountdown.load();
    while (remaining > 0)
    {
        if (timerFdFailureCountdown
                .compare_exchange_weak(remaining, remaining - 1))
        {
            if (remaining == 1)
            {
                timerFdFailureCountdown.store(-1);
                errno = EMFILE;
                return -1;
            }
            break;
        }
    }
    return __real_timerfd_create(clockId, flags);
}

static_assert(std::is_nothrow_destructible_v<EventLoopThreadPool>);

TEST(EventLoopThreadPoolTest, RejectsNullBaseLoop)
{
    EXPECT_THROW(
        EventLoopThreadPool(nullptr, 1),
        std::invalid_argument
    );
}

TEST(EventLoopThreadPoolTest, ZeroWorkersUseBaseWithoutStoppingIt)
{
    EventLoop base;
    EventLoopThreadPool pool(&base, 0);
    EXPECT_FALSE(pool.started());
    EXPECT_THROW(pool.getNextLoop(), std::logic_error);
    EXPECT_THROW(pool.getAllLoops(), std::logic_error);
    EXPECT_NO_THROW(pool.stop());
    pool.start();
    pool.start();
    EXPECT_TRUE(pool.started());
    EXPECT_EQ(pool.getNextLoop(), &base);
    EXPECT_EQ(pool.getAllLoops(), std::vector<EventLoop*>({&base}));
    pool.stop();
    EXPECT_FALSE(pool.started());
    EXPECT_THROW(pool.getNextLoop(), std::logic_error);
    EXPECT_THROW(pool.getAllLoops(), std::logic_error);

    bool ran = false;
    base.queueInLoop(
        [&]()
        {
            ran = true;
            base.quit();
        }
    );
    base.loop();
    EXPECT_TRUE(ran);
}

TEST(EventLoopThreadPoolTest, RoundRobinUsesDistinctWorkerOwners)
{
    EventLoop base;
    EventLoopThreadPool pool(&base, 3);
    pool.start();
    const auto loops = pool.getAllLoops();
    ASSERT_EQ(loops.size(), 3u);
    EXPECT_NE(loops[0], loops[1]);
    EXPECT_NE(loops[1], loops[2]);
    EXPECT_NE(loops[0], loops[2]);
    pool.start();
    EXPECT_EQ(pool.getAllLoops(), loops);
    std::vector<std::future<std::thread::id>> owners;
    for (auto* loop : loops)
    {
        EXPECT_NE(loop, &base);
        EXPECT_FALSE(loop->isInLoopThread());
        auto result = std::make_shared<
            std::promise<std::thread::id>>();
        owners.push_back(result->get_future());
        loop->queueInLoop(
            [loop, result]()
            {
                if (!loop->isInLoopThread())
                {
                    result->set_exception(
                        std::make_exception_ptr(
                            std::logic_error("wrong loop owner")
                        )
                    );
                    return;
                }
                result->set_value(std::this_thread::get_id());
            }
        );
    }
    std::vector<std::thread::id> ids;
    for (auto& owner : owners)
    {
        ASSERT_EQ(owner.wait_for(3s), std::future_status::ready);
        ids.push_back(owner.get());
    }
    EXPECT_NE(ids[0], ids[1]);
    EXPECT_NE(ids[1], ids[2]);
    EXPECT_NE(ids[0], ids[2]);
    for (std::size_t turn = 0; turn < 30; ++turn)
    {
        EXPECT_EQ(pool.getNextLoop(), loops[turn % loops.size()]);
    }
    pool.stop();
}

TEST(EventLoopThreadPoolTest, RestartAndDestructionDoNotLeakFds)
{
    EventLoop base;
    const auto baseline = openFdCount();
    {
        EventLoopThreadPool pool(&base, 3);
        for (int round = 0; round < 30; ++round)
        {
            pool.start();
            ASSERT_EQ(pool.getAllLoops().size(), 3u);
            pool.stop();
            EXPECT_FALSE(pool.started());
            EXPECT_EQ(openFdCount(), baseline);
        }
        pool.start();
        // Destruction also stops an active pool.
    }
    EXPECT_EQ(openFdCount(), baseline);
}

TEST(EventLoopThreadPoolTest, FailedStartupRollsBackAndCanRetry)
{
    EventLoop base;
    const auto baseline = openFdCount();
    EventLoopThreadPool pool(&base, 3);
    timerFdFailureCountdown.store(2);
    // Fail construction of worker 2 after worker 1 is already
    // running.
    EXPECT_THROW(pool.start(), std::system_error);
    timerFdFailureCountdown.store(-1);
    EXPECT_FALSE(pool.started());
    EXPECT_EQ(openFdCount(), baseline);
    EXPECT_THROW(pool.getAllLoops(), std::logic_error);
    pool.start();
    EXPECT_TRUE(pool.started());
    EXPECT_EQ(pool.getAllLoops().size(), 3u);
    pool.stop();
    EXPECT_EQ(openFdCount(), baseline);
}

TEST(EventLoopThreadPoolTest, ManagementRejectsForeignThread)
{
    EventLoop base;
    EventLoopThreadPool pool(&base, 2);
    pool.start();
    int rejected = 0;
    std::jthread foreign(
        [&]()
        {
            const auto expectRejection = [&](auto call)
            {
                try
                {
                    call();
                }
                catch (const std::logic_error&)
                {
                    ++rejected;
                }
            };
            expectRejection([&]()
                            { EventLoopThreadPool other(&base, 1); });
            expectRejection([&]() { pool.start(); });
            expectRejection([&]() { pool.stop(); });
            expectRejection([&]() { pool.getNextLoop(); });
            expectRejection([&]() { pool.getAllLoops(); });
            expectRejection([&]() { pool.started(); });
        }
    );
    foreign.join();
    EXPECT_EQ(rejected, 6);
    EXPECT_TRUE(pool.started());
    pool.stop();
}

TEST(EventLoopThreadPoolTest, WorkerCannotStopItsOwnPool)
{
    std::promise<bool> rejected;
    auto result = rejected.get_future();
    EventLoop base;
    EventLoopThreadPool pool(&base, 1);
    pool.start();
    pool.getNextLoop()->queueInLoop(
        [&]()
        {
            try
            {
                pool.stop();
                rejected.set_value(false);
            }
            catch (const std::logic_error&)
            {
                rejected.set_value(true);
            }
        }
    );
    ASSERT_EQ(result.wait_for(3s), std::future_status::ready);
    EXPECT_TRUE(result.get());
    pool.stop();
}

TEST(EventLoopThreadPoolTest, StopsAllWorkersBeforeJoiningFirst)
{
    std::promise<void> secondReady;
    auto ready = secondReady.get_future();
    std::promise<void> secondExited;
    auto exited = secondExited.get_future();
    std::promise<void> firstEntered;
    auto entered = firstEntered.get_future();
    std::promise<bool> firstCompleted;
    auto completed = firstCompleted.get_future();
    EventLoop base;
    EventLoopThreadPool pool(&base, 2);
    pool.start();
    const auto loops = pool.getAllLoops();
    signalOnLoopDestruction(loops[1], secondReady, secondExited);
    ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
    loops[0]->queueInLoop(
        [&]()
        {
            firstEntered.set_value();
            // A finite wait lets the old sequential-join
            // implementation fail this assertion without hanging the
            // test runner.
            firstCompleted.set_value(
                exited.wait_for(3s) == std::future_status::ready
            );
        }
    );
    ASSERT_EQ(entered.wait_for(3s), std::future_status::ready);
    pool.stop();
    EXPECT_TRUE(completed.get());
}

TEST(EventLoopThreadPoolTest, WorkerErrorStillJoinsOtherWorkers)
{
    std::promise<void> secondReady;
    auto ready = secondReady.get_future();
    std::promise<void> secondExited;
    auto exited = secondExited.get_future();
    std::promise<void> failed;
    auto entered = failed.get_future();
    EventLoop base;
    const auto baseline = openFdCount();
    EventLoopThreadPool pool(&base, 2);
    pool.start();
    const auto loops = pool.getAllLoops();
    signalOnLoopDestruction(loops[1], secondReady, secondExited);
    ASSERT_EQ(ready.wait_for(3s), std::future_status::ready);
    loops[0]->queueInLoop(
        [&]()
        {
            failed.set_value();
            throw std::runtime_error("pool worker failed");
        }
    );
    ASSERT_EQ(entered.wait_for(3s), std::future_status::ready);
    bool detected = false;
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!detected && std::chrono::steady_clock::now() < deadline)
    {
        try
        {
            pool.getAllLoops();
        }
        catch (const std::runtime_error&)
        {
            detected = true;
        }
        std::this_thread::yield();
    }
    EXPECT_TRUE(detected);
    EXPECT_THROW(pool.getNextLoop(), std::runtime_error);
    try
    {
        pool.stop();
        ADD_FAILURE() << "missing worker error";
    }
    catch (const std::runtime_error& error)
    {
        EXPECT_STREQ(error.what(), "pool worker failed");
    }
    EXPECT_FALSE(pool.started());
    EXPECT_EQ(exited.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(openFdCount(), baseline);
    EXPECT_NO_THROW(pool.stop());
    pool.start();
    pool.stop();
}

TEST(EventLoopThreadPoolTest, MultipleProducersRunOnSelectedWorkers)
{
    EventLoop base;
    constexpr int tasksPerWorker = 2000;
    std::vector<std::atomic<int>> counts(4);
    std::vector<std::promise<void>> finished(4);
    std::vector<std::future<void>> results;
    for (auto& promise : finished)
    {
        results.push_back(promise.get_future());
    }
    std::atomic<int> wrongOwner{0};
    EventLoopThreadPool pool(&base, 4);
    pool.start();
    const auto loops = pool.getAllLoops();
    std::vector<std::jthread> producers;
    for (int producer = 0; producer < 4; ++producer)
    {
        producers.emplace_back(
            [&, producer]()
            {
                for (int task = 0; task < tasksPerWorker; ++task)
                {
                    const auto index = (task + producer) % 4;
                    auto* loop = loops[index];
                    loop->queueInLoop(
                        [&, index, loop]()
                        {
                            if (!loop->isInLoopThread())
                            {
                                wrongOwner.fetch_add(1);
                            }
                            if (counts[index].fetch_add(1) + 1 ==
                                tasksPerWorker)
                            {
                                finished[index].set_value();
                            }
                        }
                    );
                }
            }
        );
    }
    producers.clear();
    for (auto& result : results)
    {
        ASSERT_EQ(result.wait_for(3s), std::future_status::ready);
        result.get();
    }
    pool.stop();
    for (auto& count : counts)
    {
        EXPECT_EQ(count.load(), tasksPerWorker);
    }
    EXPECT_EQ(wrongOwner.load(), 0);
}

TEST(EventLoopThreadPoolDeathTest, DestructorHandlesWorkerException)
{
    EXPECT_EXIT(
        {
            ::alarm(5);
            std::set_terminate([]() { ::_exit(3); });
            std::promise<void> failed;
            auto entered = failed.get_future();
            EventLoop base;
            {
                EventLoopThreadPool pool(&base, 2);
                pool.start();
                pool.getNextLoop()->queueInLoop(
                    [&]()
                    {
                        failed.set_value();
                        throw std::runtime_error("worker error");
                    }
                );
                entered.wait();
            }
            ::_exit(0);
        },
        testing::ExitedWithCode(0),
        ""
    );
}

TEST(
    EventLoopThreadPoolDeathTest, WorkerDestructionTerminatesDirectly
)
{
    EXPECT_EXIT(
        {
            ::alarm(5);
            std::set_terminate(
                []() { ::_exit(std::current_exception() ? 4 : 0); }
            );
            EventLoop base;
            auto* pool = new EventLoopThreadPool(&base, 1);
            pool->start();
            pool->getNextLoop()->queueInLoop([pool]()
                                             { delete pool; });
            for (;;)
            {
                ::pause();
            }
        },
        testing::ExitedWithCode(0),
        ""
    );
}
