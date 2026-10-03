#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "net/EventLoop.h"

using service::gateway::EventLoop;
using namespace std::chrono_literals;

TEST(TimerQueueTest, OneShotTimersRunInDeadlineOrder)
{
    EventLoop loop;
    std::vector<int> fired;
    loop.runAfter(25ms, [&] { fired.push_back(3); });
    loop.runAfter(5ms, [&] { fired.push_back(1); });
    loop.runAfter(15ms, [&] { fired.push_back(2); });
    loop.runAfter(45ms, [&] { loop.quit(); });

    loop.loop();

    EXPECT_EQ(fired, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(loop.pendingTimerCount(), 0U);
}

TEST(TimerQueueTest, CancelEarliestAndRepeatingTimer)
{
    EventLoop loop;
    int canceledCalls = 0;
    int repeatedCalls = 0;
    const auto canceled = loop.runAfter(
        5ms,
        [&] { ++canceledCalls; }
    );
    EXPECT_TRUE(loop.cancelTimer(canceled));
    EXPECT_FALSE(loop.cancelTimer(canceled));

    service::gateway::TimerId repeating = 0;
    repeating = loop.runEvery(
        5ms,
        [&]
        {
            ++repeatedCalls;
            if (repeatedCalls == 3)
            {
                EXPECT_TRUE(loop.cancelTimer(repeating));
            }
        }
    );
    loop.runAfter(40ms, [&] { loop.quit(); });

    loop.loop();

    EXPECT_EQ(canceledCalls, 0);
    EXPECT_EQ(repeatedCalls, 3);
    EXPECT_EQ(loop.pendingTimerCount(), 0U);
}

TEST(TimerQueueTest, CallbackCanAddAndCancelOtherTimers)
{
    EventLoop loop;
    std::vector<int> fired;
    const auto later = loop.runAfter(
        30ms,
        [&] { fired.push_back(3); }
    );
    loop.runAfter(
        5ms,
        [&]
        {
            fired.push_back(1);
            EXPECT_TRUE(loop.cancelTimer(later));
            loop.runAfter(5ms, [&] { fired.push_back(2); });
        }
    );
    loop.runAfter(45ms, [&] { loop.quit(); });

    loop.loop();

    EXPECT_EQ(fired, (std::vector<int>{1, 2}));
}

TEST(TimerQueueTest, RejectsInvalidInputAndForeignThreadMutation)
{
    EventLoop loop;
    EXPECT_THROW(loop.runAfter(0ms, [] {}), std::invalid_argument);
    EXPECT_THROW(loop.runEvery(-1ms, [] {}), std::invalid_argument);
    EXPECT_THROW(loop.runAfter(1ms, {}), std::invalid_argument);

    bool rejected = false;
    std::thread other(
        [&]
        {
            try
            {
                loop.runAfter(1ms, [] {});
            }
            catch (const std::logic_error&)
            {
                rejected = true;
            }
        }
    );
    other.join();
    EXPECT_TRUE(rejected);
}
TEST(TimerQueueTest, CancelManyTimersKeepsHeapIndexValid)
{
    EventLoop loop;
    constexpr std::size_t count = 2000;
    std::vector<unsigned char> executions(count, 0);
    std::vector<service::gateway::TimerId> ids;
    ids.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
    {
        ids.push_back(loop.runAfter(
            std::chrono::milliseconds(10 + (i * 37) % 40),
            [&, i] { ++executions[i]; }
        ));
    }
    for (std::size_t i = 0; i < count; i += 3)
    {
        EXPECT_TRUE(loop.cancelTimer(ids[i]));
    }
    loop.runAfter(80ms, [&] { loop.quit(); });
    loop.loop();

    for (std::size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(executions[i], i % 3 == 0 ? 0 : 1) << i;
    }
    EXPECT_EQ(loop.pendingTimerCount(), 0U);
}