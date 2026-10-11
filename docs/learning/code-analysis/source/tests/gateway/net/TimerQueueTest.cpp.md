# tests/gateway/net/TimerQueueTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；253行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
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

TEST(TimerQueueTest, HundredThousandTimersFireOrCancelExactlyOnce)
{
    using Clock = service::gateway::Timer::Clock;
    constexpr std::size_t count = 100'000;
    constexpr std::size_t expectedFires = count / 2;
    constexpr auto delay = 5s;

    EventLoop loop;
    std::vector<std::uint8_t> calls(count, 0);
    std::vector<service::gateway::TimerId> ids;
    ids.reserve(count);
    std::vector<Clock::time_point> deadlineLowerBounds;
    deadlineLowerBounds.reserve(count);
    std::vector<std::int64_t> driftUs;
    driftUs.reserve(expectedFires);
    std::size_t fired = 0;

    const auto registrationStart = Clock::now();
    for (std::size_t i = 0; i < count; ++i)
    {
        // addAfter samples the clock later, so this estimated deadline
        // makes the recorded drift an upper bound on callback lateness.
        deadlineLowerBounds.push_back(Clock::now() + delay);
        ids.push_back(loop.runAfter(delay, [&, i]
        {
            ++calls[i];
            if ((i & 1U) == 0)
            {
                return;
            }
            driftUs.push_back(std::chrono::duration_cast<
                std::chrono::microseconds
            >(Clock::now() - deadlineLowerBounds[i]).count());
            if (++fired == expectedFires)
            {
                loop.quit();
            }
        }));
    }
    const auto registrationEnd = Clock::now();
    for (std::size_t i = count; i-- > 0;)
    {
        if ((i & 1U) == 0)
        {
            ASSERT_TRUE(loop.cancelTimer(ids[i])) << i;
        }
    }
    const auto cancellationEnd = Clock::now();
    EXPECT_EQ(loop.pendingTimerCount(), expectedFires);

    bool timedOut = false;
    const auto watchdog = loop.runAfter(20s, [&]
    {
        timedOut = true;
        loop.quit();
    });
    loop.loop();
    EXPECT_TRUE(loop.cancelTimer(watchdog));

    EXPECT_FALSE(timedOut);
    EXPECT_EQ(fired, expectedFires);
    EXPECT_EQ(loop.pendingTimerCount(), 0U);
    for (std::size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(calls[i], (i & 1U) == 0 ? 0 : 1) << i;
    }

    if (driftUs.size() == expectedFires)
    {
        std::sort(driftUs.begin(), driftUs.end());
        const auto percentile = [&](std::size_t numerator)
        {
            return driftUs[(numerator * driftUs.size() + 99) / 100 - 1];
        };
        const auto registrationMs = std::chrono::duration_cast<
            std::chrono::milliseconds
        >(registrationEnd - registrationStart).count();
        const auto cancellationMs = std::chrono::duration_cast<
            std::chrono::milliseconds
        >(cancellationEnd - registrationEnd).count();
        std::cout << "TimerQueue 100k: registered=" << registrationMs
                  << " ms, canceled=" << cancellationMs
                  << " ms, drift upper bound P50=" << percentile(50)
                  << " us, P99=" << percentile(99)
                  << " us, max=" << driftUs.back() << " us\n";
        EXPECT_GE(driftUs.front(), 0);
    }
}

TEST(TimerQueueTest, DestructionDropsPendingCallbacks)
{
    auto repeatedCalls = std::make_shared<std::atomic<int>>(0);
    auto oneShotCalls = std::make_shared<std::atomic<int>>(0);
    {
        EventLoop loop;
        loop.runAfter(20ms, [oneShotCalls] { ++*oneShotCalls; });
        loop.runEvery(1ms, [&loop, repeatedCalls]
        {
            ++*repeatedCalls;
            loop.quit();
        });
        loop.loop();
        ASSERT_EQ(repeatedCalls->load(), 1);
        ASSERT_EQ(oneShotCalls->load(), 0);
        ASSERT_EQ(loop.pendingTimerCount(), 2U);
    }

    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(repeatedCalls->load(), 1);
    EXPECT_EQ(oneShotCalls->load(), 0);
}
~~~
