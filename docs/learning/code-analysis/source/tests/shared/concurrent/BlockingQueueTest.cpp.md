# tests/shared/concurrent/BlockingQueueTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；493行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include <gtest/gtest.h>

#include <concurrent/BlockingQueue.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace std::chrono_literals;

// 如果你的 BlockingQueue 在 service namespace
using service::BlockingQueue;


// ============================================================
// Basic tests
// ============================================================

TEST(BlockingQueueTest, InitialState)
{
    BlockingQueue<int> queue(4);

    EXPECT_EQ(queue.capacity(), 4);
    EXPECT_EQ(queue.size(), 0);
    EXPECT_TRUE(queue.empty());
    EXPECT_FALSE(queue.full());
    EXPECT_FALSE(queue.closed());
}


TEST(BlockingQueueTest, ZeroCapacityThrows)
{
    EXPECT_THROW(
        BlockingQueue<int> queue(0),
        std::invalid_argument
    );
}


TEST(BlockingQueueTest, PushAndPopSingleElement)
{
    BlockingQueue<int> queue(4);

    EXPECT_TRUE(queue.push(42));

    EXPECT_EQ(queue.size(), 1);
    EXPECT_FALSE(queue.empty());

    auto value = queue.pop();

    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, 42);

    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size(), 0);
}


TEST(BlockingQueueTest, FIFOOrder)
{
    BlockingQueue<int> queue(8);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));

    auto a = queue.pop();
    auto b = queue.pop();
    auto c = queue.pop();

    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    ASSERT_TRUE(c.has_value());

    EXPECT_EQ(*a, 1);
    EXPECT_EQ(*b, 2);
    EXPECT_EQ(*c, 3);
}


TEST(BlockingQueueTest, FullState)
{
    BlockingQueue<int> queue(2);

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    EXPECT_TRUE(queue.full());
    EXPECT_EQ(queue.size(), 2);

    auto value = queue.pop();

    ASSERT_TRUE(value.has_value());
    EXPECT_FALSE(queue.full());
}


TEST(BlockingQueueTest, SupportsMoveOnlyType)
{
    BlockingQueue<std::unique_ptr<int>> queue(2);

    auto ptr = std::make_unique<int>(123);

    EXPECT_TRUE(queue.push(std::move(ptr)));

    EXPECT_EQ(ptr, nullptr);

    auto result = queue.pop();

    ASSERT_TRUE(result.has_value());
    ASSERT_NE(*result, nullptr);
    EXPECT_EQ(**result, 123);
}


// ============================================================
// Close semantics
// ============================================================

TEST(BlockingQueueTest, CloseChangesState)
{
    BlockingQueue<int> queue(4);

    EXPECT_FALSE(queue.closed());

    queue.close();

    EXPECT_TRUE(queue.closed());
}


TEST(BlockingQueueTest, CloseIsIdempotent)
{
    BlockingQueue<int> queue(4);

    queue.close();
    queue.close();
    queue.close();

    EXPECT_TRUE(queue.closed());
}


TEST(BlockingQueueTest, PushFailsAfterClose)
{
    BlockingQueue<int> queue(4);

    queue.close();

    EXPECT_FALSE(queue.push(1));
    EXPECT_EQ(queue.size(), 0);
}


TEST(BlockingQueueTest, ExistingElementsRemainAvailableAfterClose)
{
    BlockingQueue<int> queue(4);

    EXPECT_TRUE(queue.push(10));
    EXPECT_TRUE(queue.push(20));

    queue.close();

    auto a = queue.pop();
    auto b = queue.pop();
    auto c = queue.pop();

    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());

    EXPECT_EQ(*a, 10);
    EXPECT_EQ(*b, 20);

    EXPECT_FALSE(c.has_value());
}


TEST(BlockingQueueTest, PopReturnsNulloptWhenClosedAndEmpty)
{
    BlockingQueue<int> queue(4);

    queue.close();

    auto result = queue.pop();

    EXPECT_FALSE(result.has_value());
}


// ============================================================
// Blocking behaviour
// ============================================================

TEST(BlockingQueueTest, ConsumerBlocksUntilPush)
{
    BlockingQueue<int> queue(4);

    std::atomic<bool> consumer_started{false};
    std::atomic<bool> consumer_finished{false};

    int result = 0;

    std::thread consumer([&] {
        consumer_started = true;

        auto value = queue.pop();

        if (value) {
            result = *value;
        }

        consumer_finished = true;
    });

    while (!consumer_started.load()) {
        std::this_thread::yield();
    }

    std::this_thread::sleep_for(50ms);

    EXPECT_FALSE(consumer_finished.load());

    EXPECT_TRUE(queue.push(123));

    consumer.join();

    EXPECT_TRUE(consumer_finished.load());
    EXPECT_EQ(result, 123);
}


TEST(BlockingQueueTest, ProducerBlocksWhenFull)
{
    BlockingQueue<int> queue(1);

    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> producer_started{false};
    std::atomic<bool> producer_finished{false};

    std::thread producer([&] {
        producer_started = true;

        queue.push(2);

        producer_finished = true;
    });

    while (!producer_started.load()) {
        std::this_thread::yield();
    }

    std::this_thread::sleep_for(50ms);

    EXPECT_FALSE(producer_finished.load());

    auto value = queue.pop();

    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, 1);

    producer.join();

    EXPECT_TRUE(producer_finished.load());

    auto second = queue.pop();

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, 2);
}


TEST(BlockingQueueTest, CloseWakesBlockedConsumer)
{
    BlockingQueue<int> queue(4);

    std::atomic<bool> finished{false};

    std::thread consumer([&] {
        auto value = queue.pop();

        EXPECT_FALSE(value.has_value());

        finished = true;
    });

    std::this_thread::sleep_for(50ms);

    EXPECT_FALSE(finished.load());

    queue.close();

    consumer.join();

    EXPECT_TRUE(finished.load());
}


TEST(BlockingQueueTest, CloseWakesBlockedProducer)
{
    BlockingQueue<int> queue(1);

    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> finished{false};
    std::atomic<bool> push_result{true};

    std::thread producer([&] {
        push_result = queue.push(2);
        finished = true;
    });

    std::this_thread::sleep_for(50ms);

    EXPECT_FALSE(finished.load());

    queue.close();

    producer.join();

    EXPECT_TRUE(finished.load());
    EXPECT_FALSE(push_result.load());
}


// ============================================================
// Multi-producer / Multi-consumer
// ============================================================

TEST(BlockingQueueTest, MultipleProducersMultipleConsumers)
{
    constexpr int producer_count = 4;
    constexpr int consumer_count = 4;
    constexpr int items_per_producer = 10000;

    constexpr int total_items =
        producer_count * items_per_producer;

    BlockingQueue<int> queue(256);

    std::atomic<int> consumed_count{0};
    std::atomic<std::int64_t> consumed_sum{0};

    std::vector<std::thread> consumers;
    consumers.reserve(consumer_count);

    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back([&] {
            while (true) {
                auto value = queue.pop();

                if (!value) {
                    break;
                }

                consumed_sum.fetch_add(
                    *value,
                    std::memory_order_relaxed
                );

                consumed_count.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
        });
    }

    std::vector<std::thread> producers;
    producers.reserve(producer_count);

    for (int producer = 0;
         producer < producer_count;
         ++producer) {

        producers.emplace_back([&, producer] {
            const int begin =
                producer * items_per_producer;

            const int end =
                begin + items_per_producer;

            for (int value = begin;
                 value < end;
                 ++value) {

                ASSERT_TRUE(queue.push(value));
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    queue.close();

    for (auto& consumer : consumers) {
        consumer.join();
    }

    EXPECT_EQ(consumed_count.load(), total_items);

    const std::int64_t expected_sum =
        static_cast<std::int64_t>(total_items - 1)
        * total_items / 2;

    EXPECT_EQ(consumed_sum.load(), expected_sum);
}


// ============================================================
// Stronger correctness test:
// Verify that every produced value appears exactly once.
// ============================================================

TEST(BlockingQueueTest, NoLostOrDuplicatedItems)
{
    constexpr int producer_count = 4;
    constexpr int consumer_count = 4;
    constexpr int items_per_producer = 5000;

    constexpr int total_items =
        producer_count * items_per_producer;

    BlockingQueue<int> queue(128);

    std::vector<std::atomic<int>> seen(total_items);

    for (auto& value : seen) {
        value.store(0, std::memory_order_relaxed);
    }

    std::vector<std::thread> consumers;

    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back([&] {
            while (auto value = queue.pop()) {
                seen[*value].fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
        });
    }

    std::vector<std::thread> producers;

    for (int producer = 0;
         producer < producer_count;
         ++producer) {

        producers.emplace_back([&, producer] {
            const int begin =
                producer * items_per_producer;

            const int end =
                begin + items_per_producer;

            for (int value = begin;
                 value < end;
                 ++value) {

                ASSERT_TRUE(queue.push(value));
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    queue.close();

    for (auto& consumer : consumers) {
        consumer.join();
    }

    for (int i = 0; i < total_items; ++i) {
        EXPECT_EQ(
            seen[i].load(std::memory_order_relaxed),
            1
        ) << "value " << i
          << " was lost or duplicated";
    }
}


~~~
