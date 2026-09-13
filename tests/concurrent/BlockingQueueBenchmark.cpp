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
// Performance tests
// ============================================================

TEST(BlockingQueuePerformanceTest, SingleProducerSingleConsumer)
{
    constexpr std::size_t task_count = 1'000'000;

    BlockingQueue<std::size_t> queue(1024);

    std::uint64_t checksum = 0;

    const auto start =
        std::chrono::steady_clock::now();

    std::thread consumer([&] {
        while (auto value = queue.pop()) {
            checksum += *value;
        }
    });

    for (std::size_t i = 0; i < task_count; ++i) {
        ASSERT_TRUE(queue.push(i));
    }

    queue.close();

    consumer.join();

    const auto end =
        std::chrono::steady_clock::now();

    const auto duration =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(end - start);

    const double seconds =
        std::chrono::duration<double>(
            end - start
        ).count();

    const double throughput =
        static_cast<double>(task_count)
        / seconds;

    std::cout
        << "\n[SPSC BlockingQueue]\n"
        << "Tasks:      " << task_count << '\n'
        << "Time:       " << duration.count()
        << " ms\n"
        << "Throughput: "
        << static_cast<std::uint64_t>(throughput)
        << " items/sec\n";

    const std::uint64_t expected =
        static_cast<std::uint64_t>(task_count - 1)
        * task_count / 2;

    EXPECT_EQ(checksum, expected);
}


TEST(BlockingQueuePerformanceTest, MultiProducerMultiConsumer)
{
    constexpr int producer_count = 4;
    constexpr int consumer_count = 4;

    constexpr std::size_t items_per_producer =
        250'000;

    constexpr std::size_t total_items =
        producer_count * items_per_producer;

    BlockingQueue<std::uint64_t> queue(4096);

    std::atomic<std::uint64_t> consumed{0};

    std::vector<std::thread> consumers;

    consumers.reserve(consumer_count);

    const auto start =
        std::chrono::steady_clock::now();

    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back([&] {
            while (queue.pop()) {
                consumed.fetch_add(
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

        producers.emplace_back([&] {
            for (std::size_t i = 0;
                 i < items_per_producer;
                 ++i) {

                if (!queue.push(i)) {
                    return;
                }
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

    const auto end =
        std::chrono::steady_clock::now();

    const double seconds =
        std::chrono::duration<double>(
            end - start
        ).count();

    const double throughput =
        static_cast<double>(total_items)
        / seconds;

    std::cout
        << "\n[MPMC BlockingQueue]\n"
        << "Producers:  " << producer_count << '\n'
        << "Consumers:  " << consumer_count << '\n'
        << "Tasks:      " << total_items << '\n'
        << "Time:       " << seconds << " sec\n"
        << "Throughput: "
        << static_cast<std::uint64_t>(throughput)
        << " items/sec\n";

    EXPECT_EQ(consumed.load(), total_items);
}