#include <algorithm>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

#include "concurrent/ThreadPool.h"

namespace service::test
{

namespace
{

// ============================================================
// Mandelbrot workload
// ============================================================

constexpr std::size_t kMaxIterations = 2000;

double mandelbrotEscape(const std::complex<double>& c)
{
    constexpr double escapeRadius = 1024.0;
    constexpr double escapeRadiusSquared = escapeRadius *
                                           escapeRadius;

    std::complex<double> z = c;

    std::size_t iteration = 1;

    while (std::norm(z) <= escapeRadiusSquared &&
           iteration < kMaxIterations)
    {
        z = z * z + c;
        ++iteration;
    }

    if (iteration == kMaxIterations)
    {
        return static_cast<double>(kMaxIterations);
    }

    return static_cast<double>(iteration) -
           std::log2(
               std::log(std::norm(z)) / std::log(escapeRadiusSquared)
           );
}

// ============================================================
// Calculate a range of pixels
// ============================================================

void calculateMandelbrotRange(
    std::vector<double>& image,
    std::size_t width,
    std::size_t height,
    std::size_t begin,
    std::size_t end
)
{
    constexpr double reMin = -2.01;
    constexpr double reMax = 0.51;
    constexpr double imMin = -1.26;
    constexpr double imMax = 1.26;

    for (std::size_t i = begin; i < end; ++i)
    {
        const std::size_t x = i % width;
        const std::size_t y = i / width;

        const double real = reMin +
                            (reMax - reMin) * static_cast<double>(x) /
                                static_cast<double>(width - 1);

        const double imag = imMin +
                            (imMax - imMin) * static_cast<double>(y) /
                                static_cast<double>(height - 1);

        image[i] = mandelbrotEscape(std::complex<double>(real, imag));
    }
}

// ============================================================
// Simple statistics
// ============================================================

struct Statistics
{
    double mean;
    double standardDeviation;
};

Statistics analyze(const std::vector<double>& samples)
{
    const double mean = std::accumulate(
                            samples.begin(),
                            samples.end(),
                            0.0
                        ) /
                        static_cast<double>(samples.size());

    double variance = 0.0;

    for (const double sample : samples)
    {
        const double diff = sample - mean;
        variance += diff * diff;
    }

    variance /= static_cast<double>(samples.size());

    return {mean, std::sqrt(variance)};
}

// ============================================================
// Sequential execution
// ============================================================

double runSequential(
    std::vector<double>& image, std::size_t width, std::size_t height
)
{
    const auto start = std::chrono::steady_clock::now();

    calculateMandelbrotRange(image, width, height, 0, width * height);

    const auto end = std::chrono::steady_clock::now();

    return std::chrono::duration<double, std::milli>(end - start)
        .count();
}

// ============================================================
// ThreadPool execution
// ============================================================

double runThreadPool(
    ThreadPool& pool,
    std::vector<double>& image,
    std::size_t width,
    std::size_t height,
    std::size_t taskCount
)
{
    const std::size_t total = width * height;

    const std::size_t blockSize = (total + taskCount - 1) / taskCount;

    std::vector<std::future<void>> futures;
    futures.reserve(taskCount);

    const auto start = std::chrono::steady_clock::now();

    for (std::size_t task = 0; task < taskCount; ++task)
    {
        const std::size_t begin = task * blockSize;

        const std::size_t end = std::min(begin + blockSize, total);

        if (begin >= end)
        {
            break;
        }

        futures.emplace_back(pool.submit(
            [&image, width, height, begin, end]
            {
                calculateMandelbrotRange(
                    image,
                    width,
                    height,
                    begin,
                    end
                );
            }
        ));
    }

    for (auto& future : futures)
    {
        future.get();
    }

    const auto end = std::chrono::steady_clock::now();

    return std::chrono::duration<double, std::milli>(end - start)
        .count();
}

} // namespace

// ============================================================
// Benchmark
// ============================================================

TEST(ThreadPoolPerformanceTest, MandelbrotBenchmark)
{
    constexpr std::size_t workerCount = 4;

    // 可以根据机器性能调整。
    constexpr std::size_t imageSize = 700;

    constexpr std::size_t repeatCount = 10;

    constexpr std::size_t queueSize = 1024;

    const std::size_t totalPixels = imageSize * imageSize;

    ThreadPool pool(workerCount, queueSize);

    pool.start();

    std::cout << "\n========================================\n"
              << "ThreadPool Mandelbrot Benchmark\n"
              << "========================================\n"
              << "Workers:        " << workerCount << '\n'
              << "Image:          " << imageSize << 'x' << imageSize
              << '\n'
              << "Pixels:         " << totalPixels << '\n'
              << "Repeats:        " << repeatCount << '\n'
              << "Max iterations: " << kMaxIterations << '\n'
              << '\n';

    // ========================================================
    // Sequential benchmark
    // ========================================================

    std::vector<double> sequentialImage(totalPixels);

    std::vector<double> sequentialTimings;

    sequentialTimings.reserve(repeatCount);

    for (std::size_t i = 0; i < repeatCount; ++i)
    {
        sequentialTimings.push_back(
            runSequential(sequentialImage, imageSize, imageSize)
        );
    }

    const Statistics sequentialStats = analyze(sequentialTimings);

    std::cout << "[Sequential]\n"
              << "Mean:     " << sequentialStats.mean << " ms\n"
              << "StdDev:   " << sequentialStats.standardDeviation
              << " ms\n\n";

    // ========================================================
    // ThreadPool benchmark
    // ========================================================

    // 跟 BS_thread_pool 类似：
    // 测不同 task 数量，而不是只测 workerCount。
    //
    // 可以观察任务切分粒度对性能的影响。
    const std::vector<std::size_t> taskCounts =
        {1, 2, 4, 8, 16, 32, 64, 128, 256};

    double bestTime = sequentialStats.mean;

    std::size_t bestTaskCount = 1;

    for (const std::size_t taskCount : taskCounts)
    {
        std::vector<double> image(totalPixels);

        std::vector<double> timings;

        timings.reserve(repeatCount);

        for (std::size_t i = 0; i < repeatCount; ++i)
        {
            timings.push_back(runThreadPool(
                pool,
                image,
                imageSize,
                imageSize,
                taskCount
            ));
        }

        const Statistics stats = analyze(timings);

        const double speedup = sequentialStats.mean / stats.mean;

        const std::size_t effectiveParallelism = std::min(
            workerCount,
            taskCount
        );

        const double efficiency = speedup / static_cast<double>(
                                                effectiveParallelism
                                            );
        const double pixelsPerSecond = static_cast<double>(
                                           totalPixels
                                       ) /
                                       (stats.mean / 1000.0);

        std::cout << "[" << std::setw(2) << taskCount << " tasks]\n"

                  << "Mean:        " << stats.mean << " ms\n"

                  << "StdDev:      " << stats.standardDeviation
                  << " ms\n"

                  << "Speedup:     " << speedup << "x\n"

                  << "Efficiency:  " << efficiency * 100.0 << "%\n"

                  << "Throughput:  "
                  << static_cast<std::size_t>(pixelsPerSecond)
                  << " pixels/sec\n\n";

        if (stats.mean < bestTime)
        {
            bestTime = stats.mean;

            bestTaskCount = taskCount;
        }

        // correctness:
        // 多线程计算结果必须和单线程一致。
        EXPECT_EQ(image, sequentialImage);
    }

    // ========================================================
    // Summary
    // ========================================================

    const double maxSpeedup = sequentialStats.mean / bestTime;

    std::cout << "----------------------------------------\n"
              << "Best task count: " << bestTaskCount << '\n'
              << "Best time:       " << bestTime << " ms\n"
              << "Maximum speedup: " << maxSpeedup << "x\n"
              << "----------------------------------------\n";

    pool.shutdown();
}

} // namespace service::test