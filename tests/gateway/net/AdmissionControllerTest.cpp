#include "net/AdmissionController.h"

#include <atomic>
#include <barrier>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace service::gateway
{
namespace
{
TEST(AdmissionControllerTest, RequiresPositiveLimits)
{
    EXPECT_THROW(
        AdmissionController(AdmissionController::Limits{0, 1}),
        std::invalid_argument
    );
    EXPECT_THROW(
        AdmissionController(AdmissionController::Limits{1, 0}),
        std::invalid_argument
    );
}

TEST(AdmissionControllerTest, SameIpUsesOneLimitAcrossDifferentPorts)
{
    AdmissionController controller({8, 2});
    const InetAddress first("127.0.0.1", 10001);
    const InetAddress second("127.0.0.1", 10002);
    auto one = controller.tryAcquire(first);
    auto two = controller.tryAcquire(second);
    ASSERT_TRUE(one);
    ASSERT_TRUE(two);
    EXPECT_FALSE(
        controller.tryAcquire(InetAddress("127.0.0.1", 10003))
    );
    EXPECT_EQ(controller.activeForIp(second), 2u);
    EXPECT_EQ(controller.snapshot().rejectedPerIp, 1u);
    one.reset();
    EXPECT_EQ(controller.activeForIp(second), 1u);
    EXPECT_TRUE(controller.tryAcquire(first));
    EXPECT_EQ(controller.snapshot().peak, 2u);
}

TEST(AdmissionControllerTest, GlobalCapacityCountsDistinctIps)
{
    AdmissionController controller({2, 2});
    auto first = controller.tryAcquire(InetAddress("127.0.0.1", 1));
    auto second = controller.tryAcquire(InetAddress("127.0.0.2", 1));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_FALSE(controller.tryAcquire(InetAddress("127.0.0.3", 1)));
    auto snapshot = controller.snapshot();
    EXPECT_EQ(snapshot.active, 2u);
    EXPECT_EQ(snapshot.trackedIps, 2u);
    EXPECT_EQ(snapshot.rejectedTotal, 1u);
    EXPECT_EQ(snapshot.rejectedGlobal, 1u);
    EXPECT_EQ(
        controller.activeForIp(InetAddress("127.0.0.3", 1)),
        0u
    );
    first.reset();
    EXPECT_TRUE(controller.tryAcquire(InetAddress("127.0.0.3", 2)));
}

TEST(AdmissionControllerTest, LastSharedOwnerReturnsReservationOnce)
{
    AdmissionController controller({1, 1});
    auto lease = controller.tryAcquire(InetAddress("127.0.0.1", 1));
    auto handoff = lease;
    lease.reset();
    EXPECT_EQ(controller.snapshot().active, 1u);
    handoff.reset();
    EXPECT_EQ(controller.snapshot().active, 0u);
    EXPECT_EQ(controller.snapshot().trackedIps, 0u);
    EXPECT_TRUE(controller.tryAcquire(InetAddress("127.0.0.2", 1)));
}

TEST(AdmissionControllerTest, LeaseCanOutliveController)
{
    std::shared_ptr<AdmissionController::Lease> lease;
    {
        AdmissionController controller({1, 1});
        lease = controller.tryAcquire(InetAddress("127.0.0.1", 1));
        ASSERT_TRUE(lease);
    }
    EXPECT_NO_THROW(lease.reset());
}

TEST(AdmissionControllerTest, RetiredIpEntriesDoNotAccumulate)
{
    AdmissionController controller({3, 1});
    for (int ip = 1; ip <= 200; ++ip)
    {
        auto lease = controller.tryAcquire(
            InetAddress("127.0.0." + std::to_string(ip), 1)
        );
        ASSERT_TRUE(lease);
        EXPECT_EQ(controller.snapshot().trackedIps, 1u);
    }
    EXPECT_EQ(controller.snapshot().active, 0u);
    EXPECT_EQ(controller.snapshot().trackedIps, 0u);
}

TEST(
    AdmissionControllerTest, ConcurrentReservationsCannotExceedIpLimit
)
{
    AdmissionController controller({16, 3});
    constexpr int contenders = 24;
    std::barrier acquired(contenders + 1);
    std::barrier release(contenders + 1);
    std::atomic<int> winners{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < contenders; ++i)
    {
        threads.emplace_back(
            [&, i]
            {
                auto lease = controller.tryAcquire(InetAddress(
                    "127.0.0.1",
                    static_cast<std::uint16_t>(1000 + i)
                ));
                if (lease)
                {
                    ++winners;
                }
                acquired.arrive_and_wait();
                release.arrive_and_wait();
            }
        );
    }
    acquired.arrive_and_wait();
    EXPECT_EQ(winners.load(), 3);
    EXPECT_EQ(controller.snapshot().active, 3u);
    EXPECT_EQ(controller.snapshot().rejectedPerIp, contenders - 3u);
    release.arrive_and_wait();
    for (auto& thread : threads)
    {
        thread.join();
    }
    EXPECT_EQ(controller.snapshot().active, 0u);
    EXPECT_EQ(controller.snapshot().trackedIps, 0u);
}

TEST(AdmissionControllerTest, LocalRejectionsRemainObservable)
{
    AdmissionController controller({8, 2});
    controller.recordLocalRejection();
    const auto snapshot = controller.snapshot();
    EXPECT_EQ(snapshot.rejectedTotal, 1u);
    EXPECT_EQ(snapshot.rejectedLocal, 1u);
    EXPECT_EQ(snapshot.rejectedGlobal, 0u);
    EXPECT_EQ(snapshot.rejectedPerIp, 0u);
}
} // namespace
} // namespace service::gateway
