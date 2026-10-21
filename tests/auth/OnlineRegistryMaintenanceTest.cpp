#include "net/OnlineRegistryMaintenance.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <gtest/gtest.h>

#include "net/ConnectionSchedulers.h"

using namespace service::gateway;
using namespace std::chrono_literals;
namespace
{
struct FakeTimers
{
    struct Timer
    {
        std::shared_ptr<bool> live;
        ConnectionScheduler::Action action;
    };
    std::vector<Timer> timers;
    int schedules{0};
    int failAt{0};

    ConnectionScheduler scheduler()
    {
        return {
            {},
            [this](auto, auto action)
            {
                if (++schedules == failAt)
                    throw std::runtime_error("timer allocation");
                auto live = std::make_shared<bool>(true);
                timers.push_back({live, std::move(action)});
                return ConnectionTimer([live] { *live = false; });
            }
        };
    }
    void fire(std::size_t index, bool alreadyReady = false)
    {
        auto saved = timers.at(index);
        if (alreadyReady || *saved.live)
            saved.action();
    }
};
service::auth::JwtTokens tokens()
{
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef";
    config.now = [] { return std::int64_t{1700000000}; };
    return service::auth::JwtTokens(std::move(config));
}
TEST(
    OnlineRegistryMaintenanceTest, RejectsInvalidDependenciesAndPeriod
)
{
    FakeTimers timer;
    auto registry = std::make_shared<OnlineRegistry>();
    EXPECT_THROW(
        (OnlineRegistryMaintenance({}, timer.scheduler())),
        std::invalid_argument
    );
    EXPECT_THROW(
        (OnlineRegistryMaintenance(registry, {})),
        std::invalid_argument
    );
    EXPECT_THROW(
        (OnlineRegistryMaintenance(registry, timer.scheduler(), 0ms)),
        std::invalid_argument
    );
    timer.failAt = 1;
    EXPECT_THROW(
        (OnlineRegistryMaintenance(registry, timer.scheduler())),
        std::runtime_error
    );
}
TEST(
    OnlineRegistryMaintenanceTest,
    ReclaimsDetachedRoomWithoutAnyNewTraffic
)
{
    auto now = std::chrono::steady_clock::time_point{};
    int clockReads = 0;
    OnlineRegistryConfig config;
    config.grace = 10ms;
    config.now = [&]
    {
        ++clockReads;
        return now;
    };
    auto registry = std::make_shared<OnlineRegistry>(
        std::move(config)
    );
    auto auth = tokens();
    auto grant = registry->install(
        arena::domain::PlayerId{42},
        7,
        {},
        auth,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(registry->joinRoom(
        grant->lease->binding,
        arena::domain::RoomId{99}
    ));
    grant->lease->release();
    FakeTimers timer;
    OnlineRegistryMaintenance upkeep(
        registry,
        timer.scheduler(),
        1ms
    );
    const auto readsBefore = clockReads;
    now += 10ms;
    timer.fire(0);
    EXPECT_GT(clockReads, readsBefore);
    EXPECT_EQ(registry->retainedCount(), 0U);
    EXPECT_EQ(registry->roomCount(), 0U);
    ASSERT_EQ(timer.timers.size(), 2U);
    upkeep.stop();
    const auto afterStop = clockReads;
    timer.fire(
        1,
        true
    ); // Already-ready cancellation must not rearm upkeep.
    EXPECT_EQ(clockReads, afterStop);
    EXPECT_EQ(timer.timers.size(), 2U);
    EXPECT_FALSE(upkeep.running());
}
TEST(
    OnlineRegistryMaintenanceTest,
    StopPreservesPendingAuthButShutdownClearsEverything
)
{
    auto registry = std::make_shared<OnlineRegistry>();
    auto auth = tokens();
    auto grant = registry->install(
        arena::domain::PlayerId{42},
        7,
        {},
        auth,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(registry->joinRoom(
        grant->lease->binding,
        arena::domain::RoomId{99}
    ));
    FakeTimers timer;
    OnlineRegistryMaintenance upkeep(registry, timer.scheduler());
    upkeep.stop();
    EXPECT_EQ(registry->activeCount(), 1U);
    EXPECT_EQ(registry->retainedCount(), 1U);
    upkeep.shutdown();
    EXPECT_EQ(registry->activeCount(), 0U);
    EXPECT_EQ(registry->retainedCount(), 0U);
    EXPECT_EQ(registry->roomCount(), 0U);
    grant->lease->release();
    timer.fire(0, true);
    EXPECT_EQ(registry->retainedCount(), 0U);
    EXPECT_FALSE(
        registry
            ->install(arena::domain::PlayerId{43}, 8, {}, auth, [] {})
    );
}
TEST(
    OnlineRegistryMaintenanceTest,
    ForeignStopRejectsBeforeTimerMutation
)
{
    FakeTimers timer;
    OnlineRegistryMaintenance upkeep(
        std::make_shared<OnlineRegistry>(),
        timer.scheduler()
    );
    std::thread foreign(
        [&] { EXPECT_THROW(upkeep.stop(), std::logic_error); }
    );
    foreign.join();
    EXPECT_TRUE(upkeep.running());
    EXPECT_TRUE(*timer.timers[0].live);
    upkeep.stop();
}
TEST(
    OnlineRegistryMaintenanceTest,
    RetainedTimerCallbackDoesNotKeepMaintenanceAlive
)
{
    FakeTimers timer;
    int clockReads = 0;
    OnlineRegistryConfig config;
    config.now = [&]
    {
        ++clockReads;
        return std::chrono::steady_clock::now();
    };
    auto registry = std::make_shared<OnlineRegistry>(
        std::move(config)
    );
    {
        OnlineRegistryMaintenance upkeep(registry, timer.scheduler());
    }
    const auto readsBefore = clockReads;
    timer.fire(0, true);
    EXPECT_EQ(clockReads, readsBefore);
    EXPECT_EQ(timer.timers.size(), 1U);
}
TEST(
    OnlineRegistryMaintenanceTest,
    FailedRearmCannotLeavePlayersOrRoomsHanging
)
{
    FakeTimers timer;
    timer.failAt = 2;
    auto registry = std::make_shared<OnlineRegistry>();
    auto auth = tokens();
    auto grant = registry->install(
        arena::domain::PlayerId{42},
        7,
        {},
        auth,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(registry->joinRoom(
        grant->lease->binding,
        arena::domain::RoomId{99}
    ));
    OnlineRegistryMaintenance upkeep(registry, timer.scheduler());
    timer.fire(0);
    EXPECT_FALSE(upkeep.running());
    EXPECT_EQ(registry->retainedCount(), 0U);
    EXPECT_EQ(registry->roomCount(), 0U);
    EXPECT_FALSE(grant->lease->current());
}
TEST(
    OnlineRegistryMaintenanceTest,
    StopLetsAsioManagementContextFinishNaturally
)
{
    boost::asio::io_context context;
    auto registry = std::make_shared<OnlineRegistry>();
    OnlineRegistryMaintenance upkeep(
        registry,
        connectionScheduler(context.get_executor())
    );
    boost::asio::post(context, [&] { upkeep.stop(); });
    EXPECT_GT(context.run(), 0U);
    EXPECT_FALSE(upkeep.running());
    upkeep.shutdown();
}
} // namespace
