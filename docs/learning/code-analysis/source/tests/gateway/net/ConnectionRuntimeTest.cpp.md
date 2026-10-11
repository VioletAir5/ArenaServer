# tests/gateway/net/ConnectionRuntimeTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；261行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include "net/ConnectionRuntime.h"

#include <deque>

#include <gtest/gtest.h>

#include "net/BackpressureFeature.h"
using namespace service::gateway;
namespace
{
struct Transport : IConnection
{
    bool open = true, paused = false;
    int closes = 0, resumes = 0;
    std::uint64_t id() const noexcept override
    {
        return 7;
    }
    void send(std::span<const std::byte>) override
    {
    }
    void shutdown() override
    {
    }
    void close() override
    {
        open = false;
        ++closes;
    }
    bool connected() const noexcept override
    {
        return open;
    }
    void pauseRead() override
    {
        paused = true;
    }
    void resumeRead() override
    {
        paused = false;
        ++resumes;
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};
struct ConnectionRuntimeTest : ::testing::Test
{
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    std::deque<std::function<void()>> posted;
    std::vector<std::function<void()>> timers;
    std::shared_ptr<ConnectionRuntime>
        runtime = std::make_shared<ConnectionRuntime>(
            transport,
            std::make_shared<ConnectionContext>(
                7, InetAddress("127.0.0.1", 1), ConnectionClock::now()
            ),
            ConnectionScheduler{
                [this](auto action)
                { posted.push_back(std::move(action)); },
                [this](auto, auto action)
                {
                    auto live = std::make_shared<bool>(true);
                    timers.push_back(
                        [live, action]
                        {
                            if (*live)
                                action();
                        }
                    );
                    return ConnectionTimer([live] { *live = false; });
                }},
            [](auto&, auto) {}
        );
    void drain()
    {
        while (!posted.empty())
        {
            auto f = std::move(posted.front());
            posted.pop_front();
            f();
        }
    }
};
struct Count : IConnectionFeature
{
    int packets = 0, closed = 0;
    FeatureResult result = FeatureResult::Continue;
    FeatureResult onPacket(IConnectionControl&, PacketView) override
    {
        ++packets;
        return result;
    }
    void onClosed() noexcept override
    {
        ++closed;
    }
};
TEST_F(ConnectionRuntimeTest, PauseReasonsDoNotResumeEachOther)
{
    runtime->start();
    runtime->setReadPaused(ReadPauseReason::Backpressure, true);
    runtime->pauseRead();
    runtime->setReadPaused(ReadPauseReason::Backpressure, false);
    EXPECT_TRUE(transport->paused);
    runtime->resumeRead();
    EXPECT_FALSE(transport->paused);
    EXPECT_EQ(transport->resumes, 1);
}
TEST_F(ConnectionRuntimeTest, PacketShortCircuitAndDeferredCloseOnce)
{
    auto& first = runtime->features().add<Count>();
    auto& second = runtime->features().add<Count>();
    runtime->start();
    first.result = FeatureResult::Drop;
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop);
    EXPECT_EQ(second.packets, 0);
    first.result = FeatureResult::Close;
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Close);
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(transport->closes, 0);
    EXPECT_TRUE(transport->paused);
    runtime->close();
    drain();
    runtime->transportClosed();
    EXPECT_EQ(transport->closes, 1);
    EXPECT_EQ(first.closed, 1);
    EXPECT_EQ(second.closed, 1);
}
TEST_F(ConnectionRuntimeTest, RegistrationFreezesAtStart)
{
    runtime->start();
    EXPECT_THROW(runtime->features().add<Count>(), std::logic_error);
}
TEST_F(
    ConnectionRuntimeTest,
    CancelledTimerAndClosedRuntimeNeverInvokeFeature
)
{
    int fired = 0;
    runtime->start();
    auto timer = runtime->scheduleAt(
        ConnectionClock::now(),
        [&](auto&) { ++fired; }
    );
    timer.reset();
    timers[0]();
    auto other = runtime->scheduleAt(
        ConnectionClock::now(),
        [&](auto&) { ++fired; }
    );
    runtime->transportClosed();
    timers[1]();
    EXPECT_EQ(fired, 0);
}
TEST_F(
    ConnectionRuntimeTest, ExceptionsCloseAndOutputReentryIsCoalesced
)
{
    struct Reentrant : IConnectionFeature
    {
        ConnectionRuntime* runtime;
        int calls = 0;
        explicit Reentrant(ConnectionRuntime* value)
            : runtime(value)
        {
        }
        void onOutputBufferChanged(
            IConnectionControl&, std::size_t, std::size_t
        ) override
        {
            if (++calls == 1)
                runtime->outputChanged(2);
            else
                throw std::runtime_error("feature");
        }
    };
    auto& feature = runtime->features().add<Reentrant>(runtime.get());
    runtime->start();
    runtime->outputChanged(1);
    EXPECT_EQ(feature.calls, 2);
    EXPECT_FALSE(runtime->connected());
    drain();
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(
    ConnectionRuntimeTest, CloseNotificationWaitsUntilFeatureReturns
)
{
    struct Closing : IConnectionFeature
    {
        ConnectionRuntime* runtime;
        bool inside = false, notifiedInside = false;
        explicit Closing(ConnectionRuntime* value)
            : runtime(value)
        {
        }
        FeatureResult
        onPacket(IConnectionControl&, PacketView) override
        {
            inside = true;
            runtime->transportClosed();
            inside = false;
            return FeatureResult::Continue;
        }
        void onClosed() noexcept override
        {
            notifiedInside = inside;
        }
    };
    auto& feature = runtime->features().add<Closing>(runtime.get());
    runtime->start();
    runtime->inspectPacket({1, {}});
    EXPECT_FALSE(feature.notifiedInside);
}
TEST_F(ConnectionRuntimeTest, BackpressureHysteresisCancelsOldTimer)
{
    runtime->features().add<BackpressureFeature>(2, 4);
    runtime->start();
    runtime->outputChanged(6);
    ASSERT_TRUE(transport->paused);
    runtime->outputChanged(3);
    EXPECT_TRUE(transport->paused);
    runtime->pauseRead();
    runtime->outputChanged(2);
    EXPECT_TRUE(transport->paused);
    timers[0]();
    EXPECT_TRUE(runtime->connected());
    runtime->resumeRead();
    EXPECT_FALSE(transport->paused);
    runtime->outputChanged(5);
    timers[1]();
    drain();
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(ConnectionRuntimeTest, CloseCancelsBackpressureTimer)
{
    runtime->features().add<BackpressureFeature>(2, 4);
    runtime->start();
    runtime->outputChanged(6);
    runtime->transportClosed();
    timers[0]();
    drain();
    EXPECT_EQ(transport->closes, 0);
}
TEST(BackpressureFeatureTest, RejectsInvalidConfiguration)
{
    EXPECT_THROW(BackpressureFeature(4, 4), std::invalid_argument);
    EXPECT_THROW(
        BackpressureFeature(2, 1024 * 1024),
        std::invalid_argument
    );
    EXPECT_THROW(
        BackpressureFeature(2, 4, ConnectionClock::duration::zero()),
        std::invalid_argument
    );
}
} // namespace
~~~
