# tests/gateway/net/ConnectionFeatureTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；310行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include <limits>

#include <gtest/gtest.h>

#include "net/ConnectionRuntime.h"
#include "net/PacketEcho.h"
#include "net/PacketWhitelistFeature.h"
#include "net/PreLoginHandshake.h"
#include "net/TokenBucketFeature.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"
using namespace service::gateway;
namespace
{
struct Transport : IConnection
{
    bool open = true;
    int closes = 0;
    std::string sent;
    std::uint64_t id() const noexcept override
    {
        return 1;
    }
    void send(std::span<const std::byte> bytes) override
    {
        sent.append(
            reinterpret_cast<const char*>(bytes.data()),
            bytes.size()
        );
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
    }
    void resumeRead() override
    {
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};
struct ConnectionFeatureTest : testing::Test
{
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    ConnectionClock::time_point now{};
    std::vector<std::function<void()>> posted;
    std::shared_ptr<ConnectionRuntime>
    make(ConnectionMessageCallback handler = [](auto&, auto) {})
    {
        return std::make_shared<ConnectionRuntime>(
            transport,
            std::make_shared<ConnectionContext>(
                1,
                InetAddress("127.0.0.1", 1),
                now
            ),
            ConnectionScheduler{
                [&](auto action)
                { posted.push_back(std::move(action)); },
                [](auto, auto) { return ConnectionTimer{}; }},
            std::move(handler)
        );
    }
    std::string frame(std::string_view payload)
    {
        auto packet = service::protocol::PacketCodec::encode(
            1,
            7,
            std::as_bytes(std::span(payload))
        );
        return {packet->peek(), packet->readableBytes()};
    }
};
TEST_F(ConnectionFeatureTest, BucketBurstRefillCapacityAndDrop)
{
    auto runtime = make();
    runtime->features().add<TokenBucketFeature>(
        2,
        2,
        FeatureResult::Drop,
        [&] { return now; }
    );
    runtime->start();
    EXPECT_EQ(
        runtime->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    EXPECT_EQ(
        runtime->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop);
    now += std::chrono::milliseconds(250);
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop);
    now += std::chrono::milliseconds(250);
    EXPECT_EQ(
        runtime->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    now += std::chrono::seconds(100);
    EXPECT_EQ(
        runtime->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    EXPECT_EQ(
        runtime->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop);
    now -= std::chrono::seconds(1);
    EXPECT_EQ(runtime->inspectPacket({1, {}}), FeatureResult::Drop);
    EXPECT_TRUE(runtime->connected());
}
TEST_F(
    ConnectionFeatureTest, BucketsArePerConnectionAndCloseOnExhaustion
)
{
    auto first = make(), second = make();
    first->features().add<TokenBucketFeature>(1, 0.1);
    second->features().add<TokenBucketFeature>(1, 0.1);
    first->start();
    second->start();
    EXPECT_EQ(first->inspectPacket({1, {}}), FeatureResult::Continue);
    EXPECT_EQ(first->inspectPacket({1, {}}), FeatureResult::Close);
    EXPECT_EQ(
        second->inspectPacket({1, {}}),
        FeatureResult::Continue
    );
    EXPECT_FALSE(first->connected());
    EXPECT_TRUE(second->connected());
}
TEST_F(ConnectionFeatureTest, WhitelistRejectsDisallowedAndEmptyLists)
{
    auto first = make(), second = make();
    first->features().add<PacketWhitelistFeature>(
        std::vector<std::uint16_t>{1, 1, 7}
    );
    second->features().add<PacketWhitelistFeature>(
        std::vector<std::uint16_t>{}
    );
    first->start();
    second->start();
    EXPECT_EQ(first->inspectPacket({7, {}}), FeatureResult::Continue);
    EXPECT_EQ(first->inspectPacket({2, {}}), FeatureResult::Close);
    EXPECT_EQ(second->inspectPacket({1, {}}), FeatureResult::Close);
}
TEST(
    TokenBucketFeatureTest,
    RejectsNonfiniteAndNonlimitingConfiguration
)
{
    EXPECT_THROW(TokenBucketFeature(0.5, 1), std::invalid_argument);
    EXPECT_THROW(TokenBucketFeature(1, 0), std::invalid_argument);
    EXPECT_THROW(
        TokenBucketFeature(
            std::numeric_limits<double>::infinity(),
            1
        ),
        std::invalid_argument
    );
    EXPECT_THROW(
        TokenBucketFeature(
            1,
            std::numeric_limits<double>::quiet_NaN()
        ),
        std::invalid_argument
    );
    EXPECT_THROW(
        TokenBucketFeature(1, 1, FeatureResult::Continue),
        std::invalid_argument
    );
    EXPECT_THROW(
        TokenBucketFeature(1, 1, FeatureResult::Close, {}),
        std::invalid_argument
    );
}
TEST_F(ConnectionFeatureTest, OnlyCompleteFramesConsumeTokens)
{
    auto runtime = make(makePacketEchoHandler());
    runtime->features().add<TokenBucketFeature>(2, 0.1);
    runtime->start();
    const auto bytes = frame("partial");
    for (std::size_t i = 0; i < bytes.size() - 1; ++i)
    {
        runtime->received(std::as_bytes(std::span(bytes.data() + i, 1)
        ));
        EXPECT_TRUE(transport->sent.empty());
        EXPECT_TRUE(runtime->connected());
    }
    runtime->received(
        std::as_bytes(std::span(bytes.data() + bytes.size() - 1, 1))
    );
    EXPECT_EQ(transport->sent.size(), bytes.size());
    runtime->received(std::as_bytes(std::span(bytes)));
    EXPECT_EQ(transport->sent.size(), bytes.size() * 2);
    runtime->received(std::as_bytes(std::span(bytes)));
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(transport->sent.size(), bytes.size() * 2);
}
TEST_F(
    ConnectionFeatureTest,
    DroppedFrameDoesNotPreventNextCoalescedFrame
)
{
    struct Filter : IConnectionFeature
    {
        FeatureResult
        onPacket(IConnectionControl&, PacketView view) override
        {
            auto value = std::string_view(
                reinterpret_cast<const char*>(view.payload.data()),
                view.payload.size()
            );
            return value == "drop" ? FeatureResult::Drop
                                   : FeatureResult::Continue;
        }
    };
    auto runtime = make(makePacketEchoHandler());
    runtime->features().add<Filter>();
    runtime->start();
    auto bytes = frame("drop") + frame("keep");
    runtime->received(std::as_bytes(std::span(bytes)));
    EXPECT_TRUE(runtime->connected());
    auto expected = service::protocol::PacketCodec::encode(
        2,
        7,
        std::as_bytes(std::span("keep", 4))
    );
    EXPECT_EQ(
        transport->sent,
        std::string(expected->peek(), expected->readableBytes())
    );
}
TEST_F(
    ConnectionFeatureTest,
    WhitelistRejectsValidEnvelopeBeforeBusinessHandler
)
{
    auto runtime = make(makePreLoginHandshakeHandler(
        std::make_shared<ConnectionContext>(1, InetAddress("127.0.0.1", 1), now)
    ));
    runtime->features().add<PacketWhitelistFeature>(
        std::vector<std::uint16_t>{2}
    );
    runtime->start();
    arena::protocol::Envelope envelope;
    envelope.mutable_ping()->set_nonce(42);
    auto payload = service::protocol::EnvelopeCodec::encode(envelope);
    ASSERT_TRUE(payload);
    auto bytes = frame(*payload);
    runtime->received(std::as_bytes(std::span(bytes)));
    EXPECT_FALSE(runtime->connected());
    EXPECT_TRUE(transport->sent.empty());
}
TEST_F(ConnectionFeatureTest, RawBytesDoNotBecomePacketEvents)
{
    struct Count : IConnectionFeature
    {
        int calls = 0;
        FeatureResult
        onPacket(IConnectionControl&, PacketView) override
        {
            ++calls;
            return FeatureResult::Continue;
        }
    };
    auto runtime = make();
    auto& count = runtime->features().add<Count>();
    runtime->start();
    auto bytes = frame("raw");
    runtime->received(std::as_bytes(std::span(bytes)));
    EXPECT_EQ(count.calls, 0);
}
TEST_F(
    ConnectionFeatureTest, OnClosedWaitsForActualTransportNotification
)
{
    struct Count : IConnectionFeature
    {
        int calls = 0;
        void onClosed() noexcept override
        {
            ++calls;
        }
    };
    auto runtime = make();
    auto& count = runtime->features().add<Count>();
    runtime->start();
    runtime->close();
    posted[0]();
    EXPECT_EQ(transport->closes, 1);
    EXPECT_EQ(count.calls, 0);
    runtime->transportClosed();
    runtime->transportClosed();
    EXPECT_EQ(count.calls, 1);
}
} // namespace
~~~
