#include <limits>

#include "net/FrameProtocol.h"
#include "net/PacketEcho.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"

#include "ProtocolTestHarness.h"

namespace
{
using namespace service::gateway;
namespace protocol = service::protocol;
using PacketPolicyTest = test::ProtocolTest;

TEST_F(PacketPolicyTest, BucketBurstRefillCapacityAndBackwardClock)
{
    PacketRatePolicy::Clock::time_point now{};
    PacketRatePolicy
        bucket(2, 2, PacketDecision::Drop, [&] { return now; });
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Drop
    );
    now += std::chrono::milliseconds(250);
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Drop
    );
    now += std::chrono::milliseconds(250);
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    now += std::chrono::seconds(100);
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Drop
    );
    now -= std::chrono::seconds(1);
    EXPECT_EQ(
        bucket.inspect(*context, {1, {}}),
        PacketDecision::Drop
    );
}

TEST_F(PacketPolicyTest, BucketsAreIndependentPerProtocol)
{
    const auto now = []
    { return PacketRatePolicy::Clock::time_point{}; };
    PacketRatePolicy first(1, 0.1, PacketDecision::Close, now);
    PacketRatePolicy second(1, 0.1, PacketDecision::Close, now);
    EXPECT_EQ(
        first.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        first.inspect(*context, {1, {}}),
        PacketDecision::Close
    );
    EXPECT_EQ(
        second.inspect(*context, {1, {}}),
        PacketDecision::Continue
    );
}

TEST_F(
    PacketPolicyTest, PacketTypeAllowlistDeniesUnknownAndEmptyLists
)
{
    PacketTypePolicy selected({1, 1, 7});
    PacketTypePolicy empty({});
    EXPECT_EQ(
        selected.inspect(*context, {7, {}}),
        PacketDecision::Continue
    );
    EXPECT_EQ(
        selected.inspect(*context, {2, {}}),
        PacketDecision::Close
    );
    EXPECT_EQ(
        empty.inspect(*context, {1, {}}),
        PacketDecision::Close
    );
}

TEST_F(PacketPolicyTest, OnlyCompleteFramesConsumeTokens)
{
    PacketPolicyChain policies;
    policies.add<PacketRatePolicy>(
        2,
        0.1,
        PacketDecision::Close,
        [] { return PacketRatePolicy::Clock::time_point{}; }
    );
    start(makePacketEchoHandler(1024, counters, std::move(policies)));
    const auto bytes = frame("partial");
    for (std::size_t i = 0; i + 1 < bytes.size(); ++i)
    {
        receive(std::string_view(bytes.data() + i, 1));
        EXPECT_TRUE(transport->output.empty());
        EXPECT_TRUE(host->connected());
    }
    receive(std::string_view(bytes.data() + bytes.size() - 1, 1));
    EXPECT_EQ(transport->output.size(), bytes.size());
    receive(bytes);
    EXPECT_EQ(transport->output.size(), bytes.size() * 2);
    receive(bytes);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(transport->output.size(), bytes.size() * 2);
}

TEST_F(PacketPolicyTest, DroppedFrameDoesNotPreventNextCoalescedFrame)
{
    struct Filter final : PacketPolicy
    {
        PacketDecision
        inspect(const ConnectionContext&, PacketView view) override
        {
            const auto value = std::string_view(
                reinterpret_cast<const char*>(view.payload.data()),
                view.payload.size()
            );
            return value == "drop" ? PacketDecision::Drop
                                   : PacketDecision::Continue;
        }
    };
    PacketPolicyChain policies;
    policies.add<Filter>();
    start(makePacketEchoHandler(1024, counters, std::move(policies)));
    receive(frame("drop") + frame("keep"));
    const auto expected = protocol::PacketCodec::encode(
        2,
        7,
        std::as_bytes(std::span("keep", 4))
    );
    EXPECT_EQ(
        transport->output,
        std::string(expected->peek(), expected->readableBytes())
    );
    EXPECT_TRUE(host->connected());
}

TEST_F(PacketPolicyTest, PolicyExceptionsFailClosedBeforeFrameHandler)
{
    struct Throwing final : PacketPolicy
    {
        PacketDecision
        inspect(const ConnectionContext&, PacketView) override
        {
            throw std::runtime_error("policy failure");
        }
    };
    PacketPolicyChain policies;
    policies.add<Throwing>();
    int calls = 0;
    start(makeFrameProtocolHandler(
        [&](FrameContext&)
        {
            ++calls;
            return FrameResult::Handled;
        },
        1024,
        counters,
        std::move(policies)
    ));
    receive(frame("a"));
    EXPECT_EQ(calls, 0);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::CallbackFailure), 1);
}

TEST(
    PacketRatePolicyConfigurationTest,
    RejectsNonfiniteAndNonlimitingConfiguration
)
{
    EXPECT_THROW(PacketRatePolicy(0.5, 1), std::invalid_argument);
    EXPECT_THROW(PacketRatePolicy(1, 0), std::invalid_argument);
    EXPECT_THROW(
        PacketRatePolicy(std::numeric_limits<double>::infinity(), 1),
        std::invalid_argument
    );
    EXPECT_THROW(
        PacketRatePolicy(1, std::numeric_limits<double>::quiet_NaN()),
        std::invalid_argument
    );
    EXPECT_THROW(
        PacketRatePolicy(1, 1, PacketDecision::Continue),
        std::invalid_argument
    );
    EXPECT_THROW(
        PacketRatePolicy(1, 1, PacketDecision::Close, {}),
        std::invalid_argument
    );
}

TEST(PacketPolicyChainTest, SealedChainRejectsReconfiguration)
{
    PacketPolicyChain chain;
    chain.seal();
    EXPECT_THROW(
        chain.add<PacketTypePolicy>(std::vector<std::uint16_t>{1}),
        std::logic_error
    );
}
} // namespace
