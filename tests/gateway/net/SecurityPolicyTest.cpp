#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "net/ConnectionRuntime.h"
#include "net/PacketPipeline.h"
#include "net/PacketWhitelistFeature.h"
#include "net/PreLoginHandshake.h"
#include "net/TokenBucketFeature.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace
{
using namespace service::gateway;
struct Transport final : IConnection
{
    bool open = true;
    int closes = 0;
    std::uint64_t id() const noexcept override
    {
        return 1;
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
    }
    void resumeRead() override
    {
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};
struct Router final : PacketHandler
{
    explicit Router(int& calls)
        : calls(calls)
    {
    }
    PacketHandleResult handle(PacketContext&) override
    {
        ++calls;
        return PacketHandleResult::Handled;
    }
    int& calls;
};
struct SecurityPolicyTest : testing::Test
{
    RejectionCounters::Ptr
        counters = std::make_shared<RejectionCounters>();
    ConnectionContext::Ptr
        context = std::make_shared<ConnectionContext>(
            1,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now(),
            counters
        );
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    int routed = 0;
    std::vector<std::function<void()>> posted;

    std::shared_ptr<ConnectionRuntime> make()
    {
        auto decoder = std::make_unique<EnvelopeDecodeHandler>();
        auto gate = std::make_unique<AuthenticationGateHandler>(
            context->session
        );
        gate->setNext(std::make_unique<Router>(routed));
        decoder->setNext(std::move(gate));
        return std::make_shared<ConnectionRuntime>(
            transport,
            context,
            ConnectionScheduler{
                [&](auto task) { posted.push_back(std::move(task)); },
                [](auto, auto) { return ConnectionTimer{}; }},
            makePacketHandler(
                PacketPipeline(std::move(decoder)),
                256,
                counters
            )
        );
    }
    std::string
    frame(std::string_view payload, std::uint16_t type = 1)
    {
        auto result = service::protocol::PacketCodec::encode(
            type,
            7,
            std::as_bytes(std::span(payload))
        );
        return {result->peek(), result->readableBytes()};
    }
    std::string ping(std::uint16_t type = 1)
    {
        arena::protocol::Envelope value;
        value.mutable_ping()->set_nonce(42);
        return frame(
            *service::protocol::EnvelopeCodec::encode(value),
            type
        );
    }
    std::string business()
    {
        // Error is a valid non-handshake envelope; the test router
        // accepts it only after authentication. No new production
        // message type is added.
        arena::protocol::Envelope value;
        value.mutable_error()->set_code(42);
        return frame(*service::protocol::EnvelopeCodec::encode(value)
        );
    }
    void receive(
        const std::shared_ptr<ConnectionRuntime>& runtime,
        std::string_view bytes
    )
    {
        runtime->received(std::as_bytes(std::span(bytes)));
    }
};
TEST_F(
    SecurityPolicyTest,
    BurstStopsBeforeDecodeRouteAndRemainingCoalescedFrames
)
{
    auto runtime = make();
    runtime->features().add<TokenBucketFeature>(
        2,
        0.1,
        FeatureResult::Close,
        [] { return ConnectionClock::time_point{}; }
    );
    runtime->start();
    receive(runtime, ping() + ping() + frame("\x62\x80") + ping());
    EXPECT_EQ(routed, 2);
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(
        counters->count(RejectionReason::PacketRateExceeded),
        1
    );
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
    EXPECT_EQ(posted.size(), 1);
    receive(runtime, ping());
    runtime->close();
    EXPECT_EQ(
        counters->count(RejectionReason::PacketRateExceeded),
        1
    );
    posted.front()();
    runtime->transportClosed();
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(
    SecurityPolicyTest, DropCountsEachRejectedPacketWithoutRoutingIt
)
{
    auto runtime = make();
    runtime->features().add<TokenBucketFeature>(
        1,
        0.1,
        FeatureResult::Drop,
        [] { return ConnectionClock::time_point{}; }
    );
    runtime->start();
    receive(runtime, ping() + ping() + ping());
    EXPECT_EQ(routed, 1);
    EXPECT_TRUE(runtime->connected());
    EXPECT_EQ(
        counters->count(RejectionReason::PacketRateExceeded),
        2
    );
}
TEST_F(SecurityPolicyTest, PendingBusinessCannotReachRouter)
{
    auto runtime = make();
    runtime->start();
    receive(runtime, business() + ping());
    EXPECT_EQ(routed, 0);
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
}
TEST_F(SecurityPolicyTest, PendingPingAndLoginDoNotAuthenticate)
{
    auto runtime = make();
    runtime->start();
    arena::protocol::Envelope login;
    login.mutable_login_request()->set_account("alice");
    receive(
        runtime,
        ping() +
            frame(*service::protocol::EnvelopeCodec::encode(login))
    );
    EXPECT_EQ(routed, 2);
    EXPECT_EQ(
        context->session->loginState(),
        ConnectionSession::LoginState::Pending
    );
    receive(runtime, business());
    EXPECT_EQ(routed, 2);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
}
TEST_F(
    SecurityPolicyTest,
    HandshakePayloadCannotBypassGateWithBusinessOuterType
)
{
    auto runtime = make();
    runtime->start();
    receive(runtime, ping(2));
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
}
TEST_F(SecurityPolicyTest, AuthenticatedBusinessReachesRouter)
{
    auto runtime = make();
    runtime->start();
    ASSERT_TRUE(context->session->markAuthenticated());
    receive(runtime, business());
    EXPECT_EQ(routed, 1);
    EXPECT_TRUE(runtime->connected());
    EXPECT_EQ(counters->snapshot(), RejectionCounters::Snapshot{});
}
TEST_F(SecurityPolicyTest, ExpiredSessionCannotRestartLoginOrPing)
{
    auto runtime = make();
    runtime->start();
    ASSERT_TRUE(context->session->expireLogin());
    EXPECT_FALSE(context->session->markAuthenticated());
    receive(runtime, ping());
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
}
TEST_F(
    SecurityPolicyTest, WhitelistRejectionIsCountedOnceBeforeDecode
)
{
    auto runtime = make();
    runtime->features().add<PacketWhitelistFeature>(
        std::vector<std::uint16_t>{1}
    );
    runtime->start();
    receive(runtime, ping(2));
    runtime->close();
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(
        counters->count(RejectionReason::PacketTypeNotAllowed),
        1
    );
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 0);
}
TEST_F(SecurityPolicyTest, MalformedEnvelopeCountsDecodeReasonOnly)
{
    auto runtime = make();
    runtime->start();
    receive(runtime, frame("\x62\x80") + ping());
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 1);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 0);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
}
TEST_F(SecurityPolicyTest, MalformedFrameCountsFrameReasonOnly)
{
    auto runtime = make();
    runtime->start();
    auto bytes = ping();
    bytes[0] = 'X';
    receive(runtime, bytes);
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::InvalidFrame), 1);
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
}
TEST_F(
    SecurityPolicyTest,
    TailRejectAndCallbackFailureHaveDistinctReasons
)
{
    auto callback = makePacketHandler(
        PacketPipeline(std::make_unique<PacketHandler>()),
        256,
        counters
    );
    const auto bytes = ping();
    callback(*transport, std::as_bytes(std::span(bytes)));
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);

    struct Throwing : PacketHandler
    {
        PacketHandleResult handle(PacketContext&) override
        {
            throw std::runtime_error("business failure");
        }
    };
    transport->open = true;
    auto throwing = makePacketHandler(
        PacketPipeline(std::make_unique<Throwing>()),
        256,
        counters
    );
    auto runtime = std::make_shared<ConnectionRuntime>(
        transport,
        context,
        ConnectionScheduler{
            [](auto) {},
            [](auto, auto) { return ConnectionTimer{}; }},
        std::move(throwing)
    );
    runtime->start();
    receive(runtime, ping());
    EXPECT_EQ(counters->count(RejectionReason::CallbackFailure), 1);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);
}
TEST(
    SecurityPolicyConfigurationTest,
    GateAndCountersFailClosedOnMissingDependencies
)
{
    EXPECT_THROW(
        AuthenticationGateHandler(nullptr),
        std::invalid_argument
    );
    EXPECT_THROW(
        makePreLoginHandshakeHandler(nullptr),
        std::invalid_argument
    );
    EXPECT_THROW(
        (ConnectionContext(
            1,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now(),
            nullptr
        )),
        std::invalid_argument
    );
    auto session = std::make_shared<ConnectionSession>();
    AuthenticationGateHandler gate(session);
    Transport transport;
    service::protocol::PacketHeader header;
    PacketContext packet{transport, header, {}};
    EXPECT_EQ(gate.handle(packet), PacketHandleResult::Reject);
    EXPECT_EQ(
        packet.rejectionReason,
        RejectionReason::InvalidEnvelope
    );
}
TEST(
    RejectionCountersTest,
    SharedWorkersDoNotLoseCountsAndCountsOutliveConnection
)
{
    auto counters = std::make_shared<RejectionCounters>();
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i)
        workers.emplace_back(
            [counters]
            {
                for (int j = 0; j < 10000; ++j)
                    counters->record(RejectionReason::Unauthenticated
                    );
            }
        );
    for (auto& worker : workers)
        worker.join();
    {
        ConnectionContext context(
            1,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now(),
            counters
        );
        context.rejections->record(RejectionReason::PacketRateExceeded
        );
    }
    EXPECT_EQ(
        counters->count(RejectionReason::Unauthenticated),
        40000
    );
    EXPECT_EQ(counters->snapshot()[0], 1);
    for (std::size_t i = 0; i < RejectionCounters::kReasons; ++i)
        EXPECT_NE(
            RejectionCounters::name(static_cast<RejectionReason>(i)),
            "unknown"
        );
    EXPECT_EQ(defaultRejectionCounters(), defaultRejectionCounters());
}
} // namespace