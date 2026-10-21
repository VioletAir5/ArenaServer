#include <thread>

#include "net/ArenaProtocol.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "net/PreLoginHandshake.h"
#include "protocol/EnvelopeCodec.hpp"

#include "ProtocolTestHarness.h"

namespace
{
using namespace service::gateway;
namespace protocol = service::protocol;
namespace wire = arena::protocol;

struct SecurityPolicyTest : test::ProtocolTest
{
    int routed{0};
    void make(PacketPolicyFactory policies = {})
    {
        auto registry = std::make_shared<MessageRegistry>();
        registry->registerHandler(
            wire::MSG_PING,
            [this](auto request, auto complete)
            {
                ++routed;
                wire::Envelope response;
                response.mutable_pong()->set_nonce(
                    request->envelope.ping().nonce()
                );
                complete(std::move(response));
            },
            {false}
        );
        registry->registerHandler(
            wire::MSG_LOGIN_REQUEST,
            [this](auto, auto complete)
            {
                ++routed;
                wire::Envelope response;
                response.mutable_login_response()->set_accepted(true);
                complete(std::move(response));
            },
            {false}
        );
        registry->registerHandler(
            static_cast<MsgId>(100),
            [this](auto, auto complete)
            {
                ++routed;
                wire::Envelope response;
                response.mutable_pong();
                complete(std::move(response));
            }
        );
        registry->seal();
        start(makeArenaProtocolHandler(
            context,
            registry,
            std::move(policies)
        ));
    }
    static std::string ping(std::uint16_t type = 1)
    {
        wire::Envelope request;
        request.mutable_ping()->set_nonce(42);
        return frame(*protocol::EnvelopeCodec::encode(request), type);
    }
    static std::string business()
    {
        wire::Envelope request;
        request.set_msg_id(100);
        request.mutable_error()->set_code(42);
        return frame(*protocol::EnvelopeCodec::encode(request));
    }
};

TEST_F(
    SecurityPolicyTest,
    BurstStopsBeforeDecodeAndFollowingCoalescedFrames
)
{
    make(
        [](auto& policies, const auto&)
        {
            policies.template add<PacketRatePolicy>(
                2,
                0.1,
                PacketDecision::Close,
                [] { return PacketRatePolicy::Clock::time_point{}; }
            );
        }
    );
    receive(ping() + ping() + frame("\x62\x80") + ping());
    EXPECT_EQ(routed, 2);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(
        counters->count(RejectionReason::PacketRateExceeded),
        1
    );
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
}

TEST_F(SecurityPolicyTest, DropCountsEachRejectedPacketWithoutRouting)
{
    make(
        [](auto& policies, const auto&)
        {
            policies.template add<PacketRatePolicy>(
                1,
                0.1,
                PacketDecision::Drop,
                [] { return PacketRatePolicy::Clock::time_point{}; }
            );
        }
    );
    receive(ping() + ping() + ping());
    EXPECT_EQ(routed, 1);
    EXPECT_TRUE(host->connected());
    EXPECT_EQ(
        counters->count(RejectionReason::PacketRateExceeded),
        2
    );
}

TEST_F(
    SecurityPolicyTest,
    PendingBusinessIsRejectedByDispatcherBeforeHandler
)
{
    make();
    receive(business());
    EXPECT_EQ(routed, 0);
    EXPECT_TRUE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
}

TEST_F(
    SecurityPolicyTest,
    PublicPingAndLoginNeverAuthenticateByThemselves
)
{
    make();
    wire::Envelope request;
    request.mutable_login_request()->set_account("alice");
    receive(
        ping() + frame(*protocol::EnvelopeCodec::encode(request))
    );
    EXPECT_EQ(routed, 2);
    EXPECT_EQ(
        context->session->loginState(),
        ConnectionSession::LoginState::Pending
    );
    receive(business());
    EXPECT_EQ(routed, 2);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
}

TEST_F(
    SecurityPolicyTest,
    ResponseOuterTypeCannotEnterPublicRequestHandler
)
{
    make();
    receive(ping(2));
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);
}

TEST_F(SecurityPolicyTest, AuthenticatedBusinessReachesHandler)
{
    make();
    ASSERT_TRUE(context->session->markAuthenticated());
    receive(business());
    EXPECT_EQ(routed, 1);
    EXPECT_TRUE(host->connected());
    EXPECT_EQ(counters->snapshot(), RejectionCounters::Snapshot{});
}

TEST_F(
    SecurityPolicyTest, ExpiredSessionCannotRestartPublicLoginOrPing
)
{
    make();
    ASSERT_TRUE(context->session->expireLogin());
    EXPECT_FALSE(context->session->markAuthenticated());
    receive(ping());
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
}

TEST_F(SecurityPolicyTest, PacketTypeRejectionPrecedesProtobufDecode)
{
    make(
        [](auto& policies, const auto&)
        {
            policies.template add<PacketTypePolicy>(
                std::vector<std::uint16_t>{1}
            );
        }
    );
    receive(frame("\x62\x80", 2));
    EXPECT_EQ(routed, 0);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(
        counters->count(RejectionReason::PacketTypeNotAllowed),
        1
    );
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
}

TEST_F(SecurityPolicyTest, MalformedEnvelopeStopsFollowingFrames)
{
    make();
    receive(frame("\x62\x80") + ping());
    EXPECT_EQ(routed, 0);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 1);
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
}

TEST_F(SecurityPolicyTest, MalformedFrameCountsOnlyFrameReason)
{
    make();
    auto bytes = ping();
    bytes[0] = 'X';
    receive(bytes);
    EXPECT_EQ(routed, 0);
    EXPECT_EQ(counters->count(RejectionReason::InvalidFrame), 1);
    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
}

TEST(
    SecurityPolicyConfigurationTest,
    MissingContextRegistryAndCountersFailClosed
)
{
    EXPECT_THROW(
        makePreLoginHandshakeHandler(nullptr),
        std::invalid_argument
    );
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1),
        ConnectionClock::now()
    );
    EXPECT_THROW(
        makeArenaProtocolHandler(context, nullptr),
        std::invalid_argument
    );
    EXPECT_THROW(
        makeArenaProtocolHandler(
            context,
            std::make_shared<MessageRegistry>()
        ),
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
}

TEST(
    RejectionCountersTest,
    SharedWorkersDoNotLoseCountsAndCountsOutliveConnection
)
{
    const auto counters = std::make_shared<RejectionCounters>();
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i)
        workers.emplace_back(
            [counters]
            {
                for (int j = 0; j < 10000; ++j)
                    counters->record(
                        RejectionReason::Unauthenticated
                    );
            }
        );
    for (auto& worker : workers)
        worker.join();
    EXPECT_EQ(
        counters->count(RejectionReason::Unauthenticated),
        40000
    );
    for (std::size_t i = 0; i < RejectionCounters::kReasons; ++i)
        EXPECT_NE(
            RejectionCounters::name(static_cast<RejectionReason>(i)),
            "unknown"
        );
}
} // namespace
