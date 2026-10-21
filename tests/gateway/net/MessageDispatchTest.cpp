#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>

#include "net/ConnectionRuntime.h"
#include "net/MessageRouter.h"
#include "net/PacketBuffer.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace
{
using namespace service::gateway;
namespace wire = arena::protocol;
struct Transport final : IConnection
{
    std::string output;
    bool open = true;
    std::uint64_t id() const noexcept override
    {
        return 1;
    }
    void send(std::span<const std::byte> bytes) override
    {
        output.append(
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
struct MessageDispatchTest : testing::Test
{
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    ConnectionContext::Ptr
        context = std::make_shared<ConnectionContext>(
            1,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now(),
            std::make_shared<RejectionCounters>()
        );
    std::vector<std::function<void()>> posts;
    std::vector<std::function<void()>> deadlines;
    int cancellations = 0;
    std::shared_ptr<ConnectionRuntime> runtime;

    void start(
        std::shared_ptr<MessageRegistry> registry,
        std::shared_ptr<spdlog::logger> logger = {}
    )
    {
        registry->seal();
        auto decoder = std::make_unique<EnvelopeDecodeHandler>();
        decoder->setNext(
            std::make_unique<MessageRouter>(
                context,
                std::move(registry),
                std::move(logger)
            )
        );
        runtime = std::make_shared<ConnectionRuntime>(
            transport,
            context,
            ConnectionScheduler{
                [&](auto task) { posts.push_back(std::move(task)); },
                [&](auto, auto task)
                {
                    deadlines.push_back(std::move(task));
                    return ConnectionTimer([&] { ++cancellations; });
                }
            },
            makePacketHandler(
                PacketPipeline(std::move(decoder)),
                1024,
                context->rejections
            )
        );
        runtime->start();
    }
    void receive(const wire::Envelope& request)
    {
        const auto payload = service::protocol::EnvelopeCodec::encode(
            request
        );
        ASSERT_TRUE(payload);
        const auto frame = service::protocol::PacketCodec::encode(
            1,
            7,
            std::as_bytes(std::span(*payload))
        );
        ASSERT_TRUE(frame);
        runtime->received(
            std::as_bytes(
                std::span(frame->peek(), frame->readableBytes())
            )
        );
    }
    wire::Envelope response()
    {
        wire::Envelope result;
        service::protocol::PacketCodec decoder(kMaxPacketSize);
        int responses = 0;
        decoder.feed(
            std::as_bytes(std::span(transport->output)),
            [&](const auto& header, std::string_view payload)
            {
                EXPECT_EQ(header.m_type, 2);
                EXPECT_EQ(header.m_requestId, 7);
                EXPECT_TRUE(
                    service::protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(payload)),
                        result
                    )
                );
                ++responses;
                return true;
            }
        );
        EXPECT_EQ(responses, 1);
        return result;
    }
};
TEST_F(
    MessageDispatchTest,
    UnknownMessageReturnsStableTypedErrorAndRequestId
)
{
    start(std::make_shared<MessageRegistry>());
    wire::Envelope request;
    request.set_msg_id(999);
    request.set_request_id(1234567890123ULL);
    request.mutable_ping()->set_nonce(9);
    receive(request);
    const auto reply = response();
    EXPECT_EQ(reply.msg_id(), wire::MSG_ERROR);
    EXPECT_EQ(reply.request_id(), 1234567890123ULL);
    ASSERT_TRUE(reply.has_error());
    EXPECT_EQ(reply.error().code(), 1001);
    EXPECT_EQ(reply.error().message(), "unknown message");
    EXPECT_TRUE(runtime->connected());
}
TEST_F(MessageDispatchTest, TypedBusinessExceptionMapsToStableError)
{
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [](auto, auto)
        { throw BusinessError(wire::ERR_BUSINESS_FAILURE); },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(77);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    const auto reply = response();
    EXPECT_EQ(reply.request_id(), 77);
    ASSERT_TRUE(reply.has_error());
    EXPECT_EQ(reply.error().code(), 2001);
    EXPECT_EQ(reply.error().message(), "business failure");
    EXPECT_TRUE(runtime->connected());
}
TEST_F(
    MessageDispatchTest, TimeoutRepliesOnceAndLateCompletionIsIgnored
)
{
    auto registry = std::make_shared<MessageRegistry>();
    RequestCompletion late;
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [&](auto, auto complete) { late = std::move(complete); },
        MessageRouteOptions{false, std::chrono::milliseconds(5)}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(88);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    EXPECT_TRUE(transport->output.empty());
    ASSERT_EQ(deadlines.size(), 1);
    deadlines.front()();
    const auto reply = response();
    EXPECT_EQ(reply.request_id(), 88);
    ASSERT_TRUE(reply.has_error());
    EXPECT_EQ(reply.error().code(), 1004);
    EXPECT_EQ(reply.error().message(), "request timed out");
    const auto before = transport->output;
    wire::Envelope success;
    success.mutable_login_response()->set_accepted(true);
    late(std::move(success));
    EXPECT_EQ(transport->output, before);
    EXPECT_EQ(cancellations, 1);
}
TEST_F(
    MessageDispatchTest, MalformedBusinessReplyBecomesInternalError
)
{
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [](auto, auto complete) { complete(wire::Envelope{}); },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(99);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    const auto reply = response();
    ASSERT_TRUE(reply.has_error());
    EXPECT_EQ(reply.error().code(), 9000);
    EXPECT_EQ(reply.error().message(), "internal error");
    EXPECT_TRUE(runtime->connected());
}

TEST_F(MessageDispatchTest, ResponseRespectsOuterPacketBudget)
{
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_PING,
        [](auto, auto complete)
        {
            wire::Envelope reply;
            reply.mutable_pong();
            reply.GetReflection()
                ->MutableUnknownFields(&reply)
                ->AddLengthDelimited(
                    100,
                    std::string(
                        service::protocol::EnvelopeCodec::kMaxBytes -
                            16,
                        'x'
                    )
                );
            complete(std::move(reply));
        },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_PING);
    request.set_request_id(107);
    request.mutable_ping();
    receive(request);
    const auto reply = response();
    ASSERT_TRUE(reply.has_error());
    EXPECT_EQ(reply.error().code(), wire::ERR_INTERNAL);
    EXPECT_TRUE(runtime->connected());
}
TEST_F(
    MessageDispatchTest,
    LegacyRequestIdFallbackAndSpdlogTraceStayConsistent
)
{
    std::ostringstream output;
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(
        output
    );
    auto logger = std::make_shared<spdlog::logger>(
        "request-test",
        sink
    );
    logger->set_pattern("%v");
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [](auto request, auto complete)
        {
            EXPECT_EQ(request->requestId, 7);
            EXPECT_EQ(request->context->connectionId, 1);
            wire::Envelope reply;
            reply.mutable_login_response()->set_accepted(true);
            complete(reply);
            complete(
                std::move(reply)
            ); // A copied completion still replies once.
        },
        MessageRouteOptions{false}
    );
    start(registry, logger);
    wire::Envelope
        request; // no MsgId, no inner request_id: v1 fallback
    request.mutable_login_request()->set_credential(
        "never-log-this-secret"
    );
    receive(request);
    const auto reply = response();
    EXPECT_EQ(reply.request_id(), 7);
    EXPECT_EQ(reply.msg_id(), 0);
    EXPECT_TRUE(reply.login_response().accepted());
    EXPECT_EQ(cancellations, 1);
    const auto logs = output.str();
    EXPECT_NE(
        logs.find(
            "event=request_start connection_id=1 request_id=7 "
            "msg_id=10"
        ),
        std::string::npos
    );
    EXPECT_NE(
        logs.find(
            "event=request_handler connection_id=1 "
            "request_id=7 msg_id=10"
        ),
        std::string::npos
    );
    EXPECT_NE(
        logs.find(
            "event=request_end connection_id=1 request_id=7 "
            "msg_id=10 code=0"
        ),
        std::string::npos
    );
    EXPECT_EQ(logs.find("never-log-this-secret"), std::string::npos);
}
TEST_F(
    MessageDispatchTest,
    AuthenticationIsCheckedBeforeRegisteredBusinessHandler
)
{
    int calls = 0;
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [&](auto, auto complete)
        {
            ++calls;
            wire::Envelope reply;
            reply.mutable_login_response()->set_accepted(true);
            complete(std::move(reply));
        }
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(101);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    EXPECT_EQ(response().error().code(), 1003);
    EXPECT_EQ(calls, 0);
    EXPECT_TRUE(deadlines.empty());
    transport->output.clear();
    ASSERT_TRUE(context->session->markAuthenticated());
    receive(request);
    EXPECT_TRUE(response().login_response().accepted());
    EXPECT_EQ(calls, 1);
}
TEST_F(
    MessageDispatchTest, MismatchedMsgIdAndPayloadNeverInvokeHandler
)
{
    int calls = 0;
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_PING,
        [&](auto, auto) { ++calls; },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_PING);
    request.set_request_id(102);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    EXPECT_EQ(response().error().code(), 1002);
    EXPECT_EQ(calls, 0);
}
TEST_F(
    MessageDispatchTest,
    OldCompletionAndTimerCannotFinishReusedRequestId
)
{
    std::vector<RequestCompletion> callbacks;
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [&](auto, auto complete)
        { callbacks.push_back(std::move(complete)); },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(103);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    deadlines[0]();
    EXPECT_EQ(response().error().code(), 1004);
    transport->output.clear();
    receive(request);
    wire::Envelope reply;
    reply.mutable_login_response()->set_accepted(true);
    callbacks[0](reply);
    deadlines[0]();
    EXPECT_TRUE(transport->output.empty());
    callbacks[1](std::move(reply));
    EXPECT_TRUE(response().login_response().accepted());
}
TEST_F(
    MessageDispatchTest,
    DuplicateActiveRequestTerminatesOriginalWithOneError
)
{
    RequestCompletion late;
    int calls = 0;
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [&](auto, auto complete)
        {
            ++calls;
            late = std::move(complete);
        },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(104);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    receive(request);
    EXPECT_EQ(response().error().code(), 1006);
    EXPECT_EQ(calls, 1);
    const auto before = transport->output;
    late(std::unexpected(wire::ERR_BUSINESS_FAILURE));
    EXPECT_EQ(transport->output, before);
}
TEST_F(MessageDispatchTest, PendingRequestsHaveAnExplicitBound)
{
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [](auto, auto) {},
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.mutable_login_request()->set_account("alice");
    for (int i = 1; i <= 65; ++i)
    {
        request.set_request_id(i);
        receive(request);
    }
    EXPECT_EQ(response().error().code(), 1005);
    EXPECT_EQ(deadlines.size(), 64);
}
TEST_F(
    MessageDispatchTest,
    DisconnectCancelsTimersAndLateCallbacksOwnNoConnection
)
{
    RequestCompletion late;
    std::shared_ptr<const RequestMessage> ownedRequest;
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [&](auto request, auto complete)
        {
            ownedRequest = std::move(request);
            late = std::move(complete);
        },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(105);
    request.mutable_login_request()->set_account("copied");
    receive(request);
    std::weak_ptr<ConnectionRuntime> weak = runtime;
    runtime->close();
    posts[0]();
    runtime->transportClosed();
    runtime.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(cancellations, 1);
    EXPECT_EQ(
        ownedRequest->envelope.login_request().account(),
        "copied"
    );
    late(std::unexpected(wire::ERR_BUSINESS_FAILURE));
    deadlines[0]();
    EXPECT_TRUE(transport->output.empty());
}
TEST_F(
    MessageDispatchTest,
    UntypedExceptionsAndInvalidTypedCodesMapToInternal
)
{
    auto registry = std::make_shared<MessageRegistry>();
    registry->registerHandler(
        wire::MSG_LOGIN_REQUEST,
        [](auto, auto)
        {
            throw std::runtime_error("private implementation detail");
        },
        MessageRouteOptions{false}
    );
    start(registry);
    wire::Envelope request;
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.set_request_id(106);
    request.mutable_login_request()->set_account("alice");
    receive(request);
    EXPECT_EQ(response().error().code(), 9000);
    EXPECT_EQ(
        transport->output.find("private implementation detail"),
        std::string::npos
    );
    EXPECT_EQ(
        BusinessError(static_cast<ErrorCode>(9999)).code(),
        wire::ERR_INTERNAL
    );
}
TEST(
    MessageRegistryTest,
    RejectsDuplicateMissingInvalidAndLateRegistrations
)
{
    MessageRegistry registry;
    MessageHandler handler = [](auto, auto) {};
    EXPECT_THROW(
        registry.registerHandler(wire::MSG_UNSPECIFIED, handler),
        std::invalid_argument
    );
    EXPECT_THROW(
        registry.registerHandler(wire::MSG_ERROR, handler),
        std::invalid_argument
    );
    EXPECT_THROW(
        registry.registerHandler(wire::MSG_PING, {}),
        std::invalid_argument
    );
    EXPECT_THROW(
        registry.registerHandler(
            wire::MSG_PING,
            handler,
            {false, std::chrono::milliseconds(0)}
        ),
        std::invalid_argument
    );
    registry.registerHandler(wire::MSG_PING, handler);
    EXPECT_THROW(
        registry.registerHandler(wire::MSG_PING, handler),
        std::invalid_argument
    );
    registry.seal();
    EXPECT_THROW(
        registry.registerHandler(wire::MSG_LOGIN_REQUEST, handler),
        std::logic_error
    );
}

} // namespace
