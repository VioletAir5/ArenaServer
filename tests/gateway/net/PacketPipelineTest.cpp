#include "net/PacketPipeline.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "net/PreLoginHandshake.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace
{
using namespace service::gateway;

class TestConnection final : public IConnection
{
public:
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

    bool open{true};
    int closes{0};
    std::string sent;
};

// Test-only node: nullopt forwards using the same base handle as real
// filters.
class RecordingHandler final : public PacketHandler
{
public:
    using Action = std::function<
        std::optional<PacketHandleResult>(PacketContext&)>;
    explicit RecordingHandler(Action action)
        : m_action(std::move(action))
    {
    }

    PacketHandleResult handle(PacketContext& context) override
    {
        auto result = m_action(context);
        return result ? *result : PacketHandler::handle(context);
    }

private:
    Action m_action;
};

class OnePacketHandler final : public PacketHandler
{
public:
    explicit OnePacketHandler(int& destructions)
        : m_destructions(destructions)
    {
    }
    ~OnePacketHandler() override
    {
        ++m_destructions;
    }

    PacketHandleResult handle(PacketContext&) override
    {
        return ++m_calls == 1 ? PacketHandleResult::Handled
                              : PacketHandleResult::Reject;
    }

private:
    int m_calls{0};
    int& m_destructions;
};

std::span<const std::byte> bytes(std::string_view value)
{
    return std::as_bytes(std::span(value));
}

std::string frame(std::string_view payload)
{
    auto encoded = service::protocol::PacketCodec::encode(
        service::protocol::PacketHeader::kEchoRequest,
        7,
        bytes(payload)
    );
    if (!encoded)
    {
        throw std::runtime_error("encode failed");
    }
    return {encoded->peek(), encoded->readableBytes()};
}
} // namespace

TEST(PacketPipelineTest, ForwardsInOrderAndStopsWhenHandled)
{
    TestConnection connection;
    service::protocol::PacketHeader header;
    PacketContext context{connection, header, "payload"};
    std::vector<int> order;
    auto first = std::make_unique<RecordingHandler>(
        [&](PacketContext& packet)
        {
            EXPECT_EQ(packet.payload, "payload");
            order.push_back(1);
            return std::nullopt;
        }
    );
    auto second = std::make_unique<RecordingHandler>(
        [&](PacketContext&)
        {
            order.push_back(2);
            return PacketHandleResult::Handled;
        }
    );
    second->setNext(
        std::make_unique<RecordingHandler>(
            [&](PacketContext&)
            {
                order.push_back(3);
                return std::nullopt;
            }
        )
    );
    first->setNext(std::move(second));
    PacketPipeline pipeline(std::move(first));

    EXPECT_EQ(pipeline.process(context), PacketHandleResult::Handled);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(PacketPipelineTest, RejectStopsBeforeDownstreamHandler)
{
    TestConnection connection;
    service::protocol::PacketHeader header;
    PacketContext context{connection, header, "payload"};
    auto rejecting = std::make_unique<RecordingHandler>(
        [](PacketContext&) { return PacketHandleResult::Reject; }
    );
    int downstream = 0;
    rejecting->setNext(
        std::make_unique<RecordingHandler>(
            [&](PacketContext&)
            {
                ++downstream;
                return PacketHandleResult::Handled;
            }
        )
    );
    PacketPipeline pipeline(std::move(rejecting));

    EXPECT_EQ(pipeline.process(context), PacketHandleResult::Reject);
    EXPECT_EQ(downstream, 0);
}

TEST(
    PacketPipelineTest, UnhandledFrameIsRejectedAndEmptyHeadIsInvalid
)
{
    EXPECT_THROW((PacketPipeline(nullptr)), std::invalid_argument);
    auto head = std::make_unique<PacketHandler>();
    head->setNext(std::make_unique<PacketHandler>());
    auto callback = makePacketHandler(
        PacketPipeline(std::move(head)),
        256
    );
    TestConnection connection;
    callback(connection, bytes(frame("unknown")));
    EXPECT_FALSE(connection.connected());
    EXPECT_EQ(connection.closes, 1);
}

TEST(
    PacketPipelineTest,
    EnvelopeDecoderFeedsNextHandlerAndRejectsMalformed
)
{
    arena::protocol::Envelope envelope;
    envelope.mutable_ping()->set_nonce(123);
    const auto wire = service::protocol::EnvelopeCodec::encode(
        envelope
    );
    ASSERT_TRUE(wire);
    TestConnection connection;
    service::protocol::PacketHeader header;
    auto decoder = std::make_unique<EnvelopeDecodeHandler>();
    int routed = 0;
    decoder->setNext(
        std::make_unique<RecordingHandler>(
            [&](PacketContext& packet)
            {
                EXPECT_TRUE(packet.envelope.has_value());
                EXPECT_EQ(packet.envelope->ping().nonce(), 123);
                ++routed;
                return PacketHandleResult::Handled;
            }
        )
    );
    PacketPipeline pipeline(std::move(decoder));
    PacketContext valid{connection, header, *wire};
    EXPECT_EQ(pipeline.process(valid), PacketHandleResult::Handled);
    EXPECT_EQ(routed, 1);
    PacketContext malformed{
        connection,
        header,
        std::string_view("\x62\x80", 2)
    };
    EXPECT_EQ(
        pipeline.process(malformed),
        PacketHandleResult::Reject
    );
    EXPECT_FALSE(malformed.envelope.has_value());
    EXPECT_EQ(routed, 1);
}

TEST(
    PacketPipelineTest,
    HandlerRunsOnlyForCompleteFramesAndClosesOnReject
)
{
    int calls = 0;
    auto node = std::make_unique<RecordingHandler>(
        [&](PacketContext& packet)
        {
            ++calls;
            return packet.payload == "bad"
                       ? PacketHandleResult::Reject
                       : PacketHandleResult::Handled;
        }
    );
    auto handler = makePacketHandler(
        PacketPipeline(std::move(node)),
        256
    );
    TestConnection connection;
    const std::string first = frame("one");
    handler(connection, bytes(std::string_view(first).substr(0, 6)));
    EXPECT_EQ(calls, 0);
    EXPECT_TRUE(connection.connected());
    handler(connection, bytes(first.substr(6) + frame("two")));
    EXPECT_EQ(calls, 2);
    EXPECT_TRUE(connection.connected());
    handler(connection, bytes(frame("bad") + frame("ignored")));
    EXPECT_EQ(calls, 3);
    EXPECT_EQ(connection.closes, 1);
    EXPECT_FALSE(connection.connected());
}

TEST(
    PacketPipelineTest,
    FreshChainsIsolateStateAndCallbackCopiesRetainNodes
)
{
    int destructions = 0;
    auto factory = [&]
    {
        return makePacketHandler(
            PacketPipeline(
                std::make_unique<OnePacketHandler>(destructions)
            ),
            256
        );
    };
    auto first = factory();
    auto second = factory();
    auto retained = first;
    TestConnection connection1;
    TestConnection connection2;
    first(connection1, bytes(frame("one")));
    second(connection2, bytes(frame("one")));
    EXPECT_TRUE(connection1.connected());
    EXPECT_TRUE(connection2.connected());
    first = {};
    EXPECT_EQ(destructions, 0);
    retained(connection1, bytes(frame("two")));
    EXPECT_FALSE(connection1.connected());
    EXPECT_TRUE(connection2.connected());
    retained = {};
    EXPECT_EQ(destructions, 1);
    second = {};
    EXPECT_EQ(destructions, 2);
}

TEST(
    PacketPipelineTest,
    HandlerExceptionClosesAndDiscardsRemainingFrames
)
{
    int calls = 0;
    auto node = std::make_unique<RecordingHandler>(
        [&](PacketContext&) -> std::optional<PacketHandleResult>
        {
            ++calls;
            throw std::runtime_error("policy failure");
        }
    );
    auto handler = makePacketHandler(
        PacketPipeline(std::move(node)),
        256
    );
    TestConnection connection;
    handler(connection, bytes(frame("one") + frame("ignored")));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(connection.closes, 1);
    EXPECT_FALSE(connection.connected());
}

TEST(
    PacketPipelineTest, PingHandlerForwardsOtherEnvelopesToLaterRouter
)
{
    arena::protocol::Envelope envelope;
    envelope.mutable_login_request()->set_account("alice");
    const auto wire = service::protocol::EnvelopeCodec::encode(
        envelope
    );
    ASSERT_TRUE(wire);
    auto decoder = std::make_unique<EnvelopeDecodeHandler>();
    auto ping = std::make_unique<PingHandler>();
    int routed = 0;
    ping->setNext(
        std::make_unique<RecordingHandler>(
            [&](PacketContext& context)
            {
                EXPECT_TRUE(context.envelope->has_login_request());
                ++routed;
                return PacketHandleResult::Handled;
            }
        )
    );
    decoder->setNext(std::move(ping));
    auto callback = makePacketHandler(
        PacketPipeline(std::move(decoder)),
        256
    );
    TestConnection connection;
    callback(connection, bytes(frame(*wire)));
    EXPECT_EQ(routed, 1);
    EXPECT_TRUE(connection.connected());
    EXPECT_TRUE(connection.sent.empty());
}
