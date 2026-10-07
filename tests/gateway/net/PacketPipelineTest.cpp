#include "net/PacketPipeline.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace
{
using namespace service::gateway;

class TestConnection final : public IConnection
{
public:
    std::uint64_t id() const noexcept override { return 1; }
    void send(std::span<const std::byte> bytes) override
    {
        sent.append(reinterpret_cast<const char*>(bytes.data()),
                    bytes.size());
    }
    void shutdown() override {}
    void close() override { open = false; ++closes; }
    bool connected() const noexcept override { return open; }
    void pauseRead() override {}
    void resumeRead() override {}
    std::size_t pendingBytes() const noexcept override { return 0; }

    bool open{true};
    int closes{0};
    std::string sent;
};

std::span<const std::byte> bytes(std::string_view value)
{
    return std::as_bytes(std::span(value));
}

std::string frame(std::string_view payload)
{
    auto encoded = service::protocol::PacketCodec::encode(
        service::protocol::PacketHeader::kEchoRequest, 7, bytes(payload));
    if (!encoded)
    {
        throw std::runtime_error("encode failed");
    }
    return {encoded->peek(), encoded->readableBytes()};
}
} // namespace

TEST(PacketPipelineTest, RunsInOrderAndShortCircuitsHandledOrReject)
{
    TestConnection connection;
    service::protocol::PacketHeader header;
    PacketContext context{connection, header, "payload"};
    PacketPipeline pipeline;
    std::vector<int> order;
    pipeline.addStage([&](PacketContext& packet) {
        EXPECT_EQ(packet.payload, "payload");
        order.push_back(1);
        return PacketStageResult::Continue;
    });
    pipeline.addStage([&](PacketContext&) {
        order.push_back(2);
        return PacketStageResult::Handled;
    });
    pipeline.addStage([&](PacketContext&) {
        order.push_back(3);
        return PacketStageResult::Continue;
    });
    EXPECT_EQ(pipeline.process(context), PacketStageResult::Handled);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));

    PacketPipeline rejecting;
    rejecting.addStage([](PacketContext&) {
        return PacketStageResult::Reject;
    });
    rejecting.addStage([&](PacketContext&) {
        order.push_back(4);
        return PacketStageResult::Continue;
    });
    EXPECT_EQ(rejecting.process(context), PacketStageResult::Reject);
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
    EXPECT_THROW(pipeline.addStage({}), std::invalid_argument);
}

TEST(PacketPipelineTest, EnvelopeDecodeStageFeedsLaterStagesAndRejectsMalformed)
{
    arena::protocol::Envelope envelope;
    envelope.mutable_ping()->set_nonce(123);
    const auto wire = service::protocol::EnvelopeCodec::encode(envelope);
    ASSERT_TRUE(wire);
    TestConnection connection;
    service::protocol::PacketHeader header;
    PacketPipeline pipeline;
    pipeline.addStage(PacketPipeline::decodeEnvelope);
    int routed = 0;
    pipeline.addStage([&](PacketContext& packet) {
        EXPECT_TRUE(packet.envelope.has_value());
        EXPECT_EQ(packet.envelope->ping().nonce(), 123);
        ++routed;
        return PacketStageResult::Handled;
    });
    PacketContext valid{connection, header, *wire};
    EXPECT_EQ(pipeline.process(valid), PacketStageResult::Handled);
    EXPECT_EQ(routed, 1);
    PacketContext malformed{
        connection, header, std::string_view("\x62\x80", 2)
    };
    EXPECT_EQ(pipeline.process(malformed), PacketStageResult::Reject);
    EXPECT_FALSE(malformed.envelope.has_value());
    EXPECT_EQ(routed, 1);
}

TEST(PacketPipelineTest, HandlerRunsOnlyForCompleteFramesAndClosesOnReject)
{
    PacketPipeline pipeline;
    int calls = 0;
    pipeline.addStage([&](PacketContext& packet) {
        ++calls;
        return packet.payload == "bad"
                   ? PacketStageResult::Reject
                   : PacketStageResult::Handled;
    });
    auto handler = makePacketHandler(std::move(pipeline), 256);
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