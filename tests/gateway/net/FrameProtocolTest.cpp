#include "net/FrameProtocol.h"

#include "net/PacketEcho.h"

#include "ProtocolTestHarness.h"

namespace
{
using namespace service::gateway;
using FrameProtocolTest = test::ProtocolTest;

TEST_F(FrameProtocolTest, SplitFrameIsDeliveredOnlyAfterItsLastByte)
{
    int calls = 0;
    std::string payload;
    start(makeFrameProtocolHandler(
        [&](FrameContext& packet)
        {
            ++calls;
            payload = packet.payload;
            EXPECT_EQ(packet.header.m_requestId, 7);
            return FrameResult::Handled;
        },
        1024,
        counters
    ));
    const auto bytes = frame("split");
    for (std::size_t i = 0; i + 1 < bytes.size(); ++i)
        receive(std::string_view(bytes.data() + i, 1));
    EXPECT_EQ(calls, 0);
    receive(std::string_view(bytes.data() + bytes.size() - 1, 1));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(payload, "split");
}

TEST_F(FrameProtocolTest, CoalescedFramesAreDeliveredInWireOrder)
{
    std::vector<std::string> payloads;
    start(makeFrameProtocolHandler(
        [&](FrameContext& packet)
        {
            payloads.emplace_back(packet.payload);
            return FrameResult::Handled;
        },
        1024,
        counters
    ));
    receive(frame("a") + frame("b") + frame("c"));
    EXPECT_EQ(payloads, (std::vector<std::string>{"a", "b", "c"}));
}

TEST_F(
    FrameProtocolTest,
    RejectStopsFollowingCoalescedFramesAndRecordsOnce
)
{
    int calls = 0;
    start(makeFrameProtocolHandler(
        [&](FrameContext&)
        {
            ++calls;
            return FrameResult::Reject;
        },
        1024,
        counters
    ));
    receive(frame("a") + frame("b"));
    EXPECT_EQ(calls, 1);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);
}

TEST_F(
    FrameProtocolTest,
    CallbackExceptionClosesWithoutEscapingTheReadEvent
)
{
    start(makeFrameProtocolHandler(
        [](FrameContext&) -> FrameResult
        { throw std::runtime_error("private failure"); },
        1024,
        counters
    ));
    EXPECT_NO_THROW(receive(frame("a")));
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::CallbackFailure), 1);
}

TEST_F(FrameProtocolTest, InvalidFrameNeverCallsPayloadHandler)
{
    int calls = 0;
    start(makeFrameProtocolHandler(
        [&](FrameContext&)
        {
            ++calls;
            return FrameResult::Handled;
        },
        1024,
        counters
    ));
    auto bytes = frame("a");
    bytes[0] = 'X';
    receive(bytes);
    EXPECT_EQ(calls, 0);
    EXPECT_FALSE(host->connected());
    EXPECT_EQ(counters->count(RejectionReason::InvalidFrame), 1);
}

TEST_F(
    FrameProtocolTest,
    ClosingInsideCallbackStopsDecoderWithoutDestroyingBorrowedData
)
{
    int calls = 0;
    start(makeFrameProtocolHandler(
        [&](FrameContext& packet)
        {
            ++calls;
            packet.connection.close();
            EXPECT_EQ(packet.payload, "keep-alive-until-return");
            return FrameResult::Handled;
        },
        1024,
        counters
    ));
    receive(frame("keep-alive-until-return") + frame("later"));
    EXPECT_EQ(calls, 1);
}

TEST_F(
    FrameProtocolTest,
    DrainInsideFirstCallbackStopsRemainingCoalescedFrames
)
{
    int calls = 0;
    start(makeFrameProtocolHandler(
        [&](FrameContext& packet)
        {
            ++calls;
            packet.connection.shutdown();
            EXPECT_TRUE(packet.connection.connected());
            EXPECT_FALSE(packet.connection.acceptingInput());
            EXPECT_EQ(packet.payload, "first");
            return FrameResult::Handled;
        },
        1024,
        counters
    ));
    receive(frame("first") + frame("must-not-dispatch"));
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(counters->snapshot(), RejectionCounters::Snapshot{});
}

TEST(
    FrameProtocolConfigurationTest,
    RejectsEmptyHandlerCountersAndOversizedLimit
)
{
    EXPECT_THROW(
        makeFrameProtocolHandler({}, 1024),
        std::invalid_argument
    );
    const FrameHandler handler = [](FrameContext&)
    { return FrameResult::Handled; };
    EXPECT_THROW(
        makeFrameProtocolHandler(handler, 1024, nullptr),
        std::invalid_argument
    );
    EXPECT_THROW(
        makeFrameProtocolHandler(handler, kMaxPacketSize + 1),
        std::invalid_argument
    );
}
} // namespace
