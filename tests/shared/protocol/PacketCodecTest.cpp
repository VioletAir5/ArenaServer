#include "protocol/PacketCodec.hpp"

#include <array>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace service::protocol;

namespace
{
std::string frame(std::string_view payload, std::uint32_t id = 1)
{
    auto encoded = PacketCodec::encode(
        PacketHeader::kEchoRequest,
        id,
        std::as_bytes(std::span(payload))
    );
    if (!encoded)
    {
        throw std::runtime_error("encode");
    }
    return {encoded->peek(), encoded->readableBytes()};
}
auto bytes(std::string_view data)
{
    return std::as_bytes(std::span(data));
}
} // namespace

TEST(PacketCodecTest, DecodesBytewiseHeadersAndCoalescedBinaryFrames)
{
    const std::string binary{"a\0b\xff", 4};
    const auto first = frame(binary, 0x01020304);
    const auto next = frame("", 42) + frame("tail", 43);
    PacketCodec codec;
    std::vector<std::string> payloads;
    std::vector<std::uint32_t> ids;
    auto receive =
        [&](const PacketHeader& header, std::string_view payload)
    {
        ids.push_back(header.m_requestId);
        payloads.emplace_back(payload);
        return true;
    };
    for (char byte : first)
    {
        EXPECT_EQ(
            codec.feed(bytes(std::string_view(&byte, 1)), receive),
            FrameDecodeStatus::NeedMore
        );
    }
    EXPECT_EQ(
        codec.feed(bytes(next), receive),
        FrameDecodeStatus::NeedMore
    );
    EXPECT_EQ(
        (std::vector<std::string>{binary, "", "tail"}),
        payloads
    );
    EXPECT_EQ((std::vector<std::uint32_t>{0x01020304, 42, 43}), ids);
    EXPECT_EQ(codec.pendingBytes(), 0);
}

TEST(PacketCodecTest, ProcessesAggregateLargerThanItsBound)
{
    PacketCodec codec(32);
    const auto single = frame("payload");
    std::string stream;
    for (int i = 0; i < 1000; ++i)
    {
        stream += single;
    }
    int count = 0;
    EXPECT_EQ(
        codec.feed(
            bytes(stream),
            [&](auto&, auto payload)
            {
                EXPECT_EQ(payload, "payload");
                ++count;
                return true;
            }
        ),
        FrameDecodeStatus::NeedMore
    );
    EXPECT_EQ(count, 1000);
    EXPECT_EQ(codec.pendingBytes(), 0);
}

TEST(PacketCodecTest, RejectsConfiguredLengthAsSoonAsPrefixArrives)
{
    PacketCodec codec(32);
    const std::array<std::byte, 8> prefix{
        std::byte{0x41}, std::byte{0x52},
        std::byte{0x45}, std::byte{0x4e},
        std::byte{0},
        std::byte{0},
        std::byte{0},
        std::byte{33}};
    int delivered = 0;
    auto receive = [&](auto&, auto)
    {
        ++delivered;
        return true;
    };
    EXPECT_EQ(
        codec.feed(prefix, receive),
        FrameDecodeStatus::Invalid
    );
    EXPECT_EQ(codec.pendingBytes(), PacketHeader::kPrefixSize);
    EXPECT_EQ(
        codec.feed(bytes(frame("valid")), receive),
        FrameDecodeStatus::Invalid
    );
    EXPECT_EQ(delivered, 0);
}

TEST(PacketCodecTest, RejectsMalformedHeaderBeforeBodyRetention)
{
    for (auto offset : {9u, 14u})
    {
        auto wire = frame(std::string(128, 'x'));
        wire[offset] = static_cast<char>(99);
        PacketCodec codec;
        bool delivered = false;
        EXPECT_EQ(
            codec.feed(
                bytes(wire),
                [&](auto&, auto)
                {
                    delivered = true;
                    return true;
                }
            ),
            FrameDecodeStatus::Invalid
        );
        EXPECT_FALSE(delivered);
        EXPECT_EQ(codec.pendingBytes(), PacketHeader::kHeaderSize);
    }
}

TEST(PacketCodecTest, CallbackStopIsTerminalAndDoesNotDeliverTail)
{
    PacketCodec codec;
    int count = 0;
    auto receive = [&](auto&, auto)
    {
        ++count;
        return false;
    };
    EXPECT_EQ(
        codec.feed(bytes(frame("first") + frame("second")), receive),
        FrameDecodeStatus::Stopped
    );
    EXPECT_EQ(codec.pendingBytes(), 0);
    EXPECT_EQ(
        codec.feed(bytes(frame("third")), receive),
        FrameDecodeStatus::Stopped
    );
    EXPECT_EQ(count, 1);
}

TEST(PacketCodecTest, ThrowingAndReentrantCallbacksInvalidateDecoder)
{
    PacketCodec codec;
    EXPECT_THROW(
        codec.feed(
            bytes(frame("first")),
            [&](auto&, auto) -> bool
            {
                codec.feed({}, [](auto&, auto) { return true; });
                return true;
            }
        ),
        std::logic_error
    );
    EXPECT_EQ(
        codec.feed({}, [](auto&, auto) { return true; }),
        FrameDecodeStatus::Invalid
    );
    PacketCodec second;
    EXPECT_THROW(
        second.feed(
            bytes(frame("x")),
            [](auto&, auto) -> bool
            { throw std::runtime_error("application"); }
        ),
        std::runtime_error
    );
    EXPECT_EQ(
        second.feed({}, [](auto&, auto) { return true; }),
        FrameDecodeStatus::Invalid
    );
}

TEST(
    PacketCodecTest,
    SupportsSharedFourMiBCeilingAndRejectsInvalidEncode
)
{
    const std::string payload(
        PacketHeader::kMaxPacketSize - PacketHeader::kMetadataSize,
        'x'
    );
    auto output = PacketCodec::encode(
        PacketHeader::kEchoRequest,
        99,
        bytes(payload)
    );
    ASSERT_TRUE(output);
    PacketCodec codec;
    int count = 0;
    auto input = bytes(
        std::string_view(output->peek(), output->readableBytes())
    );
    while (!input.empty())
    {
        auto chunk = input.first(
            std::min<std::size_t>(8192, input.size())
        );
        EXPECT_EQ(
            codec.feed(
                chunk,
                [&](const auto& header, auto body)
                {
                    EXPECT_EQ(
                        header.m_length,
                        PacketHeader::kMaxPacketSize
                    );
                    EXPECT_EQ(header.m_requestId, 99);
                    EXPECT_EQ(body, payload);
                    ++count;
                    return true;
                }
            ),
            FrameDecodeStatus::NeedMore
        );
        input = input.subspan(chunk.size());
    }
    EXPECT_EQ(count, 1);
    EXPECT_EQ(codec.pendingBytes(), 0);
    EXPECT_FALSE(PacketCodec::encode(99, 1, {}));
    EXPECT_FALSE(
        PacketCodec::encode(PacketHeader::kEchoRequest, 1, {}, 99)
    );
    EXPECT_FALSE(PacketCodec::encode(
        PacketHeader::kEchoRequest,
        1,
        bytes(payload + "x")
    ));
    EXPECT_THROW(PacketCodec(6), std::invalid_argument);
    EXPECT_THROW(
        PacketCodec(PacketHeader::kMaxPacketSize + 1),
        std::invalid_argument
    );
}

TEST(PacketCodecTest, RejectsBadMagicAtFourBytes)
{
    PacketCodec codec;
    const std::array<std::byte, 4> badMagic{
        std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}};
    int delivered = 0;
    auto receive = [&](auto&, auto) {
        ++delivered;
        return true;
    };
    EXPECT_EQ(codec.feed(badMagic, receive), FrameDecodeStatus::Invalid);
    EXPECT_EQ(codec.pendingBytes(), badMagic.size());
    EXPECT_EQ(codec.feed(bytes(frame("ignored")), receive),
              FrameDecodeStatus::Invalid);
    EXPECT_EQ(delivered, 0);
}