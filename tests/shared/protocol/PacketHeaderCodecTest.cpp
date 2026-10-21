#include "protocol/PacketHeaderCodec.hpp"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "protocol/Buffer.hpp"

using service::protocol::Buffer;
using service::protocol::FrameDecodeStatus;
using service::protocol::HeaderDecodeStatus;
using service::protocol::PacketHeader;
using service::protocol::PacketHeaderCodec;

TEST(ProtocolBufferTest, PrependsHeaderWithoutMovingPayload)
{
    Buffer buffer;
    ASSERT_EQ(buffer.prependableBytes(), PacketHeader::kHeaderSize);
    ASSERT_TRUE(buffer.append("abc", 3));
    const char prefix[] = "xy";
    ASSERT_TRUE(buffer.prepend(prefix, 2));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "xyabc"
    );
    EXPECT_EQ(
        buffer.prependableBytes(),
        PacketHeader::kHeaderSize - 2
    );
    ASSERT_TRUE(buffer.retrieve(5));
    EXPECT_EQ(buffer.prependableBytes(), PacketHeader::kHeaderSize);
    EXPECT_EQ(buffer.readableBytes(), 0);
}

TEST(
    ProtocolBufferTest, CompactsBeforeGrowingAndPreservesDataOnFailure
)
{
    Buffer buffer(16, 20, 4);
    ASSERT_TRUE(buffer.append("abcdefghij", 10));
    ASSERT_TRUE(buffer.retrieve(8));
    ASSERT_TRUE(buffer.append("klmnopqrst", 10));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "ijklmnopqrst"
    );
    EXPECT_FALSE(buffer.append("uvwxyz", 6));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "ijklmnopqrst"
    );
}

TEST(ProtocolBufferTest, DirectWriteRequiresWritableSpace)
{
    Buffer buffer(8, 8, 4);
    std::memcpy(buffer.writePtr(), "abcd", 4);
    ASSERT_TRUE(buffer.hasWritten(4));
    EXPECT_FALSE(buffer.hasWritten(1));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "abcd"
    );
    EXPECT_FALSE(buffer.prepend(nullptr, 1));
    EXPECT_FALSE(buffer.retrieve(5));
}

TEST(PacketHeaderCodecTest, EncodesExactBigEndianHeaderAndDecodesIt)
{
    Buffer buffer;
    ASSERT_TRUE(buffer.append("abc", 3));
    PacketHeader header;
    header.m_length = PacketHeader::kMetadataSize + 3;
    header.m_type = PacketHeader::kEchoRequest;
    header.m_requestId = 0x01020304;
    ASSERT_TRUE(PacketHeaderCodec::encode(buffer, header));

    const std::array<unsigned char, PacketHeader::kHeaderSize>
        expected{65, 82, 69, 78, 0, 0, 0, 10, 0, 1, 1, 2, 3, 4, 1};
    ASSERT_EQ(buffer.readableBytes(), PacketHeader::kHeaderSize + 3);
    EXPECT_EQ(
        std::memcmp(buffer.peek(), expected.data(), expected.size()),
        0
    );
    EXPECT_EQ(
        std::string_view(buffer.peek() + expected.size(), 3),
        "abc"
    );

    PacketHeader decoded;
    ASSERT_EQ(
        PacketHeaderCodec::decode(
            buffer.peek(),
            expected.size(),
            decoded
        ),
        HeaderDecodeStatus::Complete
    );
    EXPECT_EQ(decoded.m_length, header.m_length);
    EXPECT_EQ(decoded.m_type, header.m_type);
    EXPECT_EQ(decoded.m_requestId, header.m_requestId);
    EXPECT_EQ(decoded.m_version, header.m_version);
}

TEST(PacketHeaderCodecTest, RejectsIncompleteAndInvalidLengthsEarly)
{
    const std::array<char, PacketHeader::kHeaderSize>
        valid{65, 82, 69, 78, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 1};
    PacketHeader output;
    output.m_requestId = 42;
    EXPECT_EQ(
        PacketHeaderCodec::decode(valid.data(), 3, output),
        HeaderDecodeStatus::Incomplete
    );
    EXPECT_EQ(
        PacketHeaderCodec::decode(valid.data(), 4, output),
        HeaderDecodeStatus::Incomplete
    );
    EXPECT_EQ(
        PacketHeaderCodec::decode(valid.data(), 14, output),
        HeaderDecodeStatus::Incomplete
    );
    EXPECT_EQ(output.m_requestId, 42U);

    auto invalid = valid;
    invalid[7] = 6;
    EXPECT_EQ(
        PacketHeaderCodec::decode(invalid.data(), 8, output),
        HeaderDecodeStatus::Invalid
    );
    invalid = valid;
    invalid[4] = 1; // 0x01000007 exceeds 4 MiB.
    EXPECT_EQ(
        PacketHeaderCodec::decode(invalid.data(), 8, output),
        HeaderDecodeStatus::Invalid
    );
    EXPECT_EQ(output.m_requestId, 42U);
}

TEST(PacketHeaderCodecTest, RejectsUnknownTypeAndVersion)
{
    std::array<char, PacketHeader::kHeaderSize>
        bytes{65, 82, 69, 78, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 1};
    PacketHeader output;
    bytes[9] = 3;
    EXPECT_EQ(
        PacketHeaderCodec::decode(bytes.data(), bytes.size(), output),
        HeaderDecodeStatus::Invalid
    );
    bytes[9] = 1;
    bytes.back() = 2;
    EXPECT_EQ(
        PacketHeaderCodec::decode(bytes.data(), bytes.size(), output),
        HeaderDecodeStatus::Invalid
    );
}

TEST(
    PacketHeaderCodecTest,
    EncodeRejectsMismatchedLengthWithoutMutation
)
{
    Buffer buffer;
    ASSERT_TRUE(buffer.append("abc", 3));
    PacketHeader header;
    header.m_length = PacketHeader::kMetadataSize + 2;
    header.m_type = PacketHeader::kEchoRequest;
    EXPECT_FALSE(PacketHeaderCodec::encode(buffer, header));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "abc"
    );

    header.m_length = PacketHeader::kMaxPacketSize + 1;
    EXPECT_FALSE(PacketHeaderCodec::encode(buffer, header));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "abc"
    );
}

TEST(PacketHeaderCodecTest, AcceptsMaximumLegalBodyLength)
{
    PacketHeader header;
    header.m_length = PacketHeader::kMaxPacketSize;
    header.m_type = PacketHeader::kEchoResponse;
    header.m_requestId = 0xffffffffU;
    const std::array<char, PacketHeader::kHeaderSize>
        wire{65, 82, 69, 78, 0, 64, 0, 0, 0, 2, -1, -1, -1, -1, 1};
    PacketHeader decoded;
    EXPECT_EQ(
        PacketHeaderCodec::decode(wire.data(), wire.size(), decoded),
        HeaderDecodeStatus::Complete
    );
    EXPECT_EQ(decoded.m_length, header.m_length);
    EXPECT_EQ(decoded.m_requestId, header.m_requestId);
}
TEST(PacketHeaderCodecTest, ParsesFragmentedAndCoalescedFrames)
{
    auto makeFrame = [](std::string_view payload, std::uint32_t id)
    {
        Buffer output;
        EXPECT_TRUE(output.append(payload.data(), payload.size()));
        PacketHeader header;
        header.m_length = PacketHeader::kMetadataSize +
                          payload.size();
        header.m_type = PacketHeader::kEchoRequest;
        header.m_requestId = id;
        EXPECT_TRUE(PacketHeaderCodec::encode(output, header));
        return std::string(output.peek(), output.readableBytes());
    };
    const std::string first = makeFrame("hello", 1);
    const std::string second = makeFrame("", 2);
    const std::string third = makeFrame("world", 3);
    const std::string wire = first + second + third;

    Buffer input;
    std::vector<std::string> payloads;
    std::vector<std::uint32_t> ids;
    auto onFrame =
        [&](const PacketHeader& header, std::string_view payload)
    {
        ids.push_back(header.m_requestId);
        payloads.emplace_back(payload);
        return true;
    };
    const std::array<std::size_t, 6> chunkSizes{1, 2, 8, 5, 4, 100};
    std::size_t offset = 0;
    for (const std::size_t size : chunkSizes)
    {
        const std::size_t count = std::min(
            size,
            wire.size() - offset
        );
        ASSERT_TRUE(input.append(wire.data() + offset, count));
        offset += count;
        EXPECT_EQ(
            PacketHeaderCodec::decodeFrames(input, onFrame),
            FrameDecodeStatus::NeedMore
        );
    }
    EXPECT_EQ(offset, wire.size());
    EXPECT_EQ((std::vector<std::uint32_t>{1, 2, 3}), ids);
    EXPECT_EQ(
        (std::vector<std::string>{"hello", "", "world"}),
        payloads
    );
    EXPECT_EQ(input.readableBytes(), 0);
}

TEST(PacketHeaderCodecTest, InvalidLengthStopsBeforePayloadAllocation)
{
    Buffer input;
    const char oversized[] = {65, 82, 69, 78, 1, 0, 0, 0};
    ASSERT_TRUE(input.append(oversized, sizeof(oversized)));
    int delivered = 0;
    EXPECT_EQ(
        PacketHeaderCodec::decodeFrames(
            input,
            [&](const PacketHeader&, std::string_view)
            {
                ++delivered;
                return true;
            }
        ),
        FrameDecodeStatus::Invalid
    );
    EXPECT_EQ(delivered, 0);
    EXPECT_EQ(input.readableBytes(), sizeof(oversized));
}

TEST(PacketHeaderCodecTest, CallbackCanStopAfterOneFrame)
{
    Buffer first;
    PacketHeader header;
    header.m_length = PacketHeader::kMetadataSize;
    header.m_type = PacketHeader::kEchoRequest;
    ASSERT_TRUE(PacketHeaderCodec::encode(first, header));
    Buffer input;
    ASSERT_TRUE(input.append(first.peek(), first.readableBytes()));
    ASSERT_TRUE(input.append(first.peek(), first.readableBytes()));
    int calls = 0;
    EXPECT_EQ(
        PacketHeaderCodec::decodeFrames(
            input,
            [&](const PacketHeader&, std::string_view)
            {
                ++calls;
                return false;
            }
        ),
        FrameDecodeStatus::Stopped
    );
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(input.readableBytes(), first.readableBytes());
}
TEST(ProtocolBufferTest, GrowsSafelyAfterPrependingHeader)
{
    Buffer buffer(16, 64, 11);
    ASSERT_TRUE(buffer.append("abcde", 5));
    ASSERT_TRUE(buffer.prepend("01234567890", 11));
    ASSERT_EQ(buffer.prependableBytes(), 0);
    ASSERT_TRUE(buffer.append("f", 1));
    EXPECT_EQ(
        std::string_view(buffer.peek(), buffer.readableBytes()),
        "01234567890abcdef"
    );
}

TEST(PacketHeaderCodecTest, OneByteThroughMaximumPayloadBoundaries)
{
    constexpr std::size_t maxPayload = PacketHeader::kMaxPacketSize -
                                       PacketHeader::kMetadataSize;
    for (const std::size_t payloadSize :
         {std::size_t{1}, maxPayload - 1, maxPayload})
    {
        SCOPED_TRACE(payloadSize);
        const std::string payload(payloadSize, 'x');
        Buffer output;
        ASSERT_TRUE(output.append(payload.data(), payload.size()));
        PacketHeader header;
        header.m_length = PacketHeader::kMetadataSize + payloadSize;
        header.m_type = PacketHeader::kEchoRequest;
        header.m_requestId = 0x10203040;
        ASSERT_TRUE(PacketHeaderCodec::encode(output, header));
        ASSERT_EQ(
            output.readableBytes(),
            PacketHeader::kPrefixSize + header.m_length
        );

        Buffer input;
        ASSERT_TRUE(
            input.append(output.peek(), output.readableBytes())
        );
        int deliveries = 0;
        EXPECT_EQ(
            PacketHeaderCodec::decodeFrames(
                input,
                [&](const PacketHeader& decoded,
                    std::string_view bytes)
                {
                    ++deliveries;
                    EXPECT_EQ(decoded.m_length, header.m_length);
                    EXPECT_EQ(
                        decoded.m_requestId,
                        header.m_requestId
                    );
                    EXPECT_EQ(bytes.size(), payload.size());
                    if (bytes.size() == payload.size())
                    {
                        EXPECT_EQ(
                            std::memcmp(
                                bytes.data(),
                                payload.data(),
                                payload.size()
                            ),
                            0
                        );
                    }
                    return true;
                }
            ),
            FrameDecodeStatus::NeedMore
        );
        EXPECT_EQ(deliveries, 1);
        EXPECT_EQ(input.readableBytes(), 0);
    }

    const std::string tooLarge(maxPayload + 1, 'x');
    Buffer output;
    ASSERT_TRUE(output.append(tooLarge.data(), tooLarge.size()));
    PacketHeader header;
    header.m_length = PacketHeader::kMaxPacketSize + 1;
    header.m_type = PacketHeader::kEchoRequest;
    EXPECT_FALSE(PacketHeaderCodec::encode(output, header));
    EXPECT_EQ(output.readableBytes(), tooLarge.size());
}

TEST(PacketHeaderCodecTest, TenThousandFramesAcrossRandomChunks)
{
    constexpr std::size_t frameCount = 10000;
    std::mt19937 random(0xA11CE);
    std::uniform_int_distribution<int> lengthDist(1, 64);
    std::uniform_int_distribution<int> byteDist(0, 255);
    std::uniform_int_distribution<int> chunkDist(1, 31);
    std::vector<std::string> expected;
    expected.reserve(frameCount);
    std::string wire;
    Buffer output;

    for (std::size_t index = 0; index < frameCount; ++index)
    {
        std::string payload(lengthDist(random), '\0');
        for (char& byte : payload)
        {
            byte = static_cast<char>(byteDist(random));
        }
        expected.push_back(payload);
        ASSERT_TRUE(output.append(payload.data(), payload.size()));
        PacketHeader header;
        header.m_length = PacketHeader::kMetadataSize +
                          payload.size();
        header.m_type = index % 2 == 0 ? PacketHeader::kEchoRequest
                                       : PacketHeader::kEchoResponse;
        header.m_requestId = static_cast<std::uint32_t>(index + 1);
        ASSERT_TRUE(PacketHeaderCodec::encode(output, header));
        wire.append(output.peek(), output.readableBytes());
        ASSERT_TRUE(output.retrieve(output.readableBytes()));
    }

    Buffer input;
    std::size_t delivered = 0;
    std::size_t chunks = 0;
    std::size_t offset = 0;
    while (offset < wire.size())
    {
        const std::size_t count = std::min(
            static_cast<std::size_t>(chunkDist(random)),
            wire.size() - offset
        );
        ASSERT_TRUE(input.append(wire.data() + offset, count));
        offset += count;
        ++chunks;
        ASSERT_EQ(
            PacketHeaderCodec::decodeFrames(
                input,
                [&](const PacketHeader& header,
                    std::string_view payload)
                {
                    if (delivered >= expected.size())
                    {
                        ADD_FAILURE() << "extra frame";
                        return false;
                    }
                    EXPECT_EQ(header.m_requestId, delivered + 1);
                    EXPECT_EQ(
                        header.m_type,
                        delivered % 2 == 0
                            ? PacketHeader::kEchoRequest
                            : PacketHeader::kEchoResponse
                    );
                    const std::string& original = expected[delivered];
                    EXPECT_EQ(payload.size(), original.size());
                    if (payload.size() == original.size())
                    {
                        EXPECT_EQ(
                            std::memcmp(
                                payload.data(),
                                original.data(),
                                original.size()
                            ),
                            0
                        );
                    }
                    ++delivered;
                    return true;
                }
            ),
            FrameDecodeStatus::NeedMore
        );
    }
    EXPECT_GE(chunks, frameCount);
    EXPECT_EQ(delivered, frameCount);
    EXPECT_EQ(input.readableBytes(), 0);
}

TEST(
    PacketHeaderCodecTest,
    OversizedAndNegativeLengthBitPatternsAreInvalid
)
{
    for (const std::uint32_t bodyLength :
         {PacketHeader::kMaxPacketSize + 1,
          std::uint32_t{0x80000000U},
          std::uint32_t{0xffffffffU}})
    {
        SCOPED_TRACE(bodyLength);
        const std::uint32_t networkMagic = ::htonl(
            PacketHeader::kMagic
        );
        const std::uint32_t networkLength = ::htonl(bodyLength);
        Buffer input;
        ASSERT_TRUE(
            input.append(&networkMagic, sizeof(networkMagic))
        );
        ASSERT_TRUE(
            input.append(&networkLength, sizeof(networkLength))
        );
        PacketHeader output;
        output.m_requestId = 77;
        EXPECT_EQ(
            PacketHeaderCodec::decode(
                input.peek(),
                input.readableBytes(),
                output
            ),
            HeaderDecodeStatus::Invalid
        );
        int delivered = 0;
        EXPECT_EQ(
            PacketHeaderCodec::decodeFrames(
                input,
                [&](const PacketHeader&, std::string_view)
                {
                    ++delivered;
                    return true;
                }
            ),
            FrameDecodeStatus::Invalid
        );
        EXPECT_EQ(output.m_requestId, 77U);
        EXPECT_EQ(delivered, 0);
        EXPECT_EQ(input.readableBytes(), PacketHeader::kPrefixSize);
    }
}

TEST(
    PacketHeaderCodecTest, EveryTruncatedPrefixOrPayloadNeedsMoreBytes
)
{
    Buffer frame;
    ASSERT_TRUE(frame.append("abc", 3));
    PacketHeader header;
    header.m_length = PacketHeader::kMetadataSize + 3;
    header.m_type = PacketHeader::kEchoRequest;
    ASSERT_TRUE(PacketHeaderCodec::encode(frame, header));

    for (std::size_t prefixSize = 1;
         prefixSize < frame.readableBytes();
         ++prefixSize)
    {
        SCOPED_TRACE(prefixSize);
        Buffer input;
        ASSERT_TRUE(input.append(frame.peek(), prefixSize));
        int delivered = 0;
        EXPECT_EQ(
            PacketHeaderCodec::decodeFrames(
                input,
                [&](const PacketHeader&, std::string_view)
                {
                    ++delivered;
                    return true;
                }
            ),
            FrameDecodeStatus::NeedMore
        );
        EXPECT_EQ(delivered, 0);
        EXPECT_EQ(input.readableBytes(), prefixSize);

        ASSERT_TRUE(input.append(
            frame.peek() + prefixSize,
            frame.readableBytes() - prefixSize
        ));
        EXPECT_EQ(
            PacketHeaderCodec::decodeFrames(
                input,
                [&](const PacketHeader&, std::string_view payload)
                {
                    ++delivered;
                    EXPECT_EQ(payload, "abc");
                    return true;
                }
            ),
            FrameDecodeStatus::NeedMore
        );
        EXPECT_EQ(delivered, 1);
        EXPECT_EQ(input.readableBytes(), 0);
    }
}

TEST(PacketHeaderCodecTest, RejectsBadMagicBeforeLengthAndEncode)
{
    const std::array<char, PacketHeader::kHeaderSize>
        wire{0, 0, 0, 0, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 1};
    PacketHeader decoded;
    decoded.m_requestId = 42;
    EXPECT_EQ(
        PacketHeaderCodec::decode(wire.data(), 3, decoded),
        HeaderDecodeStatus::Incomplete
    );
    EXPECT_EQ(
        PacketHeaderCodec::decode(wire.data(), 4, decoded),
        HeaderDecodeStatus::Invalid
    );
    EXPECT_EQ(decoded.m_requestId, 42U);

    Buffer buffer;
    PacketHeader header;
    header.m_magic = 0;
    header.m_length = PacketHeader::kMetadataSize;
    header.m_type = PacketHeader::kEchoRequest;
    EXPECT_FALSE(PacketHeaderCodec::encode(buffer, header));
    EXPECT_EQ(buffer.readableBytes(), 0);
}