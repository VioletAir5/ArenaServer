#include "protocol/PacketHeaderCodec.hpp"

#include <arpa/inet.h>

#include <array>
#include <cstdint>
#include <cstring>

namespace service::protocol
{
namespace
{
bool valid(const PacketHeader& header) noexcept
{
    return header.m_magic == PacketHeader::kMagic &&
           header.m_length >= PacketHeader::kMetadataSize &&
           header.m_length <= PacketHeader::kMaxPacketSize &&
           header.m_version == PacketHeader::kCurrentVersion &&
           (header.m_type == PacketHeader::kEchoRequest ||
            header.m_type == PacketHeader::kEchoResponse);
}
} // namespace

bool PacketHeaderCodec::encode(
    Buffer& buffer, const PacketHeader& header
)
{
    if (!valid(header) ||
        header.m_length - PacketHeader::kMetadataSize !=
            buffer.readableBytes() ||
        buffer.prependableBytes() < PacketHeader::kHeaderSize)
    {
        return false;
    }

    std::array<char, PacketHeader::kHeaderSize> bytes{};
    const std::uint32_t networkLength = ::htonl(header.m_length);
    const std::uint16_t networkType = ::htons(header.m_type);
    const std::uint32_t networkRequestId = ::htonl(header.m_requestId
    );
    const std::uint32_t networkMagic = ::htonl(header.m_magic);

    std::memcpy(bytes.data(), &networkMagic, sizeof(networkMagic));
    std::memcpy(
        bytes.data() + PacketHeader::kMagicSize,
        &networkLength,
        sizeof(networkLength)
    );
    std::memcpy(
        bytes.data() + PacketHeader::kMagicSize +
            PacketHeader::kLengthSize,
        &networkType,
        sizeof(networkType)
    );
    std::memcpy(
        bytes.data() + PacketHeader::kMagicSize +
            PacketHeader::kLengthSize + PacketHeader::kTypeSize,
        &networkRequestId,
        sizeof(networkRequestId)
    );
    bytes.back() = static_cast<char>(header.m_version);
    return buffer.prepend(bytes.data(), bytes.size());
}

HeaderDecodeStatus PacketHeaderCodec::decode(
    const char* data, std::size_t len, PacketHeader& header
) noexcept
{
    if (len < PacketHeader::kMagicSize)
    {
        return HeaderDecodeStatus::Incomplete;
    }
    if (data == nullptr)
    {
        return HeaderDecodeStatus::Invalid;
    }

    std::uint32_t networkMagic = 0;
    std::memcpy(&networkMagic, data, sizeof(networkMagic));

    PacketHeader candidate;
    candidate.m_magic = ::ntohl(networkMagic);
    if (candidate.m_magic != PacketHeader::kMagic)
    {
        return HeaderDecodeStatus::Invalid;
    }

    if (len < PacketHeader::kPrefixSize)
    {
        return HeaderDecodeStatus::Incomplete;
    }

    std::uint32_t networkLength = 0;
    std::memcpy(
        &networkLength,
        data + PacketHeader::kMagicSize,
        sizeof(networkLength)
    );

    candidate.m_length = ::ntohl(networkLength);
    if (candidate.m_length < PacketHeader::kMetadataSize ||
        candidate.m_length > PacketHeader::kMaxPacketSize)
    {
        return HeaderDecodeStatus::Invalid;
    }
    if (len < PacketHeader::kHeaderSize)
    {
        return HeaderDecodeStatus::Incomplete;
    }

    std::uint16_t networkType = 0;
    std::uint32_t networkRequestId = 0;
    std::memcpy(
        &networkType,
        data + PacketHeader::kMagicSize + PacketHeader::kLengthSize,
        sizeof(networkType)
    );
    std::memcpy(
        &networkRequestId,
        data + PacketHeader::kMagicSize + PacketHeader::kLengthSize +
            PacketHeader::kTypeSize,
        sizeof(networkRequestId)
    );
    candidate.m_type = ::ntohs(networkType);
    candidate.m_requestId = ::ntohl(networkRequestId);
    candidate.m_version = static_cast<std::uint8_t>(
        data[PacketHeader::kHeaderSize - 1]
    );
    if (!valid(candidate))
    {
        return HeaderDecodeStatus::Invalid;
    }
    header = candidate;
    return HeaderDecodeStatus::Complete;
}

FrameDecodeStatus PacketHeaderCodec::decodeFrames(
    Buffer& input, const FrameCallback& onFrame
)
{
    while (true)
    {
        PacketHeader header;
        const HeaderDecodeStatus status = decode(
            input.peek(),
            input.readableBytes(),
            header
        );
        if (status == HeaderDecodeStatus::Incomplete)
        {
            return FrameDecodeStatus::NeedMore;
        }
        if (status == HeaderDecodeStatus::Invalid)
        {
            return FrameDecodeStatus::Invalid;
        }

        const std::size_t frameSize = PacketHeader::kPrefixSize +
                                      header.m_length;
        if (input.readableBytes() < frameSize)
        {
            return FrameDecodeStatus::NeedMore;
        }

        const std::string_view payload(
            input.peek() + PacketHeader::kHeaderSize,
            header.m_length - PacketHeader::kMetadataSize
        );
        const bool keepGoing = onFrame(header, payload);
        input.retrieve(frameSize);
        if (!keepGoing)
        {
            return FrameDecodeStatus::Stopped;
        }
    }
}
} // namespace service::protocol
