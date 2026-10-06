#include "protocol/PacketCodec.hpp"

#include <arpa/inet.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace service::protocol
{
namespace
{
std::size_t checkedLimit(std::size_t limit)
{
    if (limit < PacketHeader::kMetadataSize ||
        limit > PacketHeader::kMaxPacketSize)
    {
        throw std::invalid_argument("invalid packet body limit");
    }
    return limit;
}
} // namespace

PacketCodec::PacketCodec(std::size_t limit)
    : m_maxBodySize(checkedLimit(limit))
    , m_input(
          std::min<std::size_t>(
              4096, limit + PacketHeader::kLengthSize
          ),
          limit + PacketHeader::kLengthSize,
          0
      )
{
}

FrameDecodeStatus PacketCodec::feed(
    std::span<const std::byte> bytes,
    const PacketHeaderCodec::FrameCallback& onFrame
)
{
    if (m_feeding)
    {
        throw std::logic_error("reentrant PacketCodec::feed");
    }
    if (m_status != FrameDecodeStatus::NeedMore)
    {
        return m_status;
    }
    m_feeding = true;
    struct Guard
    {
        bool& flag;
        ~Guard()
        {
            flag = false;
        }
    } guard{m_feeding};
    try
    {
        for (;;)
        {
            const auto size = m_input.readableBytes();
            std::size_t target = PacketHeader::kLengthSize;
            if (size >= PacketHeader::kLengthSize)
            {
                PacketHeader header;
                auto status = PacketHeaderCodec::decode(
                    m_input.peek(),
                    size,
                    header
                );
                std::uint32_t networkLength;
                std::memcpy(
                    &networkLength,
                    m_input.peek(),
                    sizeof(networkLength)
                );
                const auto bodyLength = ::ntohl(networkLength);
                if (status == HeaderDecodeStatus::Invalid ||
                    bodyLength > m_maxBodySize)
                {
                    return m_status = FrameDecodeStatus::Invalid;
                }
                target = size < PacketHeader::kHeaderSize
                             ? PacketHeader::kHeaderSize
                             : PacketHeader::kLengthSize + bodyLength;
                if (size == target &&
                    status == HeaderDecodeStatus::Complete)
                {
                    auto result = PacketHeaderCodec::decodeFrames(
                        m_input,
                        onFrame
                    );
                    if (result != FrameDecodeStatus::NeedMore)
                    {
                        return m_status = result;
                    }
                    continue;
                }
            }
            if (bytes.empty())
            {
                return FrameDecodeStatus::NeedMore;
            }
            const auto count = std::min(target - size, bytes.size());
            if (!m_input.append(bytes.data(), count))
            {
                return m_status = FrameDecodeStatus::Invalid;
            }
            bytes = bytes.subspan(count);
        }
    }
    catch (...)
    {
        m_status = FrameDecodeStatus::Invalid;
        throw;
    }
}

std::size_t PacketCodec::pendingBytes() const noexcept
{
    return m_input.readableBytes();
}

std::optional<Buffer> PacketCodec::encode(
    std::uint16_t type,
    std::uint32_t requestId,
    std::span<const std::byte> payload,
    std::uint8_t version
)
{
    if (payload.size() >
        PacketHeader::kMaxPacketSize - PacketHeader::kMetadataSize)
    {
        return std::nullopt;
    }
    PacketHeader header;
    header.m_length = PacketHeader::kMetadataSize + payload.size();
    header.m_type = type;
    header.m_requestId = requestId;
    header.m_version = version;
    const auto wireSize = PacketHeader::kHeaderSize + payload.size();
    Buffer output(std::min<std::size_t>(4096, wireSize), wireSize);
    if (!output.append(payload.data(), payload.size()) ||
        !PacketHeaderCodec::encode(output, header))
    {
        return std::nullopt;
    }
    return output;
}
} // namespace service::protocol
