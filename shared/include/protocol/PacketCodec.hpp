#pragma once
#include <optional>
#include <span>

#include "protocol/PacketHeaderCodec.hpp"

namespace service::protocol
{
// One decoder per stream, used serially by its owner. Retains at most
// one incomplete frame; input may contain arbitrarily many coalesced
// frames.
class PacketCodec
{
public:
    explicit PacketCodec(
        std::size_t maxBodySize = PacketHeader::kMaxPacketSize
    );
    PacketCodec(const PacketCodec&) = delete;
    PacketCodec& operator=(const PacketCodec&) = delete;

    // Copies incomplete input; payload views expire at callback
    // return. Invalid and callback-false Stopped are terminal.
    // Remaining bytes in this call are discarded on either terminal
    // result. No reentrant feed. A throwing callback invalidates this
    // decoder and rethrows.
    FrameDecodeStatus
    feed(std::span<const std::byte>, const PacketHeaderCodec::FrameCallback&);
    std::size_t pendingBytes() const noexcept;

    static std::optional<Buffer> encode(
        std::uint16_t type,
        std::uint32_t requestId,
        std::span<const std::byte> payload,
        std::uint8_t version = PacketHeader::kCurrentVersion
    );

private:
    const std::size_t m_maxBodySize;
    Buffer m_input;
    FrameDecodeStatus m_status{FrameDecodeStatus::NeedMore};
    bool m_feeding{false};
};
} // namespace service::protocol
