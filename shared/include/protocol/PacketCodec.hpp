/** @file
 * @brief Incrementally decodes and encodes framed packets.
 */
#pragma once
#include <optional>
#include <span>

#include "protocol/PacketHeaderCodec.hpp"

namespace service::protocol
{
// One decoder per stream, used serially by its owner. Retains at most
// one incomplete frame; input may contain arbitrarily many coalesced
// frames.
/** @brief Incrementally decodes and encodes framed packets. */
class PacketCodec
{
public:
    /** @brief Creates an incremental decoder with a body-size limit.
     * @param[in] maxBodySize Largest accepted body in bytes.
     */
    explicit PacketCodec(
        std::size_t maxBodySize = PacketHeader::kMaxPacketSize
    );
    /** @brief Copy construction is disabled because decode state is
     * stream-local. */
    PacketCodec(const PacketCodec&) = delete;
    PacketCodec& operator=(const PacketCodec&) = delete;

    // Copies incomplete input; payload views expire at callback
    // return. Invalid and callback-false Stopped are terminal.
    // Remaining bytes in this call are discarded on either terminal
    // result. No reentrant feed. A throwing callback invalidates this
    // decoder and rethrows.
    FrameDecodeStatus feed(
        std::span<const std::byte>,
        const PacketHeaderCodec::FrameCallback&
    );
    /** @brief Returns bytes retained for an incomplete frame. */
    std::size_t pendingBytes() const noexcept;

    /** @brief Encodes a complete packet when its fields satisfy
     * protocol limits.
     * @param[in] type Packet message type.
     * @param[in] requestId Request correlation identifier.
     * @param[in] payload Body bytes to copy into the frame.
     * @param[in] version Wire protocol version.
     * @return The encoded frame, or no value for invalid input.
     */
    static std::optional<Buffer> encode(
        std::uint16_t type,
        std::uint32_t requestId,
        std::span<const std::byte> payload,
        std::uint8_t version = PacketHeader::kCurrentVersion
    );

private:
    /// Largest accepted packet body in bytes.
    const std::size_t m_maxBodySize;
    /// Incomplete frame bytes retained across feed calls.
    Buffer m_input;
    /// Current incremental decode result.
    FrameDecodeStatus m_status{FrameDecodeStatus::NeedMore};
    /// Guards against reentrant decoder use.
    bool m_feeding{false};
};
} // namespace service::protocol
