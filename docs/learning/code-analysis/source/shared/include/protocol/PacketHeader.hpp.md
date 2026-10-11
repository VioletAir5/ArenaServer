# shared/include/protocol/PacketHeader.hpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；49行。

[对应分析](../../../../03-protocol.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Defines the fixed wire header and protocol limits.
 */
#pragma once

#include <cstddef>
#include <cstdint>

namespace service::protocol
{
// Wire format: magic (u32), body length (u32), type (u16), request id (u32),
// version (u8), then payload. All multibyte integers are unsigned and
// big-endian. The length counts the seven metadata bytes after the
// length field plus payload; magic is outside the length count.
/** @brief Defines the fixed wire header and protocol limits. */
struct PacketHeader
{
    static constexpr std::size_t kLengthSize = sizeof(std::uint32_t);
    static constexpr std::size_t kTypeSize = sizeof(std::uint16_t);
    static constexpr std::size_t kRequestIdSize = sizeof(std::uint32_t
    );
    static constexpr std::size_t kVersionSize = sizeof(std::uint8_t);
    static constexpr std::size_t kMagicSize = sizeof(std::uint32_t);
    static constexpr std::size_t kPrefixSize = kMagicSize + kLengthSize;
    static constexpr std::size_t kMetadataSize = kTypeSize +
                                                 kRequestIdSize +
                                                 kVersionSize;

    static constexpr std::size_t kHeaderSize = kPrefixSize +
                                               kMetadataSize;
    static constexpr std::uint32_t kMaxPacketSize = 4 * 1024 * 1024;
    static constexpr std::uint8_t kCurrentVersion = 1;
    static constexpr std::uint16_t kEchoRequest = 1;
    static constexpr std::uint16_t kEchoResponse = 2;

    static constexpr std::uint32_t kMagic = 0x4152454E;

    /// Wire-format magic identifying Arena packets.
    std::uint32_t m_magic{kMagic};
    /// Metadata and payload length in network byte order on the wire.
    std::uint32_t m_length{0};
    /// Identifier used to correlate requests and responses.
    std::uint32_t m_requestId{0};
    /// Packet message type.
    std::uint16_t m_type{0};
    /// Protocol version carried by the frame.
    std::uint8_t m_version{kCurrentVersion};
};
} // namespace service::protocol
~~~
