#pragma once

#include <cstddef>
#include <cstdint>

namespace service::protocol
{
// Wire format: body length (u32), type (u16), request id (u32),
// version (u8), then payload. All multibyte integers are unsigned and
// big-endian. The length counts the seven metadata bytes after the
// length field plus payload.
struct PacketHeader
{
    static constexpr std::size_t kLengthSize = sizeof(std::uint32_t);
    static constexpr std::size_t kTypeSize = sizeof(std::uint16_t);
    static constexpr std::size_t kRequestIdSize = sizeof(std::uint32_t
    );
    static constexpr std::size_t kVersionSize = sizeof(std::uint8_t);
    static constexpr std::size_t kMetadataSize = kTypeSize +
                                                 kRequestIdSize +
                                                 kVersionSize;
    static constexpr std::size_t kHeaderSize = kLengthSize +
                                               kMetadataSize;
    static constexpr std::uint32_t kMaxPacketSize = 4 * 1024 * 1024;
    static constexpr std::uint8_t kCurrentVersion = 1;
    static constexpr std::uint16_t kEchoRequest = 1;
    static constexpr std::uint16_t kEchoResponse = 2;

    std::uint32_t m_length{0};
    std::uint32_t m_requestId{0};
    std::uint16_t m_type{0};
    std::uint8_t m_version{kCurrentVersion};
};
} // namespace service::protocol
