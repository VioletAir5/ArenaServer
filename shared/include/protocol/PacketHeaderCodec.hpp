#pragma once

#include <cstddef>
#include <functional>
#include <string_view>

#include "protocol/Buffer.hpp"
#include "protocol/PacketHeader.hpp"

namespace service::protocol
{
enum class HeaderDecodeStatus
{
    Incomplete,
    Invalid,
    Complete
};
enum class FrameDecodeStatus
{
    NeedMore,
    Invalid,
    Stopped
};

class PacketHeaderCodec
{
public:
    using FrameCallback = std::function<
        bool(const PacketHeader&, std::string_view)>;
    // Prepend the header to an existing payload. The length must
    // equal the payload size plus kMetadataSize. Failure leaves the
    // buffer unchanged.
    static bool encode(Buffer& buffer, const PacketHeader& header);

    // Rejects an invalid magic after four bytes and an invalid body
    // length after the eight-byte prefix.
    // The output is unchanged until a complete valid header is
    // available.
    static HeaderDecodeStatus decode(
        const char* data, std::size_t len, PacketHeader& header
    ) noexcept;

    // Parse every complete frame. A false callback return stops after
    // the delivered frame. The callback must not mutate or destroy
    // input. Invalid means the caller must close the connection.
    static FrameDecodeStatus
    decodeFrames(Buffer& input, const FrameCallback& onFrame);
};
} // namespace service::protocol
