# shared/include/protocol/PacketHeaderCodec.hpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；55行。

[对应分析](../../../../03-protocol.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Validates and serializes packet headers and frames.
 */
#pragma once

#include <cstddef>
#include <functional>
#include <string_view>

#include "protocol/Buffer.hpp"
#include "protocol/PacketHeader.hpp"

namespace service::protocol
{
/** @brief Classifies a fixed header as incomplete, invalid, or complete. */
enum class HeaderDecodeStatus
{
    Incomplete,
    Invalid,
    Complete
};
/** @brief Describes incremental frame progress or a terminal result. */
enum class FrameDecodeStatus
{
    NeedMore,
    Invalid,
    Stopped
};

/** @brief Validates and serializes packet headers and frames. */
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
~~~
