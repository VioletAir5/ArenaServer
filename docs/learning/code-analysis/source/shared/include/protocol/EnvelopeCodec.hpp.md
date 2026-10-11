# shared/include/protocol/EnvelopeCodec.hpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；32行。

[对应分析](../../../../03-protocol.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Encodes and decodes protocol envelope messages.
 */
#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string>

#include "envelope.pb.h"

namespace service::protocol
{
// Bounds apply to the serialized protobuf payload inside PacketCodec's
// existing frame. A failed decode leaves the output message unchanged.
/** @brief Encodes and decodes protocol envelope messages. */
class EnvelopeCodec
{
public:
    static constexpr std::size_t kMaxBytes = 256 * 1024;
    static constexpr int kMaxDepth = 16;

    static bool decode(
        std::span<const std::byte> bytes,
        arena::protocol::Envelope& output
    );
    static std::optional<std::string> encode(
        const arena::protocol::Envelope& envelope
    );
};
} // namespace service::protocol
~~~
