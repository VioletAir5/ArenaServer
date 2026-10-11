# services/gateway/src/net/PacketBuffer.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；29行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Provides the bounded byte buffer used by gateway transports.
 */
#include "net/PacketBuffer.h"

namespace service::gateway
{
PacketBuffer::PacketBuffer()
    : Buffer(kInitialBufferSize, kMaxBufferSize, 0)
{
}

PacketBuffer::PacketBuffer(
    std::size_t initialSize, std::size_t maxCapacity
)
    : Buffer(initialSize, maxCapacity, 0)
{
}

const char* PacketBuffer::readPtr() const noexcept
{
    return peek();
}

bool PacketBuffer::skip(std::size_t length) noexcept
{
    return retrieve(length);
}
} // namespace service::gateway
~~~
