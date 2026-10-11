# services/gateway/include/net/PacketBuffer.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；38行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Provides the bounded byte buffer used by gateway transports.
 */
#pragma once

#include <cstddef>

#include "protocol/Buffer.hpp"

namespace service::gateway
{
static constexpr std::size_t kInitialBufferSize = 4 * 1024; // 4 KB
static constexpr std::size_t kMaxBufferSize = 1024 * 1024;  // 1 MB
static constexpr std::size_t kMaxPacketSize = 256 * 1024;   // 256 KB

// Gateway capacity policy and legacy names over the shared buffer
// storage.
/** @brief Provides the bounded byte buffer used by gateway transports. */
class PacketBuffer final : public service::protocol::Buffer
{
public:
    /** @brief Constructs the packet buffer. */
    PacketBuffer();
    /** @brief Constructs the packet buffer. */
    PacketBuffer(std::size_t initialSize, std::size_t maxCapacity);

    /** @brief Copy construction is disabled. */
    PacketBuffer(const PacketBuffer&) = delete;
    PacketBuffer& operator=(const PacketBuffer&) = delete;
    /** @brief Constructs the packet buffer. */
    PacketBuffer(PacketBuffer&&) = default;
    PacketBuffer& operator=(PacketBuffer&&) = default;

    /** @brief Returns the readable byte pointer. */
    const char* readPtr() const noexcept;
    bool skip(std::size_t length) noexcept;
};
} // namespace service::gateway
~~~
