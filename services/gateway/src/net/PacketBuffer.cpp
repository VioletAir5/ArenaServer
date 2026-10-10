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
