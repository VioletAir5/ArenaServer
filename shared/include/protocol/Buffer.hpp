/** @file
 * @brief Stores readable bytes and grows within its configured limit.
 */
#pragma once

#include <cstddef>
#include <vector>

#include "protocol/PacketHeader.hpp"

namespace service::protocol
{
// A bounded byte buffer with reserved space for prepending one wire
// header.
/** @brief Stores readable bytes and grows within its configured limit. */
class Buffer
{
public:
    explicit Buffer(
        std::size_t initialCapacity = 4096,
        std::size_t maxCapacity = PacketHeader::kMaxPacketSize +
                                  PacketHeader::kHeaderSize + 8192,
        std::size_t prependReserve = PacketHeader::kHeaderSize
    );

    std::size_t prependableBytes() const noexcept;
    /** @brief Returns the count of readable bytes. */
    std::size_t readableBytes() const noexcept;
    std::size_t writableBytes() const noexcept;
    const char* peek() const noexcept;
    /** @brief Returns the start of writable storage. */
    char* writePtr() noexcept;

    bool append(const void* data, std::size_t length);
    bool prepend(const void* data, std::size_t length);
    /** @brief Advances the write index after bytes were filled. */
    bool hasWritten(std::size_t length) noexcept;
    bool retrieve(std::size_t length) noexcept;

private:
    bool ensureWritable(std::size_t length);

    /// Stores the max capacity.
    std::size_t m_maxCapacity;
    /// Stores the prepend reserve.
    std::size_t m_prependReserve;
    /// Stores the storage.
    std::vector<char> m_storage;
    /// Stores the read index.
    std::size_t m_readIndex;
    /// Stores the write index.
    std::size_t m_writeIndex;
};
} // namespace service::protocol
