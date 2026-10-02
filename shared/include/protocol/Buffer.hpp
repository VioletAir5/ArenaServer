#pragma once

#include <cstddef>
#include <vector>

#include "protocol/PacketHeader.hpp"

namespace service::protocol
{
// A bounded byte buffer with reserved space for prepending one wire
// header.
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
    std::size_t readableBytes() const noexcept;
    std::size_t writableBytes() const noexcept;
    const char* peek() const noexcept;
    char* writePtr() noexcept;

    bool append(const void* data, std::size_t length);
    bool prepend(const void* data, std::size_t length);
    bool hasWritten(std::size_t length) noexcept;
    bool retrieve(std::size_t length) noexcept;

private:
    bool ensureWritable(std::size_t length);

    std::size_t m_maxCapacity;
    std::size_t m_prependReserve;
    std::vector<char> m_storage;
    std::size_t m_readIndex;
    std::size_t m_writeIndex;
};
} // namespace service::protocol
