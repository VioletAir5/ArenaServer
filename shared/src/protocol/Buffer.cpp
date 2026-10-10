/** @file
 * @brief Stores readable bytes and grows within its configured limit.
 */
#include "protocol/Buffer.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace service::protocol
{
Buffer::Buffer(
    std::size_t initialCapacity,
    std::size_t maxCapacity,
    std::size_t prependReserve
)
    : m_maxCapacity(maxCapacity)
    , m_prependReserve(prependReserve)
    , m_storage(initialCapacity)
    , m_readIndex(prependReserve)
    , m_writeIndex(prependReserve)
{
    if (initialCapacity == 0 || initialCapacity < prependReserve ||
        initialCapacity > maxCapacity)
    {
        throw std::invalid_argument("invalid buffer capacity");
    }
}

std::size_t Buffer::prependableBytes() const noexcept
{
    return m_readIndex;
}

std::size_t Buffer::readableBytes() const noexcept
{
    return m_writeIndex - m_readIndex;
}

std::size_t Buffer::writableBytes() const noexcept
{
    return m_storage.size() - m_writeIndex;
}

const char* Buffer::peek() const noexcept
{
    return m_storage.data() + m_readIndex;
}

char* Buffer::writePtr() noexcept
{
    return m_storage.data() + m_writeIndex;
}

bool Buffer::append(const void* data, std::size_t length)
{
    if (length == 0)
    {
        return true;
    }
    if (data == nullptr || !ensureWritable(length))
    {
        return false;
    }
    std::memcpy(writePtr(), data, length);
    m_writeIndex += length;
    return true;
}

bool Buffer::prepend(const void* data, std::size_t length)
{
    if (length == 0)
    {
        return true;
    }
    if (data == nullptr || length > prependableBytes())
    {
        return false;
    }
    m_readIndex -= length;
    std::memcpy(m_storage.data() + m_readIndex, data, length);
    return true;
}

bool Buffer::hasWritten(std::size_t length) noexcept
{
    if (length > writableBytes())
    {
        return false;
    }
    m_writeIndex += length;
    return true;
}

bool Buffer::retrieve(std::size_t length) noexcept
{
    if (length > readableBytes())
    {
        return false;
    }
    m_readIndex += length;
    if (m_readIndex == m_writeIndex)
    {
        m_readIndex = m_prependReserve;
        m_writeIndex = m_prependReserve;
    }
    return true;
}

bool Buffer::ensureWritable(std::size_t length)
{
    if (length <= writableBytes())
    {
        return true;
    }

    const std::size_t readable = readableBytes();
    const std::size_t usableFront = m_readIndex > m_prependReserve
                                        ? m_readIndex -
                                              m_prependReserve
                                        : 0;
    if (length <= usableFront + writableBytes())
    {
        std::memmove(
            m_storage.data() + m_prependReserve,
            peek(),
            readable
        );
        m_readIndex = m_prependReserve;
        m_writeIndex = m_readIndex + readable;
        return true;
    }

    // Calculate after compaction, not from the old write index.
    if (readable > m_maxCapacity - m_prependReserve ||
        length > m_maxCapacity - m_prependReserve - readable)
    {
        return false;
    }
    const std::size_t required = m_prependReserve + readable + length;
    const std::size_t capacity = std::min(
        m_maxCapacity,
        std::max(
            required,
            m_storage.size() > m_maxCapacity / 2
                ? m_maxCapacity
                : m_storage.size() * 2
        )
    );
    std::vector<char> enlarged(capacity);
    std::memcpy(enlarged.data() + m_prependReserve, peek(), readable);
    m_storage.swap(enlarged);
    m_readIndex = m_prependReserve;
    m_writeIndex = m_readIndex + readable;
    return true;
}
} // namespace service::protocol
