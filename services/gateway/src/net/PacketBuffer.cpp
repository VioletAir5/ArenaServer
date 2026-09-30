
#include "net/PacketBuffer.h"

#include <stdexcept>

#include "cstring"

namespace service
{
PacketBuffer::PacketBuffer()
    : _max_capacity(kMaxBufferSize)
    , _data(kInitialBufferSize)
{
}

PacketBuffer::PacketBuffer(size_t initSize, size_t capacity)
    : _max_capacity(capacity)
    , _data(initSize)
{
    if (initSize > capacity)
    {
        throw std::invalid_argument("initial size exceeds max capacity");
    }
}

size_t PacketBuffer::readableBytes() const noexcept
{
    return _writePos - _readPos;
}

size_t PacketBuffer::writableBytes() const noexcept
{
    return _data.size() - _writePos;
}

const char* PacketBuffer::readPtr() const noexcept
{
    return reinterpret_cast<const char*>(_data.data() + _readPos);
}

char* PacketBuffer::writePtr() noexcept
{
    return reinterpret_cast<char*>(_data.data() + _writePos);
}

bool PacketBuffer::append(const void* data, size_t len)
{
    if (len == 0)
    {
        return true;
    }

    if (data == nullptr)
    {
        return false;
    }

    if (!ensureWritableBytes(len))
    {
        return false;
    }

    std::memcpy(writePtr(), data, len);
    _writePos += len;
    return true;
}

bool PacketBuffer::skip(size_t len)
{
    if (len > readableBytes())
    {
        return false;
    }
    _readPos += len;
    if (_readPos == _writePos)
    {
        _readPos = 0;
        _writePos = 0;
    }

    return true;
}

bool PacketBuffer::ensureWritableBytes(size_t len)
{
    if (writableBytes() >= len)
    {
        return true;
    }

    const size_t readable = readableBytes();

    if (_readPos + writableBytes() >= len)
    {
        std::memmove(_data.data(), _data.data() + _readPos, readable);

        _readPos = 0;
        _writePos = readable;
        return true;
    }

    // need resize
    size_t required = _writePos + len;
    if (required > _max_capacity)
        return false;

    size_t newSize;
    if (_data.size() >= _max_capacity / 2)
    {
        newSize = _max_capacity;
    }
    else
    {
        newSize = std::max(_data.size() * 2, required);
    }
    _data.resize(newSize);

    return true;
}
} // namespace service
