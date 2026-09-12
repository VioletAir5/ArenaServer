#pragma once

#include<cstdint>
#include<cstddef>
#include<string>
#include<vector>


namespace service
{
static constexpr size_t kInitialBufferSize = 4 * 1024;       // 4 KB
static constexpr size_t kMaxBufferSize     = 1024 * 1024;    // 1 MB
static constexpr size_t kMaxPacketSize     = 256 * 1024;     // 256 KB

class PacketBuffer {
public:

    PacketBuffer();
    PacketBuffer(size_t, size_t);
    ~PacketBuffer() = default;

    PacketBuffer(const PacketBuffer&) = delete;
    PacketBuffer& operator=(const PacketBuffer&) = delete;

    PacketBuffer(PacketBuffer&&) = default;
    PacketBuffer& operator=(PacketBuffer&&) = default;

    size_t readableBytes() const noexcept;
    size_t writableBytes() const noexcept;

    const char* readPtr() const noexcept;
    char* writePtr() noexcept;

    bool append(const void* data, size_t len);
    bool skip(size_t len);

private:
    bool ensureWritableBytes(size_t len);

private:
    size_t _max_capacity{};
    std::vector<char> _data;
    size_t _readPos{};
    size_t _writePos{};
};
} // namespace service

