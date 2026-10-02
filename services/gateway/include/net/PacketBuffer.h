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
class PacketBuffer final : public service::protocol::Buffer
{
public:
    PacketBuffer();
    PacketBuffer(std::size_t initialSize, std::size_t maxCapacity);

    PacketBuffer(const PacketBuffer&) = delete;
    PacketBuffer& operator=(const PacketBuffer&) = delete;
    PacketBuffer(PacketBuffer&&) = default;
    PacketBuffer& operator=(PacketBuffer&&) = default;

    const char* readPtr() const noexcept;
    bool skip(std::size_t length) noexcept;
};
} // namespace service::gateway
