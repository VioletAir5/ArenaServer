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
