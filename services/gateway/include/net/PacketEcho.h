/** @file
 * @brief Replies to supported packets with an echo response.
 */
#pragma once

#include "net/PacketBuffer.h"
#include "net/PacketPipeline.h"

namespace service::gateway
{
// Handles binary echo requests; forwards other packet types.
/** @brief Represents echo handler. */
class EchoHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override;
};

// Body limit includes the seven metadata bytes. Each response fits
// within the transport output budget; the shared ceiling remains 4
// MiB.
ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize = kMaxPacketSize);
} // namespace service::gateway
