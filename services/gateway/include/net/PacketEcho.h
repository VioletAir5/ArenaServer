/** @file
 * @brief Builds the binary echo protocol using bounded framing.
 */
#pragma once

#include "net/FrameProtocol.h"
#include "net/PacketBuffer.h"

namespace service::gateway
{
ConnectionMessageCallback makePacketEchoHandler(
    std::size_t maxBodySize = kMaxPacketSize,
    RejectionCounters::Ptr counters = defaultRejectionCounters(),
    PacketPolicyChain policies = {}
);
} // namespace service::gateway
