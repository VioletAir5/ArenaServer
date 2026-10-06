#pragma once
#include "net/ConnectionHandler.h"
#include "net/PacketBuffer.h"

namespace service::gateway
{
// Body limit includes the seven metadata bytes. Each response fits
// within the transport output budget; the protocol's shared ceiling
// remains 4 MiB.
ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize = kMaxPacketSize);
} // namespace service::gateway
