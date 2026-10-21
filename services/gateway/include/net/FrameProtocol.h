/** @file
 * @brief Adapts transport byte chunks to complete frames and packet
 * policies.
 */
#pragma once

#include <string_view>

#include "net/ConnectionHandler.h"
#include "net/PacketPolicy.h"
#include "protocol/PacketHeader.hpp"

namespace service::gateway
{
/** @brief Borrows one frame only for the duration of its callback. */
struct FrameContext
{
    /// Explicit protocol channel, never a concrete socket adapter.
    ConnectionChannel& connection;
    /// Borrowed parsed wire metadata.
    const protocol::PacketHeader& header;
    /// Borrowed frame body, invalid after the callback returns.
    std::string_view payload;
    /// Counter recorded when the callback rejects this frame.
    RejectionReason rejectionReason{RejectionReason::HandlerRejected};
};

enum class FrameResult
{
    Handled,
    Reject
};

using FrameHandler = std::function<FrameResult(FrameContext&)>;

/** @brief Creates one bounded frame decoder and policy chain per
 * connection.
 * @param[in] policies Fresh policies, configured before construction.
 */
ConnectionMessageCallback makeFrameProtocolHandler(
    FrameHandler handler,
    std::size_t maxBodySize,
    RejectionCounters::Ptr counters = defaultRejectionCounters(),
    PacketPolicyChain policies = {}
);
} // namespace service::gateway
