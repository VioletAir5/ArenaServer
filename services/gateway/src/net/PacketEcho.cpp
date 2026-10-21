/** @file
 * @brief Implements the binary echo protocol without business
 * routing.
 */
#include "net/PacketEcho.h"

#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
ConnectionMessageCallback makePacketEchoHandler(
    std::size_t maxBodySize,
    RejectionCounters::Ptr counters,
    PacketPolicyChain policies
)
{
    return makeFrameProtocolHandler(
        [](FrameContext& frame)
        {
            if (frame.header.m_type !=
                protocol::PacketHeader::kEchoRequest)
                return FrameResult::Reject;
            const auto response = protocol::PacketCodec::encode(
                protocol::PacketHeader::kEchoResponse,
                frame.header.m_requestId,
                std::as_bytes(std::span(frame.payload))
            );
            if (!response)
                return FrameResult::Reject;
            frame.connection.send(
                std::as_bytes(
                    std::span(
                        response->peek(),
                        response->readableBytes()
                    )
                )
            );
            return FrameResult::Handled;
        },
        maxBodySize,
        std::move(counters),
        std::move(policies)
    );
}
} // namespace service::gateway
