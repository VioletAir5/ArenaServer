/** @file
 * @brief Implements bounded framing independently of message
 * dispatch.
 */
#include "net/FrameProtocol.h"

#include <stdexcept>

#include "net/PacketBuffer.h"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
ConnectionMessageCallback makeFrameProtocolHandler(
    FrameHandler handler,
    std::size_t maxBodySize,
    RejectionCounters::Ptr counters,
    PacketPolicyChain policies
)
{
    if (!handler || !counters || maxBodySize > kMaxPacketSize)
        throw std::invalid_argument(
            "invalid frame protocol configuration"
        );
    policies.seal();
    auto codec = std::make_shared<protocol::PacketCodec>(maxBodySize);
    auto chain = std::make_shared<PacketPolicyChain>(
        std::move(policies)
    );
    return [codec,
            chain,
            handler = std::move(handler),
            counters = std::move(counters)](
               ConnectionChannel& connection,
               std::span<const std::byte> bytes
           )
    {
        if (!connection.acceptingInput())
            return;
        try
        {
            const auto status = codec->feed(
                bytes,
                [&](const auto& header, std::string_view payload)
                {
                    const auto decision = chain->inspect(
                        connection.context(),
                        PacketView{
                            header.m_type,
                            std::as_bytes(std::span(payload))
                        }
                    );
                    if (decision == PacketDecision::Close)
                    {
                        connection.close();
                        return false;
                    }
                    if (decision == PacketDecision::Drop)
                        return connection.acceptingInput();
                    FrameContext frame{connection, header, payload};
                    if (handler(frame) == FrameResult::Reject)
                    {
                        counters->record(frame.rejectionReason);
                        connection.close();
                        return false;
                    }
                    return connection.acceptingInput();
                }
            );
            if (status == protocol::FrameDecodeStatus::Invalid)
            {
                counters->record(RejectionReason::InvalidFrame);
                connection.close();
            }
        }
        catch (...)
        {
            counters->record(RejectionReason::CallbackFailure);
            connection.close();
        }
    };
}
} // namespace service::gateway
