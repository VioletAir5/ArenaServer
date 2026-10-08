#include "net/PreLoginHandshake.h"

#include <span>
#include <utility>

#include "net/PacketBuffer.h"
#include "net/PacketPipeline.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
ConnectionMessageCallback makePreLoginHandshakeHandler()
{
    PacketPipeline pipeline;
    pipeline.addStage(PacketPipeline::decodeEnvelope);
    pipeline.addStage(
        [](PacketContext& context)
        {
            if (context.header.m_type !=
                    protocol::PacketHeader::kEchoRequest ||
                !context.envelope->has_ping())
            {
                return PacketStageResult::Reject;
            }
            arena::protocol::Envelope reply;
            reply.set_request_id(context.envelope->request_id());
            reply.mutable_pong()->set_nonce(
                context.envelope->ping().nonce()
            );
            const auto payload = protocol::EnvelopeCodec::encode(reply
            );
            if (!payload)
            {
                return PacketStageResult::Reject;
            }
            auto frame = protocol::PacketCodec::encode(
                protocol::PacketHeader::kEchoResponse,
                context.header.m_requestId,
                std::as_bytes(std::span(*payload))
            );
            if (!frame)
            {
                return PacketStageResult::Reject;
            }
            context.connection.send(std::as_bytes(
                std::span(frame->peek(), frame->readableBytes())
            ));
            return context.connection.connected()
                       ? PacketStageResult::Handled
                       : PacketStageResult::Reject;
        }
    );
    return makePacketHandler(std::move(pipeline), kMaxPacketSize);
}
} // namespace service::gateway