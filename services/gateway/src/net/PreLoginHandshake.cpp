#include "net/PreLoginHandshake.h"

#include <memory>
#include <span>
#include <stdexcept>
#include <utility>

#include "net/PacketBuffer.h"
#include "net/PacketPipeline.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
PacketHandleResult PingHandler::handle(PacketContext& context)
{
    if (context.header.m_type !=
            protocol::PacketHeader::kEchoRequest ||
        !context.envelope || !context.envelope->has_ping())
    {
        return PacketHandler::handle(context);
    }
    arena::protocol::Envelope reply;
    reply.set_request_id(context.envelope->request_id());
    reply.mutable_pong()->set_nonce(context.envelope->ping().nonce());
    const auto payload = protocol::EnvelopeCodec::encode(reply);
    if (!payload)
    {
        return PacketHandleResult::Reject;
    }
    auto frame = protocol::PacketCodec::encode(
        protocol::PacketHeader::kEchoResponse,
        context.header.m_requestId,
        std::as_bytes(std::span(*payload))
    );
    if (!frame)
    {
        return PacketHandleResult::Reject;
    }
    context.connection.send(std::as_bytes(
        std::span(frame->peek(), frame->readableBytes())
    ));
    return context.connection.connected()
               ? PacketHandleResult::Handled
               : PacketHandleResult::Reject;
}

ConnectionMessageCallback makePreLoginHandshakeHandler(ConnectionContext::Ptr context)
{
    if (!context)
        throw std::invalid_argument("handshake requires connection context");
    auto decoder = std::make_unique<EnvelopeDecodeHandler>();
    auto gate = std::make_unique<AuthenticationGateHandler>(context->session);
    gate->setNext(std::make_unique<PingHandler>());
    decoder->setNext(std::move(gate));
    return makePacketHandler(
        PacketPipeline(std::move(decoder)),
        kMaxPacketSize,
        context->rejections
    );
}
} // namespace service::gateway
