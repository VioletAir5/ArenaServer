#include "net/PacketEcho.h"

#include <memory>
#include <span>

#include "net/PacketPipeline.h"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
PacketHandleResult EchoHandler::handle(PacketContext& context)
{
    if (context.header.m_type != protocol::PacketHeader::kEchoRequest)
    {
        return PacketHandler::handle(context);
    }
    auto response = protocol::PacketCodec::encode(
        protocol::PacketHeader::kEchoResponse,
        context.header.m_requestId,
        std::as_bytes(std::span(context.payload))
    );
    if (!response)
    {
        return PacketHandleResult::Reject;
    }
    context.connection.send(std::as_bytes(
        std::span(response->peek(), response->readableBytes())
    ));
    return context.connection.connected()
               ? PacketHandleResult::Handled
               : PacketHandleResult::Reject;
}

ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize)
{
    return makePacketHandler(
        PacketPipeline(std::make_unique<EchoHandler>()),
        maxBodySize
    );
}
} // namespace service::gateway
