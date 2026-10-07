#include "net/PacketEcho.h"

#include <span>
#include <utility>

#include "net/PacketPipeline.h"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize)
{
    PacketPipeline pipeline;
    pipeline.addStage([](PacketContext& context)
    {
        return context.header.m_type ==
                       protocol::PacketHeader::kEchoRequest
                   ? PacketStageResult::Continue
                   : PacketStageResult::Reject;
    });
    pipeline.addStage([](PacketContext& context)
    {
        auto response = protocol::PacketCodec::encode(
            protocol::PacketHeader::kEchoResponse,
            context.header.m_requestId,
            std::as_bytes(std::span(context.payload))
        );
        if (!response)
        {
            return PacketStageResult::Reject;
        }
        context.connection.send(std::as_bytes(std::span(
            response->peek(), response->readableBytes()
        )));
        return context.connection.connected()
                   ? PacketStageResult::Handled
                   : PacketStageResult::Reject;
    });
    return makePacketHandler(std::move(pipeline), maxBodySize);
}
} // namespace service::gateway
