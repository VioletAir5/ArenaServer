#include "net/PacketPipeline.h"

#include <exception>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>

#include "net/PacketBuffer.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
void PacketPipeline::addStage(Stage stage)
{
    if (!stage)
    {
        throw std::invalid_argument("empty packet stage");
    }
    m_stages.push_back(std::move(stage));
}

PacketStageResult PacketPipeline::process(PacketContext& context) const
{
    for (const auto& stage : m_stages)
    {
        const auto result = stage(context);
        if (result != PacketStageResult::Continue)
        {
            return result;
        }
    }
    return PacketStageResult::Continue;
}

PacketStageResult PacketPipeline::decodeEnvelope(PacketContext& context)
{
    arena::protocol::Envelope decoded;
    if (!protocol::EnvelopeCodec::decode(
            std::as_bytes(std::span(context.payload)), decoded))
    {
        return PacketStageResult::Reject;
    }
    context.envelope.emplace(std::move(decoded));
    return PacketStageResult::Continue;
}

ConnectionMessageCallback makePacketHandler(
    PacketPipeline pipeline, std::size_t maxBodySize
)
{
    if (maxBodySize > kMaxPacketSize)
    {
        throw std::invalid_argument(
            "gateway packet body limit exceeds 256 KiB"
        );
    }
    auto codec = std::make_shared<protocol::PacketCodec>(maxBodySize);
    return [codec, pipeline = std::move(pipeline)](
               IConnection& connection,
               std::span<const std::byte> bytes
           )
    {
        try
        {
            const auto status = codec->feed(
                bytes,
                [&](const protocol::PacketHeader& header,
                    std::string_view payload)
                {
                    PacketContext context{connection, header, payload};
                    if (pipeline.process(context) ==
                        PacketStageResult::Reject)
                    {
                        connection.close();
                        return false;
                    }
                    return connection.connected();
                }
            );
            if (status == protocol::FrameDecodeStatus::Invalid)
            {
                std::cerr << "PacketCodec invalid frame id="
                          << connection.id() << '\n';
                connection.close();
            }
        }
        catch (const std::exception& error)
        {
            std::cerr << "PacketPipeline callback failed: "
                      << error.what() << '\n';
            connection.close();
        }
        catch (...)
        {
            connection.close();
        }
    };
}
} // namespace service::gateway