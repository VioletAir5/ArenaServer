# services/gateway/src/net/PacketPipeline.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；145行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Runs packet handlers in order until one stops processing.
 */
#include "net/PacketPipeline.h"

#include <exception>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>

#include "net/ConnectionFeature.h"
#include "net/PacketBuffer.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
void PacketHandler::setNext(std::unique_ptr<PacketHandler> next)
{
    m_next = std::move(next);
}

PacketHandleResult PacketHandler::handle(PacketContext& context)
{
    return m_next ? m_next->handle(context)
                  : PacketHandleResult::Reject;
}

PacketPipeline::PacketPipeline(std::unique_ptr<PacketHandler> first)
    : m_first(std::move(first))
{
    if (!m_first)
    {
        throw std::invalid_argument("empty packet handler chain");
    }
}

PacketHandleResult PacketPipeline::process(PacketContext& context)
{
    return m_first->handle(context);
}

PacketHandleResult
EnvelopeDecodeHandler::handle(PacketContext& context)
{
    arena::protocol::Envelope decoded;
    if (!protocol::EnvelopeCodec::decode(
            std::as_bytes(std::span(context.payload)),
            decoded
        ))
    {
        context.rejectionReason = RejectionReason::InvalidEnvelope;
        return PacketHandleResult::Reject;
    }
    context.envelope.emplace(std::move(decoded));
    return PacketHandler::handle(context);
}

ConnectionMessageCallback
makePacketHandler(
    PacketPipeline pipeline,
    std::size_t maxBodySize,
    RejectionCounters::Ptr counters
)
{
    if (!counters)
        throw std::invalid_argument("packet handler requires rejection counters");
    if (maxBodySize > kMaxPacketSize)
    {
        throw std::invalid_argument(
            "gateway packet body limit exceeds 256 KiB"
        );
    }
    auto codec = std::make_shared<protocol::PacketCodec>(maxBodySize);
    auto chain = std::make_shared<PacketPipeline>(std::move(pipeline)
    );
    return [codec, chain = std::move(chain), counters = std::move(counters)](
               IConnection& connection,
               std::span<const std::byte> bytes
           )
    {
        if (!connection.connected())
            return;
        try
        {
            const auto status = codec->feed(
                bytes,
                [&](const protocol::PacketHeader& header,
                    std::string_view payload)
                {
                    if (auto* features = dynamic_cast<
                            PacketFeatureHost*>(&connection))
                    {
                        const auto decision = features->inspectPacket(
                            PacketView{
                                header.m_type,
                                std::as_bytes(std::span(payload))}
                        );
                        if (decision == FeatureResult::Close)
                        {
                            connection.close();
                            return false;
                        }
                        if (decision == FeatureResult::Drop)
                            return connection.connected();
                    }
                    PacketContext context{
                        connection,
                        header,
                        payload};
                    if (chain->process(context) ==
                        PacketHandleResult::Reject)
                    {
                        counters->record(context.rejectionReason);
                        connection.close();
                        return false;
                    }
                    return connection.connected();
                }
            );
            if (status == protocol::FrameDecodeStatus::Invalid)
            {
                counters->record(RejectionReason::InvalidFrame);
                std::cerr << "PacketCodec invalid frame id="
                          << connection.id() << '\n';
                connection.close();
            }
        }
        catch (const std::exception& error)
        {
            counters->record(RejectionReason::CallbackFailure);
            std::cerr << "PacketPipeline callback failed: "
                      << error.what() << '\n';
            connection.close();
        }
        catch (...)
        {
            counters->record(RejectionReason::CallbackFailure);
            connection.close();
        }
    };
}
} // namespace service::gateway
~~~
