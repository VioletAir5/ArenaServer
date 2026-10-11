# services/gateway/src/net/PreLoginHandshake.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；104行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Handles the pre-login ping exchange.
 */
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

std::shared_ptr<const MessageRegistry> defaultMessageRegistry()
{
    static const auto registry = []
    {
        auto routes = std::make_shared<MessageRegistry>();
        routes->registerHandler(
            arena::protocol::MSG_PING,
            [](auto request, auto complete)
            {
                arena::protocol::Envelope reply;
                reply.mutable_pong()->set_nonce(
                    request->envelope.ping().nonce()
                );
                complete(std::move(reply));
            },
            MessageRouteOptions{false}
        );
        routes->registerHandler(
            arena::protocol::MSG_LOGIN_REQUEST,
            [](auto, auto complete) {
                complete(
                    std::unexpected(arena::protocol::ERR_UNAVAILABLE)
                );
            },
            MessageRouteOptions{false}
        );
        routes->seal();
        return std::shared_ptr<const MessageRegistry>(std::move(routes
        ));
    }();
    return registry;
}

ConnectionMessageCallback makePreLoginHandshakeHandler(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry
)
{
    if (!context)
        throw std::invalid_argument(
            "handshake requires connection context"
        );
    auto decoder = std::make_unique<EnvelopeDecodeHandler>();
    decoder->setNext(std::make_unique<MessageRouter>(
        context,
        registry ? std::move(registry) : defaultMessageRegistry()
    ));
    return makePacketHandler(
        PacketPipeline(std::move(decoder)),
        kMaxPacketSize,
        context->rejections
    );
}
} // namespace service::gateway
~~~
