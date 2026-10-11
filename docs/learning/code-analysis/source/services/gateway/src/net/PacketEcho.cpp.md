# services/gateway/src/net/PacketEcho.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；45行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Replies to supported packets with an echo response.
 */
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
~~~
