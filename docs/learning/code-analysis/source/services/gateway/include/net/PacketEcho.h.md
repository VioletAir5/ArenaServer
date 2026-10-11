# services/gateway/include/net/PacketEcho.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；24行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Replies to supported packets with an echo response.
 */
#pragma once

#include "net/PacketBuffer.h"
#include "net/PacketPipeline.h"

namespace service::gateway
{
// Handles binary echo requests; forwards other packet types.
/** @brief Represents echo handler. */
class EchoHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override;
};

// Body limit includes the seven metadata bytes. Each response fits
// within the transport output budget; the shared ceiling remains 4
// MiB.
ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize = kMaxPacketSize);
} // namespace service::gateway
~~~
