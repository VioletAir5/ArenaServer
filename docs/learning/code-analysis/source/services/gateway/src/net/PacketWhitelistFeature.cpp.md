# services/gateway/src/net/PacketWhitelistFeature.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；37行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Rejects packet types outside the configured allowlist.
 */
#include "net/PacketWhitelistFeature.h"

#include <algorithm>
#include <iostream>
namespace service::gateway
{
PacketWhitelistFeature::PacketWhitelistFeature(
    std::vector<std::uint16_t> allowed
)
    : m_allowed(std::move(allowed))
{
    std::sort(m_allowed.begin(), m_allowed.end());
    m_allowed.erase(
        std::unique(m_allowed.begin(), m_allowed.end()),
        m_allowed.end()
    );
}
FeatureResult PacketWhitelistFeature::onPacket(
    IConnectionControl& control, PacketView packet
)
{
    if (std::binary_search(
            m_allowed.begin(),
            m_allowed.end(),
            packet.type
        ))
        return FeatureResult::Continue;
    control.context().rejections->record(RejectionReason::PacketTypeNotAllowed);
    std::cerr << "connection packet type rejected id="
              << control.context().connectionId
              << " type=" << packet.type << '\n';
    return FeatureResult::Close;
}
} // namespace service::gateway
~~~
