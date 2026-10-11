# services/gateway/include/net/PacketWhitelistFeature.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；23行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Rejects packet types outside the configured allowlist.
 */
#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// Immutable outer PacketHeader::type allowlist, before payload
// decoding. Empty list denies every packet; admission/peer IP policy
// is separate.
/** @brief Rejects packet types outside the configured allowlist. */
class PacketWhitelistFeature final : public IConnectionFeature
{
public:
    explicit PacketWhitelistFeature(std::vector<std::uint16_t> allowed
    );
    FeatureResult onPacket(IConnectionControl&, PacketView) override;

private:
    /// Stores the allowed.
    std::vector<std::uint16_t> m_allowed;
};
} // namespace service::gateway
~~~
