# services/gateway/include/net/TokenBucketFeature.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；36行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Limits packet admission with a token bucket.
 */
#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// One token per complete packet. Excess packets are closed or
// dropped; Drop consumes the frame and never forwards it to business
// handlers.
/** @brief Limits packet admission with a token bucket. */
class TokenBucketFeature final : public IConnectionFeature
{
public:
    using Now = std::function<ConnectionClock::time_point()>;
    TokenBucketFeature(
        double capacity,
        double packetsPerSecond,
        FeatureResult exhausted = FeatureResult::Close,
        Now now = ConnectionClock::now
    );
    void onConnected(IConnectionControl&) override;
    FeatureResult onPacket(IConnectionControl&, PacketView) override;

private:
    const double m_capacity, m_rate;
    /// Stores the exhausted.
    const FeatureResult m_exhausted;
    /// Stores the now.
    Now m_now;
    /// Stores the tokens.
    double m_tokens;
    /// Stores the last.
    ConnectionClock::time_point m_last{};
};
} // namespace service::gateway
~~~
