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
