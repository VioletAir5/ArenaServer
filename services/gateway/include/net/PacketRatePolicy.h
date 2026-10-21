/** @file
 * @brief Limits complete frame admission using a per-connection token
 * bucket.
 */
#pragma once

#include "net/PacketPolicy.h"

namespace service::gateway
{
/** @brief Applies packet rate admission independently of transport
 * reads. */
class PacketRatePolicy final : public PacketPolicy
{
public:
    using Clock = std::chrono::steady_clock;
    using Now = std::function<Clock::time_point()>;
    PacketRatePolicy(
        double capacity,
        double packetsPerSecond,
        PacketDecision exhausted = PacketDecision::Close,
        Now now = Clock::now
    );
    PacketDecision
    inspect(const ConnectionContext&, PacketView) override;

private:
    /// Maximum accumulated packet tokens.
    const double m_capacity;
    /// Refill rate in packets per second.
    const double m_rate;
    /// Action taken when a complete frame has no token.
    const PacketDecision m_exhausted;
    /// Monotonic clock dependency, replaceable in deterministic
    /// tests.
    Now m_now;
    /// Current token balance, mutated only by the protocol owner.
    double m_tokens;
    /// Last observed refill time; backward clock steps do not mint
    /// tokens.
    Clock::time_point m_last;
};
} // namespace service::gateway
