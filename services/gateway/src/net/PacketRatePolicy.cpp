/** @file
 * @brief Implements complete-frame token bucket admission.
 */
#include "net/PacketRatePolicy.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace service::gateway
{
PacketRatePolicy::PacketRatePolicy(
    double capacity, double rate, PacketDecision exhausted, Now now
)
    : m_capacity(capacity)
    , m_rate(rate)
    , m_exhausted(exhausted)
    , m_now(std::move(now))
    , m_tokens(capacity)
{
    if (!std::isfinite(capacity) || capacity < 1 ||
        !std::isfinite(rate) || rate <= 0 || !m_now ||
        (exhausted != PacketDecision::Drop &&
         exhausted != PacketDecision::Close))
        throw std::invalid_argument("invalid packet token bucket");
    m_last = m_now();
}

PacketDecision PacketRatePolicy::inspect(
    const ConnectionContext& context, PacketView
)
{
    const auto now = m_now();
    if (now > m_last)
    {
        const double elapsed = std::chrono::duration<double>(
                                   now - m_last
        )
                                   .count();
        m_tokens = std::min(m_capacity, m_tokens + elapsed * m_rate);
        m_last = now;
    }
    if (m_tokens >= 1)
    {
        m_tokens -= 1;
        return PacketDecision::Continue;
    }
    context.rejections->record(RejectionReason::PacketRateExceeded);
    return m_exhausted;
}
} // namespace service::gateway
