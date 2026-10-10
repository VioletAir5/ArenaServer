#include "net/TokenBucketFeature.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <stdexcept>
namespace service::gateway
{
TokenBucketFeature::TokenBucketFeature(
    double capacity, double rate, FeatureResult exhausted, Now now
)
    : m_capacity(capacity)
    , m_rate(rate)
    , m_exhausted(exhausted)
    , m_now(std::move(now))
    , m_tokens(capacity)
{
    if (!std::isfinite(capacity) || capacity < 1 ||
        !std::isfinite(rate) || rate <= 0 || !m_now ||
        (exhausted != FeatureResult::Drop &&
         exhausted != FeatureResult::Close))
        throw std::invalid_argument("invalid token bucket");
}
void TokenBucketFeature::onConnected(IConnectionControl&)
{
    m_last = m_now();
}
FeatureResult
TokenBucketFeature::onPacket(IConnectionControl& control, PacketView)
{
    auto now = m_now();
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
        return FeatureResult::Continue;
    }
    control.context().rejections->record(RejectionReason::PacketRateExceeded);
    if (m_exhausted == FeatureResult::Close)
        std::cerr << "connection packet rate exceeded id="
                  << control.context().connectionId << '\n';
    return m_exhausted;
}
} // namespace service::gateway
