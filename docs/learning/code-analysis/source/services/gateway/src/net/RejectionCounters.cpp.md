# services/gateway/src/net/RejectionCounters.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；50行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Counts rejected connections by reason.
 */
#include "net/RejectionCounters.h"

namespace service::gateway
{
void RejectionCounters::record(RejectionReason reason) noexcept
{
    const auto index = static_cast<std::size_t>(reason);
    if (index < kReasons)
        m_counts[index].fetch_add(1, std::memory_order_relaxed);
}
std::uint64_t RejectionCounters::count(RejectionReason reason
) const noexcept
{
    const auto index = static_cast<std::size_t>(reason);
    return index < kReasons
               ? m_counts[index].load(std::memory_order_relaxed)
               : 0;
}
RejectionCounters::Snapshot
RejectionCounters::snapshot() const noexcept
{
    Snapshot result{};
    for (std::size_t i = 0; i < kReasons; ++i)
        result[i] = m_counts[i].load(std::memory_order_relaxed);
    return result;
}
std::string_view RejectionCounters::name(RejectionReason reason
) noexcept
{
    constexpr std::array<std::string_view, kReasons> names{
        "packet_rate_exceeded",
        "packet_type_not_allowed",
        "unauthenticated",
        "invalid_frame",
        "invalid_envelope",
        "handler_rejected",
        "callback_failure",
        "slow_client"};
    const auto index = static_cast<std::size_t>(reason);
    return index < kReasons ? names[index] : "unknown";
}
RejectionCounters::Ptr defaultRejectionCounters()
{
    static auto counters = std::make_shared<RejectionCounters>();
    return counters;
}
} // namespace service::gateway
~~~
