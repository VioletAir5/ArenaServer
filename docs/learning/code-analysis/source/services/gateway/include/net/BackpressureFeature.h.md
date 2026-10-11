# services/gateway/include/net/BackpressureFeature.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；38行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Pauses reads while queued output exceeds configured limits.
 */
#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// Policy only: transport always enforces its 1 MiB ceiling before
// copying.
/** @brief Pauses reads while queued output exceeds configured limits. */
class BackpressureFeature final : public IConnectionFeature
{
public:
    static constexpr std::size_t kLowWaterMark = 256 * 1024;
    static constexpr std::size_t kHighWaterMark = 512 * 1024;
    static constexpr std::size_t kHardOutputLimit = 1024 * 1024;
    static constexpr auto kSlowClientTimeout = std::chrono::seconds(5
    );
    explicit BackpressureFeature(
        std::size_t low = kLowWaterMark,
        std::size_t high = kHighWaterMark,
        ConnectionClock::duration timeout = kSlowClientTimeout
    );
    void onOutputBufferChanged(
        IConnectionControl&, std::size_t, std::size_t
    ) override;
    void onClosed() noexcept override;

private:
    const std::size_t m_low, m_high;
    /// Stores the timeout.
    const ConnectionClock::duration m_timeout;
    /// Stores the paused.
    bool m_paused{false};
    /// Stores the timer.
    ConnectionTimer m_timer;
};
} // namespace service::gateway
~~~
