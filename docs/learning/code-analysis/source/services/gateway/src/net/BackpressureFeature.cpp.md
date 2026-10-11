# services/gateway/src/net/BackpressureFeature.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；55行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Pauses reads while queued output exceeds configured limits.
 */
#include "net/BackpressureFeature.h"

#include <iostream>
#include <stdexcept>
namespace service::gateway
{
BackpressureFeature::BackpressureFeature(
    std::size_t low,
    std::size_t high,
    ConnectionClock::duration timeout
)
    : m_low(low)
    , m_high(high)
    , m_timeout(timeout)
{
    if (low >= high || high >= kHardOutputLimit ||
        timeout <= ConnectionClock::duration::zero())
        throw std::invalid_argument(
            "invalid backpressure thresholds/timeout"
        );
}
void BackpressureFeature::onOutputBufferChanged(
    IConnectionControl& control, std::size_t, std::size_t bytes
)
{
    if (!m_paused && bytes >= m_high)
    {
        m_paused = true;
        m_timer = control.scheduleAt(
            ConnectionClock::now() + m_timeout,
            [](IConnectionControl& c)
            {
                c.context().rejections->record(RejectionReason::SlowClient);
                std::cerr << "connection slow client timeout id="
                          << c.context().connectionId << '\n';
                c.requestClose();
            }
        );
        control.setReadPaused(ReadPauseReason::Backpressure, true);
    }
    else if (m_paused && bytes <= m_low)
    {
        m_paused = false;
        m_timer.reset();
        control.setReadPaused(ReadPauseReason::Backpressure, false);
    }
}
void BackpressureFeature::onClosed() noexcept
{
    m_timer.reset();
}
} // namespace service::gateway
~~~
