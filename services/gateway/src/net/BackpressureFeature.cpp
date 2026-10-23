/** @file
 * @brief Pauses reads while queued output exceeds configured limits.
 */
#include "net/BackpressureFeature.h"

#include <iostream>
#include <stdexcept>

#include "net/ConnectionChannel.h"
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
    ConnectionChannel& control, std::size_t, std::size_t bytes
)
{
    if (!m_paused && bytes >= m_high)
    {
        m_paused = true;
        m_timer = control.scheduleAt(
            ConnectionClock::now() + m_timeout,
            [](ConnectionChannel& c)
            {
                c.context().rejections->record(
                    RejectionReason::SlowClient
                );
                std::cerr << "connection slow client timeout id="
                          << c.context().connectionId << '\n';
                c.close();
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
