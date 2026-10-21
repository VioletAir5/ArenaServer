/** @file
 * @brief Pauses reads while queued output exceeds configured limits.
 */
#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// Policy only: transport always enforces its 1 MiB ceiling before
// copying.
/** @brief Pauses reads while queued output exceeds configured limits.
 */
class BackpressureFeature final : public IConnectionFeature
{
public:
    static constexpr std::size_t kLowWaterMark = 256 * 1024;
    static constexpr std::size_t kHighWaterMark = 512 * 1024;
    static constexpr std::size_t kHardOutputLimit = 1024 * 1024;
    static constexpr auto kSlowClientTimeout = std::chrono::seconds(
        5
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
