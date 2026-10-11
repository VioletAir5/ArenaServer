# services/gateway/include/net/Timer.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；75行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Represents a scheduled callback and its deadline.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <utility>

namespace service::gateway
{

/** @brief Represents a scheduled callback and its deadline. */
class Timer
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;
    using Callback = std::function<void()>;

    Timer(
        std::uint64_t id,
        TimePoint expiration,
        Duration interval,
        Callback callback
    )
        : m_id(id)
        , m_expiration(expiration)
        , m_interval(interval)
        , m_callback(std::move(callback))
    {
    }

    std::uint64_t id() const noexcept
    {
        return m_id;
    }

    TimePoint expiration() const noexcept
    {
        return m_expiration;
    }

    bool repeat() const noexcept
    {
        return m_interval != Duration::zero();
    }

    void run() const
    {
        if (m_callback)
        {
            m_callback();
        }
    }

    void restart(TimePoint now)
    {
        m_expiration = now + m_interval;
    }

private:
    /// Identifier assigned by the owning timer queue.
    std::uint64_t m_id;
    /// Stores the expiration.
    TimePoint m_expiration;
    /// Stores the interval.
    Duration m_interval;
    /// Stores the callback.
    Callback m_callback;
};

} // namespace service::gateway
~~~
