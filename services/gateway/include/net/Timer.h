#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <utility>

namespace service::gateway
{

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
    std::uint64_t m_id;
    TimePoint m_expiration;
    Duration m_interval;
    Callback m_callback;
};

} // namespace service::gateway