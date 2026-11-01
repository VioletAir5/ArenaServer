/** @file
 * @brief Implements cancellable owner timers and executor scheduling.
 */
#include "runtime/OwnerScheduler.h"

#include <exception>
#include <memory>
#include <utility>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

namespace arena::runtime
{
OwnerTimer::OwnerTimer(std::function<void()> cancel)
    : m_cancel(std::move(cancel))
{
}
OwnerTimer::~OwnerTimer()
{
    reset();
}
OwnerTimer::OwnerTimer(OwnerTimer&& other) noexcept
    : m_cancel(std::exchange(other.m_cancel, {}))
{
}
OwnerTimer& OwnerTimer::operator=(OwnerTimer&& other) noexcept
{
    if (this != &other)
    {
        reset();
        m_cancel = std::exchange(other.m_cancel, {});
    }
    return *this;
}
void OwnerTimer::reset() noexcept
{
    if (auto cancel = std::exchange(m_cancel, {}))
    {
        try
        {
            cancel();
        }
        catch (...)
        {
            std::terminate();
        }
    }
}
OwnerScheduler ownerScheduler(boost::asio::any_io_executor executor)
{
    return {
        [executor](auto action)
        { boost::asio::post(executor, std::move(action)); },
        [executor](auto when, auto action)
        {
            auto timer = std::make_shared<boost::asio::steady_timer>(
                executor
            );
            auto live = std::make_shared<bool>(true);
            timer->expires_at(when);
            timer->async_wait(
                [timer, live, action = std::move(action)](auto error)
                {
                    if (!error && *live)
                        action();
                }
            );
            return OwnerTimer(
                [timer, live]
                {
                    *live = false;
                    boost::system::error_code ignored;
                    timer->cancel(ignored);
                }
            );
        }
    };
}
} // namespace arena::runtime
