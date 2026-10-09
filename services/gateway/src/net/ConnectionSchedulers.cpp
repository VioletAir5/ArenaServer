#include "net/ConnectionSchedulers.h"

#include <algorithm>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/EventLoop.h"
namespace service::gateway
{
ConnectionScheduler connectionScheduler(EventLoop& loop)
{
    return {
        [&loop](auto action) { loop.queueInLoop(std::move(action)); },
        [&loop](auto when, auto action)
        {
            const auto delay = std::max(
                when - ConnectionClock::now(),
                ConnectionClock::duration::zero()
            );
            auto live = std::make_shared<bool>(true);
            auto id = loop.runAfter(
                delay,
                [live, action = std::move(action)]
                {
                    if (*live)
                        action();
                }
            );
            return ConnectionTimer(
                [&loop, id, live]
                {
                    *live = false;
                    loop.cancelTimer(id);
                }
            );
        }};
}
ConnectionScheduler
connectionScheduler(boost::asio::any_io_executor executor)
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
            return ConnectionTimer(
                [timer, live]
                {
                    *live = false;
                    boost::system::error_code ignored;
                    timer->cancel(ignored);
                }
            );
        }};
}
} // namespace service::gateway