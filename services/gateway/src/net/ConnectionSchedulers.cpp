/** @file
 * @brief Adapts event-loop timers to connection feature scheduling.
 */
#include "net/ConnectionSchedulers.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "net/EventLoop.h"

#include "runtime/OwnerScheduler.h"
namespace service::gateway
{
arena::runtime::OwnerScheduler connectionScheduler(EventLoop& loop)
{
    return {
        [&loop](auto action) { loop.queueInLoop(std::move(action)); },
        [&loop](auto when, auto action)
        {
            const auto delay = std::max(
                when - arena::runtime::OwnerClock::now(),
                arena::runtime::OwnerClock::duration::zero()
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
            return arena::runtime::OwnerTimer(
                [&loop, id, live]
                {
                    *live = false;
                    loop.cancelTimer(id);
                }
            );
        }
    };
}
} // namespace service::gateway
