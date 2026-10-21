/** @file
 * @brief Adapts event-loop timers to connection feature scheduling.
 */
#pragma once
#include <boost/asio/any_io_executor.hpp>

#include "net/ConnectionFeature.h"
namespace service::gateway
{
class EventLoop;
// Timers and cancellation are owner-only. defer is thread-safe and
// always queues; callers must stop foreign posts before releasing the
// loop/context. ConnectionSession's close endpoint supplies that
// lifetime barrier.
ConnectionScheduler connectionScheduler(EventLoop&);
/** @brief Creates a scheduler for the supplied executor. */
ConnectionScheduler connectionScheduler(boost::asio::any_io_executor);
} // namespace service::gateway
