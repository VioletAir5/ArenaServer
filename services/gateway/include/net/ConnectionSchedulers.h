/** @file
 * @brief Adapts event-loop timers to connection feature scheduling.
 */
#pragma once
#include <boost/asio/any_io_executor.hpp>

#include "net/ConnectionFeature.h"
namespace service::gateway
{
class EventLoop;
// Schedulers and cancellation are owner-only. Loop/context must
// outlive runtime.
ConnectionScheduler connectionScheduler(EventLoop&);
/** @brief Creates a scheduler for the supplied executor. */
ConnectionScheduler connectionScheduler(boost::asio::any_io_executor);
} // namespace service::gateway
