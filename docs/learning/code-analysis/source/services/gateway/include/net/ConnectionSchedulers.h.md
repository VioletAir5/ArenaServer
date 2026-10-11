# services/gateway/include/net/ConnectionSchedulers.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；16行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
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
~~~
