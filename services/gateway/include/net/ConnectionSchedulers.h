/** @file
 * @brief Adapts event-loop timers to connection feature scheduling.
 */
#pragma once
#include "runtime/OwnerScheduler.h"
namespace service::gateway
{
class EventLoop;
// Timers and cancellation are owner-only. defer is thread-safe and
// always queues; callers must stop foreign posts before releasing the
// loop/context. ConnectionHost's mailbox closes before registry
// release and supplies that lifetime barrier for retained completion
// posters.
arena::runtime::OwnerScheduler connectionScheduler(EventLoop&);
} // namespace service::gateway
