# services/gateway/include/net/EpollPoller.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；40行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Registers channels and collects ready Linux epoll events.
 */
#pragma once

#include <sys/epoll.h>

#include <vector>

#include "net/Channel.h"
#include "os/FileDescriptor.h"
namespace service::gateway
{
/** @brief Registers channels and collects ready Linux epoll events. */
class EpollPoller
{
private:
    static constexpr int M_kInitialEventSize = 64;

    /// Stores the epoll fd.
    FileDescriptor m_epollFd{};
    /// Stores the events.
    std::vector<epoll_event> m_events;

public:
    /** @brief Constructs the epoll poller. */
    EpollPoller();
    /** @brief Releases owned resources. */
    ~EpollPoller() = default;

    /** @brief Adds channel. */
    void addChannel(Channel*);
    /** @brief Updates channel. */
    void updateChannel(Channel*);
    /** @brief Removes channel. */
    void removeChannel(Channel*);

    void poll(int, std::vector<Channel*>&);
};
}; // namespace service::gateway
~~~
