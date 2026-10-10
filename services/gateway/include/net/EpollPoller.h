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