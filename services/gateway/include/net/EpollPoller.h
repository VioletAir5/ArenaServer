#pragma once

#include <sys/epoll.h>

#include <vector>

#include "net/Channel.h"
#include "os/FileDescriptor.h"
namespace service::gateway
{
class EpollPoller
{
private:
    static constexpr int M_kInitialEventSize = 64;

    FileDescriptor m_epollFd{};
    std::vector<epoll_event> m_events;

public:
    EpollPoller();
    ~EpollPoller() = default;

    void addChannel(Channel*);
    void updateChannel(Channel*);
    void removeChannel(Channel*);

    void poll(int, std::vector<Channel*>&);
};
}; // namespace service::gateway