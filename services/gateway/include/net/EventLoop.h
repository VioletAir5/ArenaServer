#pragma once

#include <sys/epoll.h>

#include <vector>

#include "net/Channel.h"
#include "net/EpollPoller.h"

namespace service::gateway
{
class EventLoop
{
public:
    using Functor = std::function<void()>;

private:
    bool m_quit{false};

    EpollPoller m_poller;
    std::vector<Channel*> m_activeChannels;

    std::vector<Functor> m_pendingFunctors;

public:
    EventLoop() = default;
    ~EventLoop() = default;

    void loop();
    void quit();

    void queueInLoop(Functor cb);

    void addChannel(Channel*);
    void updateChannel(Channel*);
    void removeChannel(Channel*);
};
}; // namespace service::gateway