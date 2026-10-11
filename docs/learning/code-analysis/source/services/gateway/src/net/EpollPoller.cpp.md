# services/gateway/src/net/EpollPoller.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；132行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Registers channels and collects ready Linux epoll events.
 */
#include "net/EpollPoller.h"

#include <sys/epoll.h>

#include <cerrno>
#include <system_error>

#include "net/Channel.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{
EpollPoller::EpollPoller()
    : m_epollFd(::epoll_create1(EPOLL_CLOEXEC))
    , m_events(M_kInitialEventSize)
{
    if (!m_epollFd.valid())
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll_create1"
        );
    }
}

void EpollPoller::addChannel(Channel* channel)
{
    epoll_event event;
    event.events = channel->events();
    event.data.ptr = channel;

    if (::epoll_ctl(
            m_epollFd.get(),
            EPOLL_CTL_ADD,
            channel->fd(),
            &event
        ) < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll ADD event error"
        );
    }
}

void EpollPoller::updateChannel(Channel* channel)
{
    epoll_event event;
    event.events = channel->events();
    event.data.ptr = channel;

    if (::epoll_ctl(
            m_epollFd.get(),
            EPOLL_CTL_MOD,
            channel->fd(),
            &event
        ))
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll UPDATE error"
        );
    }
}

void EpollPoller::removeChannel(Channel* channel)
{
    if (::epoll_ctl(
            m_epollFd.get(),
            EPOLL_CTL_DEL,
            channel->fd(),
            nullptr
        ))
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll REMOVE error"
        );
    }
}

void EpollPoller::poll(
    int timeoutMs, std::vector<Channel*>& activeChannels
)
{
    activeChannels.clear();

    int ready = ::epoll_wait(
        m_epollFd.get(),
        m_events.data(),
        static_cast<int>(m_events.size()),
        timeoutMs
    );

    if (ready < 0)
    {
        if (errno == EINTR)
        {
            return;
        }

        throw std::system_error(
            errno,
            std::generic_category(),
            "epoll_wait"
        );
    }

    activeChannels.reserve(ready);

    for (int i = 0; i < ready; ++i)
    {
        auto* channel = static_cast<Channel*>(m_events[i].data.ptr);

        channel->setRevents(m_events[i].events);

        activeChannels.push_back(channel);
    }

    if (ready == static_cast<int>(m_events.size()))
    {
        m_events.resize(m_events.size() * 2);
    }
}
}; // namespace service::gateway
~~~
