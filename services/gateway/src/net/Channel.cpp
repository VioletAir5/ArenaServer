#include "net/Channel.h"

#include <sys/epoll.h>

#include <cstdint>

namespace service::gateway
{
Channel::Channel(int fd)
    : m_fd(fd)
    , m_events(0)
    , m_revents(0)
{
}

int Channel::fd() const noexcept
{
    return m_fd;
}

uint32_t Channel::events() const noexcept
{
    return m_events;
}

uint32_t Channel::revents() const noexcept
{
    return m_revents;
}

void Channel::setReadCallback(EventCallback cb)
{
    m_readCallback = cb;
}

void Channel::setWriteCallback(EventCallback cb)
{
    m_writeCallback = cb;
}

void Channel::setCloseCallback(EventCallback cb)
{
    m_closeCallback = cb;
}

void Channel::setErrorCallback(EventCallback cb)
{
    m_errorCallback = cb;
}

void Channel::setEvents(uint32_t events)
{
    if (m_events == events)
    {
        return;
    }

    const uint32_t previous = m_events;
    m_events = events;

    if (m_update)
    {
        try
        {
            m_update();
        }
        catch (...)
        {
            m_events = previous;
            throw;
        }
    }
}

void Channel::setRevents(uint32_t revents) noexcept
{
    m_revents = revents;
}

void Channel::enableReading()
{
    setEvents(m_events | EPOLLIN);
}

void Channel::disableReading()
{
    setEvents(m_events & ~EPOLLIN);
}

void Channel::enableWriting()
{
    setEvents(m_events | EPOLLOUT);
}

void Channel::disableWriting()
{
    setEvents(m_events & ~EPOLLOUT);
}

void Channel::enablePeerClose()
{
    setEvents(m_events | EPOLLRDHUP);
}

void Channel::enableEdgeTriggered()
{
    setEvents(m_events | EPOLLET);
}

bool Channel::isWriting() const noexcept
{
    return (m_events & EPOLLOUT) != 0;
}

void Channel::setEventLoopCallback(EventLoopUpdateCallback cb)
{
    m_update = cb;
}

void Channel::handleEvent()
{
    if (m_revents & (EPOLLERR | EPOLLHUP))
    {
        if (m_errorCallback)
            m_errorCallback();
    }

    if (m_revents & EPOLLOUT)
    {
        if (m_writeCallback)
            m_writeCallback();
    }

    if (m_revents & EPOLLIN)
    {
        if (m_readCallback)
            m_readCallback();
    }

    if (m_revents & EPOLLRDHUP)
    {
        if (m_closeCallback)
            m_closeCallback();
    }
}

}; // namespace service::gateway