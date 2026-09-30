#include "net/Channel.h"

#include <sys/epoll.h>

#include <cstdint>

namespace service
{
Channel::Channel(int fd)
    : m_fd(fd)
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

void Channel::setEvents(uint32_t events) noexcept
{
    m_events = events;
}

void Channel::setRevents(uint32_t revents) noexcept
{
    m_revents = revents;
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

}; // namespace service