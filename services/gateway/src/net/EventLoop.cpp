#include "net/EventLoop.h"

#include "net/Channel.h"

namespace service::gateway
{
void EventLoop::loop()
{
    m_quit = false;
    while (!m_quit)
    {
        int timeoutMs = 1000;
        m_poller.poll(timeoutMs, m_activeChannels);
        for (auto channel : m_activeChannels)
        {
            channel->handleEvent();
        }

        auto pending = std::move(m_pendingFunctors);

        m_pendingFunctors.clear();

        for (auto& functor : pending)
        {
            functor();
        }
    }
}

void EventLoop::quit()
{
    m_quit = true;
}

void EventLoop::queueInLoop(Functor cb)
{
    m_pendingFunctors.push_back(std::move(cb));
}

void EventLoop::addChannel(Channel* channel)
{
    m_poller.addChannel(channel);
}

void EventLoop::updateChannel(Channel* channel)
{
    m_poller.updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel)
{
    m_poller.removeChannel(channel);
}
} // namespace service::gateway