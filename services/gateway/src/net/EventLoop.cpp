/** @file
 * @brief Runs channel callbacks, timers, and queued work on one thread.
 */
#include "net/EventLoop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace service::gateway
{
namespace
{
int newWakeupFd()
{
    const int fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (fd < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "eventfd"
        );
    }
    return fd;
}
} // namespace

EventLoop::EventLoop()
    : m_threadId(std::this_thread::get_id())
    , m_wakeupFd(newWakeupFd())
    , m_wakeupChannel(std::make_unique<Channel>(m_wakeupFd.get()))
{
    m_wakeupChannel->setReadCallback([this]() { handleWakeup(); });
    m_wakeupChannel->enableReading();
    addChannel(m_wakeupChannel.get());
    m_timerQueue = std::make_unique<TimerQueue>(this);
}

void EventLoop::loop()
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "EventLoop::loop called from another thread"
        );
    }

    while (!m_quit.load())
    {
        m_poller.poll(-1, m_activeChannels);
        for (Channel*& entry : m_activeChannels)
        {
            Channel* channel = std::exchange(entry, nullptr);
            if (channel != nullptr)
            {
                channel->handleEvent();
            }
        }
        m_activeChannels.clear();
        doPendingFunctors();
    }
}

void EventLoop::quit()
{
    m_quit.store(true);
    if (!isInLoopThread())
    {
        wakeup();
    }
}

void EventLoop::runInLoop(Functor cb)
{
    if (isInLoopThread())
    {
        cb();
    }
    else
    {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor cb)
{
    bool needWakeup = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        needWakeup = m_pendingFunctors.empty();
        m_pendingFunctors.emplace_back(std::move(cb));
        m_peakPendingFunctors = std::max(
            m_peakPendingFunctors,
            m_pendingFunctors.size()
        );
    }
    // An empty-to-nonempty transition must wake the poller even when
    // a task is queued by the owner before loop() or during a
    // callback.
    if (needWakeup)
    {
        wakeup();
    }
}

std::size_t EventLoop::peakPendingFunctorCount() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_peakPendingFunctors;
}

TimerId EventLoop::runAfter(Timer::Duration delay, Functor cb)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "timer operations require EventLoop thread"
        );
    }
    return m_timerQueue->addAfter(delay, std::move(cb));
}

TimerId EventLoop::runEvery(Timer::Duration interval, Functor cb)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "timer operations require EventLoop thread"
        );
    }
    return m_timerQueue->addEvery(interval, std::move(cb));
}

bool EventLoop::cancelTimer(TimerId id)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "timer operations require EventLoop thread"
        );
    }
    return m_timerQueue->cancel(id);
}

std::size_t EventLoop::pendingTimerCount() const
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "timer operations require EventLoop thread"
        );
    }
    return m_timerQueue->size();
}
void EventLoop::addChannel(Channel* channel)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "EventLoop::addChannel requires owner thread"
        );
    }
    m_poller.addChannel(channel);
}

void EventLoop::updateChannel(Channel* channel)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "EventLoop::updateChannel requires owner thread"
        );
    }
    m_poller.updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel)
{
    if (!isInLoopThread())
    {
        throw std::logic_error(
            "EventLoop::removeChannel requires owner thread"
        );
    }
    m_poller.removeChannel(channel);
    // epoll DEL cannot retract events already copied by poll().
    // Keep the vector shape stable while a callback removes another
    // ready Channel; the dispatcher skips invalidated entries.
    std::replace(m_activeChannels.begin(), m_activeChannels.end(),
                 channel, static_cast<Channel*>(nullptr));
}

void EventLoop::wakeup()
{
    const std::uint64_t one = 1;
    while (true)
    {
        const ssize_t n = ::write(
            m_wakeupFd.get(),
            &one,
            sizeof(one)
        );
        if (n == static_cast<ssize_t>(sizeof(one)))
        {
            return;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        // A saturated eventfd is already readable, so its wakeup is
        // pending.
        if (n < 0 && errno == EAGAIN)
        {
            return;
        }
        throw std::system_error(
            n < 0 ? errno : EIO,
            std::generic_category(),
            "write eventfd"
        );
    }
}

void EventLoop::handleWakeup()
{
    std::uint64_t value = 0;
    while (true)
    {
        const ssize_t n = ::read(
            m_wakeupFd.get(),
            &value,
            sizeof(value)
        );
        if (n == static_cast<ssize_t>(sizeof(value)))
        {
            return;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        if (n < 0 && errno == EAGAIN)
        {
            return;
        }
        throw std::system_error(
            n < 0 ? errno : EIO,
            std::generic_category(),
            "read eventfd"
        );
    }
}

void EventLoop::doPendingFunctors()
{
    std::vector<Functor> functors;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        functors.swap(m_pendingFunctors);
    }
    for (auto& functor : functors)
    {
        functor();
    }
}

bool EventLoop::isInLoopThread() const noexcept
{
    return std::this_thread::get_id() == m_threadId;
}
} // namespace service::gateway
