#include "net/TimerQueue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "net/EventLoop.h"

namespace service::gateway
{
namespace
{
int newTimerFd()
{
    const int fd = ::timerfd_create(
        CLOCK_MONOTONIC,
        TFD_NONBLOCK | TFD_CLOEXEC
    );
    if (fd < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_create"
        );
    }
    return fd;
}
} // namespace

TimerQueue::TimerQueue(EventLoop* loop)
    : m_loop(loop)
    , m_timerFd(newTimerFd())
    , m_channel(m_timerFd.get())
{
    m_channel.setReadCallback([this]() { handleRead(); });
    m_channel.enableReading();
    m_loop->addChannel(&m_channel);
}

TimerQueue::~TimerQueue()
{
    m_loop->removeChannel(&m_channel);
}

TimerId
TimerQueue::addAfter(Timer::Duration delay, Timer::Callback callback)
{
    if (delay <= Timer::Duration::zero() || !callback)
    {
        throw std::invalid_argument(
            "timer delay and callback must be valid"
        );
    }
    const TimerId id = m_nextId++;
    insert(Timer(
        id,
        Timer::Clock::now() + delay,
        Timer::Duration::zero(),
        std::move(callback)
    ));
    arm();
    return id;
}

TimerId TimerQueue::addEvery(
    Timer::Duration interval, Timer::Callback callback
)
{
    if (interval <= Timer::Duration::zero() || !callback)
    {
        throw std::invalid_argument(
            "timer interval and callback must be valid"
        );
    }
    const TimerId id = m_nextId++;
    insert(Timer(
        id,
        Timer::Clock::now() + interval,
        interval,
        std::move(callback)
    ));
    arm();
    return id;
}

bool TimerQueue::cancel(TimerId id)
{
    const auto it = m_indices.find(id);
    if (it != m_indices.end())
    {
        removeAt(it->second);
        arm();
        return true;
    }
    if (id != 0 && id == m_runningId)
    {
        m_cancelRunning = true;
        return true;
    }
    return false;
}

std::size_t TimerQueue::size() const noexcept
{
    return m_heap.size();
}

bool TimerQueue::earlier(const Timer& lhs, const Timer& rhs) noexcept
{
    return lhs.expiration() < rhs.expiration() ||
           (lhs.expiration() == rhs.expiration() &&
            lhs.id() < rhs.id());
}

void TimerQueue::swapEntries(std::size_t lhs, std::size_t rhs)
{
    std::swap(m_heap[lhs], m_heap[rhs]);
    m_indices[m_heap[lhs].id()] = lhs;
    m_indices[m_heap[rhs].id()] = rhs;
}

void TimerQueue::siftUp(std::size_t index)
{
    while (index > 0)
    {
        const std::size_t parent = (index - 1) / 2;
        if (!earlier(m_heap[index], m_heap[parent]))
        {
            break;
        }
        swapEntries(index, parent);
        index = parent;
    }
}

void TimerQueue::siftDown(std::size_t index)
{
    while (index < m_heap.size())
    {
        const std::size_t left = index * 2 + 1;
        if (left >= m_heap.size())
        {
            break;
        }
        const std::size_t right = left + 1;
        std::size_t child = left;
        if (right < m_heap.size() &&
            earlier(m_heap[right], m_heap[left]))
        {
            child = right;
        }
        if (!earlier(m_heap[child], m_heap[index]))
        {
            break;
        }
        swapEntries(index, child);
        index = child;
    }
}

void TimerQueue::insert(Timer timer)
{
    const std::size_t index = m_heap.size();
    m_heap.emplace_back(std::move(timer));
    try
    {
        m_indices.emplace(m_heap.back().id(), index);
    }
    catch (...)
    {
        m_heap.pop_back();
        throw;
    }
    siftUp(index);
}

Timer TimerQueue::removeAt(std::size_t index)
{
    Timer removed = std::move(m_heap[index]);
    m_indices.erase(removed.id());
    const std::size_t last = m_heap.size() - 1;
    if (index != last)
    {
        m_heap[index] = std::move(m_heap[last]);
        m_indices[m_heap[index].id()] = index;
    }
    m_heap.pop_back();
    if (index < m_heap.size())
    {
        if (index > 0 &&
            earlier(m_heap[index], m_heap[(index - 1) / 2]))
        {
            siftUp(index);
        }
        else
        {
            siftDown(index);
        }
    }
    return removed;
}

void TimerQueue::arm()
{
    itimerspec spec{};
    if (!m_heap.empty())
    {
        using namespace std::chrono;
        const auto remaining = m_heap.front().expiration() -
                               Timer::Clock::now();
        const auto ns = std::max<std::int64_t>(
            1,
            duration_cast<nanoseconds>(remaining).count()
        );
        spec.it_value.tv_sec = ns / 1000000000;
        spec.it_value.tv_nsec = ns % 1000000000;
    }
    if (::timerfd_settime(m_timerFd.get(), 0, &spec, nullptr) < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_settime"
        );
    }
}

void TimerQueue::handleRead()
{
    std::uint64_t expirations = 0;
    for (;;)
    {
        const ssize_t n = ::read(
            m_timerFd.get(),
            &expirations,
            sizeof(expirations)
        );
        if (n == static_cast<ssize_t>(sizeof(expirations)))
        {
            break;
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
            "read timerfd"
        );
    }

    while (!m_heap.empty() &&
           m_heap.front().expiration() <= Timer::Clock::now())
    {
        Timer timer = removeAt(0);
        m_runningId = timer.id();
        m_cancelRunning = false;
        try
        {
            timer.run();
        }
        catch (...)
        {
            m_runningId = 0;
            arm();
            throw;
        }
        const bool repeat = timer.repeat() && !m_cancelRunning;
        m_runningId = 0;
        if (repeat)
        {
            timer.restart(Timer::Clock::now());
            insert(std::move(timer));
        }
    }
    arm();
}
} // namespace service::gateway