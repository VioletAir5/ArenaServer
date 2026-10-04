#include "net/EventLoopThreadPool.h"

#include <exception>
#include <stdexcept>
#include <utility>

namespace service::gateway
{
namespace
{
// Stop every worker even when a worker reports a callback exception.
// Never quit through a cached loop pointer: the worker may have
// exited.
std::exception_ptr
stopWorkers(std::vector<std::unique_ptr<EventLoopThread>>& threads)
{
    std::exception_ptr firstError;
    for (auto& thread : threads)
    {
        try
        {
            thread->requestStop();
        }
        catch (...)
        {
            if (!firstError)
            {
                firstError = std::current_exception();
            }
        }
    }
    for (auto& thread : threads)
    {
        try
        {
            thread->stop();
        }
        catch (...)
        {
            if (!firstError)
            {
                firstError = std::current_exception();
            }
        }
    }
    threads.clear();
    return firstError;
}
} // namespace

EventLoopThreadPool::EventLoopThreadPool(
    EventLoop* baseLoop, std::size_t threadCount
)
    : m_baseLoop(baseLoop)
    , m_threadCount(threadCount)
{
    if (m_baseLoop == nullptr)
    {
        throw std::invalid_argument(
            "EventLoopThreadPool requires base EventLoop"
        );
    }
    requireBaseThread();
}

EventLoopThreadPool::~EventLoopThreadPool() noexcept
{
    // Off-owner destruction cannot safely touch pool state or
    // self-join.
    if (!m_baseLoop->isInLoopThread())
    {
        std::terminate();
    }
    // Worker callback errors are reported by explicit stop(), not
    // teardown. EventLoopThread destructors still terminate for fatal
    // join failures.
    stopWorkers(m_threads);
}

void EventLoopThreadPool::requireBaseThread() const
{
    if (!m_baseLoop->isInLoopThread())
    {
        throw std::logic_error(
            "EventLoopThreadPool requires base EventLoop thread"
        );
    }
}

void EventLoopThreadPool::start()
{
    requireBaseThread();
    if (m_started)
    {
        return;
    }

    std::vector<std::unique_ptr<EventLoopThread>> threads;
    threads.reserve(m_threadCount);
    try
    {
        for (std::size_t i = 0; i < m_threadCount; ++i)
        {
            auto thread = std::make_unique<EventLoopThread>();
            thread->startLoop();
            threads.push_back(std::move(thread));
        }
    }
    catch (...)
    {
        // Keep the original startup error; rollback joins successful
        // workers.
        stopWorkers(threads);
        throw;
    }

    m_threads = std::move(threads);
    m_next = 0;
    m_started = true;
}

void EventLoopThreadPool::stop()
{
    requireBaseThread();
    const auto error = stopWorkers(m_threads);
    m_next = 0;
    m_started = false;
    if (error)
    {
        std::rethrow_exception(error);
    }
}

EventLoop* EventLoopThreadPool::getNextLoop()
{
    requireBaseThread();
    if (!m_started)
    {
        throw std::logic_error("EventLoopThreadPool is not started");
    }
    if (m_threads.empty())
    {
        return m_baseLoop;
    }

    // Validate the worker publication instead of returning a stale
    // cache.
    EventLoop* loop = m_threads[m_next]->startLoop();
    m_next = (m_next + 1) % m_threads.size();
    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops() const
{
    requireBaseThread();
    if (!m_started)
    {
        throw std::logic_error("EventLoopThreadPool is not started");
    }
    if (m_threads.empty())
    {
        return {m_baseLoop};
    }

    std::vector<EventLoop*> loops;
    loops.reserve(m_threads.size());
    for (const auto& thread : m_threads)
    {
        loops.push_back(thread->startLoop());
    }
    return loops;
}

bool EventLoopThreadPool::started() const
{
    requireBaseThread();
    return m_started;
}
} // namespace service::gateway
