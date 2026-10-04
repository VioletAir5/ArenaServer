#include "net/EventLoopThread.h"

#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace service::gateway
{
EventLoopThread::~EventLoopThread()
{
    stopThread();
}

void EventLoopThread::stopThread()
{
    if (m_thread && m_thread->joinable() &&
        m_thread->get_id() == std::this_thread::get_id())
    {
        throw std::logic_error(
            "EventLoopThread cannot join itself; use EventLoop::quit"
        );
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_loop != nullptr)
        {
            // Keep the worker's stack loop alive until quit finishes.
            // The worker clears m_loop under this same lock.
            m_loop->quit();
        }
    }

    // The worker needs m_mutex to unpublish the loop before exiting.
    if (m_thread && m_thread->joinable())
    {
        m_thread->join();
    }
}

void EventLoopThread::stop()
{
    stopThread();
    std::exception_ptr error;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        error = std::exchange(m_exception, {});
    }
    if (error)
    {
        std::rethrow_exception(error);
    }
}

EventLoop* EventLoopThread::startLoop()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_thread)
    {
        m_thread = std::make_unique<std::jthread>(
            [this]() { threadFunc(); }
        );
    }
    // A pointer is not a completion predicate: it can become null
    // again after successful startup. Failure must also wake waiters.
    m_cv.wait(lock, [this] { return m_startComplete; });
    if (m_exception)
    {
        std::rethrow_exception(m_exception);
    }
    if (m_loop == nullptr)
    {
        throw std::logic_error("EventLoopThread has already exited");
    }
    return m_loop;
}

void EventLoopThread::threadFunc()
{
    try
    {
        EventLoop loop;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_loop = &loop;
            m_startComplete = true;
        }
        m_cv.notify_all();

        try
        {
            loop.loop();
        }
        catch (...)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_exception = std::current_exception();
        }

        // Clear the published pointer before the local loop unwinds.
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_loop = nullptr;
        }
    }
    catch (...)
    {
        // EventLoop construction failed: the starting caller must
        // observe completion and the original exception.
        std::lock_guard<std::mutex> lock(m_mutex);
        m_exception = std::current_exception();
        m_startComplete = true;
    }
    m_cv.notify_all();
}
} // namespace service::gateway
