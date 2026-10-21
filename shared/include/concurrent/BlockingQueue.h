/** @file
 * @brief Transfers queued values between producer and consumer
 * threads.
 */
#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>

namespace service
{
/** @brief Bounded queue shared by producers and consumers.
 * @tparam T Value stored in the queue.
 */
template <typename T> class BlockingQueue
{
private:
    /// Maximum queued item count.
    size_t _capacity;
    /// Whether producers may still add items.
    bool _closed;
    /// Items shared by producers and consumers.
    std::deque<T> _queue;
    /// Guards the queue and closed flag.
    mutable std::mutex _mtx;
    /// Wakes producers when capacity becomes available.
    std::condition_variable _not_full;
    /// Wakes consumers when an item arrives or the queue closes.
    std::condition_variable _not_empty;

public:
    /** @brief Constructs the blocking queue. */
    explicit BlockingQueue(size_t);
    /** @brief Releases owned resources. */
    ~BlockingQueue() = default;

    // life time
    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;
    /** @brief Move construction is disabled. */
    BlockingQueue(BlockingQueue&&) = delete;
    BlockingQueue& operator=(BlockingQueue&&) = delete;

    // product and consume
    template <typename U> bool push(U&&);

    std::optional<T> pop();

    // state
    void close();
    /** @brief Reports whether the queue has been closed. */
    bool closed() const;

    // observer
    size_t size() const;
    size_t capacity() const;
    bool empty() const;
    bool full() const;
};

// ==========================
// implementation
// ==========================
template <typename T>
BlockingQueue<T>::BlockingQueue(size_t capacity)
    : _capacity(capacity)
    , _closed(false)
{
    if (_capacity == 0)
    {
        throw std::invalid_argument("capacity can't be zero");
    }
}

template <typename T>
template <typename U>
bool BlockingQueue<T>::push(U&& task)
{
    std::unique_lock<std::mutex> lock(_mtx);
    _not_full.wait(
        lock,
        [this]
        { return (this->_queue.size() < _capacity) || this->_closed; }
    );
    if (_closed)
    {
        return false;
    }
    _queue.push_back(std::forward<U>(task));
    lock.unlock();
    _not_empty.notify_one();
    return true;
}

template <typename T> std::optional<T> BlockingQueue<T>::pop()
{
    std::unique_lock<std::mutex> lock(_mtx);
    _not_empty.wait(
        lock,
        [this] { return !(this->_queue.empty()) || this->_closed; }
    );
    std::optional<T> task;
    if (!_queue.empty())
    {
        task = std::move(_queue.front());
        _queue.pop_front();
        lock.unlock();
        _not_full.notify_one();
    }
    return task;
}

template <typename T> void BlockingQueue<T>::close()
{
    std::unique_lock<std::mutex> lock(_mtx);
    if (_closed)
    {
        return;
    }
    _closed = true;
    lock.unlock();

    _not_empty.notify_all();
    _not_full.notify_all();
}

template <typename T> bool BlockingQueue<T>::closed() const
{
    std::lock_guard<std::mutex> lock(_mtx);
    return _closed;
}
template <typename T> size_t BlockingQueue<T>::size() const
{
    std::lock_guard<std::mutex> lock(_mtx);
    return _queue.size();
}

template <typename T> size_t BlockingQueue<T>::capacity() const
{
    return _capacity;
}

template <typename T> bool BlockingQueue<T>::empty() const
{
    std::lock_guard<std::mutex> lock(_mtx);
    return _queue.empty();
}

template <typename T> bool BlockingQueue<T>::full() const
{
    std::lock_guard<std::mutex> lock(_mtx);
    return _queue.size() == _capacity;
}
} // namespace service
