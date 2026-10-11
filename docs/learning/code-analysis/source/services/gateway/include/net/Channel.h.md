# services/gateway/include/net/Channel.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；93行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Associates a file descriptor with event callbacks and epoll interest.
 */
#pragma once

#include <sys/epoll.h>

#include <cstdint>
#include <functional>
#include <memory>

#include <os/FileDescriptor.h>

namespace service::gateway
{

/** @brief Associates a file descriptor with event callbacks and epoll interest. */
class Channel
{
    using EventCallback = std::function<void()>;
    using EventLoopUpdateCallback = std::function<void()>;

private:
    /// Owned file descriptor.
    int m_fd;

    /// Stores the events.
    uint32_t m_events;
    /// Stores the revents.
    uint32_t m_revents;

    /// Stores the read callback.
    EventCallback m_readCallback;
    /// Stores the write callback.
    EventCallback m_writeCallback;
    /// Stores the close callback.
    EventCallback m_closeCallback;
    /// Stores the error callback.
    EventCallback m_errorCallback;

    /// Stores the update.
    EventLoopUpdateCallback m_update;
    /// Thread that owns mutable connection state.
    std::weak_ptr<void> m_owner;
    /// Stores the tied.
    bool m_tied{false};

public:
    /** @brief Constructs the channel. */
    Channel(int fd);
    /** @brief Releases owned resources. */
    ~Channel() = default;

    /** @brief Returns the underlying file descriptor. */
    int fd() const noexcept;

    uint32_t events() const noexcept;
    uint32_t revents() const noexcept;

    /** @brief Sets read callback. */
    void setReadCallback(EventCallback cb);
    /** @brief Sets write callback. */
    void setWriteCallback(EventCallback cb);
    /** @brief Sets close callback. */
    void setCloseCallback(EventCallback cb);
    /** @brief Sets error callback. */
    void setErrorCallback(EventCallback cb);

    /** @brief Sets events. */
    void setEvents(uint32_t events);
    /** @brief Sets revents. */
    void setRevents(uint32_t revents) noexcept;

    /** @brief Sets event loop callback. */
    void setEventLoopCallback(EventLoopUpdateCallback cb);

    // Keep a shared owner alive across all callbacks in one event.
    void tie(const std::shared_ptr<void>& owner);
    /** @brief Handles event. */
    void handleEvent();

    void enableReading();
    void disableReading();
    void enableWriting();
    void disableWriting();
    void enablePeerClose();
    void enableEdgeTriggered();

    /** @brief Reports whether writing. */
    bool isWriting() const noexcept;
};

} // namespace service::gateway
~~~
