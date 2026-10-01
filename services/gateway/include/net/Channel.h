#pragma once

#include <sys/epoll.h>

#include <cstdint>
#include <functional>

#include <os/FileDescriptor.h>

namespace service::gateway
{

class Channel
{
    using EventCallback = std::function<void()>;
    using EventLoopUpdateCallback = std::function<void()>;

private:
    int m_fd;

    uint32_t m_events;
    uint32_t m_revents;

    EventCallback m_readCallback;
    EventCallback m_writeCallback;
    EventCallback m_closeCallback;
    EventCallback m_errorCallback;

    EventLoopUpdateCallback m_update;

public:
    Channel(int fd);
    ~Channel() = default;

    int fd() const noexcept;

    uint32_t events() const noexcept;
    uint32_t revents() const noexcept;

    void setReadCallback(EventCallback cb);
    void setWriteCallback(EventCallback cb);
    void setCloseCallback(EventCallback cb);
    void setErrorCallback(EventCallback cb);

    void setEvents(uint32_t events);
    void setRevents(uint32_t revents) noexcept;

    void setEventLoopCallback(EventLoopUpdateCallback cb);

    void handleEvent();

    void enableReading();
    void disableReading();
    void enableWriting();
    void disableWriting();
    void enablePeerClose();
    void enableEdgeTriggered();

    bool isWriting() const noexcept;
};

} // namespace service::gateway
