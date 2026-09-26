#pragma once

#include <cstdint>
#include <functional>

#include <os/FileDescriptor.h>

namespace service
{

class Channel
{
    using EventCallback = std::function<void()>;

private:
    int m_fd;

    uint32_t m_events;
    uint32_t m_revents;

    EventCallback m_readCallback;
    EventCallback m_writeCallback;
    EventCallback m_closeCallback;
    EventCallback m_errorCallback;

public:
    Channel(int fd);
    ~Channel() = default;

    int fd() const noexcept;

    uint32_t events() const noexcept;
    uint32_t revents() const noexcept;

    void setEvents(uint32_t events) noexcept;
    void setRevents(uint32_t revents) noexcept;

    void setReadCallback(EventCallback cb);
    void setWriteCallback(EventCallback cb);
    void setCloseCallback(EventCallback cb);
    void setErrorCallback(EventCallback cb);

    void handleEvent();
};

} // namespace service
