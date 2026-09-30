#pragma once

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/PacketBuffer.h"
#include "net/Socket.h"
namespace service::gateway
{
class TcpConnection
{
public:
    using CloseCallback = std::function<void(TcpConnection*)>;
    TcpConnection(EventLoop*, Socket);

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    void setCloseCallback(CloseCallback);

    void send(const void* data, size_t len);

    Channel& channel() noexcept;
    int fd() const noexcept;

private:
    EventLoop* m_loop;

    CloseCallback m_closeCallback;

    Socket m_socket;
    Channel m_channel;

    PacketBuffer m_inputBuffer;
    PacketBuffer m_outputBuffer;

    bool m_closing{true};
};
}; // namespace service::gateway