#pragma once

#include <functional>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Socket.h"
namespace service::gateway
{
class Acceptor
{
public:
    using NewConnectionCallback = std::function<
        void(Socket&&, const InetAddress&)>;

    Acceptor(EventLoop* loop, const InetAddress& listenAddr);
    ~Acceptor();

    bool listening() const noexcept;
    void stop();

    void setNewConnectionCallback(NewConnectionCallback cb);

private:
    void handleRead();

private:
    EventLoop* m_loop;

    Socket m_listenSocket;
    Channel m_listenChannel;

    NewConnectionCallback m_newConnectionCallback;

    bool m_listening{false};
};
}; // namespace service::gateway