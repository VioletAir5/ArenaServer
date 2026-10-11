# services/gateway/include/net/Acceptor.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；61行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Accepts new nonblocking TCP sockets on an event loop.
 */
#pragma once

#include <chrono>
#include <functional>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/Socket.h"
namespace service::gateway
{
/** @brief Accepts new nonblocking TCP sockets on an event loop. */
class Acceptor
{
public:
    using NewConnectionCallback = std::function<
        void(Socket&&, const InetAddress&)>;

    /** @brief Constructs the acceptor. */
    Acceptor(EventLoop* loop, const InetAddress& listenAddr);
    /** @brief Releases owned resources. */
    ~Acceptor();

    bool listening() const noexcept;
    void stop();

    /** @brief Sets new connection callback. */
    void setNewConnectionCallback(NewConnectionCallback cb);

private:
    /** @brief Accepts ready client sockets until the queue is drained. */
    void handleRead();
    void suspendForResourceError();
    /** @brief Schedules retry. */
    void scheduleRetry();
    void retryListening();

private:
    /// Event loop that owns this object and its callbacks.
    EventLoop* m_loop;

    /// Stores the listen socket.
    Socket m_listenSocket;
    /// Stores the listen channel.
    Channel m_listenChannel;
    // Reserved descriptor lets EMFILE still accept and reject one peer.
    service::FileDescriptor m_reserveFd;
    /// Stores the retry timer id.
    TimerId m_retryTimerId{0};
    static constexpr std::chrono::milliseconds kRetryDelay{100};

    /// Stores the new connection callback.
    NewConnectionCallback m_newConnectionCallback;

    /// Stores the listening.
    bool m_listening{false};
};
}; // namespace service::gateway
~~~
