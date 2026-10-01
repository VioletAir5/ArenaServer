#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <string_view>

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
    using MessageCallback = std::function<
        void(TcpConnection&, std::string_view)>;

    static constexpr std::size_t kLowWaterMark = 256 * 1024;
    static constexpr std::size_t kHighWaterMark = 512 * 1024;
    static constexpr std::size_t kHardOutputLimit = kMaxBufferSize;
    static constexpr std::chrono::seconds kSlowClientTimeout{5};

    TcpConnection(EventLoop*, Socket);

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    void setCloseCallback(CloseCallback);
    // The message view is valid only for the duration of the
    // callback.
    void setMessageCallback(MessageCallback);

    void send(const void* data, size_t len);
    void closeIfSlow(std::chrono::steady_clock::time_point now);

    Channel& channel() noexcept;
    int fd() const noexcept;

private:
    void pauseReadingIfNeeded();
    void resumeReadingIfNeeded();

    EventLoop* m_loop;

    CloseCallback m_closeCallback;
    MessageCallback m_messageCallback;

    Socket m_socket;
    Channel m_channel;

    PacketBuffer m_inputBuffer;
    PacketBuffer m_outputBuffer;

    std::chrono::steady_clock::time_point m_highWaterSince{};
    bool m_readPaused{false};
    bool m_closing{false};
    bool m_readClosed{false};
};
} // namespace service::gateway
