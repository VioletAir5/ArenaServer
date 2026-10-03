#include "net/TcpConnection.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include <iostream>
#include <system_error>
#include <utility>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/Socket.h"

namespace service::gateway
{
TcpConnection::TcpConnection(EventLoop* loop, Socket socket)
    : m_loop(loop)
    , m_socket(std::move(socket))
    , m_channel(m_socket.get())
{
    m_channel.enableReading();
    m_channel.enablePeerClose();
    m_channel.enableEdgeTriggered();

    m_channel.setReadCallback([this]() { handleRead(); });

    m_channel.setWriteCallback([this]() { handleWrite(); });

    m_channel.setCloseCallback([this]() { handleRead(); });

    m_channel.setErrorCallback([this]() { handleError(); });

    m_channel.setEventLoopCallback(
        [this]() { m_loop->updateChannel(&this->channel()); }
    );

    m_loop->addChannel(&m_channel);
}

void TcpConnection::handleRead()
{
    if (m_state == TcpConnectionState::PeerHalfClosed ||
        m_state == TcpConnectionState::WriteDrainingPeerClosed ||
        m_state == TcpConnectionState::Disconnected || m_readPaused)
    {
        return;
    }

    char buffer[8192];
    while (!m_readPaused)
    {
        const ssize_t n = ::read(
            m_socket.get(),
            buffer,
            sizeof(buffer)
        );

        if (n > 0)
        {
            m_lastActivity = std::chrono::steady_clock::now();
            if (!m_inputBuffer.append(buffer, static_cast<size_t>(n)))
            {
                handleClose();
                return;
            }

            const size_t readable = m_inputBuffer.readableBytes();
            if (m_messageCallback)
            {
                m_messageCallback(
                    *this,
                    std::string_view(
                        m_inputBuffer.readPtr(),
                        readable
                    )
                );
            }
            m_inputBuffer.skip(readable);

            if (m_state == TcpConnectionState::Disconnected)
            {
                return;
            }
            continue;
        }

        if (n == 0)
        {
            if (m_state == TcpConnectionState::WriteHalfClosed)
            {
                handleClose();
            }
            else if (m_state == TcpConnectionState::WriteDraining)
            {
                m_state = TcpConnectionState::WriteDrainingPeerClosed;
                if (m_outputBuffer.readableBytes() == 0)
                {
                    finishShutdownWrite();
                }
                else
                {
                    m_channel.enableWriting();
                }
            }
            else if (m_outputBuffer.readableBytes() == 0)
            {
                handleClose();
            }
            else
            {
                m_state = TcpConnectionState::PeerHalfClosed;
                m_channel.enableWriting();
            }
            return;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }

        handleIoError(errno);
        return;
    }
}
void TcpConnection::send(const void* data, size_t len)
{
    if ((m_state != TcpConnectionState::Connected &&
         m_state != TcpConnectionState::PeerHalfClosed) ||
        len == 0)
    {
        return;
    }

    const char* ptr = static_cast<const char*>(data);

    size_t remaining = len;

    // 如果现在没有积压数据，
    // 可以先尝试直接 write。
    if (m_outputBuffer.readableBytes() == 0)
    {
        while (true)
        {
            ssize_t n = ::send(m_socket.get(), ptr, remaining, MSG_NOSIGNAL);

            if (n > 0)
            {
                m_lastActivity = std::chrono::steady_clock::now();
                ptr += n;
                remaining -= static_cast<size_t>(n);

                if (remaining == 0)
                {
                    return;
                }

                continue;
            }

            if (n < 0 && errno == EINTR)
            {
                continue;
            }

            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                break;
            }

            handleIoError(errno);
            return;
        }
    }

    if (remaining > 0)
    {
        if (remaining >
                kHardOutputLimit - m_outputBuffer.readableBytes() ||
            !m_outputBuffer.append(ptr, remaining))
        {
            std::cerr << "TcpConnection output limit reached fd="
                      << fd() << '\n';
            handleClose();
            return;
        }

        notifyHighWaterIfNeeded();
        if (m_state == TcpConnectionState::Disconnected)
        {
            return;
        }
        if (!m_channel.isWriting())
        {
            m_channel.enableWriting();
        }
    }
}

void TcpConnection::shutdownWrite()
{
    if (m_state != TcpConnectionState::Connected &&
        m_state != TcpConnectionState::PeerHalfClosed)
    {
        return;
    }

    m_state = m_state == TcpConnectionState::PeerHalfClosed
        ? TcpConnectionState::WriteDrainingPeerClosed
        : TcpConnectionState::WriteDraining;
    if (m_outputBuffer.readableBytes() == 0)
    {
        finishShutdownWrite();
    }
    else if (!m_channel.isWriting())
    {
        m_channel.enableWriting();
    }
}

void TcpConnection::finishShutdownWrite()
{
    if ((m_state != TcpConnectionState::WriteDraining &&
         m_state != TcpConnectionState::WriteDrainingPeerClosed) ||
        m_outputBuffer.readableBytes() != 0)
    {
        return;
    }

    try
    {
        m_socket.shutdownWrite();
    }
    catch (const std::system_error& error)
    {
        std::cerr << "TcpConnection shutdown write failed fd="
                  << fd() << ": " << error.what() << '\n';
        handleClose();
        return;
    }

    if (m_state == TcpConnectionState::WriteDrainingPeerClosed)
    {
        handleClose();
        return;
    }

    m_state = TcpConnectionState::WriteHalfClosed;
    if (m_channel.isWriting())
    {
        m_channel.disableWriting();
    }
}

void TcpConnection::handleWrite()
{
    if (m_state == TcpConnectionState::Disconnected)
    {
        return;
    }

    while (m_outputBuffer.readableBytes() > 0)
    {
        const ssize_t n = ::send(
            m_socket.get(),
            m_outputBuffer.readPtr(),
            m_outputBuffer.readableBytes(),
            MSG_NOSIGNAL
        );

        if (n > 0)
        {
            m_lastActivity = std::chrono::steady_clock::now();
            m_outputBuffer.skip(static_cast<size_t>(n));
            continue;
        }

        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            break;
        }

        handleIoError(errno);
        return;
    }

    notifyLowWaterIfNeeded();
    if (m_state == TcpConnectionState::Disconnected ||
        m_outputBuffer.readableBytes() != 0)
    {
        return;
    }

    if (m_state == TcpConnectionState::WriteDraining ||
        m_state == TcpConnectionState::WriteDrainingPeerClosed)
    {
        finishShutdownWrite();
        return;
    }

    if (m_state == TcpConnectionState::PeerHalfClosed)
    {
        handleClose();
        return;
    }

    if (m_channel.isWriting())
    {
        m_channel.disableWriting();
    }
}

void TcpConnection::pauseReading()
{
    if (m_state == TcpConnectionState::Disconnected ||
        m_state == TcpConnectionState::PeerHalfClosed ||
        m_state == TcpConnectionState::WriteDrainingPeerClosed ||
        m_readPaused)
    {
        return;
    }

    m_channel.disableReading();
    m_readPaused = true;
}

void TcpConnection::resumeReading()
{
    if (m_state == TcpConnectionState::Disconnected ||
        m_state == TcpConnectionState::PeerHalfClosed ||
        m_state == TcpConnectionState::WriteDrainingPeerClosed ||
        !m_readPaused)
    {
        return;
    }

    m_channel.enableReading();
    m_readPaused = false;

    // Re-enabling EPOLLIN in ET mode may not create another edge for
    // bytes already in the socket. Drain them before returning.
    handleRead();
}

void TcpConnection::notifyHighWaterIfNeeded()
{
    if (m_aboveHighWater ||
        m_outputBuffer.readableBytes() < kHighWaterMark)
    {
        return;
    }

    m_aboveHighWater = true;
    m_highWaterSince = std::chrono::steady_clock::now();
    if (m_highWaterMarkCallback)
    {
        m_highWaterMarkCallback(
            *this,
            m_outputBuffer.readableBytes()
        );
    }
}

void TcpConnection::notifyLowWaterIfNeeded()
{
    if (!m_aboveHighWater ||
        m_outputBuffer.readableBytes() > kLowWaterMark)
    {
        return;
    }

    m_aboveHighWater = false;
    if (m_lowWaterMarkCallback)
    {
        m_lowWaterMarkCallback(*this, m_outputBuffer.readableBytes());
    }
}

void TcpConnection::closeIfSlow(
    std::chrono::steady_clock::time_point now
)
{
    if (m_state == TcpConnectionState::Disconnected ||
        !m_aboveHighWater ||
        now - m_highWaterSince < kSlowClientTimeout)
    {
        return;
    }

    std::cerr << "TcpConnection slow client timeout fd=" << fd()
              << '\n';
    handleClose();
}

void TcpConnection::handleError()
{
    if (m_state == TcpConnectionState::Disconnected)
    {
        return;
    }

    int error = 0;
    socklen_t len = sizeof(error);

    if (::getsockopt(
            m_socket.get(),
            SOL_SOCKET,
            SO_ERROR,
            &error,
            &len
        ) < 0)
    {
        error = errno;
    }

    if (error == 0)
    {
        // EPOLLHUP without SO_ERROR can be an orderly close. Drain
        // readable bytes/EOF instead of discarding pending output.
        handleRead();
        return;
    }

    handleIoError(error);
}

void TcpConnection::handleIoError(int error)
{
    if (error == ECONNRESET)
    {
        std::cerr << "TcpConnection peer reset fd=" << fd()
                  << ", error=" << error << '\n';
    }
    else if (error == EPIPE)
    {
        std::cerr << "TcpConnection peer write closed fd=" << fd()
                  << ", error=" << error << '\n';
    }
    else
    {
        std::cerr << "TcpConnection I/O error fd=" << fd()
                  << ", error=" << error << '\n';
    }
    handleClose();
}

void TcpConnection::handleClose()
{
    if (m_state == TcpConnectionState::Disconnected)
    {
        return;
    }

    m_state = TcpConnectionState::Disconnected;

    // 不在这里 close(fd)
    // 不在这里 removeChannel()
    // 不在这里 delete this

    if (m_closeCallback)
    {
        m_closeCallback(this);
    }
}

void TcpConnection::setCloseCallback(CloseCallback cb)
{
    m_closeCallback = std::move(cb);
}

void TcpConnection::setMessageCallback(MessageCallback cb)
{
    m_messageCallback = std::move(cb);
}

void TcpConnection::setHighWaterMarkCallback(WatermarkCallback cb)
{
    m_highWaterMarkCallback = std::move(cb);
}

void TcpConnection::setLowWaterMarkCallback(WatermarkCallback cb)
{
    m_lowWaterMarkCallback = std::move(cb);
}

Channel& TcpConnection::channel() noexcept
{
    return m_channel;
}

int TcpConnection::fd() const noexcept
{
    return m_socket.get();
}

std::size_t TcpConnection::pendingOutputBytes() const noexcept
{
    return m_outputBuffer.readableBytes();
}
std::chrono::steady_clock::time_point
TcpConnection::lastActivity() const noexcept
{
    return m_lastActivity;
}

bool TcpConnection::closing() const noexcept
{
    return m_state == TcpConnectionState::Disconnected;
}
} // namespace service::gateway
