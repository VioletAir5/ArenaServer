#include "net/TcpConnection.h"

#include <unistd.h>

#include <iostream>
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
    if (m_closing || m_readClosed || m_readPaused)
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

            if (m_closing)
            {
                return;
            }
            continue;
        }

        if (n == 0)
        {
            m_readClosed = true;
            if (m_outputBuffer.readableBytes() == 0)
            {
                handleClose();
            }
            else
            {
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

        handleError();
        return;
    }
}
void TcpConnection::send(const void* data, size_t len)
{
    if (m_closing || len == 0)
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
            ssize_t n = ::write(m_socket.get(), ptr, remaining);

            if (n > 0)
            {
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

            handleError();
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

        pauseReadingIfNeeded();
        if (!m_channel.isWriting())
        {
            m_channel.enableWriting();
        }
    }
}
void TcpConnection::handleWrite()
{
    if (m_closing)
    {
        return;
    }

    while (m_outputBuffer.readableBytes() > 0)
    {
        const ssize_t n = ::write(
            m_socket.get(),
            m_outputBuffer.readPtr(),
            m_outputBuffer.readableBytes()
        );

        if (n > 0)
        {
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

        handleError();
        return;
    }

    resumeReadingIfNeeded();
    if (m_closing || m_outputBuffer.readableBytes() != 0)
    {
        return;
    }

    if (m_readClosed)
    {
        handleClose();
        return;
    }

    if (m_channel.isWriting())
    {
        m_channel.disableWriting();
    }
}

void TcpConnection::pauseReadingIfNeeded()
{
    if (m_readPaused ||
        m_outputBuffer.readableBytes() < kHighWaterMark)
    {
        return;
    }

    m_channel.disableReading();
    m_readPaused = true;
    m_highWaterSince = std::chrono::steady_clock::now();
}

void TcpConnection::resumeReadingIfNeeded()
{
    if (!m_readPaused ||
        m_outputBuffer.readableBytes() > kLowWaterMark)
    {
        return;
    }

    if (!m_readClosed)
    {
        m_channel.enableReading();
    }
    m_readPaused = false;

    // Re-enabling EPOLLIN in ET mode may not create another edge for
    // bytes already in the socket. Drain them before returning.
    if (!m_readClosed)
    {
        handleRead();
    }
}

void TcpConnection::closeIfSlow(
    std::chrono::steady_clock::time_point now
)
{
    if (m_closing || !m_readPaused ||
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

    std::cerr << "TcpConnection error fd=" << fd()
              << ", error=" << error << '\n';

    handleClose();
}

void TcpConnection::handleClose()
{
    if (m_closing)
    {
        return;
    }

    m_closing = true;

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

Channel& TcpConnection::channel() noexcept
{
    return m_channel;
}

int TcpConnection::fd() const noexcept
{
    return m_socket.get();
}
} // namespace service::gateway