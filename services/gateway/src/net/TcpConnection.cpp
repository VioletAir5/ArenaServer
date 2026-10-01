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
    m_channel.setEvents(EPOLLIN | EPOLLRDHUP);

    m_channel.setReadCallback([this]() { handleRead(); });

    m_channel.setWriteCallback([this]() { handleWrite(); });

    m_channel.setCloseCallback([this]() { handleClose(); });

    m_channel.setErrorCallback([this]() { handleError(); });

    m_loop->addChannel(&m_channel);
}

void TcpConnection::handleRead()
{
    if (m_closing || m_readClosed)
    {
        return;
    }

    char buffer[8192];
    bool peerClosed = false;

    while (true)
    {
        ssize_t n = ::read(m_socket.get(), buffer, sizeof(buffer));

        if (n > 0)
        {
            if (!m_inputBuffer.append(buffer, static_cast<size_t>(n)))
            {
                handleError();
                return;
            }
            continue;
        }

        if (n == 0)
        {
            peerClosed = true;
            break;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            break;
        }

        handleError();
        return;
    }

    if (m_inputBuffer.readableBytes() > 0)
    {
        const size_t readable = m_inputBuffer.readableBytes();
        if (m_messageCallback)
        {
            m_messageCallback(
                *this,
                std::string_view(m_inputBuffer.readPtr(), readable)
            );
        }
        m_inputBuffer.skip(readable);
    }

    if (m_closing)
    {
        return;
    }

    if (peerClosed)
    {
        m_readClosed = true;
        if (m_outputBuffer.readableBytes() == 0)
        {
            handleClose();
        }
        else
        {
            m_channel.setEvents(EPOLLOUT);
            m_loop->updateChannel(&m_channel);
        }
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
        if (!m_outputBuffer.append(ptr, remaining))
        {
            handleError();
            return;
        }

        if ((m_channel.events() & EPOLLOUT) == 0)
        {
            m_channel.setEvents(m_channel.events() | EPOLLOUT);

            m_loop->updateChannel(&m_channel);
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
        ssize_t n = ::write(
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
            return;
        }

        handleError();
        return;
    }

    if (m_readClosed)
    {
        handleClose();
        return;
    }

    if (m_channel.events() & EPOLLOUT)
    {
        m_channel.setEvents(m_channel.events() & ~EPOLLOUT);

        m_loop->updateChannel(&m_channel);
    }
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