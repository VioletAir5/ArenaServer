/** @file
 * @brief Provides the accptor implementation.
 */
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "net/Acceptor.h"
#include "net/EventLoop.h"

namespace service::gateway
{

Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddress)
    : m_loop(loop)
    , m_listenSocket(Socket::createTcpNonBlocking())
    , m_listenChannel(m_listenSocket.get())
{
    if (m_loop == nullptr)
    {
        throw std::invalid_argument(
            "Acceptor requires valid EventLoop"
        );
    }

    m_reserveFd = service::FileDescriptor(
        ::open("/dev/null", O_RDONLY | O_CLOEXEC)
    );
    if (!m_reserveFd.valid())
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "accept reserve fd"
        );
    }

    m_listenSocket.setReuseAddr(true);

    m_listenSocket.bind(listenAddress);

    m_listenSocket.listen(SOMAXCONN);

    m_listenChannel.setEvents(EPOLLIN);

    m_listenChannel.setReadCallback([this]() { handleRead(); });

    m_loop->addChannel(&m_listenChannel);

    m_listening = true;
}

Acceptor::~Acceptor()
{
    stop();
}

void Acceptor::stop()
{
    if (!m_listening)
    {
        return;
    }

    if (m_retryTimerId != 0)
    {
        m_loop->cancelTimer(m_retryTimerId);
        m_retryTimerId = 0;
    }
    m_loop->removeChannel(&m_listenChannel);
    m_listening = false;
    m_listenSocket.close();
    m_reserveFd = service::FileDescriptor();
}

void Acceptor::setNewConnectionCallback(
    NewConnectionCallback callback
)
{
    m_newConnectionCallback = std::move(callback);
}

bool Acceptor::listening() const noexcept
{
    return m_listening;
}

void Acceptor::handleRead()
{
    while (m_listening)
    {
        InetAddress peer;
        std::optional<Socket> clientSocket;
        try
        {
            clientSocket = m_listenSocket.accept(&peer);
        }
        catch (const std::system_error& error)
        {
            const int code = error.code().value();
            std::cerr << "accept failed: " << error.what() << '\n';
            if (code == EMFILE || code == ENFILE || code == ENOBUFS ||
                code == ENOMEM)
            {
                suspendForResourceError();
            }
            return;
        }

        if (!clientSocket)
        {
            return;
        }

        if (m_newConnectionCallback)
        {
            m_newConnectionCallback(std::move(*clientSocket), peer);
        }
    }
}
void Acceptor::suspendForResourceError()
{
    // A queued peer keeps an LT listener readable. Disable interest
    // before retrying so epoll_wait can sleep and service other work.
    m_listenChannel.disableReading();
    m_loop->updateChannel(&m_listenChannel);

    m_reserveFd = service::FileDescriptor();
    int rejected;
    do
    {
        rejected = ::accept4(
            m_listenSocket.get(),
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC
        );
    } while (rejected < 0 && errno == EINTR);
    {
        service::FileDescriptor rejectedPeer(rejected);
    }
    m_reserveFd = service::FileDescriptor(
        ::open("/dev/null", O_RDONLY | O_CLOEXEC)
    );
    scheduleRetry();
}

void Acceptor::scheduleRetry()
{
    try
    {
        m_retryTimerId = m_loop->runAfter(
            kRetryDelay,
            [this]() { retryListening(); }
        );
    }
    catch (...)
    {
        // If no retry can be scheduled, close the listener rather
        // than leaving it permanently suspended or spinning.
        stop();
        throw;
    }
}

void Acceptor::retryListening()
{
    m_retryTimerId = 0;
    if (!m_listening)
    {
        return;
    }
    if (!m_reserveFd.valid())
    {
        m_reserveFd = service::FileDescriptor(
            ::open("/dev/null", O_RDONLY | O_CLOEXEC)
        );
        if (!m_reserveFd.valid())
        {
            scheduleRetry();
            return;
        }
    }
    m_listenChannel.enableReading();
    m_loop->updateChannel(&m_listenChannel);
    // This listener is LT: an outstanding accept queue becomes
    // readable again even without a new incoming connection.
}

} // namespace service::gateway