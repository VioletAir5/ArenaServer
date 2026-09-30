#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include "os/FileDescriptor.h"

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
    if (m_loop != nullptr && m_listening)
    {
        m_loop->removeChannel(&m_listenChannel);

        m_listening = false;
    }
}

void Acceptor::setNewConnectionCallback(NewConnectionCallback callback
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
    while (true)
    {
        sockaddr_in peerAddress{};

        socklen_t peerLength = sizeof(peerAddress);

        int connfd = ::accept4(
            m_listenSocket.get(),
            reinterpret_cast<sockaddr*>(&peerAddress),
            &peerLength,
            SOCK_NONBLOCK | SOCK_CLOEXEC
        );

        if (connfd >= 0)
        {
            Socket clientSocket{FileDescriptor(connfd)};

            InetAddress peer(peerAddress);

            if (m_newConnectionCallback)
            {
                m_newConnectionCallback(
                    std::move(clientSocket),
                    peer
                );
            }

            continue;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }

        std::cerr << "accept4 failed errno=" << errno << '\n';

        return;
    }
}

} // namespace service::gateway