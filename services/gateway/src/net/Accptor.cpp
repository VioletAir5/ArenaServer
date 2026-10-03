#include <sys/socket.h>

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

    m_loop->removeChannel(&m_listenChannel);
    m_listening = false;
    m_listenSocket.close();
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
            std::cerr << "accept failed: " << error.what() << '\n';
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
} // namespace service::gateway