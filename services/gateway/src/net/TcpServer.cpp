#include "net/TcpServer.h"

#include <cstddef>
#include <iostream>
#include <string_view>

#include "net/EventLoop.h"
#include "net/InetAddress.h"

namespace service::gateway
{
TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& listenAddress,
    std::size_t maxConnections
)
    : m_loop(loop)
    , m_acceptor(m_loop, listenAddress)
    , m_maxConnections(maxConnections)

{
    if (m_loop == nullptr)
    {
        throw std::invalid_argument(
            "TcpServer requires valid EventLoop"
        );
    }

    m_acceptor.setNewConnectionCallback(
        [this](Socket&& socket, const InetAddress& peer)
        { handleNewConnection(std::move(socket), peer); }
    );
}

void TcpServer::handleNewConnection(
    Socket&& socket, const InetAddress& peer
)
{
    if (m_connections.size() >= m_maxConnections)
    {
        std::cerr << "connection limit reached, peer="
                  << peer.toString() << '\n';

        // 什么都不用做
        // socket 离开函数自动析构
        // FileDescriptor 自动 close
        return;
    }

    int fd = socket.get();

    auto connection = std::make_unique<TcpConnection>(
        m_loop,
        std::move(socket)
    );

    connection->setMessageCallback(
        [](TcpConnection& conn, std::string_view message)
        { conn.send(message.data(), message.size()); }
    );

    connection->setCloseCallback([this](TcpConnection* conn)
                                 { handleConnectionClose(conn); });

    m_connections.emplace(fd, std::move(connection));

    std::cout << "new connection fd=" << fd
              << ", peer=" << peer.toString()
              << ", connections=" << m_connections.size() << '\n';
}

void TcpServer::handleConnectionClose(TcpConnection* connection)
{
    if (connection == nullptr)
    {
        return;
    }

    int fd = connection->fd();

    if (!m_pendingRemove.insert(fd).second)
    {
        return;
    }

    if (m_removeTaskQueued)
    {
        return;
    }

    m_removeTaskQueued = true;

    m_loop->queueInLoop([this]() { removeClosedConnections(); });
}

void TcpServer::removeClosedConnections()
{
    for (int fd : m_pendingRemove)
    {
        auto it = m_connections.find(fd);

        if (it == m_connections.end())
        {
            continue;
        }

        TcpConnection* connection = it->second.get();

        m_loop->removeChannel(&connection->channel());

        m_connections.erase(it);

        std::cout << "connection removed fd=" << fd
                  << ", connections=" << m_connections.size() << '\n';
    }

    m_pendingRemove.clear();

    m_removeTaskQueued = false;
}

} // namespace service::gateway