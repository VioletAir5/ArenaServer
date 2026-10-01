#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "net/Acceptor.h"
#include "net/Channel.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{

class TcpServer
{
public:
    TcpServer(
        EventLoop* loop,
        const InetAddress& listenAddress,
        std::size_t maxConnections
    );

    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    std::size_t connectionCount() const noexcept;
    std::size_t maxConnections() const noexcept;

private:
    void
    handleNewConnection(Socket&& socket, const InetAddress& peer);

    void handleConnectionClose(TcpConnection* connection);

    void removeClosedConnections();
    void handleSlowTimer();

private:
    EventLoop* m_loop;

    Acceptor m_acceptor;

    std::size_t m_maxConnections;

    service::FileDescriptor m_slowTimerFd;
    Channel m_slowTimerChannel;

    std::unordered_map<int, std::unique_ptr<TcpConnection>>
        m_connections;

    std::unordered_set<int> m_pendingRemove;

    bool m_removeTaskQueued{false};
};

} // namespace service::gateway