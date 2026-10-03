#include "net/TcpServer.h"

#include <chrono>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "net/EventLoop.h"
#include "net/InetAddress.h"

namespace service::gateway
{
namespace
{
EventLoop* requireLoop(EventLoop* loop)
{
    if (loop == nullptr)
    {
        throw std::invalid_argument(
            "TcpServer requires valid EventLoop"
        );
    }
    return loop;
}
} // namespace

TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& listenAddress,
    std::size_t maxConnections,
    std::chrono::milliseconds idleTimeout
)
    : m_loop(requireLoop(loop))
    , m_acceptor(m_loop, listenAddress)
    , m_maxConnections(maxConnections)
    , m_idleTimeout(idleTimeout)
{
    if (m_idleTimeout <= std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument("idle timeout must be positive");
    }

    m_acceptor.setNewConnectionCallback(
        [this](Socket&& socket, const InetAddress& peer)
        { handleNewConnection(std::move(socket), peer); }
    );
    m_slowTimerId = m_loop->runEvery(
        std::chrono::seconds(1),
        [this]() { handleSlowTimer(); }
    );
}

TcpServer::~TcpServer()
{
    // Pending EventLoop functors capture this only behind this guard.
    m_callbackGuard.reset();
    m_acceptor.stop();
    m_loop->cancelTimer(m_slowTimerId);
    if (m_stopTimerId != 0)
    {
        m_loop->cancelTimer(m_stopTimerId);
    }
    for (const auto& [fd, id] : m_idleTimers)
    {
        m_loop->cancelTimer(id);
    }
    for (const auto& [fd, connection] : m_connections)
    {
        connection->setCloseCallback({});
        m_loop->removeChannel(&connection->channel());
    }
}

void TcpServer::stop(std::chrono::milliseconds grace)
{
    if (!m_loop->isInLoopThread())
    {
        throw std::logic_error(
            "TcpServer::stop requires EventLoop thread"
        );
    }
    if (grace < std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument("stop grace must be nonnegative");
    }
    if (m_state != TcpServerState::Running)
    {
        return;
    }

    m_acceptor.stop();
    m_state = TcpServerState::Draining;
    for (auto& [fd, connection] : m_connections)
    {
        connection->shutdownWrite();
    }

    if (m_connections.empty())
    {
        finishStopIfDrained();
    }
    else if (grace == std::chrono::milliseconds::zero())
    {
        handleStopTimeout();
    }
    else
    {
        const std::weak_ptr<void> guard = m_callbackGuard;
        try
        {
            m_stopTimerId = m_loop->runAfter(
                grace,
                [this, guard]()
                {
                    if (guard.lock())
                    {
                        handleStopTimeout();
                    }
                }
            );
        }
        catch (...)
        {
            handleStopTimeout();
            throw;
        }
    }
}

bool TcpServer::stopped() const noexcept
{
    return m_state == TcpServerState::Stopped;
}

std::size_t TcpServer::connectionCount() const noexcept
{
    return m_connections.size();
}

std::size_t TcpServer::maxConnections() const noexcept
{
    return m_maxConnections;
}

void TcpServer::handleStopTimeout()
{
    std::cerr << "TcpServer stop deadline reached, connections="
              << m_connections.size() << '\n';
    m_stopTimerId = 0;
    for (auto& [fd, connection] : m_connections)
    {
        connection->handleClose();
    }
    finishStopIfDrained();
}

void TcpServer::finishStopIfDrained()
{
    if (m_state != TcpServerState::Draining ||
        !m_connections.empty())
    {
        return;
    }

    m_state = TcpServerState::Stopped;
    std::cerr << "TcpServer stopped" << '\n';
    m_loop->cancelTimer(m_slowTimerId);
    m_slowTimerId = 0;
    if (m_stopTimerId != 0)
    {
        m_loop->cancelTimer(m_stopTimerId);
        m_stopTimerId = 0;
    }
}

void TcpServer::handleSlowTimer()
{
    const auto now = std::chrono::steady_clock::now();
    for (auto& [fd, connection] : m_connections)
    {
        connection->closeIfSlow(now);
    }
}

void TcpServer::handleIdleTimer(int fd)
{
    const auto it = m_connections.find(fd);
    if (it == m_connections.end() || it->second->closing())
    {
        return;
    }

    const auto elapsed = std::chrono::steady_clock::now() -
                         it->second->lastActivity();
    if (elapsed >= m_idleTimeout)
    {
        std::cerr << "TcpConnection idle timeout fd=" << fd << '\n';
        it->second->handleClose();
        return;
    }

    const auto remaining = m_idleTimeout - elapsed;
    m_idleTimers[fd] = m_loop->runAfter(
        remaining,
        [this, fd]() { handleIdleTimer(fd); }
    );
}

void TcpServer::handleNewConnection(
    Socket&& socket, const InetAddress& peer
)
{
    if (m_state != TcpServerState::Running)
    {
        return;
    }
    if (m_connections.size() >= m_maxConnections)
    {
        std::cerr << "connection limit reached, peer="
                  << peer.toString() << '\n';
        return;
    }

    const int fd = socket.get();
    auto connection = std::make_shared<TcpConnection>(
        m_loop,
        std::move(socket)
    );
    connection->channel().tie(connection);
    connection->setMessageCallback(
        [](TcpConnection& conn, std::string_view message)
        { conn.send(message.data(), message.size()); }
    );
    connection->setHighWaterMarkCallback(
        [](TcpConnection& conn, std::size_t) { conn.pauseReading(); }
    );
    connection->setLowWaterMarkCallback(
        [](TcpConnection& conn, std::size_t) { conn.resumeReading(); }
    );
    connection->setCloseCallback([this](TcpConnection* conn)
                                 { handleConnectionClose(conn); });

    m_connections.emplace(fd, std::move(connection));
    m_idleTimers[fd] = m_loop->runAfter(
        m_idleTimeout,
        [this, fd]() { handleIdleTimer(fd); }
    );

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

    const int fd = connection->fd();
    const auto timer = m_idleTimers.find(fd);
    if (timer != m_idleTimers.end())
    {
        m_loop->cancelTimer(timer->second);
        m_idleTimers.erase(timer);
    }

    if (!m_pendingRemove.insert(fd).second)
    {
        return;
    }
    if (m_removeTaskQueued)
    {
        return;
    }
    m_removeTaskQueued = true;
    const std::weak_ptr<void> guard = m_callbackGuard;
    m_loop->queueInLoop(
        [this, guard]()
        {
            if (guard.lock())
            {
                removeClosedConnections();
            }
        }
    );
}

void TcpServer::removeClosedConnections()
{
    for (int fd : m_pendingRemove)
    {
        const auto it = m_connections.find(fd);
        if (it == m_connections.end())
        {
            continue;
        }

        std::shared_ptr<TcpConnection> connection = it->second;
        m_loop->removeChannel(&connection->channel());
        m_connections.erase(it);
        std::cout << "connection removed fd=" << fd
                  << ", connections=" << m_connections.size() << '\n';
    }
    m_pendingRemove.clear();
    m_removeTaskQueued = false;
    finishStopIfDrained();
}
} // namespace service::gateway