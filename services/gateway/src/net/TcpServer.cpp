#include "net/TcpServer.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include "net/EventLoop.h"
#include "net/InetAddress.h"

namespace service::gateway
{
namespace
{
service::FileDescriptor createSlowTimer()
{
    const int fd = ::timerfd_create(
        CLOCK_MONOTONIC,
        TFD_NONBLOCK | TFD_CLOEXEC
    );
    if (fd < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_create"
        );
    }
    return service::FileDescriptor(fd);
}
} // namespace

TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& listenAddress,
    std::size_t maxConnections
)
    : m_loop(loop)
    , m_acceptor(m_loop, listenAddress)
    , m_maxConnections(maxConnections)
    , m_slowTimerFd(createSlowTimer())
    , m_slowTimerChannel(m_slowTimerFd.get())
{
    if (m_loop == nullptr)
    {
        throw std::invalid_argument(
            "TcpServer requires valid EventLoop"
        );
    }

    itimerspec interval{};
    interval.it_value.tv_sec = 1;
    interval.it_interval.tv_sec = 1;
    if (::timerfd_settime(
            m_slowTimerFd.get(),
            0,
            &interval,
            nullptr
        ) != 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "timerfd_settime"
        );
    }

    m_slowTimerChannel.setEvents(EPOLLIN);
    m_slowTimerChannel.setReadCallback([this]() { handleSlowTimer(); }
    );
    m_loop->addChannel(&m_slowTimerChannel);
    m_acceptor.setNewConnectionCallback(
        [this](Socket&& socket, const InetAddress& peer)
        { handleNewConnection(std::move(socket), peer); }
    );
}

TcpServer::~TcpServer()
{
    m_loop->removeChannel(&m_slowTimerChannel);
}

void TcpServer::handleSlowTimer()
{
    std::uint64_t expirations = 0;
    ssize_t count;
    do
    {
        count = ::read(
            m_slowTimerFd.get(),
            &expirations,
            sizeof(expirations)
        );
    } while (count < 0 && errno == EINTR);

    if (count < 0 && errno == EAGAIN)
    {
        return;
    }
    if (count != static_cast<ssize_t>(sizeof(expirations)))
    {
        throw std::system_error(
            count < 0 ? errno : EIO,
            std::generic_category(),
            "read slow-client timer"
        );
    }

    const auto now = std::chrono::steady_clock::now();
    for (auto& [fd, connection] : m_connections)
    {
        connection->closeIfSlow(now);
    }
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