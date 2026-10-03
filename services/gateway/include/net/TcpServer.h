#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "net/Acceptor.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "net/TimerQueue.h"

namespace service::gateway
{
enum class TcpServerState
{
    Running,
    Draining,
    Stopped
};

class TcpServer
{
public:
    TcpServer(
        EventLoop* loop,
        const InetAddress& listenAddress,
        std::size_t maxConnections,
        std::chrono::milliseconds idleTimeout =
            std::chrono::seconds(60)
    );
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Call on the owning EventLoop thread. Stop accepting, send FIN
    // after queued output, then force-close stragglers at the deadline.
    void stop(
        std::chrono::milliseconds grace = std::chrono::seconds(5)
    );
    // Read these on the owning EventLoop thread.
    bool stopped() const noexcept;
    std::size_t connectionCount() const noexcept;
    std::size_t maxConnections() const noexcept;

private:
    void
    handleNewConnection(Socket&& socket, const InetAddress& peer);
    void handleConnectionClose(TcpConnection* connection);
    void removeClosedConnections();
    void handleSlowTimer();
    void handleIdleTimer(int fd);
    void handleStopTimeout();
    void finishStopIfDrained();

    EventLoop* m_loop;
    Acceptor m_acceptor;
    std::size_t m_maxConnections;
    std::chrono::milliseconds m_idleTimeout;
    TimerId m_slowTimerId{0};
    TimerId m_stopTimerId{0};
    TcpServerState m_state{TcpServerState::Running};
    std::shared_ptr<void> m_callbackGuard{
        std::make_shared<int>(0)
    };
    std::unordered_map<int, std::shared_ptr<TcpConnection>>
        m_connections;
    std::unordered_map<int, TimerId> m_idleTimers;
    std::unordered_set<int> m_pendingRemove;
    bool m_removeTaskQueued{false};
};
} // namespace service::gateway