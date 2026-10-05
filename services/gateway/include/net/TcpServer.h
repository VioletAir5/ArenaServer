#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "net/Acceptor.h"
#include "net/EventLoopThreadPool.h"
#include "net/InetAddress.h"
#include "net/ITcpServer.h"
#include "net/TimerQueue.h"

namespace service::gateway
{
enum class TcpServerState
{
    Running,
    Draining,
    Stopped
};

class TcpServer final : public ITcpServer
{
public:
    // Construct, query, stop and destroy on the base loop thread.
    // The base loop must outlive the server. Zero workers uses base
    // I/O.
    TcpServer(
        EventLoop* loop,
        const InetAddress& listenAddress,
        std::size_t maxConnections,
        std::chrono::milliseconds idleTimeout =
            std::chrono::seconds(60),
        std::size_t workerCount = 0
    );
    ~TcpServer() noexcept override;

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Stop accepting, request FIN on connection owners, then force
    // close stragglers at the deadline. Stopped includes worker
    // cleanup and join; it does not quit the externally owned base
    // loop.
    void stop(
        std::chrono::milliseconds grace = kDefaultStopGrace) override;
    // Read only on the base EventLoop thread. Count includes accepted
    // sockets queued for worker registration until cleanup is
    // acknowledged.
    bool stopped() const noexcept override;
    std::size_t connectionCount() const noexcept override;
    std::size_t maxConnections() const noexcept override;
    std::size_t workerCount() const noexcept override;

private:
    struct Worker;

    void handleNewConnection(Socket&&, const InetAddress&);
    void handleConnectionRemoved(std::uint64_t id);
    void handleStopTimeout();
    void finishStopIfDrained();
    void cleanupWorkers();

    EventLoop* m_loop;
    // The pool outlives worker state and all connection cleanup.
    EventLoopThreadPool m_threadPool;
    Acceptor m_acceptor;
    const std::size_t m_maxConnections;
    const std::size_t m_workerCount;
    TimerId m_stopTimerId{0};
    TcpServerState m_state{TcpServerState::Running};
    std::shared_ptr<void> m_callbackGuard{std::make_shared<int>(0)};
    std::vector<std::shared_ptr<Worker>> m_workers;
    // IDs only; TcpConnection owners live in Worker.
    // Only base mutates admission records; it never touches connections.
    std::unordered_set<std::uint64_t> m_admissions;
    std::uint64_t m_nextConnectionId{1};
};
} // namespace service::gateway
