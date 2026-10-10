/** @file
 * @brief Accepts TCP clients and owns their event-loop connections.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "net/Acceptor.h"
#include "net/ConnectionFeature.h"
#include "net/ConnectionHandler.h"
#include "net/EventLoopThreadPool.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/TimerQueue.h"

namespace service::gateway
{
/** @brief Tracks server startup and draining state. */
enum class TcpServerState
{
    Running,
    Draining,
    Stopped
};

/** @brief Accepts TCP clients and owns their event-loop connections. */
class TcpServer final : public ITcpServer
{
public:
    // Construct, query, stop and destroy on the base loop thread.
    // The base loop must outlive the server. Zero workers uses base
    // I/O.
    /** @brief Starts listening and prepares worker loops.
     * @param[in] loop Base loop that owns server control and outlives this object.
     * @param[in] listenAddress Local address to bind.
     * @param[in] maxConnections Maximum admitted connections.
     * @param[in] idleTimeout Idle time allowed for each connection.
     * @param[in] workerCount Number of worker loops; zero uses the base loop.
     * @param[in] handlerFactory Creates packet handlers for connections.
     * @param[in] featureFactory Creates per-connection policy features.
     */
    TcpServer(
        EventLoop* loop,
        const InetAddress& listenAddress,
        std::size_t maxConnections,
        std::chrono::milliseconds idleTimeout =
            std::chrono::seconds(60),
        std::size_t workerCount = 0,
        ConnectionHandlerFactory handlerFactory = {},
        ConnectionFeatureFactory featureFactory = {}
    );
    /** @brief Releases owned resources. */
    ~TcpServer() noexcept override;

    /** @brief Copy construction is disabled. */
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Stop accepting, request FIN on connection owners, then force
    // close stragglers at the deadline. Stopped includes worker
    // cleanup and join; it does not quit the externally owned base
    // loop.
    void stop(std::chrono::milliseconds grace = kDefaultStopGrace)
        override;
    // Read only on the base EventLoop thread. Count includes accepted
    // sockets queued for worker registration until cleanup is
    // acknowledged.
    bool stopped() const noexcept override;
    /** @brief Returns the number of active connections. */
    std::size_t connectionCount() const noexcept override;
    std::size_t maxConnections() const noexcept override;
    std::size_t workerCount() const noexcept override;

private:
    struct Worker;

    /** @brief Handles new connection. */
    void handleNewConnection(Socket&&, const InetAddress&);
    /** @brief Handles connection removed. */
    void handleConnectionRemoved(std::uint64_t id);
    /** @brief Handles stop timeout. */
    void handleStopTimeout();
    void finishStopIfDrained();
    void cleanupWorkers();

    /// Event loop that owns this object and its callbacks.
    EventLoop* m_loop;
    // The pool outlives worker state and all connection cleanup.
    EventLoopThreadPool m_threadPool;
    /// Stores the acceptor.
    Acceptor m_acceptor;
    /// Stores the max connections.
    const std::size_t m_maxConnections;
    /// Stores the worker count.
    const std::size_t m_workerCount;
    /// Stores the stop timer id.
    TimerId m_stopTimerId{0};
    /// Current lifecycle state.
    TcpServerState m_state{TcpServerState::Running};
    /// Stores the callback guard.
    std::shared_ptr<void> m_callbackGuard{std::make_shared<int>(0)};
    /// Stores the workers.
    std::vector<std::shared_ptr<Worker>> m_workers;
    // IDs only; TcpConnection owners live in Worker.
    // Only base mutates admission records; it never touches
    // connections.
    std::unordered_set<std::uint64_t> m_admissions;
    /// Stores the next connection id.
    std::uint64_t m_nextConnectionId{1};
};
} // namespace service::gateway
