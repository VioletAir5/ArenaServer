#include "net/TcpServer.h"

#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "net/ConnectionRuntime.h"
#include "net/ConnectionSchedulers.h"
#include "net/TcpConnection.h"

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
    if (!loop->isInLoopThread())
    {
        throw std::logic_error(
            "TcpServer requires base EventLoop thread"
        );
    }
    return loop;
}
} // namespace

// Connections, timers and deferred reclamation belong exclusively to
// loop. Base retains this worker state, but reads only its immutable
// loop pointer.
struct TcpServer::Worker : std::enable_shared_from_this<Worker>
{
    using Id = std::uint64_t;
    using RemovedCallback = std::function<void(Id)>;

    Worker(
        EventLoop* owner,
        std::chrono::milliseconds timeout,
        ConnectionHandlerFactory factory,
        ConnectionFeatureFactory features,
        RemovedCallback removed
    )
        : loop(owner)
        , idleTimeout(timeout)
        , handlerFactory(std::move(factory))
        , featureFactory(std::move(features))
        , onRemoved(std::move(removed))
    {
    }

    void addConnection(
        Id id,
        Socket socket,
        InetAddress peer,
        ConnectionContext::Clock::time_point acceptedAt
    )
    {
        std::shared_ptr<TcpConnection> connection;
        try
        {
            connection = std::make_shared<TcpConnection>(
                loop,
                std::move(socket)
            );
            connection->channel().tie(connection);
            auto context = std::make_shared<ConnectionContext>(
                connection->id(),
                std::move(peer),
                acceptedAt
            );
            auto runtime = makeConnectionRuntime(
                connection,
                context,
                connectionScheduler(*loop),
                handlerFactory,
                featureFactory
            );
            const std::weak_ptr<ConnectionRuntime>
                runtimeWeak = runtime;
            connection->setMessageCallback(
                [runtimeWeak](TcpConnection&, std::string_view bytes)
                {
                    if (auto r = runtimeWeak.lock())
                        r->received(std::as_bytes(std::span(bytes)));
                }
            );
            connection->setOutputCallback(
                [runtimeWeak](std::size_t bytes)
                {
                    if (auto r = runtimeWeak.lock())
                        r->outputChanged(bytes);
                }
            );
            const auto weak = weak_from_this();
            connection->setCloseCallback(
                [weak, runtimeWeak, id](TcpConnection*)
                {
                    if (auto r = runtimeWeak.lock())
                        r->transportClosed();
                    if (auto worker = weak.lock())
                    {
                        worker->deferRemoval(id);
                    }
                }
            );
            connections.emplace(id, Entry{runtime, connection.get()});
            runtime->start();
            auto [timer, inserted] = idleTimers.emplace(id, 0);
            (void)inserted;
            timer->second = loop->runAfter(
                idleTimeout,
                [weak, id]()
                {
                    if (auto worker = weak.lock())
                    {
                        worker->checkIdleConnection(id);
                    }
                }
            );
        }
        catch (const std::exception& error)
        {
            std::cerr << "connection setup failed: " << error.what()
                      << '\n';
            cancelIdleTimer(id);
            if (connection)
            {
                loop->removeChannel(&connection->channel());
                connections.erase(id);
                connection.reset();
            }
            socket.close();
            onRemoved(id);
        }
    }

    void deferRemoval(Id id)
    {
        cancelIdleTimer(id);
        if (!pendingRemove.insert(id).second || removalQueued)
        {
            return;
        }
        removalQueued = true;
        const auto weak = weak_from_this();
        loop->queueInLoop(
            [weak]()
            {
                if (auto worker = weak.lock())
                {
                    worker->removeClosedConnections();
                }
            }
        );
    }

    void removeClosedConnections()
    {
        for (Id id : pendingRemove)
        {
            const auto it = connections.find(id);
            if (it == connections.end())
            {
                continue;
            }
            loop->removeChannel(&it->second->channel());
            // No event guard remains: this runs after the Channel
            // batch. Destroy the connection/socket on owner before
            // notifying base.
            connections.erase(it);
            onRemoved(id);
        }
        pendingRemove.clear();
        removalQueued = false;
    }

    void cancelIdleTimer(Id id)
    {
        const auto it = idleTimers.find(id);
        if (it != idleTimers.end())
        {
            loop->cancelTimer(it->second);
            idleTimers.erase(it);
        }
    }

    void checkIdleConnection(Id id)
    {
        const auto it = connections.find(id);
        if (it == connections.end() || it->second->closing())
        {
            return;
        }
        const auto elapsed = std::chrono::steady_clock::now() -
                             it->second->lastActivity();
        if (elapsed >= idleTimeout)
        {
            std::cerr << "TcpConnection idle timeout fd="
                      << it->second->fd() << '\n';
            it->second->handleClose();
            return;
        }
        const auto weak = weak_from_this();
        idleTimers.at(id) = loop->runAfter(
            idleTimeout - elapsed,
            [weak, id]()
            {
                if (auto worker = weak.lock())
                {
                    worker->checkIdleConnection(id);
                }
            }
        );
    }

    void drain()
    {
        for (auto& [id, connection] : connections)
        {
            connection->shutdownWrite();
        }
    }

    void forceClose()
    {
        for (auto& [id, connection] : connections)
        {
            connection->handleClose();
        }
    }

    // Call only on owner. No active owner callback can overlap this.
    // In zero-worker mode removeChannel also invalidates the base
    // batch.
    void cleanup()
    {
        for (const auto& [id, timer] : idleTimers)
        {
            loop->cancelTimer(timer);
        }
        idleTimers.clear();
        for (const auto& [id, connection] : connections)
        {
            connection->setCloseCallback({});
            connection->close();
            loop->removeChannel(&connection->channel());
        }
        connections.clear();
        pendingRemove.clear();
        removalQueued = false;
    }

    EventLoop* const loop;
    const std::chrono::milliseconds idleTimeout;
    ConnectionHandlerFactory handlerFactory;
    ConnectionFeatureFactory featureFactory;
    const RemovedCallback onRemoved;
    struct Entry
    {
        std::shared_ptr<ConnectionRuntime> runtime;
        TcpConnection* transport; // Owned by runtime.
        TcpConnection* operator->() const noexcept
        {
            return transport;
        }
    };
    std::unordered_map<Id, Entry> connections;
    std::unordered_map<Id, TimerId> idleTimers;
    std::unordered_set<Id> pendingRemove;
    bool removalQueued{false};
};

TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& listenAddress,
    std::size_t maxConnections,
    std::chrono::milliseconds idleTimeout,
    std::size_t workerCount,
    ConnectionHandlerFactory handlerFactory,
    ConnectionFeatureFactory featureFactory
)
    : m_loop(requireLoop(loop))
    , m_threadPool(m_loop, workerCount)
    , m_acceptor(m_loop, listenAddress)
    , m_maxConnections(maxConnections)
    , m_workerCount(workerCount)
{
    if (idleTimeout <= std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument("idle timeout must be positive");
    }
    m_threadPool.start();
    const std::weak_ptr<void> guard = m_callbackGuard;
    for (auto* owner : m_threadPool.getAllLoops())
    {
        m_workers.push_back(std::make_shared<Worker>(
            owner,
            idleTimeout,
            handlerFactory,
            featureFactory,
            [base = m_loop, this, guard](std::uint64_t id)
            {
                // Never dereference TcpServer on a worker thread.
                base->queueInLoop(
                    [this, guard, id]()
                    {
                        if (guard.lock())
                        {
                            handleConnectionRemoved(id);
                        }
                    }
                );
            }
        ));
    }
    m_acceptor.setNewConnectionCallback(
        [this](Socket&& socket, const InetAddress& peer)
        { handleNewConnection(std::move(socket), peer); }
    );
}

TcpServer::~TcpServer() noexcept
{
    if (!m_loop->isInLoopThread())
    {
        std::terminate();
    }
    // All callbacks using this run on base; invalidation is
    // serialized.
    m_callbackGuard.reset();
    try
    {
        m_acceptor.stop();
        if (m_stopTimerId != 0)
        {
            m_loop->cancelTimer(m_stopTimerId);
        }
        cleanupWorkers();
    }
    catch (...)
    {
        // Losing owner cleanup cannot safely continue loop
        // destruction.
        std::terminate();
    }
}

void TcpServer::cleanupWorkers()
{
    // Queue every owner cleanup before waiting. Workers never wait
    // for base.
    std::vector<std::future<void>> completed;
    for (const auto& worker : m_workers)
    {
        if (worker->loop->isInLoopThread())
        {
            worker->cleanup();
        }
        else
        {
            auto signal = std::make_shared<std::promise<void>>();
            completed.push_back(signal->get_future());
            worker->loop->queueInLoop(
                [worker, signal]()
                {
                    try
                    {
                        worker->cleanup();
                        signal->set_value();
                    }
                    catch (...)
                    {
                        signal->set_exception(std::current_exception()
                        );
                    }
                }
            );
        }
    }
    std::exception_ptr firstError;
    for (auto& done : completed)
    {
        try
        {
            done.get();
        }
        catch (...)
        {
            if (!firstError)
            {
                firstError = std::current_exception();
            }
        }
    }
    if (firstError)
    {
        std::rethrow_exception(firstError);
    }
    // Cleanup has completed before quit; no task-draining assumption.
    m_threadPool.stop();
    m_workers.clear();
    m_admissions.clear();
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
    for (const auto& worker : m_workers)
    {
        worker->loop->runInLoop([worker]() { worker->drain(); });
    }
    if (m_admissions.empty())
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
    return m_admissions.size();
}

std::size_t TcpServer::maxConnections() const noexcept
{
    return m_maxConnections;
}

std::size_t TcpServer::workerCount() const noexcept
{
    return m_workerCount;
}

void TcpServer::handleStopTimeout()
{
    std::cerr << "TcpServer stop deadline reached, connections="
              << m_admissions.size() << '\n';
    m_stopTimerId = 0;
    for (const auto& worker : m_workers)
    {
        worker->loop->runInLoop([worker]() { worker->forceClose(); });
    }
    finishStopIfDrained();
}

void TcpServer::finishStopIfDrained()
{
    if (m_state != TcpServerState::Draining || !m_admissions.empty())
    {
        return;
    }
    if (m_stopTimerId != 0)
    {
        m_loop->cancelTimer(m_stopTimerId);
        m_stopTimerId = 0;
    }
    cleanupWorkers();
    m_state = TcpServerState::Stopped;
    std::cerr << "TcpServer stopped" << '\n';
}

void TcpServer::handleNewConnection(
    Socket&& socket, const InetAddress& peer
)
{
    const auto acceptedAt = ConnectionContext::Clock::now();
    if (m_state != TcpServerState::Running)
    {
        return;
    }
    if (m_admissions.size() >= m_maxConnections)
    {
        std::cerr << "connection limit reached, peer="
                  << peer.toString() << '\n';
        return;
    }
    if (m_nextConnectionId == 0)
    {
        throw std::overflow_error("connection identity exhausted");
    }

    auto* owner = m_threadPool.getNextLoop();
    std::shared_ptr<Worker> selected;
    for (const auto& worker : m_workers)
    {
        if (worker->loop == owner)
        {
            selected = worker;
            break;
        }
    }
    const auto id = m_nextConnectionId++;
    const int fd = socket.get();
    // std::function requires a copyable capture; the Socket remains
    // uniquely owned and is moved into TcpConnection on its selected
    // worker.
    auto accepted = std::make_shared<Socket>(std::move(socket));
    m_admissions.insert(id);
    try
    {
        owner->runInLoop(
            [selected, id, accepted, peer, acceptedAt]()
            {
                selected->addConnection(
                    id,
                    std::move(*accepted),
                    peer,
                    acceptedAt
                );
            }
        );
    }
    catch (...)
    {
        m_admissions.erase(id);
        throw;
    }
    std::cout << "new connection fd=" << fd
              << ", peer=" << peer.toString()
              << ", connections=" << m_admissions.size() << '\n';
}

void TcpServer::handleConnectionRemoved(std::uint64_t id)
{
    if (m_admissions.erase(id) == 0)
    {
        return;
    }
    std::cout << "connection removed id=" << id
              << ", connections=" << m_admissions.size() << '\n';
    finishStopIfDrained();
}
} // namespace service::gateway
