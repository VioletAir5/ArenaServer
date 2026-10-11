# services/gateway/src/net/AsioTcpServer.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；818行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Accepts and manages TCP connections with Boost.Asio.
 */
#include "net/AsioTcpServer.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <future>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AsioConnect.h"
#include "net/ConnectionRuntime.h"
#include "net/ConnectionSchedulers.h"

namespace service::gateway
{
namespace
{
using Context = boost::asio::io_context;
using Tcp = boost::asio::ip::tcp;
using Error = boost::system::error_code;

// Only the native descriptor crosses contexts. A socket object stays
// bound to its original Asio service; releasing that object on
// another context's thread would introduce lifetime and service
// ownership hazards.
struct AcceptedFd
{
    explicit AcceptedFd(int value)
        : fd(value)
    {
    }
    ~AcceptedFd()
    {
        if (fd >= 0)
        {
            ::close(fd);
        }
    }
    AcceptedFd(const AcceptedFd&) = delete;
    AcceptedFd& operator=(const AcceptedFd&) = delete;
    int take() noexcept
    {
        return std::exchange(fd, -1);
    }
    int fd;
};

struct AsioWorker : std::enable_shared_from_this<AsioWorker>
{
    using Guard = boost::asio::executor_work_guard<
        Context::executor_type>;
    std::unique_ptr<Context> ownedContext;
    Context& context;
    // Prime the reactor before startup is acknowledged, so
    // worker-internal descriptors don't appear as apparent connection
    // leaks after READY.
    boost::asio::steady_timer prime;
    std::optional<Guard> guard;
    std::thread thread;
    const std::chrono::milliseconds idleTimeout;
    ConnectionHandlerFactory handlerFactory;
    ConnectionFeatureFactory featureFactory;
    std::function<void(std::uint64_t)> onRemoved;
    struct Entry
    {
        std::shared_ptr<ConnectionRuntime> runtime;
        AsioConnect* transport;
        AsioConnect* operator->() const noexcept
        {
            return transport;
        }
    };
    std::unordered_map<std::uint64_t, Entry> connections;
    std::unordered_set<std::uint64_t> removals;
    bool removalQueued{false};
    bool draining{false};
    bool forceClosing{false};
    bool cleaning{false};

    AsioWorker(
        Context& base,
        bool owned,
        std::chrono::milliseconds idle,
        ConnectionHandlerFactory factory,
        ConnectionFeatureFactory features,
        std::function<void(std::uint64_t)> removed
    )
        : ownedContext(owned ? std::make_unique<Context>() : nullptr)
        , context(ownedContext ? *ownedContext : base)
        , prime(context)
        , idleTimeout(idle)
        , handlerFactory(std::move(factory))
        , featureFactory(std::move(features))
        , onRemoved(std::move(removed))
    {
        if (ownedContext)
        {
            guard.emplace(context.get_executor());
        }
    }

    ~AsioWorker()
    {
        // Every path, including a later worker construction failure,
        // must join on the base thread before releasing this context.
        if (thread.joinable())
        {
            std::terminate();
        }
    }

    void start()
    {
        if (!ownedContext)
        {
            return;
        }
        thread = std::thread(
            [this]
            {
                for (;;)
                {
                    try
                    {
                        context.run();
                        break;
                    }
                    catch (const std::exception& e)
                    {
                        std::cerr << "Asio worker handler failed: "
                                  << e.what() << '\n';
                    }
                    catch (...)
                    {
                        std::cerr << "Asio worker handler failed\n";
                    }
                }
            }
        );
    }

    void dispatch(std::function<void()> action)
    {
        if (!ownedContext)
        {
            action();
        }
        else
        {
            boost::asio::post(context, std::move(action));
        }
    }

    void
    add(std::uint64_t ticket,
        const std::shared_ptr<AcceptedFd>& accepted,
        InetAddress peer,
        ConnectionContext::Clock::time_point acceptedAt)
    {
        if (cleaning || forceClosing)
        {
            onRemoved(ticket);
            return;
        }
        try
        {
            Tcp::socket socket(context);
            Error error;
            socket.assign(Tcp::v4(), accepted->fd, error);
            if (error)
            {
                throw boost::system::system_error(error);
            }
            accepted->take();
            auto connection = std::make_shared<AsioConnect>(
                context,
                std::move(socket),
                idleTimeout
            );
            auto contextInfo = std::make_shared<ConnectionContext>(
                connection->id(),
                std::move(peer),
                acceptedAt
            );
            auto runtime = makeConnectionRuntime(
                connection,
                contextInfo,
                connectionScheduler(connection->executor()),
                handlerFactory,
                featureFactory
            );
            const std::weak_ptr<ConnectionRuntime>
                runtimeWeak = runtime;
            connection->setMessageCallback(
                [runtimeWeak](IConnection&, auto bytes)
                {
                    if (auto r = runtimeWeak.lock())
                        r->received(bytes);
                }
            );
            connection->setOutputCallback(
                [runtimeWeak](std::size_t bytes)
                {
                    if (auto r = runtimeWeak.lock())
                        r->outputChanged(bytes);
                }
            );
            connection->setCloseCallback(
                [weak = weak_from_this(),
                 runtimeWeak,
                 ticket](AsioConnect*)
                {
                    if (auto r = runtimeWeak.lock())
                        r->transportClosed();
                    if (auto worker = weak.lock())
                    {
                        worker->deferRemove(ticket);
                    }
                }
            );
            connections.emplace(
                ticket,
                Entry{runtime, connection.get()}
            );
            runtime->start();
            connection->start();
            if (draining)
            {
                connection->shutdown();
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Asio connection setup failed: " << e.what()
                      << '\n';
            auto it = connections.find(ticket);
            if (it != connections.end())
            {
                it->second->setCloseCallback({});
                it->second->close();
                connections.erase(it);
            }
            onRemoved(ticket);
        }
    }

    void deferRemove(std::uint64_t ticket)
    {
        if (cleaning)
        {
            return;
        }
        removals.insert(ticket);
        if (removalQueued)
        {
            return;
        }
        removalQueued = true;
        boost::asio::post(
            context,
            [weak = weak_from_this()]
            {
                auto worker = weak.lock();
                if (!worker || worker->cleaning)
                {
                    return;
                }
                worker->removalQueued = false;
                auto pending = std::move(worker->removals);
                worker->removals.clear();
                for (auto ticket : pending)
                {
                    if (worker->connections.erase(ticket) != 0)
                    {
                        worker->onRemoved(ticket);
                    }
                }
            }
        );
    }

    void drain()
    {
        draining = true;
        for (const auto& [ticket, connection] : connections)
        {
            (void)ticket;
            connection->shutdown();
        }
    }

    void forceClose()
    {
        forceClosing = true;
        for (const auto& [ticket, connection] : connections)
        {
            (void)ticket;
            connection->close();
        }
    }

    void cleanup()
    {
        cleaning = true;
        for (const auto& [ticket, connection] : connections)
        {
            (void)ticket;
            connection->setCloseCallback({});
            connection->close();
        }
        connections.clear();
        removals.clear();
        guard.reset();
        // run() drains cancelled completions naturally. stop() would
        // abandon ready handlers, retaining buffers until context
        // destruction.
    }
};
} // namespace

struct AsioTcpServer::Impl : std::enable_shared_from_this<Impl>
{
    enum class State
    {
        Running,
        Draining,
        Stopped
    };
    Context& base;
    const std::thread::id owner{std::this_thread::get_id()};
    Tcp::acceptor acceptor;
    boost::asio::executor_work_guard<Context::executor_type>
        baseGuard;
    boost::asio::steady_timer retryTimer;
    boost::asio::steady_timer stopTimer;
    const std::size_t capacity;
    const std::chrono::milliseconds idleTimeout;
    const std::size_t configuredWorkers;
    const ConnectionHandlerFactory handlerFactory;
    const ConnectionFeatureFactory featureFactory;
    std::vector<std::shared_ptr<AsioWorker>> workers;
    std::shared_ptr<Tcp::socket> pendingAccept;
    std::unordered_set<std::uint64_t> admissions;
    std::uint64_t nextTicket{1};
    std::size_t nextWorker{0};
    int reserveFd{-1};
    State state{State::Running};
    bool destroying{false};

    Impl(
        Context& context,
        std::size_t max,
        std::chrono::milliseconds idle,
        std::size_t count,
        ConnectionHandlerFactory factory,
        ConnectionFeatureFactory features
    )
        : base(context)
        , acceptor(context)
        , baseGuard(context.get_executor())
        , retryTimer(context)
        , stopTimer(context)
        , capacity(max)
        , idleTimeout(idle)
        , configuredWorkers(count)
        , handlerFactory(std::move(factory))
        , featureFactory(std::move(features))
    {
        if (max == 0 || idle <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument(
                "AsioTcpServer requires positive capacity and idle "
                "timeout"
            );
        }
    }

    void requireOwner() const
    {
        if (std::this_thread::get_id() != owner)
        {
            throw std::logic_error(
                "AsioTcpServer requires owner io_context thread"
            );
        }
    }

    void start(const InetAddress& address)
    {
        acceptor.open(Tcp::v4());
        acceptor.set_option(Tcp::acceptor::reuse_address(true));
        acceptor.bind(Tcp::endpoint(
            boost::asio::ip::make_address_v4(address.ip()),
            address.port()
        ));
        acceptor.listen();
        acceptor.non_blocking(true);
        const int flags = ::fcntl(acceptor.native_handle(), F_GETFD);
        if (flags < 0 || ::fcntl(
                             acceptor.native_handle(),
                             F_SETFD,
                             flags | FD_CLOEXEC
                         ) < 0)
        {
            throw std::system_error(
                errno,
                std::generic_category(),
                "listener FD_CLOEXEC"
            );
        }
        reserveFd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (reserveFd < 0)
        {
            throw std::system_error(
                errno,
                std::generic_category(),
                "reserve descriptor"
            );
        }

        const auto count = configuredWorkers == 0 ? 1
                                                  : configuredWorkers;
        for (std::size_t i = 0; i != count; ++i)
        {
            auto worker = std::make_shared<AsioWorker>(
                base,
                configuredWorkers != 0,
                idleTimeout,
                handlerFactory,
                featureFactory,
                [weak = weak_from_this()](std::uint64_t ticket)
                {
                    auto impl = weak.lock();
                    if (!impl)
                    {
                        return;
                    }
                    boost::asio::post(
                        impl->base,
                        [weak, ticket]
                        {
                            if (auto self = weak.lock();
                                self && !self->destroying)
                            {
                                self->admissions.erase(ticket);
                                self->finishIfEmpty();
                            }
                        }
                    );
                }
            );
            workers.push_back(worker);
            worker->start();
        }
        acceptNext();
    }

    void acceptNext()
    {
        if (state != State::Running || destroying)
        {
            return;
        }
        auto socket = std::make_shared<Tcp::socket>(base);
        auto peerEndpoint = std::make_shared<Tcp::endpoint>();
        pendingAccept = socket;
        acceptor.async_accept(
            *socket,
            *peerEndpoint,
            [weak = weak_from_this(), socket, peerEndpoint](Error error)
            {
                auto self = weak.lock();
                if (!self)
                {
                    return;
                }
                self->pendingAccept.reset();
                if (self->state != State::Running || self->destroying)
                {
                    return;
                }
                if (error)
                {
                    self->acceptError(error);
                    return;
                }
                const auto acceptedAt = ConnectionContext::Clock::now(
                );
                if (self->admissions.size() >= self->capacity)
                {
                    std::cerr
                        << "AsioTcpServer connection limit reached\n";
                    Error ignored;
                    socket->close(ignored);
                }
                else
                {
                    // accept(2) returned the address while the peer still
                    // existed; getpeername after completion can lose it to RST.
                    const auto& endpoint = *peerEndpoint;
                    const InetAddress peer(
                        endpoint.address().to_v4().to_string(),
                        endpoint.port()
                    );
                    const int flags = ::fcntl(
                        socket->native_handle(),
                        F_GETFD
                    );
                    if (flags < 0 || ::fcntl(
                                         socket->native_handle(),
                                         F_SETFD,
                                         flags | FD_CLOEXEC
                                     ) < 0)
                    {
                        self->acceptError(Error(
                            errno,
                            boost::system::generic_category()
                        ));
                        return;
                    }
                    const auto ticket = self->nextTicket++;
                    if (ticket == 0)
                    {
                        throw std::overflow_error(
                            "Asio admission identity exhausted"
                        );
                    }
                    // Allocate the holder before release, so
                    // allocation failure leaves the socket's RAII
                    // ownership intact.
                    auto accepted = std::make_shared<AcceptedFd>(-1);
                    accepted->fd = socket->release(error);
                    if (error)
                    {
                        self->acceptError(error);
                        return;
                    }
                    auto worker = self->workers
                                      [self->nextWorker++ %
                                       self->workers.size()];
                    self->admissions.insert(ticket);
                    try
                    {
                        worker->dispatch(
                            [worker,
                             accepted,
                             ticket,
                             peer,
                             acceptedAt] {
                                worker->add(
                                    ticket,
                                    accepted,
                                    peer,
                                    acceptedAt
                                );
                            }
                        );
                    }
                    catch (...)
                    {
                        self->admissions.erase(ticket);
                        throw;
                    }
                }
                self->acceptNext();
            }
        );
    }

    void acceptError(const Error& error)
    {
        if (state != State::Running)
        {
            return;
        }
        std::cerr << "AsioTcpServer accept: " << error.message()
                  << '\n';
        if (error.value() == EMFILE || error.value() == ENFILE)
        {
            // Release one reserved slot and drain one pending peer to
            // prevent a readable listener from causing an immediate
            // retry spin.
            if (reserveFd >= 0)
            {
                ::close(std::exchange(reserveFd, -1));
            }
            const int fd = ::accept4(
                acceptor.native_handle(),
                nullptr,
                nullptr,
                SOCK_NONBLOCK | SOCK_CLOEXEC
            );
            if (fd >= 0)
            {
                ::close(fd);
            }
            reserveFd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        }
        retryTimer.expires_after(std::chrono::milliseconds(100));
        retryTimer.async_wait(
            [weak = weak_from_this()](Error retryError)
            {
                if (auto self = weak.lock(); self && !retryError)
                {
                    self->acceptNext();
                }
            }
        );
    }

    void stop(std::chrono::milliseconds grace)
    {
        requireOwner();
        if (grace < std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument(
                "stop grace must be nonnegative"
            );
        }
        if (state != State::Running)
        {
            return;
        }
        state = State::Draining;
        stopAccepting();
        for (const auto& worker : workers)
        {
            worker->dispatch([worker] { worker->drain(); });
        }
        if (admissions.empty())
        {
            finishIfEmpty();
            return;
        }
        if (grace == std::chrono::milliseconds::zero())
        {
            forceClose();
        }
        else
        {
            stopTimer.expires_after(grace);
            stopTimer.async_wait(
                [weak = weak_from_this()](Error error)
                {
                    if (auto self = weak.lock();
                        self && !error &&
                        self->state == State::Draining)
                    {
                        std::cerr << "AsioTcpServer stop deadline "
                                     "reached\n";
                        self->forceClose();
                    }
                }
            );
        }
    }

    void forceClose()
    {
        for (const auto& worker : workers)
        {
            worker->dispatch([worker] { worker->forceClose(); });
        }
    }

    void stopAccepting()
    {
        Error ignored;
        acceptor.close(ignored);
        if (pendingAccept)
        {
            pendingAccept->close(ignored);
        }
        retryTimer.cancel(ignored);
        if (reserveFd >= 0)
        {
            ::close(std::exchange(reserveFd, -1));
        }
    }

    void cleanupWorkers()
    {
        // Post all cleanups before waiting. Workers only post
        // acknowledgements to base; they never wait for base, so
        // joining cannot deadlock.
        std::vector<std::future<void>> finished;
        for (const auto& worker : workers)
        {
            if (!worker->thread.joinable())
            {
                worker->cleanup();
                continue;
            }
            auto done = std::make_shared<std::promise<void>>();
            finished.push_back(done->get_future());
            worker->dispatch(
                [worker, done]
                {
                    worker->cleanup();
                    done->set_value();
                }
            );
        }
        for (auto& completion : finished)
        {
            completion.get();
        }
        for (const auto& worker : workers)
        {
            if (worker->thread.joinable())
            {
                worker->thread.join();
            }
        }
        workers.clear();
    }

    void finishIfEmpty()
    {
        if (state != State::Draining || !admissions.empty())
        {
            return;
        }
        Error ignored;
        stopTimer.cancel(ignored);
        cleanupWorkers();
        state = State::Stopped;
        baseGuard.reset();
        std::cerr << "AsioTcpServer stopped\n";
    }

    void destroy() noexcept
    {
        if (std::this_thread::get_id() != owner)
        {
            std::terminate();
        }
        destroying = true;
        stopAccepting();
        Error ignored;
        stopTimer.cancel(ignored);
        cleanupWorkers();
        admissions.clear();
        state = State::Stopped;
        baseGuard.reset();
    }
};

AsioTcpServer::AsioTcpServer(
    Context& context,
    const InetAddress& address,
    std::size_t max,
    std::chrono::milliseconds idle,
    std::size_t workers,
    ConnectionHandlerFactory handlerFactory,
    ConnectionFeatureFactory featureFactory
)
    : m_impl(std::make_shared<Impl>(
          context,
          max,
          idle,
          workers,
          std::move(handlerFactory),
          std::move(featureFactory)
      ))
{
    try
    {
        m_impl->start(address);
    }
    catch (...)
    {
        m_impl->destroy();
        throw;
    }
}

AsioTcpServer::~AsioTcpServer() noexcept
{
    m_impl->destroy();
}
void AsioTcpServer::stop(std::chrono::milliseconds grace)
{
    m_impl->stop(grace);
}
bool AsioTcpServer::stopped() const noexcept
{
    return m_impl->state == Impl::State::Stopped;
}
std::size_t AsioTcpServer::connectionCount() const noexcept
{
    return m_impl->admissions.size();
}
std::size_t AsioTcpServer::maxConnections() const noexcept
{
    return m_impl->capacity;
}
std::size_t AsioTcpServer::workerCount() const noexcept
{
    return m_impl->configuredWorkers;
}
} // namespace service::gateway
~~~
