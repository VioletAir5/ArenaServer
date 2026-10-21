/** @file
 * @brief Selects the gateway backend and starts its TCP or WebSocket
 * listeners.
 */
#include <pthread.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AdmissionController.h"
#include "net/AsioTcpServer.h"
#include "net/Channel.h"
#include "net/ConnectionSchedulers.h"
#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/LoginHandler.h"
#include "net/OnlineRegistryMaintenance.h"
#include "net/PacketEcho.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "net/PreLoginHandshake.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"
#include "os/FileDescriptor.h"

using namespace service::gateway;

namespace
{
std::size_t positiveLimit(const char* name, std::size_t fallback)
{
    const auto* configured = std::getenv(name);
    if (!configured)
        return fallback;
    const std::string_view text(configured);
    std::size_t result = 0;
    const auto parsed = std::from_chars(
        text.data(),
        text.data() + text.size(),
        result
    );
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() || result == 0)
        throw std::invalid_argument(
            std::string(name) + " must be a positive integer"
        );
    return result;
}

/** @brief Validates an explicitly configured TCP or WebSocket port.
 */
std::uint16_t listenerPort(const char* name, std::uint16_t fallback)
{
    const auto value = positiveLimit(name, fallback);
    if (value > 65535)
        throw std::invalid_argument(
            std::string(name) + " must be between 1 and 65535"
        );
    return static_cast<std::uint16_t>(value);
}

/** @brief Blocks termination signals before I/O workers inherit their
 * mask. */
class ShutdownSignalMask final
{
public:
    ShutdownSignalMask()
    {
        ::sigemptyset(&m_signals);
        ::sigaddset(&m_signals, SIGINT);
        ::sigaddset(&m_signals, SIGTERM);
        const auto error = ::pthread_sigmask(
            SIG_BLOCK,
            &m_signals,
            &m_previous
        );
        if (error)
            throw std::system_error(
                error,
                std::generic_category(),
                "block shutdown signals"
            );
    }

    ~ShutdownSignalMask()
    {
        // Workers and the signalfd channel have already been
        // destroyed here.
        ::pthread_sigmask(SIG_SETMASK, &m_previous, nullptr);
    }

    ShutdownSignalMask(const ShutdownSignalMask&) = delete;
    ShutdownSignalMask& operator=(const ShutdownSignalMask&) = delete;

    const sigset_t& signals() const noexcept
    {
        return m_signals;
    }

private:
    /// Termination signals consumed by the base reactor instead of a
    /// handler.
    sigset_t m_signals{};
    /// Calling thread mask restored after every worker has joined.
    sigset_t m_previous{};
};

int shutdownSignalFd(const sigset_t& signals)
{
    const auto fd = ::signalfd(
        -1,
        &signals,
        SFD_NONBLOCK | SFD_CLOEXEC
    );
    if (fd < 0)
        throw std::system_error(
            errno,
            std::generic_category(),
            "signalfd"
        );
    return fd;
}

/** @brief Converts SIGINT/SIGTERM into graceful server stop on the
 * base owner. */
class EpollShutdown final
{
public:
    EpollShutdown(
        EventLoop& loop,
        ITcpServer& server,
        const sigset_t& signals,
        std::function<void()> stopUpkeep = {}
    )
        : m_loop(loop)
        , m_server(server)
        , m_stopUpkeep(std::move(stopUpkeep))
        , m_fd(shutdownSignalFd(signals))
        , m_channel(m_fd.get())
    {
        m_channel.setReadCallback([this] { consumeSignals(); });
        m_channel.enableReading();
        m_loop.addChannel(&m_channel);
    }

    ~EpollShutdown()
    {
        if (m_poll)
            m_loop.cancelTimer(m_poll);
        // Invalidate any active epoll batch before Channel and fd
        // destruction.
        m_loop.removeChannel(&m_channel);
    }

    EpollShutdown(const EpollShutdown&) = delete;
    EpollShutdown& operator=(const EpollShutdown&) = delete;

private:
    void consumeSignals()
    {
        signalfd_siginfo info{};
        for (;;)
        {
            const auto count = ::read(
                m_fd.get(),
                &info,
                sizeof(info)
            );
            if (count == static_cast<ssize_t>(sizeof(info)))
            {
                if (!m_stopping)
                {
                    m_stopping = true;
                    if (m_stopUpkeep)
                        m_stopUpkeep();
                    m_server.stop();
                    if (m_server.stopped())
                        m_loop.quit();
                    else
                        m_poll = m_loop.runEvery(
                            std::chrono::milliseconds(10),
                            [this]
                            {
                                if (m_server.stopped())
                                    m_loop.quit();
                            }
                        );
                }
                continue;
            }
            if (count < 0 && errno == EINTR)
                continue;
            if (count < 0 &&
                (errno == EAGAIN || errno == EWOULDBLOCK))
                return;
            throw std::system_error(
                count < 0 ? errno : EIO,
                std::generic_category(),
                "read signalfd"
            );
        }
    }

    /// Reactor and server are destroyed after this registration
    /// guard.
    EventLoop& m_loop;
    ITcpServer& m_server;
    /// Periodic session upkeep must not keep the event loop alive
    /// during drain.
    std::function<void()> m_stopUpkeep;
    /// Signal fd remains open until its Channel has been removed.
    service::FileDescriptor m_fd;
    Channel m_channel;
    /// Polls for worker cleanup without stopping the reactor
    /// prematurely.
    TimerId m_poll{0};
    bool m_stopping{false};
};

void printAdmission(const AdmissionController& controller)
{
    const auto count = controller.snapshot();
    std::cerr << "connection_admission active=" << count.active
              << " peak=" << count.peak
              << " rejected=" << count.rejectedTotal
              << " global=" << count.rejectedGlobal
              << " per_ip=" << count.rejectedPerIp
              << " local=" << count.rejectedLocal << '\n';
}
void printOnline(const OnlineRegistry& registry)
{
    std::cerr << "ONLINE_COUNTS active=" << registry.activeCount()
              << " retained=" << registry.retainedCount()
              << " rooms=" << registry.roomCount() << '\n';
}
} // namespace
int main(int argc, char** argv)
{
    const std::string_view backend = argc >= 2 ? argv[1] : "epoll";
    const std::string_view protocol = argc >= 3 ? argv[2] : "raw";
    if (argc > 4 || (backend != "epoll" && backend != "asio") ||
        (protocol != "raw" && protocol != "packet" &&
         protocol != "handshake") ||
        (protocol == "handshake" && backend != "asio") ||
        (argc == 4 && protocol != "handshake"))
    {
        std::cerr << "Usage: ArenaServer [epoll|asio] "
                     "[raw|packet|handshake] [ws-origin]\n";
        return 2;
    }
    std::shared_ptr<AdmissionController> admission;
    std::shared_ptr<OnlineRegistry> online;
    OnlineRegistryConfig onlineConfig;
    std::uint16_t tcpPort = 8888;
    std::uint16_t wsPort = 8889;
    try
    {
        const auto grace = positiveLimit(
            "ARENA_DISCONNECT_GRACE_MS",
            30000
        );
        if (grace > 3600000)
            throw std::invalid_argument(
                "ARENA_DISCONNECT_GRACE_MS must be between 1 and "
                "3600000"
            );
        onlineConfig.grace = std::chrono::milliseconds(grace);
        tcpPort = listenerPort("ARENA_TCP_PORT", tcpPort);
        wsPort = listenerPort("ARENA_WS_PORT", wsPort);
        if (protocol == "handshake" && tcpPort == wsPort)
            throw std::invalid_argument(
                "ARENA_TCP_PORT and ARENA_WS_PORT must differ"
            );
        admission = std::make_shared<AdmissionController>(
            AdmissionController::Limits{
                positiveLimit("ARENA_MAX_CONNECTIONS", 1000),
                positiveLimit("ARENA_MAX_CONNECTIONS_PER_IP", 100)
            }
        );
    }
    catch (const std::invalid_argument& error)
    {
        std::cerr << error.what() << '\n';
        return 2;
    }
    PacketPolicyFactory packetPolicies;
    if (protocol == "handshake")
        packetPolicies =
            [](PacketPolicyChain& policies, const ConnectionContext&)
        {
            policies.add<PacketRatePolicy>(200.0, 100.0);
            policies.add<PacketTypePolicy>(
                std::vector<std::uint16_t>{1}
            );
        };
    ConnectionHandlerFactory handlerFactory;
    if (protocol == "packet")
    {
        handlerFactory = [](ConnectionContext::Ptr)
        { return makePacketEchoHandler(); };
    }
    if (protocol == "handshake")
    {
        auto registry = defaultMessageRegistry();
        if (const auto* key = std::getenv("ARENA_JWT_KEY"))
        {
            try
            {
                auto passwords = std::make_shared<
                    service::auth::LoginAuthenticator>(
                    std::make_shared<
                        service::auth::UnavailablePasswordAccounts>(),
                    std::make_shared<
                        service::auth::UnavailablePasswordDigest>()
                );
                service::auth::JwtConfig jwt;
                jwt.key = key;
                auto tokens = std::make_shared<
                    service::auth::JwtTokens>(std::move(jwt));
                online = std::make_shared<OnlineRegistry>(
                    onlineConfig
                );
                registry = makeLoginMessageRegistry(
                    std::move(passwords),
                    std::move(tokens),
                    online
                );
            }
            catch (const std::exception&)
            {
                std::cerr << "Invalid authentication configuration\n";
                return 2;
            }
        }
        handlerFactory =
            [registry,
             packetPolicies](ConnectionContext::Ptr connection)
        {
            return makePreLoginHandshakeHandler(
                std::move(connection),
                registry,
                packetPolicies
            );
        };
    }
    const InetAddress address("127.0.0.1", tcpPort);
    if (backend == "epoll")
    {
        // Blocking precedes worker construction; all signals reach
        // the base fd.
        ShutdownSignalMask mask;
        EventLoop loop;
        std::unique_ptr<ITcpServer>
            server = std::make_unique<TcpServer>(
                &loop,
                address,
                admission->limits().maxConnections,
                std::chrono::seconds(60),
                4,
                handlerFactory,
                ConnectionFeatureFactory{},
                admission
            );
        std::unique_ptr<OnlineRegistryMaintenance> upkeep;
        if (online)
            upkeep = std::make_unique<OnlineRegistryMaintenance>(
                online,
                connectionScheduler(loop)
            );
        {
            EpollShutdown shutdown(
                loop,
                *server,
                mask.signals(),
                [&]
                {
                    if (upkeep)
                        upkeep->stop();
                }
            );
            loop.loop();
        }
        server.reset();
        if (upkeep)
            upkeep->shutdown();
        if (online)
            printOnline(*online);
        printAdmission(*admission);
        return 0;
    }

    boost::asio::io_context context;
    std::unique_ptr<OnlineRegistryMaintenance> upkeep;
    if (online)
        upkeep = std::make_unique<OnlineRegistryMaintenance>(
            online,
            connectionScheduler(context.get_executor())
        );
    boost::asio::signal_set signals(context, SIGINT, SIGTERM);
    boost::asio::steady_timer poll(context);
    std::unique_ptr<ITcpServer>
        server = std::make_unique<AsioTcpServer>(
            context,
            address,
            admission->limits().maxConnections,
            std::chrono::seconds(60),
            4,
            handlerFactory,
            ConnectionFeatureFactory{},
            admission
        );
    std::unique_ptr<WebSocketServer> wsServer;
    if (protocol == "handshake")
    {
        const std::string origin = argc == 4
                                       ? argv[3]
                                       : "http://localhost:3000";
        wsServer = std::make_unique<WebSocketServer>(
            context,
            InetAddress("127.0.0.1", wsPort),
            origin,
            handlerFactory,
            admission->limits().maxConnections,
            ConnectionFeatureFactory{},
            admission
        );
    }
    std::function<void()> waitStopped;
    waitStopped = [&]
    {
        if (server->stopped() &&
            (!wsServer || !wsServer->connectionCount()))
        {
            return;
        }
        poll.expires_after(std::chrono::milliseconds(10));
        poll.async_wait(
            [&](auto error)
            {
                if (!error)
                {
                    waitStopped();
                }
            }
        );
    };
    signals.async_wait(
        [&](auto error, int)
        {
            if (!error)
            {
                if (upkeep)
                    upkeep->stop();
                server->stop();
                if (wsServer)
                {
                    wsServer->stop(ITcpServer::kDefaultStopGrace);
                }
                waitStopped();
            }
        }
    );
    context.run();
    wsServer.reset();
    server.reset();
    if (upkeep)
        upkeep->shutdown();
    if (online)
        printOnline(*online);
    printAdmission(*admission);
    if (protocol == "handshake")
    {
        const auto counts = defaultRejectionCounters()->snapshot();
        for (std::size_t i = 0; i < counts.size(); ++i)
            std::cerr << "connection_rejections reason="
                      << RejectionCounters::name(
                             static_cast<RejectionReason>(i)
                         )
                      << " count=" << counts[i] << '\n';
    }
}
