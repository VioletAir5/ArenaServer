/** @file
 * @brief Selects the gateway backend and starts its TCP or WebSocket
 * listeners.
 */
#include <pthread.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <openssl/rand.h>

#include "net/AdmissionController.h"
#include "net/AsioTcpServer.h"
#include "net/Channel.h"
#include "net/ConnectionSchedulers.h"
#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/PacketEcho.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "net/PreLoginHandshake.h"
#include "net/RemoteLobbyRoutes.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"
#include "os/FileDescriptor.h"

#include "runtime/RuntimeConfig.h"
#include "runtime/RuntimeControl.h"

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

std::size_t boundedLimit(
    const char* name, std::size_t fallback, std::size_t maximum
)
{
    const auto result = positiveLimit(name, fallback);
    if (result > maximum)
        throw std::invalid_argument(
            std::string(name) + " exceeds its upper bound"
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
        ::sigaddset(&m_signals, SIGHUP);
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
        std::function<void()> stopUpkeep = {},
        std::function<void()> reload = {},
        std::function<std::chrono::milliseconds()> grace = {}
    )
        : m_loop(loop)
        , m_server(server)
        , m_stopUpkeep(std::move(stopUpkeep))
        , m_reload(std::move(reload))
        , m_grace(std::move(grace))
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
                if (info.ssi_signo == SIGHUP)
                {
                    if (!m_stopping && m_reload)
                        m_reload();
                    continue;
                }
                if (!m_stopping)
                {
                    m_stopping = true;
                    if (m_stopUpkeep)
                        m_stopUpkeep();
                    m_server.stop(
                        m_grace ? m_grace()
                                : ITcpServer::kDefaultStopGrace
                    );
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
    /// Reload only queues bounded work on the independent control
    /// owner.
    std::function<void()> m_reload;
    /// Read the latest immutable drain bound at terminal shutdown.
    std::function<std::chrono::milliseconds()> m_grace;
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
              << " local=" << count.rejectedLocal
              << " not_ready=" << count.rejectedNotReady << '\n';
}
/** @brief Owns one process-wide RPC executor and joins its real
 * cleanup. Transport callbacks only post to their connection owner.
 * No business model, password verifier, token signer or per-call
 * thread is owned by Gateway.
 */
class GatewayLobbyRuntime final
{
public:
    explicit GatewayLobbyRuntime(LobbyClient::Config config)
        : m_work(boost::asio::make_work_guard(m_io))
        , m_client(
              std::make_shared<LobbyClient>(
                  m_io.get_executor(), std::move(config)
              )
          )
        , m_worker([this] { m_io.run(); })
    {
    }
    ~GatewayLobbyRuntime() noexcept
    {
        stop();
    }
    GatewayLobbyRuntime(const GatewayLobbyRuntime&) = delete;
    GatewayLobbyRuntime&
    operator=(const GatewayLobbyRuntime&) = delete;
    std::shared_ptr<LobbyClient> client() const
    {
        return m_client;
    }
    void stop() noexcept
    {
        if (!m_worker.joinable())
            return;
        m_client->stop();
        // Cancellation posts and physical RPC handlers keep io.run
        // alive until they have drained; stopping the io_context
        // would abandon those loans.
        m_work.reset();
        m_worker.join();
    }

private:
    /// One executor outlives every remote operation and its
    /// cancellation handler.
    boost::asio::io_context m_io;
    boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>
        m_work;
    /// Shared routes retain the passive client, never an authority or
    /// transport.
    std::shared_ptr<LobbyClient> m_client;
    /// Exactly one explicit Gateway RPC owner, independent of reactor
    /// choice.
    std::thread m_worker;
};

/** @brief Fences recycled connection IDs across independent Gateway
 * starts. */
std::string gatewayInstance()
{
    std::array<unsigned char, 16> bytes{};
    if (::RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) !=
        1)
        throw std::runtime_error(
            "Gateway process identity unavailable"
        );
    constexpr char hex[] = "0123456789abcdef";
    std::string value;
    value.reserve(32);
    for (auto byte : bytes)
    {
        value.push_back(hex[byte >> 4]);
        value.push_back(hex[byte & 15]);
    }
    return value;
}

/** @brief Reports actual remote admission and descriptor cleanup
 * after join. */
void printLobby(const LobbyClient& client)
{
    const auto stats = client.stats();
    std::cerr << "LOBBY_COUNTS tracked=" << stats.tracked
              << " business_inflight=" << stats.businessInflight
              << " heartbeat_inflight=" << stats.heartbeatInflight
              << " probe_inflight=" << stats.probeInflight
              << " owner_posts=" << stats.ownerPosts
              << " stopped=" << stats.stopped << '\n';
}
} // namespace
int main(int argc, char** argv)
try
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
    // Block reactor-owned signals before persistence/control workers
    // inherit masks.
    std::optional<ShutdownSignalMask> signalMask;
    std::shared_ptr<arena::runtime::ConfigStore> runtimeConfig;
    // The executor must outlive every passive client and
    // control/route callback, including early configuration failures
    // and exception unwinding.
    std::unique_ptr<GatewayLobbyRuntime> remoteRuntime;
    std::shared_ptr<arena::runtime::RuntimeControl> control;
    std::shared_ptr<const MessageRegistry> handshakeRegistry;
    AdmissionController::Limits admissionLimits{};
    auto listenersReady = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<AdmissionController> admission;
    std::shared_ptr<LobbyClient> lobby;
    std::uint16_t tcpPort = 8888;
    std::uint16_t wsPort = 8889;
    try
    {
        if (backend == "epoll")
            signalMask.emplace();
        if (const auto* file = std::getenv("ARENA_RUNTIME_CONFIG"))
        {
            const auto parsed = arena::runtime::ConfigSnapshot::read(
                file
            );
            if (!parsed)
                throw std::invalid_argument(
                    "invalid runtime configuration file"
                );
            runtimeConfig = std::make_shared<
                arena::runtime::ConfigStore>(*parsed);
        }
        tcpPort = listenerPort("ARENA_TCP_PORT", tcpPort);
        wsPort = listenerPort("ARENA_WS_PORT", wsPort);
        if (protocol == "handshake" && tcpPort == wsPort)
            throw std::invalid_argument(
                "ARENA_TCP_PORT and ARENA_WS_PORT must differ"
            );
        admissionLimits = AdmissionController::Limits{
            positiveLimit("ARENA_MAX_CONNECTIONS", 1000),
            positiveLimit("ARENA_MAX_CONNECTIONS_PER_IP", 100)
        };
    }
    catch (const std::exception& error)
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
        LobbyClient::Config config;
        config.endpoint = {
            boost::asio::ip::address_v4::loopback(),
            listenerPort("ARENA_LOBBY_PORT", 7102)
        };
        const auto* token = std::getenv("ARENA_LOBBY_TOKEN");
        if (!token)
            throw std::invalid_argument(
                "Gateway requires an explicit Lobby credential"
            );
        config.serviceToken = token;
        config.runtime = runtimeConfig;
        config.gatewayInstance = gatewayInstance();
        config.maxInflight = boundedLimit(
            "ARENA_LOBBY_MAX_INFLIGHT",
            64,
            1024
        );
        config.maxConnections = boundedLimit(
            "ARENA_MAX_CONNECTIONS",
            1000,
            65536
        );
        config.timeout = std::chrono::milliseconds(
            boundedLimit("ARENA_LOBBY_TIMEOUT_MS", 1000, 1000)
        );
        remoteRuntime = std::make_unique<GatewayLobbyRuntime>(
            std::move(config)
        );
        lobby = remoteRuntime->client();
        handshakeRegistry = makeRemoteLobbyMessageRegistry(lobby);
    }
    RequestAdmission requestAdmission;
    std::function<bool()> connectionAdmission;
    try
    {
        if (runtimeConfig || std::getenv("ARENA_HEALTH_PORT"))
        {
            arena::runtime::RuntimeControl::Config configured;
            configured.store = runtimeConfig;
            if (const auto* path = std::getenv(
                    "ARENA_RUNTIME_CONFIG"
                ))
                configured.configFile = path;
            if (const auto* text = std::getenv("ARENA_HEALTH_PORT"))
            {
                const std::string_view value(text);
                unsigned port = 0;
                const auto parsed = std::from_chars(
                    value.data(),
                    value.data() + value.size(),
                    port
                );
                if (value.empty() || parsed.ec != std::errc{} ||
                    parsed.ptr != value.data() + value.size() ||
                    port > 65535)
                    throw std::invalid_argument(
                        "invalid health port"
                    );
                configured.healthPort = static_cast<std::uint16_t>(
                    port
                );
            }
            if (const auto* node = std::getenv("ARENA_NODE_ID"))
                configured.nodeId = node;
            else
                configured.nodeId = "gateway";
            configured.dependencyReady = [listenersReady]
            { return listenersReady->load(); };
            if (lobby)
            {
                const auto weak = std::weak_ptr<LobbyClient>(lobby);
                configured.dependencyReady = [weak, listenersReady]
                {
                    const auto owner = weak.lock();
                    return listenersReady->load() && owner &&
                           owner->stats().dependencyReady;
                };
                configured.inflight = [weak]
                {
                    const auto owner = weak.lock();
                    return owner ? owner->stats().businessInflight
                                 : 0;
                };
            }
            if (configured.healthPort &&
                (configured.healthPort == tcpPort ||
                 (protocol == "handshake" &&
                  configured.healthPort == wsPort)))
                throw std::invalid_argument(
                    "health and business listener ports must differ"
                );
            control = std::make_shared<
                arena::runtime::RuntimeControl>(
                std::move(configured)
            );
            if (control->port() == tcpPort ||
                (protocol == "handshake" &&
                 control->port() == wsPort))
                throw std::invalid_argument(
                    "ephemeral health port overlaps a business "
                    "listener"
                );
            const auto
                weak = std::weak_ptr<arena::runtime::RuntimeControl>(
                    control
                );
            connectionAdmission = [weak]
            {
                const auto owner = weak.lock();
                return owner && owner->ready();
            };
            requestAdmission = [weak](std::uint32_t message)
            {
                const auto owner = weak.lock();
                if (!owner || owner->draining())
                    return false;
                if (!owner->configuredReady())
                    return false;
                // Lobby keeps authoritative room input/status
                // admissible while an unrelated Auth/Storage
                // dependency is temporarily down.
                if (message != arena::protocol::MSG_LOGIN_REQUEST &&
                    message !=
                        arena::protocol::MSG_MATCH_JOIN_REQUEST &&
                    message !=
                        arena::protocol::MSG_SETTLEMENT_REQUEST)
                    return true;
                return owner->ready();
            };
        }
        if (!control && lobby)
        {
            const auto weak = std::weak_ptr<LobbyClient>(lobby);
            connectionAdmission = [weak, listenersReady]
            {
                const auto owner = weak.lock();
                return listenersReady->load() && owner &&
                       owner->stats().dependencyReady;
            };
        }
        admission = std::make_shared<AdmissionController>(
            admissionLimits,
            std::move(connectionAdmission)
        );
    }
    catch (const std::exception&)
    {
        std::cerr << "Invalid runtime control configuration\n";
        return 2;
    }
    if (handshakeRegistry)
        handlerFactory =
            [registry = std::move(handshakeRegistry),
             packetPolicies,
             requestAdmission](ConnectionContext::Ptr connection)
        {
            return makePreLoginHandshakeHandler(
                std::move(connection),
                registry,
                packetPolicies,
                requestAdmission
            );
        };
    const auto drainGrace = [&]
    {
        return runtimeConfig
                   ? std::chrono::milliseconds(
                         runtimeConfig->snapshot()->drainGraceMs()
                     )
                   : ITcpServer::kDefaultStopGrace;
    };
    const InetAddress address("127.0.0.1", tcpPort);
    if (backend == "epoll")
    {
        // Blocking precedes worker construction; all signals reach
        // the base fd.
        const auto& mask = *signalMask;
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
        {
            EpollShutdown shutdown(
                loop,
                *server,
                mask.signals(),
                [&]
                {
                    if (control)
                        control->setDraining();
                    listenersReady->store(false);
                },
                [&]
                {
                    if (control)
                        control->requestReload();
                },
                drainGrace
            );
            listenersReady->store(true);
            std::cerr << "GATEWAY_READY tcp_port=" << tcpPort
                      << " ws_port=0 health_port="
                      << (control ? control->port() : 0)
                      << " config_version="
                      << (runtimeConfig
                              ? runtimeConfig->snapshot()->version()
                              : 0)
                      << '\n';
            loop.loop();
        }
        server.reset();
        listenersReady->store(false);
        if (remoteRuntime)
        {
            remoteRuntime->stop();
            printLobby(*lobby);
        }
        if (control)
            control->stop();
        printAdmission(*admission);
        return 0;
    }

    boost::asio::io_context context;
    boost::asio::signal_set signals(context, SIGINT, SIGTERM, SIGHUP);
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
    std::function<void()> waitSignal;
    waitSignal = [&]
    {
        signals.async_wait(
            [&](auto error, int signal)
            {
                if (error)
                    return;
                if (signal == SIGHUP)
                {
                    if (control)
                        control->requestReload();
                    waitSignal();
                    return;
                }
                if (control)
                    control->setDraining();
                listenersReady->store(false);
                const auto grace = drainGrace();
                server->stop(grace);
                if (wsServer)
                    wsServer->stop(grace);
                waitStopped();
            }
        );
    };
    waitSignal();
    listenersReady->store(true);
    std::cerr << "GATEWAY_READY tcp_port=" << tcpPort
              << " ws_port=" << wsPort
              << " authenticated=" << static_cast<bool>(lobby)
              << " health_port=" << (control ? control->port() : 0)
              << " config_version="
              << (runtimeConfig ? runtimeConfig->snapshot()->version()
                                : 0)
              << '\n';
    context.run();
    wsServer.reset();
    server.reset();
    listenersReady->store(false);
    if (remoteRuntime)
    {
        remoteRuntime->stop();
        printLobby(*lobby);
    }
    if (control)
        control->stop();
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
catch (const std::exception&)
{
    std::cerr << "gateway runtime failed\n";
    return 2;
}
