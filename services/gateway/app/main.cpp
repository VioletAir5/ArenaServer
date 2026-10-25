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
#include "net/MatchmakingService.h"
#include "net/OnlineRegistryMaintenance.h"
#include "net/PacketEcho.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "net/PreLoginHandshake.h"
#include "net/RoomService.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"
#include "os/FileDescriptor.h"

#include "auth/FilePasswordAccounts.h"
#include "room/ReplayRecorder.h"

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
/** @brief Reports committed room ticks and verifies final actor
 * reclamation. */
void printRooms(const RoomService& rooms)
{
    const auto metrics = rooms.metrics();
    std::cerr << "ROOM_COUNTS actors=" << metrics.actors
              << " ticks=" << metrics.ticks
              << " applied=" << metrics.applied
              << " rejected=" << metrics.rejected
              << " duplicates=" << metrics.duplicates
              << " managed_matches=" << metrics.managedMatches
              << " retained_results=" << metrics.retainedResults
              << " battles_started=" << metrics.battlesStarted
              << " battles_completed=" << metrics.battlesCompleted
              << " battles_aborted=" << metrics.battlesAborted
              << " skipped_wall_slots=" << metrics.skippedWallSlots
              << '\n';
}
/** @brief Reports archive completion, admission and storage failures.
 */
void printReplays(const service::room::ReplayRecorder& recorder)
{
    const auto stats = recorder.metrics();
    std::cerr << "REPLAY_COUNTS completed=" << stats.completed
              << " failed=" << stats.failed
              << " rejected=" << stats.rejected
              << " active=" << stats.active
              << " queued=" << stats.queued
              << " directory=" << recorder.directory().string()
              << " last_error=" << stats.lastError << '\n';
}
/** @brief Exports cumulative matchmaking wait and quality metrics. */
void printMatch(const MatchmakingService& matcher)
{
    const auto metrics = matcher.metrics();
    std::cerr << "MATCH_COUNTS joined=" << metrics.joined
              << " replayed=" << metrics.replayed
              << " matched_pairs=" << metrics.matchedPairs
              << " cancelled=" << metrics.cancelled
              << " expired=" << metrics.expired
              << " rejected=" << metrics.rejected
              << " released_pairs=" << metrics.releasedPairs
              << " queued=" << metrics.queued
              << " assigned=" << metrics.assignedPlayers
              << " retained=" << metrics.retained
              << " wait_ms_sum=" << metrics.totalMatchedWaitMs
              << " mmr_gap_sum=" << metrics.totalMmrGap;
    const auto histogram =
        [](std::string_view name, const auto& counts)
    {
        std::cerr << ' ' << name << '=';
        bool first = true;
        for (const auto value : counts)
        {
            if (!first)
                std::cerr << ',';
            std::cerr << value;
            first = false;
        }
    };
    histogram("matched_wait", metrics.matchedWait);
    histogram("mmr_gap", metrics.mmrGap);
    histogram("abandoned_wait", metrics.abandonedWait);
    std::cerr << '\n';
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
    std::shared_ptr<MatchmakingService> matcher;
    std::shared_ptr<RoomService> rooms;
    std::shared_ptr<service::room::ReplayRecorder> replays;
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
                std::shared_ptr<const service::auth::PasswordAccounts>
                    accounts = std::make_shared<
                        service::auth::UnavailablePasswordAccounts>();
                if (const auto* path = std::getenv(
                        "ARENA_ACCOUNTS_FILE"
                    ))
                    accounts = std::make_shared<
                        service::auth::FilePasswordAccounts>(path);
                auto passwords = std::make_shared<
                    service::auth::LoginAuthenticator>(
                    std::move(accounts),
                    std::make_shared<
                        service::auth::Pbkdf2PasswordDigest>()
                );
                service::auth::JwtConfig jwt;
                jwt.key = key;
                auto tokens = std::make_shared<
                    service::auth::JwtTokens>(std::move(jwt));
                online = std::make_shared<OnlineRegistry>(
                    onlineConfig
                );
                service::match::QueueConfig matchConfig;
                matchConfig.maxQueued = positiveLimit(
                    "ARENA_MATCH_MAX_QUEUED",
                    1024
                );
                matchConfig.maxTickets = positiveLimit(
                    "ARENA_MATCH_MAX_TICKETS",
                    4096
                );
                const auto matchTimeout = positiveLimit(
                    "ARENA_MATCH_TIMEOUT_MS",
                    30000
                );
                if (matchTimeout > 3600000)
                    throw std::invalid_argument(
                        "ARENA_MATCH_TIMEOUT_MS must be at most "
                        "3600000"
                    );
                matchConfig.timeout = std::chrono::milliseconds(
                    matchTimeout
                );
                RoomServiceConfig roomConfig;
                roomConfig.battleDurationTicks = boundedLimit(
                    "ARENA_BATTLE_TICKS",
                    100,
                    72000
                );
                roomConfig.readyTimeoutTicks = boundedLimit(
                    "ARENA_READY_TIMEOUT_TICKS",
                    600,
                    72000
                );
                roomConfig
                    .resultRetention = std::chrono::milliseconds(
                    boundedLimit(
                        "ARENA_RESULT_RETENTION_MS",
                        60000,
                        3600000
                    )
                );
                if (const auto* directory = std::getenv(
                        "ARENA_REPLAY_DIR"
                    ))
                {
                    service::room::ReplayRecorderConfig archive;
                    archive.directory = directory;
                    replays = std::make_shared<
                        service::room::ReplayRecorder>(
                        std::move(archive)
                    );
                    roomConfig.replays = replays;
                }
                rooms = std::make_shared<RoomService>(
                    online,
                    std::move(roomConfig)
                );
                matcher = std::make_shared<MatchmakingService>(
                    online,
                    [](arena::domain::PlayerId)
                        -> std::optional<std::uint32_t>
                    {
                        // Every player starts with a provisional
                        // server rating until a persistent rating
                        // adapter is configured.
                        return 1500;
                    },
                    matchConfig,
                    std::chrono::milliseconds(100),
                    1024,
                    [weak = std::weak_ptr<RoomService>(rooms)](
                        const ManagedRoom& match
                    )
                    {
                        const auto owner = weak.lock();
                        return owner && owner->openMatch(match);
                    }
                );
                registry = makeLoginMessageRegistry(
                    std::move(passwords),
                    std::move(tokens),
                    online,
                    matcher,
                    rooms
                );
            }
            catch (const std::exception&)
            {
                std::cerr << "Invalid authentication or replay "
                             "configuration\n";
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
        if (matcher)
            matcher->start(connectionScheduler(loop));
        if (rooms)
            rooms->start(connectionScheduler(loop));
        {
            EpollShutdown shutdown(
                loop,
                *server,
                mask.signals(),
                [&]
                {
                    if (rooms)
                        rooms->stop();
                    if (matcher)
                        matcher->stop();
                    if (upkeep)
                        upkeep->stop();
                }
            );
            loop.loop();
        }
        server.reset();
        if (rooms)
        {
            rooms->stop();
            printRooms(*rooms);
        }
        if (replays)
        {
            replays->stop();
            printReplays(*replays);
        }
        if (matcher)
        {
            matcher->stop();
            printMatch(*matcher);
        }
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
    if (matcher)
        matcher->start(connectionScheduler(context.get_executor()));
    if (rooms)
        rooms->start(connectionScheduler(context.get_executor()));
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
                if (rooms)
                    rooms->stop();
                if (matcher)
                    matcher->stop();
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
    std::cerr << "GATEWAY_READY tcp_port=" << tcpPort
              << " ws_port=" << wsPort
              << " authenticated=" << static_cast<bool>(online)
              << '\n';
    context.run();
    wsServer.reset();
    server.reset();
    if (rooms)
    {
        rooms->stop();
        printRooms(*rooms);
    }
    if (replays)
    {
        replays->stop();
        printReplays(*replays);
    }
    if (matcher)
    {
        matcher->stop();
        printMatch(*matcher);
    }
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
