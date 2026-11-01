/** @file
 * @brief Selects the gateway backend and starts its TCP or WebSocket
 * listeners.
 */
#include <pthread.h>
#include <sys/signalfd.h>
#include <unistd.h>

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
#include "net/PersistentStorage.h"
#include "net/PreLoginHandshake.h"
#include "net/RoomService.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"
#include "os/FileDescriptor.h"

#include "auth/FilePasswordAccounts.h"
#include "room/ReplayRecorder.h"
#include "runtime/OwnerScheduler.h"
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
    std::cerr
        << "ROOM_COUNTS actors=" << metrics.actors
        << " ticks=" << metrics.ticks
        << " applied=" << metrics.applied
        << " rejected=" << metrics.rejected
        << " duplicates=" << metrics.duplicates
        << " managed_matches=" << metrics.managedMatches
        << " retained_results=" << metrics.retainedResults
        << " battles_started=" << metrics.battlesStarted
        << " battles_completed=" << metrics.battlesCompleted
        << " battles_aborted=" << metrics.battlesAborted
        << " persistence_submitted=" << metrics.persistenceSubmitted
        << " persistence_rejected=" << metrics.persistenceRejected
        << " persistence_failed=" << metrics.persistenceFailed
        << " persistence_committed=" << metrics.persistenceCommitted
        << " pending_settlements=" << metrics.pendingSettlements
        << " blocked_settlements=" << metrics.blockedSettlements
        << " skipped_wall_slots=" << metrics.skippedWallSlots << '\n';
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
/** @brief Cancels actor timers before their reactor is destroyed on
 * every exit path. */
struct ScheduledServiceCleanup final
{
    /// Room actors scheduled on the surrounding reactor.
    std::shared_ptr<RoomService> rooms;
    /// Matchmaking owner using the same reactor timers.
    std::shared_ptr<MatchmakingService> matcher;
    /// Published false before any reactor-bound state is released.
    std::shared_ptr<std::atomic<bool>> listenersReady;

    /** @brief Also covers exceptions from later listener
     * construction. */
    ~ScheduledServiceCleanup() noexcept
    {
        listenersReady->store(false);
        if (rooms)
            rooms->stop();
        if (matcher)
            matcher->stop();
    }
};
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
    std::shared_ptr<arena::runtime::RuntimeControl> control;
    std::shared_ptr<const MessageRegistry> handshakeRegistry;
    AdmissionController::Limits admissionLimits{};
    bool useStorage = false;
    auto listenersReady = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<AdmissionController> admission;
    std::shared_ptr<OnlineRegistry> online;
    std::shared_ptr<MatchmakingService> matcher;
    std::shared_ptr<RoomService> rooms;
    std::shared_ptr<PersistentStorage> persistence;
    std::shared_ptr<service::room::ReplayRecorder> replays;
    OnlineRegistryConfig onlineConfig;
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
        useStorage = std::getenv("ARENA_STORAGE_PORT") != nullptr;
        if (runtimeConfig)
            for (const auto& node :
                 runtimeConfig->snapshot()->nodes())
                useStorage = useStorage || node.service == "storage";
        const auto grace = positiveLimit(
            "ARENA_DISCONNECT_GRACE_MS",
            30000
        );
        if (grace > 3600000)
            throw std::invalid_argument(
                "ARENA_DISCONNECT_GRACE_MS must be between 1 and "
                "3600000"
            );
        if (useStorage && (protocol != "handshake" ||
                           !std::getenv("ARENA_JWT_KEY")))
            throw std::invalid_argument(
                "Storage requires authenticated handshake mode"
            );
        onlineConfig.grace = std::chrono::milliseconds(grace);
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
        auto registry = defaultMessageRegistry();
        if (const auto* key = std::getenv("ARENA_JWT_KEY"))
        {
            try
            {
                std::shared_ptr<
                    const service::auth::LoginAuthenticator>
                    passwords;
                AsyncPasswordLogin asyncPasswords;
                DurableSettlementLookup durableLookup;
                if (useStorage)
                {
                    const auto* token = std::getenv(
                        "ARENA_INTERNAL_TOKEN"
                    );
                    const auto* spool = std::getenv(
                        "ARENA_SETTLEMENT_SPOOL_DIR"
                    );
                    if (!token || !spool || !*spool)
                        throw std::invalid_argument(
                            "Storage requires a credential and "
                            "private spool"
                        );
                    PersistentStorageConfig config;
                    config.endpoint = {
                        boost::asio::ip::address_v4::loopback(),
                        listenerPort("ARENA_STORAGE_PORT", 7105)
                    };
                    config.serviceToken = token;
                    config.runtime = runtimeConfig;
                    config.probeHealth = runtimeConfig ||
                                         std::getenv(
                                             "ARENA_HEALTH_PORT"
                                         );
                    config.spoolDirectory = spool;
                    config.idBlockSize = static_cast<std::uint32_t>(
                        boundedLimit(
                            "ARENA_STORAGE_ID_BLOCK",
                            128,
                            1024
                        )
                    );
                    config.rpcTimeout = std::chrono::milliseconds(
                        boundedLimit(
                            "ARENA_STORAGE_TIMEOUT_MS",
                            1000,
                            5000
                        )
                    );
                    config.maxResultAttempts = static_cast<
                        std::uint32_t>(boundedLimit(
                        "ARENA_SETTLEMENT_MAX_ATTEMPTS",
                        8,
                        64
                    ));
                    persistence = std::make_shared<PersistentStorage>(
                        std::move(config)
                    );
                    const bool primed = persistence->prime(
                        std::chrono::seconds(5)
                    );
                    if ((!primed && !runtimeConfig) ||
                        !persistence->metrics().spoolHealthy)
                        throw std::runtime_error(
                            "Storage startup is unavailable"
                        );
                    asyncPasswords =
                        [weak = std::weak_ptr<PersistentStorage>(
                             persistence
                         )](std::string account,
                            std::string password,
                            std::chrono::steady_clock::time_point
                                deadline,
                            std::string correlation,
                            PasswordLoginCompletion complete)
                        -> std::expected<
                            std::function<void()>,
                            service::auth::AuthError>
                    {
                        if (const auto storage = weak.lock())
                            return storage->authenticate(
                                std::move(account),
                                std::move(password),
                                deadline,
                                std::move(correlation),
                                std::move(complete)
                            );
                        return std::unexpected(
                            service::auth::AuthError::Unavailable
                        );
                    };
                    durableLookup =
                        [weak = std::weak_ptr<PersistentStorage>(
                             persistence
                         )](
                            arena::domain::PlayerId player,
                            std::uint64_t match,
                            arena::runtime::OwnerClock::time_point
                                deadline,
                            std::string correlation,
                            RoomService::SettlementCompletion complete
                        )
                        -> std::
                            expected<std::function<void()>, ErrorCode>
                    {
                        if (const auto storage = weak.lock())
                            return storage->settlement(
                                player,
                                match,
                                deadline,
                                std::move(correlation),
                                std::move(complete)
                            );
                        return std::unexpected(
                            arena::protocol::ERR_UNAVAILABLE
                        );
                    };
                }
                else
                {
                    std::shared_ptr<
                        const service::auth::PasswordAccounts>
                        accounts = std::make_shared<
                            service::auth::
                                UnavailablePasswordAccounts>();
                    if (const auto* path = std::getenv(
                            "ARENA_ACCOUNTS_FILE"
                        ))
                        accounts = std::make_shared<
                            service::auth::FilePasswordAccounts>(
                            path
                        );
                    passwords = std::make_shared<
                        service::auth::LoginAuthenticator>(
                        std::move(accounts),
                        std::make_shared<
                            service::auth::Pbkdf2PasswordDigest>()
                    );
                }
                service::auth::JwtConfig jwt;
                jwt.key = key;
                auto tokens = std::make_shared<
                    service::auth::JwtTokens>(std::move(jwt));
                online = std::make_shared<OnlineRegistry>(
                    onlineConfig
                );
                service::match::QueueConfig matchConfig;
                if (persistence)
                    matchConfig.ticketIdSource =
                        [weak = std::weak_ptr<PersistentStorage>(
                             persistence
                         )]
                    {
                        const auto storage = weak.lock();
                        return storage ? storage->takeTicketId()
                                       : std::nullopt;
                    };
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
                if (persistence)
                    roomConfig.persistSettlement =
                        [weak = std::weak_ptr<PersistentStorage>(
                             persistence
                         )](const ManagedRoom& allocation,
                            const RoomSettlement& result,
                            std::function<void(bool)> complete)
                    {
                        const auto storage = weak.lock();
                        return storage && storage->save(
                                              allocation,
                                              result,
                                              std::move(complete)
                                          );
                    };
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
                    [storage = persistence](
                        arena::domain::PlayerId player
                    ) -> std::optional<std::uint32_t>
                    {
                        return storage ? storage->rating(player)
                                       : std::optional<std::uint32_t>{
                                             1500
                                         };
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
                    rooms,
                    std::move(asyncPasswords),
                    std::move(durableLookup)
                );
            }
            catch (const std::exception&)
            {
                std::cerr
                    << "Invalid authentication, storage or replay "
                       "configuration\n";
                return 2;
            }
        }
        handshakeRegistry = std::move(registry);
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
            if (persistence)
            {
                const auto weak = std::weak_ptr<PersistentStorage>(
                    persistence
                );
                configured.dependencyReady = [weak, listenersReady]
                {
                    const auto owner = weak.lock();
                    return listenersReady->load() && owner &&
                           owner->ready();
                };
                configured.inflight = [weak]
                {
                    const auto owner = weak.lock();
                    return owner ? owner->metrics().rpcActive : 0;
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
                // Existing room actors keep local input/status during
                // a database outage.
                if (message != arena::protocol::MSG_LOGIN_REQUEST &&
                    message !=
                        arena::protocol::MSG_MATCH_JOIN_REQUEST &&
                    message !=
                        arena::protocol::MSG_SETTLEMENT_REQUEST)
                    return true;
                return owner->ready();
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
        std::unique_ptr<OnlineRegistryMaintenance> upkeep;
        if (online)
            upkeep = std::make_unique<OnlineRegistryMaintenance>(
                online,
                connectionScheduler(loop)
            );
        const ScheduledServiceCleanup actorCleanup{
            rooms,
            matcher,
            listenersReady
        };
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
                    if (control)
                        control->setDraining();
                    if (rooms)
                        rooms->stop();
                    if (matcher)
                        matcher->stop();
                    if (upkeep)
                        upkeep->stop();
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
        if (persistence)
            persistence->stop();
        if (control)
            control->stop();
        printAdmission(*admission);
        return 0;
    }

    boost::asio::io_context context;
    std::unique_ptr<OnlineRegistryMaintenance> upkeep;
    if (online)
        upkeep = std::make_unique<OnlineRegistryMaintenance>(
            online,
            arena::runtime::ownerScheduler(context.get_executor())
        );
    const ScheduledServiceCleanup actorCleanup{
        rooms,
        matcher,
        listenersReady
    };
    if (matcher)
        matcher->start(
            arena::runtime::ownerScheduler(context.get_executor())
        );
    if (rooms)
        rooms->start(
            arena::runtime::ownerScheduler(context.get_executor())
        );
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
                if (rooms)
                    rooms->stop();
                if (matcher)
                    matcher->stop();
                if (upkeep)
                    upkeep->stop();
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
              << " authenticated=" << static_cast<bool>(online)
              << " health_port=" << (control ? control->port() : 0)
              << " config_version="
              << (runtimeConfig ? runtimeConfig->snapshot()->version()
                                : 0)
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
    if (persistence)
    {
        persistence->stop();
        const auto metrics = persistence->metrics();
        std::cerr << "STORAGE_COUNTS auth_active="
                  << metrics.authActive
                  << " work_pending=" << metrics.workPending
                  << " committed=" << metrics.resultsCommitted
                  << " failed=" << metrics.failedResults
                  << " spool_files=" << metrics.spoolFiles
                  << " spool_bytes=" << metrics.spoolBytes << '\n';
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
