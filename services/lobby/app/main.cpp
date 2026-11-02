/** @file
 * @brief Starts the independent durable Lobby process and its
 * role-bound RPC.
 */
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "lobby/LobbyService.h"
#include "rpc/Server.h"
#include "runtime/RuntimeControl.h"
#include "security/SecureErase.h"

namespace
{
using namespace std::chrono_literals;
using Service = service::lobby::LobbyService;
using Server = arena::internal_rpc::Server;

/** @brief Parses bounded unsigned startup settings before opening
 * listeners. */
unsigned
setting(const char* name, unsigned fallback, unsigned maximum)
{
    const auto* configured = std::getenv(name);
    if (!configured)
        return fallback;
    std::string_view text(configured);
    unsigned value = 0;
    const auto result = std::from_chars(
        text.data(),
        text.data() + text.size(),
        value
    );
    if (text.empty() || result.ec != std::errc{} ||
        result.ptr != text.data() + text.size() || value > maximum)
        throw std::invalid_argument("invalid Lobby numeric setting");
    return value;
}

/** @brief Requires explicit credentials and state paths without
 * generated fallbacks. */
std::string required(const char* name, bool secret = false)
{
    const auto* configured = std::getenv(name);
    if (!configured)
        throw std::invalid_argument("missing Lobby setting");
    std::string value(configured);
    if (value.empty() || value.size() > (secret ? 256u : 4096u) ||
        (secret && value.size() < 32))
        throw std::invalid_argument("invalid Lobby setting");
    return value;
}

/** @brief Fences absolute steady deadlines to the Linux boot that
 * created them. */
std::string bootIdentity()
{
    std::ifstream input("/proc/sys/kernel/random/boot_id");
    std::string value;
    std::getline(input, value);
    if (!input || value.empty() || value.size() > 64)
        throw std::runtime_error("Linux boot identity unavailable");
    return value;
}

/** @brief Joins real workers on every normal and exceptional listener
 * exit. */
struct ServiceLifetime
{
    boost::asio::io_context& io;
    std::shared_ptr<Service> service;
    ~ServiceLifetime()
    {
        service->stop();
        while (!service->stats().stopped)
        {
            if (io.stopped())
                io.restart();
            io.run_for(5ms);
        }
    }
};
} // namespace

int main()
try
{
    boost::asio::io_context io;
    const auto port = setting("ARENA_LOBBY_PORT", 7102, 65535);
    const auto healthPort = setting("ARENA_HEALTH_PORT", 0, 65535);
    const auto authPort = setting("ARENA_AUTH_PORT", 7101, 65535);
    const auto storagePort = setting(
        "ARENA_STORAGE_PORT",
        7105,
        65535
    );
    if (!authPort || !storagePort || (port && port == healthPort))
        throw std::invalid_argument(
            "invalid Lobby listener/dependency port"
        );
    std::string path;
    std::shared_ptr<arena::runtime::ConfigStore> store;
    if (const auto* configured = std::getenv("ARENA_RUNTIME_CONFIG"))
    {
        path = configured;
        auto loaded = arena::runtime::ConfigSnapshot::read(path);
        if (!loaded)
            throw std::invalid_argument(
                "invalid Lobby runtime configuration"
            );
        store = std::make_shared<arena::runtime::ConfigStore>(
            *loaded
        );
    }
    const std::string nodeId = std::getenv("ARENA_NODE_ID")
                                   ? std::getenv("ARENA_NODE_ID")
                                   : "lobby";
    Service::Config config;
    config.authority.journal.directory = required(
        "ARENA_LOBBY_STATE_DIR"
    );
    config.authority.bootId = bootIdentity();
    config.authority.wallNow = []
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    };
    config.authority.registry.maxPlayers = setting(
        "ARENA_LOBBY_MAX_PLAYERS",
        4096,
        100000
    );
    config.authority.registry
        .maxRooms = setting("ARENA_MAX_ROOMS", 128, 10000);
    config.authority.registry.grace = std::chrono::milliseconds(
        setting("ARENA_DISCONNECT_GRACE_MS", 30000, 3600000)
    );
    config.authority.rooms.maxRooms = config.authority.registry
                                          .maxRooms;
    config.authority.rooms.battleDurationTicks = setting(
        "ARENA_BATTLE_TICKS",
        100,
        1000000
    );
    config.authority.rooms.readyTimeoutTicks = setting(
        "ARENA_READY_TIMEOUT_TICKS",
        600,
        1000000
    );
    config.maxJobs = setting("ARENA_LOBBY_MAX_INFLIGHT", 128, 1024);
    config.transportLease = std::chrono::milliseconds(
        setting("ARENA_TRANSPORT_LEASE_MS", 1000, 30000)
    );
    config.auth.endpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(authPort)
    };
    config.auth.token = required("ARENA_AUTH_TOKEN", true);
    config.auth.runtime = store;
    config.storage.endpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(storagePort)
    };
    config.storage.token = required(
        "ARENA_STORAGE_LOBBY_TOKEN",
        true
    );
    config.storage.runtime = store;
    auto service = std::make_shared<Service>(
        io.get_executor(),
        std::move(config)
    );
    ServiceLifetime lifetime{io, service};
    while (!service->stats().initialized && !service->stats().failed)
    {
        if (io.stopped())
            io.restart();
        io.run_for(5ms);
    }
    if (service->stats().failed)
        throw std::runtime_error("Lobby durable recovery failed");
    auto listenerReady = std::make_shared<std::atomic<bool>>(false);
    auto weakService = std::weak_ptr<Service>(service);
    arena::runtime::RuntimeControl::Config controlConfig;
    controlConfig.service = "lobby";
    controlConfig.nodeId = nodeId;
    controlConfig.store = store;
    controlConfig.configFile = path;
    controlConfig.healthPort = static_cast<std::uint16_t>(healthPort);
    controlConfig.dependencyReady = [weakService, listenerReady]
    {
        auto owner = weakService.lock();
        return listenerReady->load() && owner && owner->stats().ready;
    };
    controlConfig.inflight = [weakService]
    {
        auto owner = weakService.lock();
        return owner ? owner->stats().active : 0;
    };
    auto control = std::make_shared<arena::runtime::RuntimeControl>(
        std::move(controlConfig)
    );
    auto weakControl = std::weak_ptr<arena::runtime::RuntimeControl>(
        control
    );
    if (port && port == control->port())
        throw std::invalid_argument("Lobby listeners overlap");
    Server::Config rpcConfig;
    rpcConfig.service = "lobby";
    rpcConfig.endpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(port)
    };
    rpcConfig.callerTokens.emplace(
        "gateway",
        required("ARENA_LOBBY_TOKEN", true)
    );
    rpcConfig
        .maxActive = setting("ARENA_LOBBY_MAX_INFLIGHT", 128, 1024) +
                     1;
    rpcConfig.admission = [weakControl](const auto& request)
    {
        auto owner = weakControl.lock();
        return owner &&
               (request.method() == "health.ready" ||
                (!owner->draining() && owner->configuredReady()));
    };
    Server::Handlers handlers;
    handlers.emplace(
        "health.ready",
        [weakControl](auto, auto, auto complete)
        {
            arena::rpc::Response reply;
            auto owner = weakControl.lock();
            if (!owner || !owner->ready())
                reply.set_status(arena::rpc::UNAVAILABLE);
            complete(std::move(reply));
            return Server::Cancellation{};
        }
    );

    handlers.emplace(
        "lobby.execute",
        [weakService](auto request, auto context, auto complete)
        {
            arena::lobby::ExecuteRequest typed;
            const bool decoded = typed.ParseFromString(
                request.payload()
            );
            arena::security::eraseSecret(*request.mutable_payload());
            auto owner = weakService.lock();
            if (!decoded || !owner)
            {
                if (typed.has_message() &&
                    typed.message().has_login_request())
                    arena::security::eraseSecret(
                        *typed.mutable_message()
                             ->mutable_login_request()
                             ->mutable_credential()
                    );
                arena::rpc::Response result;
                result.set_status(
                    decoded ? arena::rpc::UNAVAILABLE
                            : arena::rpc::INVALID_ARGUMENT
                );
                complete(std::move(result));
                return Server::Cancellation{};
            }
            auto accepted = owner->execute(
                std::move(typed),
                std::move(context),
                [complete](auto result)
                {
                    arena::rpc::Response response;
                    if (!result)
                        response.set_status(result.error());
                    else
                    {
                        arena::lobby::ExecuteReply reply;
                        reply = std::move(*result);
                        auto* payload = response.mutable_payload();
                        payload->resize(reply.ByteSizeLong());
                        if (payload->size() > arena::internal_rpc::
                                                  Client::maxFrame ||
                            !reply.SerializeToArray(
                                payload->data(),
                                static_cast<int>(payload->size())
                            ))
                        {
                            arena::security::eraseSecret(*payload);
                            response.set_status(arena::rpc::INTERNAL);
                        }
                    }
                    complete(std::move(response));
                }
            );
            if (accepted)
                return std::move(*accepted);
            arena::rpc::Response result;
            result.set_status(accepted.error());
            complete(std::move(result));
            return Server::Cancellation{};
        }
    );

    handlers.emplace(
        "lobby.detach",
        [weakService](auto request, auto context, auto complete)
        {
            arena::lobby::DetachRequest typed;
            const bool decoded = typed.ParseFromString(
                request.payload()
            );
            arena::security::eraseSecret(*request.mutable_payload());
            auto owner = weakService.lock();
            if (!decoded || !owner)
            {

                arena::rpc::Response result;
                result.set_status(
                    decoded ? arena::rpc::UNAVAILABLE
                            : arena::rpc::INVALID_ARGUMENT
                );
                complete(std::move(result));
                return Server::Cancellation{};
            }
            auto accepted = owner->detach(
                std::move(typed),
                std::move(context),
                [complete](auto result)
                {
                    arena::rpc::Response response;
                    if (!result)
                        response.set_status(result.error());
                    else
                    {
                        arena::lobby::DetachReply reply;
                        reply = std::move(*result);
                        auto* payload = response.mutable_payload();
                        payload->resize(reply.ByteSizeLong());
                        if (payload->size() > arena::internal_rpc::
                                                  Client::maxFrame ||
                            !reply.SerializeToArray(
                                payload->data(),
                                static_cast<int>(payload->size())
                            ))
                        {
                            arena::security::eraseSecret(*payload);
                            response.set_status(arena::rpc::INTERNAL);
                        }
                    }
                    complete(std::move(response));
                }
            );
            if (accepted)
                return std::move(*accepted);
            arena::rpc::Response result;
            result.set_status(accepted.error());
            complete(std::move(result));
            return Server::Cancellation{};
        }
    );

    handlers.emplace(
        "lobby.heartbeat",
        [weakService](auto request, auto context, auto complete)
        {
            arena::lobby::HeartbeatRequest typed;
            const bool decoded = typed.ParseFromString(
                request.payload()
            );
            arena::security::eraseSecret(*request.mutable_payload());
            auto owner = weakService.lock();
            if (!decoded || !owner)
            {

                arena::rpc::Response result;
                result.set_status(
                    decoded ? arena::rpc::UNAVAILABLE
                            : arena::rpc::INVALID_ARGUMENT
                );
                complete(std::move(result));
                return Server::Cancellation{};
            }
            auto accepted = owner->heartbeat(
                std::move(typed),
                std::move(context),
                [complete](auto result)
                {
                    arena::rpc::Response response;
                    if (!result)
                        response.set_status(result.error());
                    else
                    {
                        arena::lobby::HeartbeatReply reply;
                        reply = std::move(*result);
                        auto* payload = response.mutable_payload();
                        payload->resize(reply.ByteSizeLong());
                        if (payload->size() > arena::internal_rpc::
                                                  Client::maxFrame ||
                            !reply.SerializeToArray(
                                payload->data(),
                                static_cast<int>(payload->size())
                            ))
                        {
                            arena::security::eraseSecret(*payload);
                            response.set_status(arena::rpc::INTERNAL);
                        }
                    }
                    complete(std::move(response));
                }
            );
            if (accepted)
                return std::move(*accepted);
            arena::rpc::Response result;
            result.set_status(accepted.error());
            complete(std::move(result));
            return Server::Cancellation{};
        }
    );

    Server server(
        io.get_executor(),
        std::move(rpcConfig),
        std::move(handlers)
    );
    boost::asio::signal_set signals(io, SIGINT, SIGTERM, SIGHUP);
    boost::asio::steady_timer poll(io);
    bool stopping = false;
    std::function<void()> waitSignal, waitStopped;
    waitStopped = [&]
    {
        if (server.stats().stopped)
            service->stop();
        if (server.stats().stopped && service->stats().stopped)
        {
            boost::system::error_code ignored;
            signals.cancel(ignored);
            return;
        }
        poll.expires_after(5ms);
        poll.async_wait(
            [&](auto error)
            {
                if (!error)
                    waitStopped();
            }
        );
    };
    waitSignal = [&]
    {
        signals.async_wait(
            [&](auto error, int signal)
            {
                if (error)
                    return;
                if (signal == SIGHUP)
                {
                    if (!stopping)
                        control->requestReload();
                    waitSignal();
                    return;
                }
                if (!stopping)
                {
                    stopping = true;
                    listenerReady->store(false);
                    control->setDraining();
                    const auto
                        grace = store ? std::chrono::milliseconds(
                                            store->snapshot()
                                                ->drainGraceMs()
                                        )
                                      : 5000ms;
                    server.stop(grace);
                    waitStopped();
                }
            }
        );
    };
    waitSignal();
    listenerReady->store(true);
    std::cout
        << "{\"service\":\"lobby\",\"state\":\"listening\",\"port\":"
        << server.port() << ",\"health_port\":" << control->port()
        << "}" << std::endl;
    if (io.stopped())
        io.restart();
    io.run();
    const auto stats = service->stats();
    std::cout
        << "{\"service\":\"lobby\",\"state\":\"stopped\",\"active\":"
        << stats.active << ",\"auth_inflight\":" << stats.authInflight
        << ",\"storage_inflight\":" << stats.storageInflight
        << ",\"journal_closed\":"
        << (stats.authority.journal.closed ? "true" : "false")
        << ",\"journal_pending\":" << stats.authority.journal.pending
        << ",\"owner_pending\":"
        << stats.authority.pending + stats.authority.roomPending
        << ",\"committed_revision\":" << stats.committedRevision
        << ",\"online\":" << stats.onlineActive
        << ",\"retained\":" << stats.onlineRetained
        << ",\"room_indexes\":" << stats.roomIndexes
        << ",\"transport_bindings\":" << stats.transports
        << ",\"receipts\":" << stats.receipts
        << ",\"queued\":" << stats.queued
        << ",\"matched_pairs\":" << stats.matchedPairs
        << ",\"room_actors\":" << stats.roomActors
        << ",\"room_ticks\":" << stats.roomTicks
        << ",\"settlements_committed\":" << stats.settlementsCommitted
        << ",\"settlements_blocked\":" << stats.settlementsBlocked
        << "}" << std::endl;
    return 0;
}
catch (...)
{
    std::cerr << "Lobby startup failed" << std::endl;
    return 2;
}
