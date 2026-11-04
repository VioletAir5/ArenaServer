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
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include "lobby/LobbyService.h"
#include "room/RoomWire.h"
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

/** @brief Parses exact bounded Room routes before admitting any
 * business work. */
std::vector<service::lobby::RoomClient::Node> roomNodes()
{
    const auto text = required("ARENA_ROOM_NODES");
    google::protobuf::ListValue values;
    if (text.size() > 4096 ||
        !google::protobuf::util::JsonStringToMessage(text, &values)
             .ok() ||
        values.values_size() < 1 || values.values_size() > 16)
        throw std::invalid_argument("invalid Room node list");
    std::vector<service::lobby::RoomClient::Node> result;
    std::set<std::string> names;
    std::set<unsigned> ports;
    for (const auto& value : values.values())
    {
        if (value.kind_case() !=
            google::protobuf::Value::kStructValue)
            throw std::invalid_argument("invalid Room node");
        const auto& fields = value.struct_value().fields();
        if (fields.size() != 3 || !fields.contains("nodeId") ||
            !fields.contains("host") || !fields.contains("port") ||
            fields.at("nodeId").kind_case() !=
                google::protobuf::Value::kStringValue ||
            fields.at("host").kind_case() !=
                google::protobuf::Value::kStringValue ||
            fields.at("port").kind_case() !=
                google::protobuf::Value::kNumberValue)
            throw std::invalid_argument("invalid Room node fields");
        const auto& id = fields.at("nodeId").string_value();
        const auto number = fields.at("port").number_value();
        if (id.empty() || id.size() > 64 ||
            !names.insert(id).second ||
            fields.at("host").string_value() != "127.0.0.1" ||
            !(number >= 1 && number <= 65535))
            throw std::invalid_argument("invalid Room route");
        const auto port = static_cast<unsigned>(number);
        if (number != port || !ports.insert(port).second)
            throw std::invalid_argument("duplicate Room route");
        result.push_back(
            {id,
             {boost::asio::ip::address_v4::loopback(),
              static_cast<std::uint16_t>(port)}}
        );
    }
    return result;
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
    config.rooms.nodes = roomNodes();
    config.rooms.token = required("ARENA_ROOM_TOKEN", true);
    config.rooms.runtime = store;
    auto gatewayToken = required("ARENA_LOBBY_TOKEN", true);
    std::string operationsToken;
    if (std::getenv("ARENA_OPERATIONS_TOKEN"))
        operationsToken = required("ARENA_OPERATIONS_TOKEN", true);
    std::set<std::string> distinct{
        config.auth.token,
        config.storage.token,
        config.rooms.token,
        gatewayToken
    };
    if (distinct.size() != 4 || (!operationsToken.empty() &&
                                 distinct.contains(operationsToken)))
        throw std::invalid_argument(
            "service credentials must be role-specific"
        );
    while (!distinct.empty())
    {
        auto node = distinct.extract(distinct.begin());
        arena::security::eraseSecret(node.value());
    }
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
        std::move(gatewayToken)
    );
    if (!operationsToken.empty())
        rpcConfig.callerTokens.emplace(
            "operations",
            std::move(operationsToken)
        );
    rpcConfig
        .maxActive = setting("ARENA_LOBBY_MAX_INFLIGHT", 128, 1024) +
                     1;
    rpcConfig.admission =
        [weakControl, weakService](const auto& request)
    {
        auto owner = weakControl.lock();
        const auto serviceOwner = weakService.lock();
        return owner &&
               (request.method() == "health.ready" ||
                (!owner->draining() &&
                 (request.method().starts_with("room.migration.")
                      ? (serviceOwner &&
                         serviceOwner->stats().initialized)
                      : owner->configuredReady())));
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
            if (request.caller_service() != "gateway")
            {
                arena::security::eraseSecret(
                    *request.mutable_payload()
                );
                arena::rpc::Response denied;
                denied.set_status(arena::rpc::UNAUTHENTICATED);
                complete(std::move(denied));
                return Server::Cancellation{};
            }
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
            if (request.caller_service() != "gateway")
            {
                arena::security::eraseSecret(
                    *request.mutable_payload()
                );
                arena::rpc::Response denied;
                denied.set_status(arena::rpc::UNAUTHENTICATED);
                complete(std::move(denied));
                return Server::Cancellation{};
            }
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
            if (request.caller_service() != "gateway")
            {
                arena::security::eraseSecret(
                    *request.mutable_payload()
                );
                arena::rpc::Response denied;
                denied.set_status(arena::rpc::UNAUTHENTICATED);
                complete(std::move(denied));
                return Server::Cancellation{};
            }
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

    handlers.emplace(
        "room.migration.start",
        [weakService](auto request, auto context, auto complete)
        {
            arena::rpc::Response denied;
            if (request.caller_service() != "operations")
            {
                denied.set_status(arena::rpc::UNAUTHENTICATED);
                complete(std::move(denied));
                return Server::Cancellation{};
            }
            arena::lobby::MigrationRequest typed;
            auto owner = weakService.lock();
            if (!service::room::wire::parseMessage(
                    request.payload(),
                    typed,
                    4096
                ) ||
                !owner)
            {
                denied.set_status(
                    owner ? arena::rpc::INVALID_ARGUMENT
                          : arena::rpc::UNAVAILABLE
                );
                complete(std::move(denied));
                return Server::Cancellation{};
            }
            auto accepted = owner->migrate(
                std::move(typed),
                std::move(context),
                [complete](auto result)
                {
                    arena::rpc::Response response;
                    if (!result)
                        response.set_status(result.error());
                    else
                    {
                        arena::lobby::MigrationReply reply;
                        reply = std::move(*result);
                        if (reply.ByteSizeLong() > 4096 ||
                            !reply.SerializeToString(
                                response.mutable_payload()
                            ))
                        {
                            response.clear_payload();
                            response.set_status(arena::rpc::INTERNAL);
                        }
                    }
                    complete(std::move(response));
                }
            );
            if (accepted)
                return std::move(*accepted);
            denied.set_status(accepted.error());
            complete(std::move(denied));
            return Server::Cancellation{};
        }
    );
    handlers.emplace(
        "room.migration.status",
        [weakService](auto request, auto, auto complete)
        {
            arena::rpc::Response response;
            if (request.caller_service() != "operations")
                response.set_status(arena::rpc::UNAUTHENTICATED);
            else
            {
                arena::lobby::MigrationStatusRequest typed;
                auto owner = weakService.lock();
                if (!service::room::wire::parseMessage(
                        request.payload(),
                        typed,
                        4096
                    ))
                    response.set_status(arena::rpc::INVALID_ARGUMENT);
                else if (!owner)
                    response.set_status(arena::rpc::UNAVAILABLE);
                else
                {
                    auto result = owner->migrationStatus(
                        std::move(typed)
                    );
                    if (!result)
                        response.set_status(result.error());
                    else
                    {
                        arena::lobby::MigrationReply reply;
                        reply = std::move(*result);
                        if (reply.ByteSizeLong() > 4096 ||
                            !reply.SerializeToString(
                                response.mutable_payload()
                            ))
                        {
                            response.clear_payload();
                            response.set_status(arena::rpc::INTERNAL);
                        }
                    }
                }
            }
            complete(std::move(response));
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
        << ",\"room_inflight\":" << stats.roomInflight
        << ",\"coordinator_pending\":" << stats.coordinatorPending
        << ",\"local_room_actors\":0"
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
