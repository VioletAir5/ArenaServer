/** @file
 * @brief Starts the independent Auth process with bounded RPC and
 * health owners.
 */
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <openssl/crypto.h>

#include "auth/AuthService.h"
#include "rpc/Server.h"
#include "runtime/RuntimeControl.h"

namespace
{
using namespace std::chrono_literals;

/** @brief Reads a finite decimal setting without accepting whitespace
 * or signs. */
unsigned
setting(const char* name, unsigned fallback, unsigned maximum)
{
    const auto* value = std::getenv(name);
    if (!value)
        return fallback;
    const std::string_view text(value);
    unsigned result = 0;
    const auto parsed = std::from_chars(
        text.data(),
        text.data() + text.size(),
        result
    );
    if (text.empty() || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() || result > maximum)
        throw std::invalid_argument("invalid Auth configuration");
    return result;
}

/** @brief Secrets have no generated/default credential fallback. */
std::string secret(const char* name, std::size_t maximum = 256)
{
    const auto* value = std::getenv(name);
    if (!value)
        throw std::invalid_argument("missing Auth credential");
    std::string result(value);
    if (result.size() < 32 || result.size() > maximum)
        throw std::invalid_argument("invalid Auth credential");
    return result;
}

void clear(std::string& value)
{
    if (!value.empty())
        OPENSSL_cleanse(value.data(), value.size());
    value.clear();
}
} // namespace

int main()
try
{
    using namespace service::auth;
    using namespace arena::internal_rpc;
    boost::asio::io_context io;
    const auto port = setting("ARENA_AUTH_PORT", 7101, 65535);
    const auto healthPort = setting("ARENA_HEALTH_PORT", 0, 65535);
    const auto storagePort = setting(
        "ARENA_STORAGE_PORT",
        7105,
        65535
    );
    if (!storagePort || (port && port == healthPort))
        throw std::invalid_argument(
            "Auth listener ports must differ"
        );
    std::shared_ptr<arena::runtime::ConfigStore> store;
    std::string configPath;
    if (const auto* path = std::getenv("ARENA_RUNTIME_CONFIG"))
    {
        configPath = path;
        auto loaded = arena::runtime::ConfigSnapshot::read(
            configPath
        );
        if (!loaded)
            throw std::invalid_argument("invalid Auth runtime file");
        store = std::make_shared<arena::runtime::ConfigStore>(
            *loaded
        );
    }
    const std::string nodeId = std::getenv("ARENA_NODE_ID")
                                   ? std::getenv("ARENA_NODE_ID")
                                   : "auth";
    if (nodeId.empty() || nodeId.size() > 64 ||
        nodeId.front() < 'a' || nodeId.front() > 'z' ||
        !std::all_of(
            nodeId.begin(),
            nodeId.end(),
            [](unsigned char c)
            {
                return (c >= 'a' && c <= 'z') ||
                       (c >= '0' && c <= '9') || c == '_' || c == '-';
            }
        ))
        throw std::invalid_argument("invalid Auth node identity");
    AuthService::Config config;
    config.jwt.key = secret("ARENA_JWT_KEY", 4096);
    config.storageToken = secret("ARENA_STORAGE_AUTH_TOKEN");
    config.storageEndpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(storagePort)
    };
    config.runtime = store;
    const auto workers = setting("ARENA_AUTH_WORKERS", 4, 16);
    const auto queued = setting("ARENA_AUTH_MAX_QUEUED", 8, 4096);
    const auto inflight = setting(
        "ARENA_AUTH_MAX_INFLIGHT",
        32,
        1024
    );
    config.maxWorkers = workers;
    config.maxQueued = queued;
    config.maxInflight = inflight;
    auto service = std::make_shared<AuthService>(
        io.get_executor(),
        std::move(config)
    );
    auto listenerReady = std::make_shared<std::atomic<bool>>(false);
    arena::runtime::RuntimeControl::Config controlConfig;
    controlConfig.service = "auth";
    controlConfig.nodeId = nodeId;
    controlConfig.store = store;
    controlConfig.configFile = configPath;
    controlConfig.healthPort = static_cast<std::uint16_t>(healthPort);
    const auto weakService = std::weak_ptr<AuthService>(service);
    controlConfig.dependencyReady = [weakService, listenerReady]
    {
        const auto owner = weakService.lock();
        return listenerReady->load() && owner &&
               owner->stats().storageReady;
    };
    controlConfig.inflight = [weakService]
    {
        const auto owner = weakService.lock();
        return owner ? owner->stats().active : 0;
    };
    auto control = std::make_shared<arena::runtime::RuntimeControl>(
        std::move(controlConfig)
    );
    if (port && port == control->port())
        throw std::invalid_argument(
            "Auth ephemeral health overlaps business listener"
        );
    const auto
        weakControl = std::weak_ptr<arena::runtime::RuntimeControl>(
            control
        );
    Server::Config rpcConfig;
    rpcConfig.service = "auth";
    rpcConfig.endpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(port)
    };
    rpcConfig.callerTokens.emplace(
        "lobby",
        secret("ARENA_AUTH_TOKEN")
    );
    rpcConfig.maxActive = inflight + 1;
    rpcConfig.admission = [weakControl](const auto& request)
    {
        const auto owner = weakControl.lock();
        return owner &&
               (request.method() == "health.ready" ||
                (!owner->draining() && owner->configuredReady()));
    };
    Server::Handlers handlers;
    handlers.emplace(
        "health.ready",
        [weakControl](auto, auto, auto complete)
        {
            arena::rpc::Response result;
            const auto owner = weakControl.lock();
            if (!owner || !owner->ready())
                result.set_status(arena::rpc::UNAVAILABLE);
            complete(std::move(result));
            return Server::Cancellation{};
        }
    );
    handlers.emplace(
        "auth.authenticate",
        [weakService](auto request, auto context, auto complete)
        {
            arena::auth::AuthenticateRequest credential;
            const bool decoded = credential.ParseFromString(
                request.payload()
            );
            clear(*request.mutable_payload());
            const auto owner = weakService.lock();
            if (!decoded || !owner)
            {
                clear(*credential.mutable_credential());
                arena::rpc::Response result;
                result.set_status(
                    decoded ? arena::rpc::UNAVAILABLE
                            : arena::rpc::INVALID_ARGUMENT
                );
                complete(std::move(result));
                return Server::Cancellation{};
            }
            auto accepted = owner->authenticate(
                std::move(credential),
                std::move(context),
                complete
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
    std::function<void()> waitSignal, waitStopped;
    waitStopped = [&]
    {
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
                    control->requestReload();
                    waitSignal();
                    return;
                }
                control->setDraining();
                listenerReady->store(false);
                const auto grace = store ? std::chrono::milliseconds(
                                               store->snapshot()
                                                   ->drainGraceMs()
                                           )
                                         : 5000ms;
                server.stop(grace);
                service->stop();
                waitStopped();
            }
        );
    };
    waitSignal();
    boost::asio::post(
        io,
        [listenerReady] { listenerReady->store(true); }
    );
    std::cout
        << "{\"service\":\"auth\",\"state\":\"listening\",\"port\":"
        << server.port() << ",\"health_port\":" << control->port()
        << ",\"node_id\":\"" << nodeId << "\",\"config_version\":\""
        << (store ? store->snapshot()->version() : 0) << "\"}\n"
        << std::flush;
    io.run();
    control->stop();
    const auto ended = service->stats();
    std::cout
        << "{\"service\":\"auth\",\"state\":\"stopped\",\"active\":"
        << ended.active << ",\"queued\":" << ended.queued
        << ",\"busy\":" << ended.busy
        << ",\"workers\":" << ended.workersAlive << "}\n"
        << std::flush;
    return 0;
}
catch (const std::exception&)
{
    std::cerr << "Auth startup/runtime failure\n";
    return 2;
}
