/** @file
 * @brief Starts one independently fenced Room proposal worker.
 */
#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <openssl/crypto.h>

#include "room/RoomWorker.h"
#include "runtime/RuntimeControl.h"

namespace
{
using namespace std::chrono_literals;
/** @brief Parses a finite unsigned decimal environment setting. */
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
        throw std::invalid_argument("invalid Room setting");
    return result;
}
/** @brief Requires an explicit bounded value without logging its
 * contents. */
std::string required(const char* name, std::size_t maximum)
{
    const auto* raw = std::getenv(name);
    if (!raw)
        throw std::invalid_argument("missing Room setting");
    std::string value(raw);
    if (value.empty() || value.size() > maximum)
        throw std::invalid_argument("invalid Room setting");
    return value;
}
/** @brief Samples actual process descriptors after physical service
 * cleanup. */
std::size_t descriptors()
{
    std::size_t count = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        (void)entry;
        ++count;
    }
    return count;
}
} // namespace
int main()
try
{
    using service::room::RoomWorker;
    boost::asio::io_context io;
    const auto port = setting("ARENA_ROOM_PORT", 7103, 65535);
    const auto healthPort = setting("ARENA_HEALTH_PORT", 0, 65535);
    if (port && port == healthPort)
        throw std::invalid_argument("Room ports overlap");
    const auto nodeId = required("ARENA_NODE_ID", 64);
    const std::filesystem::path directory(
        required("ARENA_ROOM_STATE_DIR", 4096)
    );
    if (!directory.is_absolute())
        throw std::invalid_argument(
            "Room state path must be absolute"
        );
    auto token = required("ARENA_ROOM_TOKEN", 256);
    if (token.size() < 32)
        throw std::invalid_argument("invalid Room credential");
    std::shared_ptr<arena::runtime::ConfigStore> store;
    std::string configPath;
    if (const auto* path = std::getenv("ARENA_RUNTIME_CONFIG"))
    {
        configPath = path;
        auto loaded = arena::runtime::ConfigSnapshot::read(
            configPath
        );
        if (!loaded)
            throw std::invalid_argument("invalid Room runtime file");
        store = std::make_shared<arena::runtime::ConfigStore>(
            *loaded
        );
    }
    auto holder = std::make_shared<
        std::atomic<std::weak_ptr<RoomWorker>>>();
    arena::runtime::RuntimeControl::Config controlConfig;
    controlConfig.service = "room";
    controlConfig.nodeId = nodeId;
    controlConfig.store = store;
    controlConfig.configFile = configPath;
    controlConfig.healthPort = static_cast<std::uint16_t>(healthPort);
    // The control thread reads only passive, thread-safe worker
    // observations.
    controlConfig.dependencyReady = [holder]
    {
        auto worker = holder->load().lock();
        return worker && worker->stats().ready;
    };
    controlConfig.inflight = [holder]
    {
        auto worker = holder->load().lock();
        return worker ? worker->stats().activeJobs : 0;
    };
    auto control = std::make_shared<arena::runtime::RuntimeControl>(
        std::move(controlConfig)
    );
    RoomWorker::Config config;
    config.nodeId = nodeId;
    config.token = std::move(token);
    config.directory = directory;
    config.endpoint = {
        boost::asio::ip::address_v4::loopback(),
        static_cast<std::uint16_t>(port)
    };
    config.maxInflight = setting("ARENA_ROOM_MAX_INFLIGHT", 32, 256);
    config.maxRooms = setting("ARENA_ROOM_MAX_ROOMS", 4096, 16384);
    config.maxStages = setting("ARENA_ROOM_MAX_STAGES", 4096, 16384);
    config.maxCacheBytes = setting(
        "ARENA_ROOM_MAX_CACHE_BYTES",
        16 * 1024 * 1024,
        16 * 1024 * 1024
    );
    const auto
        weakControl = std::weak_ptr<arena::runtime::RuntimeControl>(
            control
        );
    config.configuredReady = [weakControl]
    {
        auto owner = weakControl.lock();
        return owner && !owner->draining() &&
               owner->configuredReady();
    };
    auto worker = std::make_shared<RoomWorker>(
        io.get_executor(),
        std::move(config)
    );
    holder->store(std::weak_ptr<RoomWorker>(worker));
    if (worker->port() == control->port())
        throw std::invalid_argument("Room ephemeral ports overlap");
    boost::asio::signal_set signals(io, SIGINT, SIGTERM, SIGHUP);
    boost::asio::steady_timer poll(io);
    std::function<void()> waitSignal, waitStopped;
    waitStopped = [&]
    {
        if (worker->stats().stopped)
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
                const auto grace = store ? std::chrono::milliseconds(
                                               store->snapshot()
                                                   ->drainGraceMs()
                                           )
                                         : 5000ms;
                worker->stop(grace);
                waitStopped();
            }
        );
    };
    waitSignal();
    const auto initial = worker->stats();
    std::cout
        << "{\"service\":\"room\",\"state\":\"listening\",\"port\":"
        << worker->port() << ",\"health_port\":" << control->port()
        << ",\"node_id\":\"" << nodeId << "\",\"worker_id\":\""
        << initial.workerId << "\",\"config_version\":"
        << (store ? store->snapshot()->version() : 0) << "}\n"
        << std::flush;
    io.run();
    control->stop();
    const auto ended = worker->stats();
    std::cout << "{\"service\":\"room\",\"state\":\"stopped\","
                 "\"worker_id\":\""
              << ended.workerId
              << "\",\"active\":" << ended.activeJobs
              << ",\"journal_pending\":" << ended.journalPending
              << ",\"journal_closed\":"
              << (ended.journalClosed ? "true" : "false")
              << ",\"rpc_active\":" << ended.rpcActive
              << ",\"rpc_peers\":" << ended.rpcPeers
              << ",\"stages\":" << ended.stages
              << ",\"active_routes\":" << ended.activeRoutes
              << ",\"cache_bytes\":" << ended.cacheBytes
              << ",\"physical_descriptors\":" << descriptors()
              << "}\n"
              << std::flush;
    return ended.stopped ? 0 : 2;
}
catch (const std::exception&)
{
    std::cerr << "Room startup/runtime failure\n";
    return 2;
}