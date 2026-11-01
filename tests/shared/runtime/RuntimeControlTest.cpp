#include "runtime/RuntimeControl.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

namespace arena::runtime
{
namespace
{
using namespace std::chrono_literals;
using Tcp = boost::asio::ip::tcp;

std::string
document(std::uint64_t version, bool ready = true, int port = 7105)
{
    return "{\"version\":\"" + std::to_string(version) +
           "\",\"desiredReady\":" + (ready ? "true" : "false") +
           ",\"drainGraceMs\":5000,\"nodes\":[{\"service\":"
           "\"storage\","
           "\"nodeId\":\"node-a\",\"host\":\"127.0.0.1\",\"port\":" +
           std::to_string(port) + ",\"ready\":true}]}";
}

ConfigSnapshot::Ptr
snapshot(std::uint64_t version, bool ready = true, int port = 7105)
{
    const auto result = ConfigSnapshot::parse(
        document(version, ready, port)
    );
    if (!result)
        throw std::runtime_error("invalid control test document");
    return *result;
}

struct Directory
{
    Directory()
    {
        std::array<char, 48> path{};
        const std::string pattern = "/tmp/arena-control-test-XXXXXX";
        std::copy(pattern.begin(), pattern.end(), path.begin());
        if (!::mkdtemp(path.data()))
            throw std::runtime_error("control test directory failed");
        root = path.data();
        file = root / "runtime.json";
    }
    ~Directory()
    {
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    }
    void write(const std::string& text)
    {
        {
            std::ofstream output(root / "next.json");
            output << text;
        }
        std::filesystem::rename(root / "next.json", file);
    }
    std::filesystem::path root, file;
};

bool until(
    const std::function<bool()>& condition,
    std::chrono::milliseconds timeout = 2s
)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end)
    {
        if (condition())
            return true;
        std::this_thread::sleep_for(1ms);
    }
    return condition();
}

std::string request(
    std::uint16_t port,
    const std::string& path,
    const std::string& method = "GET",
    const std::string& extraHeaders = ""
)
{
    boost::asio::io_context io;
    Tcp::socket socket(io);
    socket.connect({boost::asio::ip::address_v4::loopback(), port});
    const timeval timeout{1, 0};
    ::setsockopt(
        socket.native_handle(),
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    );
    ::setsockopt(
        socket.native_handle(),
        SOL_SOCKET,
        SO_SNDTIMEO,
        &timeout,
        sizeof(timeout)
    );
    boost::asio::write(
        socket,
        boost::asio::buffer(
            method + " " + path + " HTTP/1.1\r\nHost: localhost\r\n" +
            extraHeaders + "\r\n"
        )
    );
    std::string response;
    std::array<char, 4096> buffer;
    while (true)
    {
        boost::system::error_code error;
        const auto count = socket.read_some(
            boost::asio::buffer(buffer),
            error
        );
        response.append(buffer.data(), count);
        if (error)
            break;
    }
    return response;
}

std::size_t descriptors()
{
    std::size_t result = 0;
    for (const auto& file :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        (void)file;
        ++result;
    }
    return result;
}

TEST(
    RuntimeControlTest,
    OwnNodeIntentIsReversibleAndIndependentOfDependencyAndDrain
)
{
    auto text = document(1);
    const auto readyPosition = text.find("\"ready\":true");
    text.replace(readyPosition, 12, "\"ready\":false");
    const auto parsed = ConfigSnapshot::parse(text);
    ASSERT_TRUE(parsed);
    auto store = std::make_shared<ConfigStore>(*parsed);
    std::atomic<bool> dependency{true};
    RuntimeControl::Config config;
    config.store = store;
    config.service = "storage";
    config.nodeId = "node-a";
    config.dependencyReady = [&] { return dependency.load(); };
    RuntimeControl control(std::move(config));
    EXPECT_FALSE(control.configuredReady());
    EXPECT_FALSE(control.ready());
    const auto offline = request(control.port(), "/health/ready");
    EXPECT_TRUE(offline.starts_with("HTTP/1.1 503"));
    EXPECT_NE(
        offline.find("\"desired_ready\":true"),
        std::string::npos
    );
    EXPECT_NE(
        offline.find("\"configured_ready\":false"),
        std::string::npos
    );
    EXPECT_TRUE(request(control.port(), "/health/alive")
                    .starts_with("HTTP/1.1 200"));
    ASSERT_TRUE(store->publish(snapshot(2)));
    EXPECT_TRUE(control.configuredReady());
    EXPECT_TRUE(request(control.port(), "/health/ready")
                    .starts_with("HTTP/1.1 200"));
    dependency = false;
    EXPECT_TRUE(control.configuredReady());
    EXPECT_FALSE(control.ready());
    control.setDraining();
    EXPECT_TRUE(control.configuredReady());
    EXPECT_FALSE(control.ready());
    RuntimeControl::Config missingSelf;
    missingSelf.store = store;
    missingSelf.service = "gateway";
    missingSelf.nodeId = "gateway-main";
    RuntimeControl gateway(std::move(missingSelf));
    EXPECT_TRUE(gateway.configuredReady());
    EXPECT_TRUE(gateway.ready());
}

TEST(
    RuntimeControlTest,
    AliveSurvivesDependencyFailureQuiescenceAndTerminalDrain
)
{
    auto store = std::make_shared<ConfigStore>(snapshot(1));
    std::atomic<bool> dependency{false};
    RuntimeControl::Config config;
    config.store = store;
    config.nodeId = "gateway-a";
    config.dependencyReady = [&] { return dependency.load(); };
    config.inflight = [] { return 7; };
    RuntimeControl control(std::move(config));
    EXPECT_TRUE(request(control.port(), "/health/alive")
                    .starts_with("HTTP/1.1 200"));
    EXPECT_TRUE(request(control.port(), "/health/ready")
                    .starts_with("HTTP/1.1 503"));
    dependency = true;
    auto healthy = request(control.port(), "/health/ready");
    EXPECT_TRUE(healthy.starts_with("HTTP/1.1 200"));
    EXPECT_NE(healthy.find("\"inflight\":7"), std::string::npos);
    ASSERT_TRUE(store->publish(snapshot(2, false)));
    EXPECT_FALSE(control.ready());
    EXPECT_TRUE(request(control.port(), "/health/ready")
                    .starts_with("HTTP/1.1 503"));
    EXPECT_TRUE(request(control.port(), "/health/alive")
                    .starts_with("HTTP/1.1 200"));
    ASSERT_TRUE(store->publish(snapshot(3, true)));
    EXPECT_TRUE(control.ready());
    control.setDraining();
    ASSERT_TRUE(store->publish(snapshot(4, true)));
    EXPECT_TRUE(control.draining());
    EXPECT_FALSE(control.ready());
    auto draining = request(control.port(), "/health/alive");
    EXPECT_TRUE(draining.starts_with("HTTP/1.1 200"));
    EXPECT_NE(draining.find("\"draining\":true"), std::string::npos);
    control.stop();
    EXPECT_TRUE(control.metrics().closed);
    EXPECT_FALSE(control.requestReload());
}

TEST(
    RuntimeControlTest,
    InvalidAndStaleReloadKeepOldSnapshotThenRollbackUsesNewVersion
)
{
    Directory directory;
    directory.write(document(1));
    auto store = std::make_shared<ConfigStore>(snapshot(1));
    RuntimeControl::Config config;
    config.store = store;
    config.configFile = directory.file;
    RuntimeControl control(std::move(config));
    const auto initial = store->snapshot();
    directory.write("{\"version\":\"2\"");
    ASSERT_TRUE(control.requestReload());
    ASSERT_TRUE(until([&]
                      { return control.metrics().rejected == 1; }));
    EXPECT_EQ(store->snapshot(), initial);
    directory.write(document(2, true, 7106));
    ASSERT_TRUE(control.requestReload());
    ASSERT_TRUE(until([&]
                      { return control.metrics().applied == 1; }));
    EXPECT_EQ(store->snapshot()->resolve("storage")->port, 7106);
    directory.write(document(1));
    ASSERT_TRUE(control.requestReload());
    ASSERT_TRUE(until([&]
                      { return control.metrics().rejected == 2; }));
    EXPECT_EQ(store->snapshot()->version(), 2);
    directory.write(document(3, true, 7105));
    ASSERT_TRUE(control.requestReload());
    ASSERT_TRUE(until([&]
                      { return control.metrics().applied == 2; }));
    EXPECT_EQ(store->snapshot()->resolve("storage")->port, 7105);
    EXPECT_EQ(initial->version(), 1);
    EXPECT_EQ(control.metrics().peakActive, 1);
}

TEST(
    RuntimeControlTest,
    ObserverReloadBurstCoalescesAndSameVersionStopsRecursion
)
{
    Directory directory;
    directory.write(document(2));
    auto store = std::make_shared<ConfigStore>(snapshot(1));
    std::atomic<RuntimeControl*> instance{nullptr};
    std::atomic<int> notifications{0};
    RuntimeControl::Config config;
    config.store = store;
    config.configFile = directory.file;
    config.notify = [&](auto)
    {
        ++notifications;
        for (int index = 0; index < 1000; ++index)
            instance.load()->requestReload();
    };
    RuntimeControl control(std::move(config));
    instance = &control;
    ASSERT_TRUE(control.requestReload());
    ASSERT_TRUE(until(
        [&]
        {
            return control.metrics().applied == 1 &&
                   control.metrics().rejected >= 1;
        }
    ));
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(notifications, 1);
    EXPECT_EQ(control.metrics().applied, 1);
    EXPECT_EQ(control.metrics().rejected, 1);
    EXPECT_GE(control.metrics().coalesced, 999);
    EXPECT_EQ(control.metrics().peakActive, 1);
    EXPECT_EQ(control.metrics().active, 0);
    EXPECT_EQ(store->snapshot()->version(), 2);
}

TEST(
    RuntimeControlTest, SlowHeadersAndCapacityRemainBoundedAndReadOnly
)
{
    RuntimeControl control(RuntimeControl::Config{});
    boost::asio::io_context io;
    std::vector<Tcp::socket> peers;
    for (int index = 0; index < 24; ++index)
    {
        peers.emplace_back(io);
        peers.back().connect(
            {boost::asio::ip::address_v4::loopback(), control.port()}
        );
    }
    ASSERT_TRUE(
        until([&] { return control.metrics().rejectedPeers >= 1; })
    );
    EXPECT_LE(control.metrics().peers, 16);
    ASSERT_TRUE(
        until([&] { return control.metrics().peers == 0; }, 1s)
    );
    EXPECT_TRUE(request(control.port(), "/health/alive")
                    .starts_with("HTTP/1.1 200"));
    EXPECT_TRUE(request(control.port(), "/health/ready", "POST")
                    .starts_with("HTTP/1.1 405"));
    EXPECT_TRUE(request(
                    control.port(),
                    "/health/ready",
                    "GET",
                    "Transfer-Encoding: chunked\r\n"
    )
                    .starts_with("HTTP/1.1 400"));
    EXPECT_TRUE(request(control.port(), "/unknown")
                    .starts_with("HTTP/1.1 404"));
}

TEST(
    RuntimeControlTest,
    StopJoinsNotificationAndRejectsLatePostsWithoutDescriptorLeaks
)
{
    const auto baseline = descriptors();
    {
        Directory directory;
        directory.write(document(2));
        auto store = std::make_shared<ConfigStore>(snapshot(1));
        std::atomic<bool> entered{false}, release{false},
            stopped{false};
        std::atomic<int> notifications{0};
        RuntimeControl::Config config;
        config.store = store;
        config.configFile = directory.file;
        config.notify = [&](auto)
        {
            ++notifications;
            entered = true;
            const auto end = std::chrono::steady_clock::now() + 1s;
            while (!release && std::chrono::steady_clock::now() < end)
                std::this_thread::yield();
        };
        RuntimeControl control(std::move(config));
        ASSERT_TRUE(control.requestReload());
        ASSERT_TRUE(until([&] { return entered.load(); }));
        std::thread stopping(
            [&]
            {
                control.stop();
                stopped = true;
            }
        );
        EXPECT_TRUE(until([&] { return control.draining(); }));
        EXPECT_FALSE(stopped.load());
        for (int index = 0; index < 1000; ++index)
            EXPECT_FALSE(control.requestReload());
        release = true;
        stopping.join();
        EXPECT_TRUE(stopped.load());
        EXPECT_TRUE(control.metrics().closed);
        EXPECT_EQ(control.metrics().active, 0);
        EXPECT_EQ(control.metrics().peers, 0);
        const auto retained = store->snapshot();
        std::this_thread::sleep_for(10ms);
        EXPECT_EQ(notifications, 1);
        EXPECT_EQ(store->snapshot(), retained);
        control.stop();
    }
    EXPECT_EQ(descriptors(), baseline);
}
TEST(
    RuntimeControlTest,
    RepeatedOutsideThreadStopDoesNotConfuseRetiredOwnerIdentity
)
{
    const auto baseline = descriptors();
    {
        Directory directory;
        directory.write(document(2));
        auto store = std::make_shared<ConfigStore>(snapshot(1));
        std::atomic<RuntimeControl*> instance{nullptr};
        std::atomic<bool> selfStopRejected{false};
        std::thread::id retiredOwner;
        RuntimeControl::Config config;
        config.store = store;
        config.configFile = directory.file;
        config.notify = [&](auto)
        {
            retiredOwner = std::this_thread::get_id();
            try
            {
                instance.load()->stop();
            }
            catch (const std::logic_error&)
            {
                selfStopRejected = true;
            }
        };
        RuntimeControl control(std::move(config));
        instance = &control;
        ASSERT_TRUE(control.requestReload());
        ASSERT_TRUE(
            until([&] { return control.metrics().applied == 1; })
        );
        EXPECT_TRUE(selfStopRejected.load());
        control.stop();
        ASSERT_TRUE(control.metrics().closed);
        std::atomic<int> reused{0}, rejected{0};
        for (int index = 0; index < 64; ++index)
        {
            std::thread outside(
                [&]
                {
                    if (std::this_thread::get_id() == retiredOwner)
                        ++reused;
                    try
                    {
                        control.stop();
                    }
                    catch (...)
                    {
                        ++rejected;
                    }
                }
            );
            outside.join();
        }
        // Thread reuse is platform-controlled; the property
        // identifies when this run exercised the retired-owner ID
        // while every stop must work.
        RecordProperty("retired_owner_id_reuses", reused.load());
        EXPECT_EQ(rejected.load(), 0);
        EXPECT_EQ(control.metrics().active, 0);
        EXPECT_EQ(control.metrics().peers, 0);
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    RuntimeControlTest, FixedHealthPortRestartsAfterCompletedResponse
)
{
    const auto baseline = descriptors();
    std::uint16_t port = 0;
    {
        RuntimeControl initial({});
        port = initial.port();
        EXPECT_NE(
            request(port, "/health/alive").find("200 OK"),
            std::string::npos
        );
        initial.stop();
    }
    for (unsigned restart = 0; restart < 3; ++restart)
    {
        RuntimeControl::Config config;
        config.healthPort = port;
        RuntimeControl replacement(std::move(config));
        EXPECT_NE(
            request(port, "/health/alive").find("200 OK"),
            std::string::npos
        );
        replacement.stop();
    }
    EXPECT_EQ(descriptors(), baseline);
}

} // namespace
} // namespace arena::runtime
