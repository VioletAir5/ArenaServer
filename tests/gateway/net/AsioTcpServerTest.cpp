#include "net/AsioTcpServer.h"

#include <dirent.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using Tcp = boost::asio::ip::tcp;

std::size_t fdCount()
{
    auto* directory = ::opendir("/proc/self/fd");
    if (!directory)
    {
        throw std::runtime_error("opendir");
    }
    std::size_t count = 0;
    while (auto* entry = ::readdir(directory))
    {
        if (entry->d_name[0] != '.')
        {
            ++count;
        }
    }
    ::closedir(directory);
    return count;
}

std::uint16_t freePort(boost::asio::io_context& context)
{
    Tcp::acceptor probe(context, Tcp::endpoint(Tcp::v4(), 0));
    return probe.local_endpoint().port();
}

TEST(
    AsioTcpServerTest,
    InterfaceStopJoinsWorkersAndPreservesExternalContext
)
{
    for (std::size_t workers : {0u, 1u, 4u})
    {
        boost::asio::io_context context;
        boost::asio::steady_timer prime(context);
        const auto port = freePort(context);
        const auto baseline = fdCount();
        std::unique_ptr<ITcpServer>
            server = std::make_unique<AsioTcpServer>(
                context,
                InetAddress("127.0.0.1", port),
                8,
                1s,
                workers
            );
        EXPECT_EQ(server->maxConnections(), 8);
        EXPECT_EQ(server->workerCount(), workers);
        EXPECT_EQ(server->connectionCount(), 0);
        EXPECT_FALSE(server->stopped());
        EXPECT_THROW(server->stop(-1ms), std::invalid_argument);
        EXPECT_FALSE(server->stopped());
        server->stop();
        EXPECT_TRUE(server->stopped());
        EXPECT_EQ(fdCount(), baseline);
        bool called = false;
        boost::asio::post(context, [&] { called = true; });
        context.run();
        EXPECT_TRUE(called);
        server->stop(0ms);
    }
}

TEST(
    AsioTcpServerTest,
    VirtualDestructionReclaimsActiveAndQueuedConnections
)
{
    for (std::size_t workers : {0u, 1u, 4u})
    {
        boost::asio::io_context context;
        boost::asio::steady_timer prime(context);
        const auto port = freePort(context);
        const auto baseline = fdCount();
        auto server = std::unique_ptr<ITcpServer>(new AsioTcpServer(
            context,
            InetAddress("127.0.0.1", port),
            64,
            1s,
            workers
        ));
        std::vector<Tcp::socket> peers;
        for (int i = 0; i != 32; ++i)
        {
            peers.emplace_back(context);
            peers.back().connect(
                Tcp::endpoint(
                    boost::asio::ip::address_v4::loopback(),
                    port
                )
            );
        }
        context.run_for(20ms);
        EXPECT_EQ(server->connectionCount(), 32);
        server.reset();
        context.restart();
        context.poll(); // Late accept/close/ack callbacks must be
                        // harmless.
        peers.clear();
        EXPECT_EQ(fdCount(), baseline);
    }
}

TEST(AsioTcpServerTest, ImmediateStopHandlesRegistrationHandoff)
{
    for (std::size_t workers : {0u, 4u})
    {
        boost::asio::io_context context;
        const auto port = freePort(context);
        AsioTcpServer server(
            context,
            InetAddress("127.0.0.1", port),
            64,
            1s,
            workers
        );
        Tcp::socket peer(context);
        peer.connect(
            Tcp::endpoint(
                boost::asio::ip::address_v4::loopback(),
                port
            )
        );
        context.run_one();
        ASSERT_EQ(server.connectionCount(), 1);
        // An accepted socket can be queued on its worker when stop is
        // posted immediately behind the accept completion on the base
        // context.
        boost::asio::post(context, [&] { server.stop(0ms); });
        context.run();
        EXPECT_TRUE(server.stopped());
        EXPECT_EQ(server.connectionCount(), 0);
    }
}

TEST(AsioTcpServerTest, ForeignManagementThreadIsRejected)
{
    boost::asio::io_context context;
    AsioTcpServer server(
        context,
        InetAddress("127.0.0.1", freePort(context)),
        8
    );
    bool rejected = false;
    std::thread thread(
        [&]
        {
            try
            {
                server.stop();
            }
            catch (const std::logic_error&)
            {
                rejected = true;
            }
        }
    );
    thread.join();
    EXPECT_TRUE(rejected);
    EXPECT_FALSE(server.stopped());
    server.stop();
}

TEST(
    AsioTcpServerTest,
    InvalidConfigurationAndBindFailureLeaveNoDescriptors
)
{
    boost::asio::io_context context;
    boost::asio::steady_timer prime(context);
    Tcp::acceptor occupied(context, Tcp::endpoint(Tcp::v4(), 0));
    const auto baseline = fdCount();
    auto address = InetAddress(
        "127.0.0.1",
        occupied.local_endpoint().port()
    );
    EXPECT_THROW(
        AsioTcpServer(context, address, 0),
        std::invalid_argument
    );
    EXPECT_THROW(
        AsioTcpServer(context, address, 8, 0ms),
        std::invalid_argument
    );
    EXPECT_THROW(
        AsioTcpServer(context, address, 8, 1s, 4),
        boost::system::system_error
    );
    EXPECT_EQ(fdCount(), baseline);
}
} // namespace
} // namespace service::gateway
