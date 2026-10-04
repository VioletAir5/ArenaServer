#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"

using namespace service::gateway;
using namespace std::chrono_literals;

namespace
{
std::size_t openFds()
{
    std::size_t count = 0;
    for ([[maybe_unused]] const auto& entry :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        ++count;
    }
    return count;
}

InetAddress unusedAddress()
{
    Socket reservation = Socket::createTcp();
    reservation.bind(InetAddress(0, true));
    sockaddr_in local{};
    socklen_t length = sizeof(local);
    if (::getsockname(
            reservation.get(),
            reinterpret_cast<sockaddr*>(&local),
            &length
        ) != 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "getsockname"
        );
    }
    return InetAddress(local);
}

Socket connectTo(const InetAddress& address)
{
    Socket peer = Socket::createTcp();
    if (::connect(
            peer.get(),
            address.sockaddrPtr(),
            address.length()
        ) != 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "connect"
        );
    }
    const timeval timeout{3, 0};
    if (::setsockopt(
            peer.get(),
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)
        ) != 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "recv timeout"
        );
    }
    return peer;
}
} // namespace

TEST(TcpServerWorkerTest, StopsAndJoinsIdlePoolBeforeReportingStopped)
{
    EventLoop base;
    const auto baseline = openFds();
    {
        TcpServer server(&base, InetAddress(0, true), 16, 60s, 4);
        EXPECT_EQ(server.workerCount(), 4u);
        EXPECT_FALSE(server.stopped());
        EXPECT_EQ(
            openFds(),
            baseline + 14
        ); // 4*3 worker fds + listener/reserve
        server.stop();
        EXPECT_TRUE(server.stopped());
        EXPECT_EQ(openFds(), baseline);
        EXPECT_NO_THROW(server.stop());
    }
    EXPECT_EQ(openFds(), baseline);
}

TEST(
    TcpServerWorkerTest,
    ActiveDestructionReclaimsSocketsAndSkipsLateNotices
)
{
    // Repeat with base-only and actual workers, including the base
    // loop continuing to dispatch after server destruction.
    for (std::size_t workers : {0u, 1u, 4u})
    {
        EventLoop base;
        std::vector<Socket> peers;
        const auto address = unusedAddress();
        auto server = std::make_unique<TcpServer>(
            &base,
            address,
            128,
            60s,
            workers
        );
        for (int i = 0; i < 64; ++i)
        {
            peers.push_back(connectTo(address));
        }

        bool destroyed = false;
        bool timedOut = false;
        const auto watchdog = base.runAfter(
            3s,
            [&]()
            {
                timedOut = true;
                base.quit();
            }
        );
        TimerId poll = 0;
        poll = base.runEvery(
            1ms,
            [&]()
            {
                if (server->connectionCount() != peers.size())
                {
                    return;
                }
                base.cancelTimer(poll);
                server.reset();
                destroyed = true;
                // Only this watchdog remains on base after owner
                // cleanup.
                EXPECT_EQ(base.pendingTimerCount(), 1u);
                base.queueInLoop([&]() { base.quit(); });
            }
        );
        base.loop();
        base.cancelTimer(poll);
        base.cancelTimer(watchdog);
        EXPECT_FALSE(timedOut);
        EXPECT_TRUE(destroyed);
        ASSERT_FALSE(server);
        for (auto& peer : peers)
        {
            char byte{};
            EXPECT_EQ(::recv(peer.get(), &byte, 1, MSG_DONTWAIT), 0);
        }
    }
}

TEST(
    TcpServerWorkerTest,
    GracefulStopFansOutFinAndReclaimsDeadlinePeers
)
{
    EventLoop base;
    const auto address = unusedAddress();
    std::vector<Socket> peers;
    TcpServer server(&base, address, 16, 60s, 4);
    for (int i = 0; i < 8; ++i)
    {
        peers.push_back(connectTo(address));
    }

    std::atomic<int> receivedFin{0};
    std::jthread clients(
        [&]()
        {
            for (std::size_t i = 0; i < peers.size(); ++i)
            {
                char byte{};
                if (::recv(peers[i].get(), &byte, 1, 0) == 0)
                {
                    receivedFin.fetch_add(1);
                    if (i % 2 == 0)
                    {
                        peers[i].shutdownWrite();
                    }
                }
            }
        }
    );
    bool timedOut = false;
    bool requested = false;
    const auto watchdog = base.runAfter(
        3s,
        [&]()
        {
            timedOut = true;
            base.quit();
        }
    );
    TimerId poll = 0;
    poll = base.runEvery(
        1ms,
        [&]()
        {
            if (!requested &&
                server.connectionCount() == peers.size())
            {
                requested = true;
                server.stop(50ms);
            }
            if (server.stopped())
            {
                base.cancelTimer(poll);
                base.quit();
            }
        }
    );
    base.loop();
    base.cancelTimer(poll);
    base.cancelTimer(watchdog);
    clients.join();
    EXPECT_FALSE(timedOut);
    EXPECT_TRUE(requested);
    EXPECT_EQ(receivedFin.load(), 8);
    EXPECT_TRUE(server.stopped());
    EXPECT_EQ(server.connectionCount(), 0u);
}

TEST(
    TcpServerWorkerTest, ForeignRegistrationAndConstructionAreRejected
)
{
    EventLoopThread worker;
    auto* owner = worker.startLoop();
    // Checks precede dereferencing a Channel or touching epoll.
    EXPECT_THROW(owner->addChannel(nullptr), std::logic_error);
    EXPECT_THROW(owner->updateChannel(nullptr), std::logic_error);
    EXPECT_THROW(owner->removeChannel(nullptr), std::logic_error);
    Socket socket = Socket::createTcpNonBlocking();
    EXPECT_THROW(
        TcpConnection(owner, std::move(socket)),
        std::logic_error
    );
    worker.stop();
}

TEST(
    TcpConnectionOwnershipDeathTest,
    ForeignFinalReleaseTerminatesWithoutThrow
)
{
    EXPECT_EXIT(
        {
            ::alarm(5);
            std::set_terminate(
                []() { ::_exit(std::current_exception() ? 4 : 0); }
            );
            EventLoop owner;
            auto connection = std::make_unique<TcpConnection>(
                &owner,
                Socket::createTcpNonBlocking()
            );
            owner.removeChannel(&connection->channel());
            std::jthread foreign([connection = std::move(connection)](
                                 ) mutable { connection.reset(); });
            foreign.join();
            ::_exit(3);
        },
        testing::ExitedWithCode(0),
        ""
    );
}
