#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"

using service::gateway::EventLoop;
using service::gateway::InetAddress;
using service::gateway::Socket;
using service::gateway::TcpConnection;
using service::gateway::TcpServer;

namespace
{
using namespace std::chrono_literals;

struct TcpPair
{
    Socket connection;
    Socket peer;
};

TcpPair connectPair()
{
    Socket listener = Socket::createTcpNonBlocking();
    listener.bind(InetAddress(0, true));
    listener.listen();
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    if (::getsockname(listener.get(),
                      reinterpret_cast<sockaddr*>(&address),
                      &length) != 0)
    {
        throw std::system_error(errno, std::generic_category(),
                                "getsockname");
    }

    Socket peer = Socket::createTcp();
    if (::connect(peer.get(), reinterpret_cast<sockaddr*>(&address),
                  length) != 0)
    {
        throw std::system_error(errno, std::generic_category(),
                                "connect");
    }
    auto accepted = listener.accept();
    if (!accepted)
    {
        throw std::runtime_error("completed TCP handshake not accepted");
    }
    return {std::move(*accepted), std::move(peer)};
}

// Instrument ownership in the test; no production destructor hook is needed.
struct TrackedConnection
{
    TrackedConnection(EventLoop& eventLoop, Socket socket)
        : loop(eventLoop)
        , owner(new TcpConnection(&loop, std::move(socket)),
                [this](TcpConnection* connection)
                {
                    delete connection;
                    ++destructions;
                    events.emplace_back("destroy");
                })
        , weak(owner)
        , descriptor(owner->fd())
    {
        owner->channel().tie(owner);
        owner->setCloseCallback(
            [this](TcpConnection* connection)
            {
                events.emplace_back("close-enter");
                ++closes;
                EXPECT_EQ(destructions, 0);
                loop.removeChannel(&connection->channel());
                registered = false;
                owner.reset();

                // The event guard, rather than an external owner,
                // must keep both connection and Channel alive here.
                EXPECT_FALSE(weak.expired());
                EXPECT_EQ(destructions, 0);
                EXPECT_EQ(connection->fd(), descriptor);
                connection->handleClose();
                EXPECT_EQ(closes, 1);
                events.emplace_back("close-exit");
                loop.quit();
            });
    }

    ~TrackedConnection()
    {
        if (owner && registered)
        {
            loop.removeChannel(&owner->channel());
        }
        owner.reset();
    }

    EventLoop& loop;
    int closes{0};
    int destructions{0};
    bool registered{true};
    std::vector<std::string> events;
    std::shared_ptr<TcpConnection> owner;
    std::weak_ptr<TcpConnection> weak;
    int descriptor;
};

enum class TerminalTrigger
{
    PeerFin,
    PeerReset,
    LocalShutdown
};

class ConnectionOwnershipTest
    : public testing::TestWithParam<TerminalTrigger>
{
};
} // namespace

TEST_P(ConnectionOwnershipTest, CloseReleasesConnectionExactlyOnce)
{
    auto pair = connectPair();
    EventLoop loop;
    TrackedConnection tracked(loop, std::move(pair.connection));
    bool timedOut = false;
    const auto watchdog = loop.runAfter(
        2s, [&]() { timedOut = true; loop.quit(); });

    switch (GetParam())
    {
    case TerminalTrigger::PeerFin:
        pair.peer.shutdownWrite();
        break;
    case TerminalTrigger::PeerReset:
    {
        const linger reset{1, 0};
        ASSERT_EQ(::setsockopt(pair.peer.get(), SOL_SOCKET, SO_LINGER,
                               &reset, sizeof(reset)), 0);
        pair.peer.close();
        break;
    }
    case TerminalTrigger::LocalShutdown:
    {
        tracked.owner->shutdownWrite();
        const timeval timeout{1, 0};
        ASSERT_EQ(::setsockopt(pair.peer.get(), SOL_SOCKET, SO_RCVTIMEO,
                               &timeout, sizeof(timeout)), 0);
        char byte{};
        ASSERT_EQ(::recv(pair.peer.get(), &byte, 1, 0), 0);
        EXPECT_EQ(tracked.destructions, 0);
        pair.peer.shutdownWrite();
        break;
    }
    }

    EXPECT_NO_THROW(loop.loop());
    loop.cancelTimer(watchdog);

    EXPECT_FALSE(timedOut);
    EXPECT_EQ(tracked.closes, 1);
    EXPECT_EQ(tracked.destructions, 1);
    EXPECT_TRUE(tracked.weak.expired());
    EXPECT_EQ(tracked.events,
              (std::vector<std::string>{
                  "close-enter", "close-exit", "destroy"}));
    EXPECT_EQ(::fcntl(tracked.descriptor, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

INSTANTIATE_TEST_SUITE_P(
    TcpTermination,
    ConnectionOwnershipTest,
    testing::Values(TerminalTrigger::PeerFin,
                    TerminalTrigger::PeerReset,
                    TerminalTrigger::LocalShutdown),
    [](const testing::TestParamInfo<TerminalTrigger>& info)
    {
        switch (info.param)
        {
        case TerminalTrigger::PeerFin:
            return "PeerFin";
        case TerminalTrigger::PeerReset:
            return "PeerReset";
        case TerminalTrigger::LocalShutdown:
            return "LocalShutdown";
        }
        return "Unknown";
    });

TEST(ConnectionCallbackTest, MessageFinishesAfterLastOwnerIsReleased)
{
    auto pair = connectPair();
    EventLoop loop;
    TrackedConnection tracked(loop, std::move(pair.connection));
    bool messageReturned = false;
    tracked.owner->setMessageCallback(
        [&](TcpConnection& connection, std::string_view bytes)
        {
            tracked.events.emplace_back("message-enter");
            connection.handleClose();
            EXPECT_FALSE(tracked.weak.expired());
            EXPECT_EQ(tracked.destructions, 0);
            EXPECT_EQ(bytes, "last message");
            EXPECT_EQ(connection.fd(), tracked.descriptor);
            connection.send("ignored", 7);
            tracked.events.emplace_back("message-exit");
            messageReturned = true;
        });
    ASSERT_EQ(::send(pair.peer.get(), "last message", 12, MSG_NOSIGNAL), 12);
    pair.peer.shutdownWrite();
    bool timedOut = false;
    const auto watchdog = loop.runAfter(
        2s, [&]() { timedOut = true; loop.quit(); });

    EXPECT_NO_THROW(loop.loop());
    loop.cancelTimer(watchdog);

    EXPECT_FALSE(timedOut);
    EXPECT_TRUE(messageReturned);
    EXPECT_EQ(tracked.closes, 1);
    EXPECT_EQ(tracked.destructions, 1);
    EXPECT_TRUE(tracked.weak.expired());
    EXPECT_EQ(tracked.events,
              (std::vector<std::string>{
                  "message-enter", "close-enter", "close-exit",
                  "message-exit", "destroy"}));
}

TEST(ConnectionCallbackTest, ThrowingCallbackStillReleasesConnectionOnce)
{
    auto pair = connectPair();
    EventLoop loop;
    TrackedConnection tracked(loop, std::move(pair.connection));
    tracked.owner->setMessageCallback(
        [&](TcpConnection& connection, std::string_view)
        {
            connection.handleClose();
            EXPECT_EQ(tracked.destructions, 0);
            throw std::runtime_error("callback failed");
        });
    ASSERT_EQ(::send(pair.peer.get(), "x", 1, MSG_NOSIGNAL), 1);
    const auto watchdog = loop.runAfter(2s, [&]() { loop.quit(); });

    EXPECT_THROW(loop.loop(), std::runtime_error);
    loop.cancelTimer(watchdog);

    EXPECT_EQ(tracked.closes, 1);
    EXPECT_EQ(tracked.destructions, 1);
    EXPECT_TRUE(tracked.weak.expired());
}

TEST(TcpServerOwnershipTest, QueuedRemovalIsSafeAfterServerDestruction)
{
    Socket reservation = Socket::createTcp();
    reservation.bind(InetAddress(0, true));
    sockaddr_in local{};
    socklen_t length = sizeof(local);
    ASSERT_EQ(::getsockname(reservation.get(),
                            reinterpret_cast<sockaddr*>(&local),
                            &length), 0);
    const InetAddress address(local);
    reservation.close();

    EventLoop loop;
    std::unique_ptr<service::gateway::ITcpServer> server =
        std::make_unique<TcpServer>(&loop, address, 4);
    Socket peer = Socket::createTcp();
    ASSERT_EQ(::connect(peer.get(), address.sockaddrPtr(),
                        address.length()), 0);

    bool timedOut = false;
    bool returnedFromStaleTask = false;
    const auto watchdog = loop.runAfter(
        2s, [&]() { timedOut = true; loop.quit(); });
    service::gateway::TimerId acceptanceCheck = 0;
    acceptanceCheck = loop.runEvery(
        1ms,
        [&]()
        {
            if (server->connectionCount() != 1)
            {
                return;
            }
            loop.cancelTimer(acceptanceCheck);
            // Pending functors run after the active Channel batch.
            loop.queueInLoop(
                [&]()
                {
                    server->stop(0ms); // Queues connection removal.
                    EXPECT_FALSE(server->stopped());
                    server.reset();   // Invalidates that queued task.
                    EXPECT_EQ(loop.pendingTimerCount(), 1U);
                    loop.queueInLoop(
                        [&]()
                        {
                            returnedFromStaleTask = true;
                            loop.quit();
                        });
                });
        });

    EXPECT_NO_THROW(loop.loop());
    loop.cancelTimer(watchdog);
    loop.cancelTimer(acceptanceCheck);

    EXPECT_FALSE(timedOut);
    EXPECT_FALSE(server);
    EXPECT_TRUE(returnedFromStaleTask);
    char byte{};
    EXPECT_EQ(::recv(peer.get(), &byte, 1, MSG_DONTWAIT), 0);
}
