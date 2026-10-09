#include "ConnectionRuntimeTestSupport.h"
using service::gateway::BackpressureFeature;
using service::gateway::IConnection;
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"
#include "os/FileDescriptor.h"

using service::FileDescriptor;
using service::gateway::EventLoop;
using service::gateway::Socket;
using service::gateway::TcpConnection;

namespace
{
struct SocketPair
{
    SocketPair()
    {
        int descriptors[2];
        if (::socketpair(
                AF_UNIX,
                SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                0,
                descriptors
            ) != 0)
        {
            throw std::system_error(
                errno,
                std::generic_category(),
                "socketpair"
            );
        }
        connection = FileDescriptor(descriptors[0]);
        peer = FileDescriptor(descriptors[1]);
    }

    FileDescriptor connection;
    FileDescriptor peer;
};
} // namespace

TEST(TcpConnectionTest, ConcurrentSendsDrainOrderedRecordsOnOwner)
{
    using namespace std::chrono_literals;
    SocketPair pair;
    const int flags = ::fcntl(pair.peer.get(), F_GETFL);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(
        ::fcntl(pair.peer.get(), F_SETFL, flags & ~O_NONBLOCK),
        0
    );
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    connection->channel().tie(connection);
    constexpr int producers = 4;
    constexpr int records = 8;
    constexpr int recordSize = 4096;
    std::array<std::thread, producers> senders;
    std::atomic<int> finished{0};
    for (int producer = 0; producer < producers; ++producer)
    {
        senders[producer] = std::thread(
            [&, producer]
            {
                for (int record = 0; record < records; ++record)
                {
                    const std::string payload(
                        recordSize,
                        static_cast<char>(
                            33 + producer * records + record
                        )
                    );
                    connection->send(payload.data(), payload.size());
                }
                if (++finished == producers)
                {
                    loop.queueInLoop(
                        [weak = std::weak_ptr(connection)]
                        {
                            if (auto self = weak.lock())
                            {
                                self->shutdownWrite();
                            }
                        }
                    );
                }
            }
        );
    }

    std::string received;
    int readError = 0;
    std::thread reader(
        [&]
        {
            char buffer[8192];
            for (;;)
            {
                const ssize_t count = ::read(
                    pair.peer.get(),
                    buffer,
                    sizeof(buffer)
                );
                if (count > 0)
                {
                    received.append(
                        buffer,
                        static_cast<std::size_t>(count)
                    );
                }
                else
                {
                    if (count < 0)
                    {
                        readError = errno;
                    }
                    break;
                }
            }
            loop.quit();
        }
    );
    bool timedOut = false;
    loop.runAfter(
        3s,
        [&]
        {
            timedOut = true;
            ::shutdown(connection->fd(), SHUT_WR);
            loop.quit();
        }
    );
    loop.loop();
    for (auto& sender : senders)
    {
        sender.join();
    }
    reader.join();
    EXPECT_FALSE(timedOut);
    EXPECT_EQ(readError, 0);
    EXPECT_EQ(received.size(), producers * records * recordSize);
    if (received.size() == producers * records * recordSize)
    {
        std::array<int, producers * records> seen{};
        for (std::size_t offset = 0; offset < received.size();
             offset += recordSize)
        {
            const char value = received[offset];
            ASSERT_GE(value, 33);
            ASSERT_LT(value, 33 + static_cast<int>(seen.size()));
            ++seen[value - 33];
            EXPECT_EQ(
                received.substr(offset, recordSize),
                std::string(recordSize, value)
            );
        }
        for (int count : seen)
        {
            EXPECT_EQ(count, 1);
        }
    }
    EXPECT_EQ(connection->pendingBytes(), 0);
    loop.removeChannel(&connection->channel());
    connection.reset();
}

TEST(TcpConnectionTest, PeerFinWaitsForAcceptedForeignSend)
{
    SocketPair pair;
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    const std::string payload = "reply before close";
    std::thread sender(
        [&] { connection->send(payload.data(), payload.size()); }
    );
    sender.join();
    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection->handleRead();
    ASSERT_TRUE(connection->connected());
    loop.queueInLoop([&] { loop.quit(); });
    loop.loop();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(connection->pendingBytes(), 0);
    char buffer[64]{};
    ASSERT_EQ(
        ::read(pair.peer.get(), buffer, sizeof(buffer)),
        static_cast<ssize_t>(payload.size())
    );
    EXPECT_EQ(std::string_view(buffer, payload.size()), payload);
    loop.removeChannel(&connection->channel());
}
TEST(TcpConnectionTest, ForeignOutputLimitClosesAndReleasesQueue)
{
    SocketPair pair;
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    connection->setCloseCallback([&](TcpConnection*) { ++closes; });
    const std::string payload(600 * 1024, 'x');
    std::thread first(
        [&] { connection->send(payload.data(), payload.size()); }
    );
    first.join();
    std::thread second(
        [&] { connection->send(payload.data(), payload.size()); }
    );
    second.join();
    EXPECT_LE(
        connection->pendingBytes(),
        TcpConnection::kHardOutputLimit
    );
    loop.queueInLoop([&] { loop.quit(); });
    loop.loop();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(connection->pendingBytes(), 0);
    EXPECT_EQ(closes, 1);
    loop.removeChannel(&connection->channel());
}
TEST(TcpConnectionTest, CloseDropsForeignQueuedSendsAndWeakDrain)
{
    SocketPair pair;
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    const std::string payload(64 * 1024, 'q');
    std::thread sender(
        [&] { connection->send(payload.data(), payload.size()); }
    );
    sender.join();
    ASSERT_EQ(connection->pendingBytes(), payload.size());
    connection->close();
    EXPECT_EQ(connection->pendingBytes(), 0);
    std::thread lateSender(
        [&] { connection->send(payload.data(), payload.size()); }
    );
    lateSender.join();
    EXPECT_EQ(connection->pendingBytes(), 0);
    loop.removeChannel(&connection->channel());
    std::weak_ptr<TcpConnection> weak = connection;
    connection.reset();
    EXPECT_TRUE(weak.expired());
    loop.queueInLoop([&] { loop.quit(); });
    loop.loop();
    char byte;
    EXPECT_EQ(::read(pair.peer.get(), &byte, 1), 0);
}

TEST(TcpConnectionTest, ReadsAndEchoesIncomingBytes)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    connection.setMessageCallback(
        [](TcpConnection& conn, std::string_view bytes)
        { conn.send(bytes.data(), bytes.size()); }
    );
    constexpr std::string_view message = "hello";

    ASSERT_EQ(
        ::write(pair.peer.get(), message.data(), message.size()),
        static_cast<ssize_t>(message.size())
    );
    connection.handleRead();

    char received[16]{};
    ASSERT_EQ(
        ::read(pair.peer.get(), received, sizeof(received)),
        static_cast<ssize_t>(message.size())
    );
    EXPECT_EQ(std::string_view(received, message.size()), message);
}

TEST(TcpConnectionTest, ClosedPeerSendDoesNotRaiseSigpipe)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });

    pair.peer = FileDescriptor();
    connection.send("x", 1);
    connection.send("again", 5);

    EXPECT_EQ(closes, 1);
}

TEST(TcpConnectionTest, PeerEofNotifiesCloseCallbackOnce)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closeCount = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closeCount; }
    );

    pair.peer = FileDescriptor();
    connection.handleRead();
    connection.handleClose();

    EXPECT_EQ(closeCount, 1);
}

TEST(SocketTest, NonBlockingAcceptReturnsEmptyWhenNoConnectionIsReady)
{
    Socket listener = Socket::createTcpNonBlocking();
    listener.bind(service::gateway::InetAddress(0, true));
    listener.listen();

    EXPECT_FALSE(listener.accept().has_value());
}

TEST(SocketTest, AcceptedSocketIsNonBlockingAndCloseOnExec)
{
    Socket listener = Socket::createTcpNonBlocking();
    listener.bind(service::gateway::InetAddress(0, true));
    listener.listen();

    sockaddr_in address{};
    socklen_t length = sizeof(address);
    ASSERT_EQ(
        ::getsockname(
            listener.get(),
            reinterpret_cast<sockaddr*>(&address),
            &length
        ),
        0
    );

    Socket peer = Socket::createTcp();
    ASSERT_EQ(
        ::connect(
            peer.get(),
            reinterpret_cast<sockaddr*>(&address),
            length
        ),
        0
    );

    auto accepted = listener.accept();
    ASSERT_TRUE(accepted.has_value());
    EXPECT_NE(::fcntl(accepted->get(), F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(accepted->get(), F_GETFD) & FD_CLOEXEC, 0);
}

TEST(TcpConnectionTest, EchoesBytesBeforePeerWriteShutdown)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    connection.setMessageCallback(
        [](TcpConnection& conn, std::string_view bytes)
        { conn.send(bytes.data(), bytes.size()); }
    );
    constexpr std::string_view message = "last message";

    ASSERT_EQ(
        ::write(pair.peer.get(), message.data(), message.size()),
        static_cast<ssize_t>(message.size())
    );
    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.handleRead();

    char received[32]{};
    ASSERT_EQ(
        ::read(pair.peer.get(), received, sizeof(received)),
        static_cast<ssize_t>(message.size())
    );
    EXPECT_EQ(std::string_view(received, message.size()), message);
}

TEST(TcpConnectionTest, LocalWriteShutdownKeepsReceivingUntilPeerFin)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    std::string received;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });
    connection.setMessageCallback([&](TcpConnection&,
                                      std::string_view bytes)
                                  { received.append(bytes); });

    connection.send("reply", 5);
    connection.shutdownWrite();
    connection.shutdownWrite();
    connection.send("ignored", 7);

    char buffer[32]{};
    ASSERT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 5);
    EXPECT_EQ(std::string_view(buffer, 5), "reply");
    EXPECT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 0);
    EXPECT_EQ(closes, 0);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);
    EXPECT_EQ(connection.channel().events() & EPOLLOUT, 0U);

    ASSERT_EQ(::write(pair.peer.get(), "request", 7), 7);
    connection.handleRead();
    EXPECT_EQ(received, "request");
    EXPECT_EQ(closes, 0);

    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.handleRead();
    connection.handleRead();
    EXPECT_EQ(closes, 1);
}

TEST(TcpConnectionTest, QueuedWriteShutdownWaitsForPeerFin)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    std::string input;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });
    connection.setMessageCallback([&](TcpConnection&,
                                      std::string_view bytes)
                                  { input.append(bytes); });

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    const std::string payload(128 * 1024, 'x');
    connection.send(payload.data(), payload.size());
    ASSERT_GT(connection.pendingOutputBytes(), 0U);
    connection.shutdownWrite();
    connection.send("ignored", 7);

    std::string received;
    char buffer[8192];
    bool sawFin = false;
    for (int attempt = 0; attempt < 1000 && !sawFin; ++attempt)
    {
        while (true)
        {
            const ssize_t n = ::read(
                pair.peer.get(),
                buffer,
                sizeof(buffer)
            );
            if (n > 0)
            {
                received.append(buffer, static_cast<size_t>(n));
                continue;
            }
            if (n == 0)
            {
                sawFin = true;
                break;
            }
            ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
            break;
        }
        if (!sawFin)
        {
            connection.handleWrite();
        }
    }

    EXPECT_TRUE(sawFin);
    EXPECT_EQ(received, payload);
    EXPECT_EQ(closes, 0);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);

    ASSERT_EQ(::write(pair.peer.get(), "last", 4), 4);
    connection.handleRead();
    EXPECT_EQ(input, "last");
    EXPECT_EQ(closes, 0);

    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.handleRead();
    EXPECT_EQ(closes, 1);
}

TEST(TcpConnectionTest, LocalWriteShutdownDrainsBeforeFinAfterPeerFin)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    const std::string payload(128 * 1024, 'x');
    connection.send(payload.data(), payload.size());
    ASSERT_GT(connection.pendingOutputBytes(), 0U);
    connection.shutdownWrite();
    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.handleRead();
    EXPECT_EQ(closes, 0);

    std::string received;
    char buffer[8192];
    bool sawFin = false;
    for (int attempt = 0; attempt < 1000 && !sawFin; ++attempt)
    {
        while (true)
        {
            const ssize_t n = ::read(
                pair.peer.get(),
                buffer,
                sizeof(buffer)
            );
            if (n > 0)
            {
                received.append(buffer, static_cast<size_t>(n));
                continue;
            }
            if (n == 0)
            {
                sawFin = true;
                break;
            }
            ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
            break;
        }
        if (!sawFin)
        {
            connection.handleWrite();
        }
    }

    EXPECT_TRUE(sawFin);
    EXPECT_EQ(received, payload);
    EXPECT_EQ(connection.pendingOutputBytes(), 0U);
    EXPECT_EQ(closes, 1);
}

TEST(AcceptorTest, DispatchesAcceptedSocketAndPeerAddress)
{
    uint16_t port = 0;
    {
        Socket reservation = Socket::createTcp();
        reservation.bind(service::gateway::InetAddress(0, true));
        sockaddr_in address{};
        socklen_t length = sizeof(address);
        ASSERT_EQ(
            ::getsockname(
                reservation.get(),
                reinterpret_cast<sockaddr*>(&address),
                &length
            ),
            0
        );
        port = ntohs(address.sin_port);
    }

    EventLoop loop;
    service::gateway::Acceptor acceptor(
        &loop,
        service::gateway::InetAddress(port, true)
    );
    int acceptedCount = 0;
    acceptor.setNewConnectionCallback(
        [&](Socket&& accepted,
            const service::gateway::InetAddress& peer)
        {
            EXPECT_TRUE(accepted.valid());
            EXPECT_EQ(peer.ip(), "127.0.0.1");
            ++acceptedCount;
            loop.quit();
        }
    );

    Socket client = Socket::createTcp();
    service::gateway::InetAddress address(port, true);
    ASSERT_EQ(
        ::connect(
            client.get(),
            address.sockaddrPtr(),
            address.length()
        ),
        0
    );
    loop.loop();

    EXPECT_TRUE(acceptor.listening());
    EXPECT_EQ(acceptedCount, 1);
}
TEST(
    TcpConnectionTest,
    DrainsPendingOutputBeforeClosingAfterPeerShutdown
)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closeCount = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closeCount; }
    );

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    const std::string payload(128 * 1024, 'x');
    connection.send(payload.data(), payload.size());
    ASSERT_NE(connection.channel().events() & EPOLLOUT, 0U);

    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.channel().setRevents(EPOLLRDHUP);
    connection.channel().handleEvent();
    EXPECT_EQ(closeCount, 0);

    size_t received = 0;
    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && closeCount == 0;
         ++attempt)
    {
        while (true)
        {
            const ssize_t n = ::read(
                pair.peer.get(),
                buffer,
                sizeof(buffer)
            );
            if (n > 0)
            {
                received += static_cast<size_t>(n);
                continue;
            }
            ASSERT_EQ(n, -1);
            ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
            break;
        }
        connection.handleWrite();
    }

    while (true)
    {
        const ssize_t n = ::read(
            pair.peer.get(),
            buffer,
            sizeof(buffer)
        );
        if (n <= 0)
        {
            break;
        }
        received += static_cast<size_t>(n);
    }

    EXPECT_EQ(received, payload.size());
    EXPECT_EQ(closeCount, 1);
}

TEST(
    TcpConnectionTest,
    HighWaterPausesReadsAndLowWaterResumesPendingEdge
)
{
    SocketPair pair;
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    int messages = 0;
    auto runtime = testRuntime(
        connection,
        connectionScheduler(loop),
        [&](auto)
        {
            return [&](IConnection& conn, auto bytes)
            {
                EXPECT_EQ(
                    std::string_view(
                        reinterpret_cast<const char*>(bytes.data()),
                        bytes.size()
                    ),
                    "held"
                );
                ++messages;
                conn.send(bytes);
            };
        }
    );
    const std::string payload(
        BackpressureFeature::kHighWaterMark + 128 * 1024,
        'x'
    );
    runtime->send(std::as_bytes(std::span(payload)));
    ASSERT_EQ(connection->channel().events() & EPOLLIN, 0U);
    ASSERT_NE(connection->channel().events() & EPOLLOUT, 0U);
    ASSERT_EQ(::write(pair.peer.get(), "held", 4), 4);
    connection->handleRead();
    EXPECT_EQ(messages, 0);
    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && messages == 0; ++attempt)
    {
        while (::read(pair.peer.get(), buffer, sizeof(buffer)) > 0)
        {
        }
        connection->handleWrite();
    }
    EXPECT_EQ(messages, 1);
    EXPECT_NE(connection->channel().events() & EPOLLIN, 0U);
    loop.removeChannel(&connection->channel());
}
TEST(TcpConnectionTest, SustainedHighWaterDisconnectsSlowClient)
{
    SocketPair pair;
    EventLoop loop;
    auto connection = std::make_shared<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    auto runtime = testRuntime(
        connection,
        connectionScheduler(loop),
        {},
        [](auto& features, const auto&)
        {
            features.template add<BackpressureFeature>(
                BackpressureFeature::kLowWaterMark,
                BackpressureFeature::kHighWaterMark,
                std::chrono::milliseconds(20)
            );
        }
    );
    int closes = 0;
    connection->setCloseCallback(
        [&](auto*)
        {
            runtime->transportClosed();
            ++closes;
        }
    );
    const std::string payload(
        BackpressureFeature::kHighWaterMark + 128 * 1024,
        'x'
    );
    runtime->send(std::as_bytes(std::span(payload)));
    EXPECT_TRUE(runtime->connected());
    EXPECT_EQ(closes, 0);
    loop.runAfter(
        std::chrono::milliseconds(100),
        [&] { loop.quit(); }
    );
    loop.loop();
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(connection->pendingBytes(), 0);
    connection->close();
    EXPECT_EQ(closes, 1);
    loop.removeChannel(&connection->channel());
}
TEST(TcpConnectionTest, HardOutputLimitDisconnectsImmediately)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );

    const std::string payload(
        TcpConnection::kHardOutputLimit + 8192,
        'x'
    );
    connection.send(payload.data(), payload.size());

    EXPECT_EQ(closes, 1);
}

TEST(TcpConnectionTest, OutputEventsLeaveReadPolicyToComponents)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    std::vector<std::size_t> reported;
    connection.setOutputCallback(
        [&](std::size_t bytes)
        {
            EXPECT_EQ(bytes, connection.pendingBytes());
            reported.push_back(bytes);
        }
    );
    const std::string payload(
        BackpressureFeature::kHighWaterMark + 128 * 1024,
        'x'
    );
    connection.send(payload.data(), payload.size());
    ASSERT_FALSE(reported.empty());
    EXPECT_GT(reported.front(), BackpressureFeature::kHighWaterMark);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);
    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && connection.pendingBytes();
         ++attempt)
    {
        while (::read(pair.peer.get(), buffer, sizeof(buffer)) > 0)
        {
        }
        connection.handleWrite();
    }
    EXPECT_EQ(reported.back(), 0);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);
}
TEST(TcpConnectionTest, SlowPeerPendingOutputIsBounded)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(
        &loop,
        Socket(std::move(pair.connection))
    );
    int closes = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closes; });

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );

    const std::string payload(64 * 1024, 'x');
    for (int attempt = 0; attempt < 64 && closes == 0; ++attempt)
    {
        connection.send(payload.data(), payload.size());
        EXPECT_LE(
            connection.pendingOutputBytes(),
            TcpConnection::kHardOutputLimit
        );
    }

    ASSERT_EQ(closes, 1);
    const std::size_t pendingAtClose = connection.pendingOutputBytes(
    );
    connection.send(payload.data(), payload.size());
    EXPECT_EQ(connection.pendingOutputBytes(), pendingAtClose);
}

TEST(TcpServerTest, StopWithoutConnectionsIsImmediateAndIdempotent)
{
    EventLoop loop;
    service::gateway::TcpServer server(
        &loop,
        service::gateway::InetAddress(0, true),
        4
    );

    EXPECT_FALSE(server.stopped());
    EXPECT_EQ(server.maxConnections(), 4U);
    server.stop();
    EXPECT_TRUE(server.stopped());
    EXPECT_EQ(server.connectionCount(), 0U);
    EXPECT_NO_THROW(server.stop());
}

TEST(TcpServerTest, StopFromAnotherThreadIsRejectedBeforeMutation)
{
    EventLoop loop;
    service::gateway::TcpServer server(
        &loop,
        service::gateway::InetAddress(0, true),
        4
    );
    bool rejected = false;
    std::thread other(
        [&]()
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
    other.join();

    EXPECT_TRUE(rejected);
    EXPECT_FALSE(server.stopped());
    server.stop();
    EXPECT_TRUE(server.stopped());
}
