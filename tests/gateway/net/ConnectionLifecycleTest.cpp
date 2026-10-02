#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
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
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                         0, descriptors) != 0)
        {
            throw std::system_error(errno, std::generic_category(), "socketpair");
        }
        connection = FileDescriptor(descriptors[0]);
        peer = FileDescriptor(descriptors[1]);
    }

    FileDescriptor connection;
    FileDescriptor peer;
};
} // namespace

TEST(TcpConnectionTest, ReadsAndEchoesIncomingBytes)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(&loop, Socket(std::move(pair.connection)));
    connection.setMessageCallback(
        [](TcpConnection& conn, std::string_view bytes)
        { conn.send(bytes.data(), bytes.size()); });
    constexpr std::string_view message = "hello";

    ASSERT_EQ(::write(pair.peer.get(), message.data(), message.size()),
              static_cast<ssize_t>(message.size()));
    connection.handleRead();

    char received[16]{};
    ASSERT_EQ(::read(pair.peer.get(), received, sizeof(received)),
              static_cast<ssize_t>(message.size()));
    EXPECT_EQ(std::string_view(received, message.size()), message);
}

TEST(TcpConnectionTest, PeerEofNotifiesCloseCallbackOnce)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(&loop, Socket(std::move(pair.connection)));
    int closeCount = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closeCount; });

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
    ASSERT_EQ(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                            &length), 0);

    Socket peer = Socket::createTcp();
    ASSERT_EQ(::connect(peer.get(), reinterpret_cast<sockaddr*>(&address), length),
              0);

    auto accepted = listener.accept();
    ASSERT_TRUE(accepted.has_value());
    EXPECT_NE(::fcntl(accepted->get(), F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(accepted->get(), F_GETFD) & FD_CLOEXEC, 0);
}

TEST(TcpConnectionTest, EchoesBytesBeforePeerWriteShutdown)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(&loop, Socket(std::move(pair.connection)));
    connection.setMessageCallback(
        [](TcpConnection& conn, std::string_view bytes)
        { conn.send(bytes.data(), bytes.size()); });
    constexpr std::string_view message = "last message";

    ASSERT_EQ(::write(pair.peer.get(), message.data(), message.size()),
              static_cast<ssize_t>(message.size()));
    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.handleRead();

    char received[32]{};
    ASSERT_EQ(::read(pair.peer.get(), received, sizeof(received)),
              static_cast<ssize_t>(message.size()));
    EXPECT_EQ(std::string_view(received, message.size()), message);
}

TEST(AcceptorTest, DispatchesAcceptedSocketAndPeerAddress)
{
    uint16_t port = 0;
    {
        Socket reservation = Socket::createTcp();
        reservation.bind(service::gateway::InetAddress(0, true));
        sockaddr_in address{};
        socklen_t length = sizeof(address);
        ASSERT_EQ(::getsockname(reservation.get(),
                                reinterpret_cast<sockaddr*>(&address),
                                &length), 0);
        port = ntohs(address.sin_port);
    }

    EventLoop loop;
    service::gateway::Acceptor acceptor(
        &loop, service::gateway::InetAddress(port, true));
    int acceptedCount = 0;
    acceptor.setNewConnectionCallback(
        [&](Socket&& accepted, const service::gateway::InetAddress& peer)
        {
            EXPECT_TRUE(accepted.valid());
            EXPECT_EQ(peer.ip(), "127.0.0.1");
            ++acceptedCount;
            loop.quit();
        });

    Socket client = Socket::createTcp();
    service::gateway::InetAddress address(port, true);
    ASSERT_EQ(::connect(client.get(), address.sockaddrPtr(), address.length()),
              0);
    loop.loop();

    EXPECT_TRUE(acceptor.listening());
    EXPECT_EQ(acceptedCount, 1);
}
TEST(TcpConnectionTest, DrainsPendingOutputBeforeClosingAfterPeerShutdown)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(&loop, Socket(std::move(pair.connection)));
    int closeCount = 0;
    connection.setCloseCallback([&](TcpConnection*) { ++closeCount; });

    int sendBufferSize = 4096;
    ASSERT_EQ(::setsockopt(connection.fd(), SOL_SOCKET, SO_SNDBUF,
                           &sendBufferSize, sizeof(sendBufferSize)), 0);
    const std::string payload(128 * 1024, 'x');
    connection.send(payload.data(), payload.size());
    ASSERT_NE(connection.channel().events() & EPOLLOUT, 0U);

    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    connection.channel().setRevents(EPOLLRDHUP);
    connection.channel().handleEvent();
    EXPECT_EQ(closeCount, 0);

    size_t received = 0;
    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && closeCount == 0; ++attempt)
    {
        while (true)
        {
            const ssize_t n = ::read(pair.peer.get(), buffer, sizeof(buffer));
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
        const ssize_t n = ::read(pair.peer.get(), buffer, sizeof(buffer));
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

    int highCalls = 0;
    int lowCalls = 0;
    connection.setHighWaterMarkCallback(
        [&](TcpConnection& conn, std::size_t pending)
        {
            EXPECT_GE(pending, TcpConnection::kHighWaterMark);
            ++highCalls;
            conn.pauseReading();
        }
    );
    connection.setLowWaterMarkCallback(
        [&](TcpConnection& conn, std::size_t pending)
        {
            EXPECT_LE(pending, TcpConnection::kLowWaterMark);
            ++lowCalls;
            conn.resumeReading();
        }
    );
    const std::string payload(
        TcpConnection::kHighWaterMark + 128 * 1024,
        'x'
    );
    connection.send(payload.data(), payload.size());
    ASSERT_EQ(connection.channel().events() & EPOLLIN, 0U);
    ASSERT_NE(connection.channel().events() & EPOLLOUT, 0U);
    EXPECT_EQ(highCalls, 1);

    int messages = 0;
    connection.setMessageCallback(
        [&](TcpConnection& conn, std::string_view bytes)
        {
            EXPECT_EQ(bytes, "held");
            ++messages;
            conn.send(bytes.data(), bytes.size());
        }
    );
    ASSERT_EQ(::write(pair.peer.get(), "held", 4), 4);
    connection.handleRead();
    EXPECT_EQ(messages, 0);

    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && messages == 0; ++attempt)
    {
        while (::read(pair.peer.get(), buffer, sizeof(buffer)) > 0)
        {
        }
        connection.handleWrite();
    }

    EXPECT_EQ(messages, 1);
    EXPECT_EQ(lowCalls, 1);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);
}

TEST(TcpConnectionTest, SustainedHighWaterDisconnectsSlowClient)
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
        TcpConnection::kHighWaterMark + 128 * 1024,
        'x'
    );
    connection.send(payload.data(), payload.size());
    // Without a policy callback, watermarks do not change read interest.
    ASSERT_NE(connection.channel().events() & EPOLLIN, 0U);

    const auto now = std::chrono::steady_clock::now();
    connection.closeIfSlow(now + std::chrono::seconds(1));
    EXPECT_EQ(closes, 0);

    connection.closeIfSlow(
        now + TcpConnection::kSlowClientTimeout +
        std::chrono::seconds(1)
    );
    connection.closeIfSlow(
        now + TcpConnection::kSlowClientTimeout +
        std::chrono::seconds(2)
    );
    EXPECT_EQ(closes, 1);
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

TEST(TcpConnectionTest, WatermarkCallbacksLeaveReadPolicyToCaller)
{
    SocketPair pair;
    EventLoop loop;
    TcpConnection connection(&loop, Socket(std::move(pair.connection)));

    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection.fd(), SOL_SOCKET, SO_SNDBUF,
            &sendBufferSize, sizeof(sendBufferSize)
        ),
        0
    );

    int highCalls = 0;
    int lowCalls = 0;
    connection.setHighWaterMarkCallback(
        [&](TcpConnection&, std::size_t pending)
        {
            EXPECT_GE(pending, TcpConnection::kHighWaterMark);
            ++highCalls;
        }
    );
    connection.setLowWaterMarkCallback(
        [&](TcpConnection&, std::size_t pending)
        {
            EXPECT_LE(pending, TcpConnection::kLowWaterMark);
            ++lowCalls;
        }
    );

    const std::string payload(
        TcpConnection::kHighWaterMark + 128 * 1024, 'x'
    );
    connection.send(payload.data(), payload.size());
    ASSERT_EQ(highCalls, 1);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);

    char buffer[8192];
    for (int attempt = 0; attempt < 1000 && lowCalls == 0; ++attempt)
    {
        while (::read(pair.peer.get(), buffer, sizeof(buffer)) > 0)
        {
        }
        connection.handleWrite();
    }

    EXPECT_EQ(lowCalls, 1);
    EXPECT_NE(connection.channel().events() & EPOLLIN, 0U);
}
