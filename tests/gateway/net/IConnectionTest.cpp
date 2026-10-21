#include "net/IConnection.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <gtest/gtest.h>

#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "os/FileDescriptor.h"

using service::FileDescriptor;
using service::gateway::EventLoop;
using service::gateway::IConnection;
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

class IConnectionTest : public testing::Test
{
protected:
    IConnectionTest()
        : concrete(
              std::make_shared<TcpConnection>(
                  &loop, Socket(std::move(pair.connection))
              )
          )
        , connection(concrete)
    {
        concrete->channel().tie(concrete);
        concrete->setCloseCallback([this](TcpConnection*)
                                   { ++closes; });
    }

    ~IConnectionTest() override
    {
        loop.removeChannel(&concrete->channel());
        connection.reset();
        concrete.reset();
    }

    SocketPair pair;
    EventLoop loop;
    std::shared_ptr<TcpConnection> concrete;
    IConnection::Ptr connection;
    int closes{0};
};
} // namespace

TEST_F(IConnectionTest, ReportsClosedStateAndClosesOnlyOnce)
{
    EXPECT_TRUE(connection->connected());
    const auto id = connection->id();
    EXPECT_NE(id, 0U);

    connection->close();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(connection->id(), id);
    connection->close();
    connection->shutdown();
    connection->send(std::as_bytes(std::span("ignored", 7)));
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(connection->pendingBytes(), 0U);
    EXPECT_EQ(concrete->channel().events() & EPOLLOUT, 0U);
}

TEST_F(IConnectionTest, PauseAndResumeDeliverAlreadyAvailableBytes)
{
    std::string received;
    concrete->setMessageCallback([&](TcpConnection&,
                                     std::string_view bytes)
                                 { received.append(bytes); });

    connection->pauseRead();
    connection->pauseRead();
    ASSERT_EQ(concrete->channel().events() & EPOLLIN, 0U);
    ASSERT_EQ(::write(pair.peer.get(), "held", 4), 4);
    concrete->handleRead();
    EXPECT_TRUE(received.empty());

    connection->resumeRead();
    EXPECT_EQ(received, "held");
    EXPECT_NE(concrete->channel().events() & EPOLLIN, 0U);
    connection->resumeRead();
    EXPECT_EQ(received, "held");
}

TEST_F(
    IConnectionTest, HalfClosedConnectionKeepsReceivingUntilPeerFin
)
{
    std::string received;
    concrete->setMessageCallback([&](TcpConnection&,
                                     std::string_view bytes)
                                 { received.append(bytes); });
    connection->send(std::as_bytes(std::span("reply", 5)));
    connection->shutdown();
    EXPECT_TRUE(connection->connected());
    connection->send(std::as_bytes(std::span("ignored", 7)));

    char buffer[32]{};
    ASSERT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 5);
    EXPECT_EQ(std::string_view(buffer, 5), "reply");
    EXPECT_EQ(::read(pair.peer.get(), buffer, sizeof(buffer)), 0);
    ASSERT_EQ(::write(pair.peer.get(), "last", 4), 4);
    concrete->handleRead();
    EXPECT_EQ(received, "last");
    EXPECT_EQ(closes, 0);

    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    concrete->handleRead();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(closes, 1);
}

TEST_F(IConnectionTest, QueuedSendOwnsBytesUntilDrainedBeforeFin)
{
    int sendBufferSize = 4096;
    ASSERT_EQ(
        ::setsockopt(
            concrete->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBufferSize,
            sizeof(sendBufferSize)
        ),
        0
    );
    std::string payload(128 * 1024, 'x');
    const std::string expected = payload;
    connection->send(
        std::as_bytes(std::span(payload.data(), payload.size()))
    );
    ASSERT_GT(connection->pendingBytes(), 0U);
    EXPECT_EQ(
        connection->pendingBytes(),
        concrete->pendingOutputBytes()
    );
    payload.assign(payload.size(), 'y');
    connection->shutdown();
    EXPECT_TRUE(connection->connected());

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
                received.append(buffer, static_cast<std::size_t>(n));
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
            concrete->handleWrite();
        }
    }

    EXPECT_TRUE(sawFin);
    EXPECT_EQ(received, expected);
    EXPECT_EQ(connection->pendingBytes(), 0U);
    EXPECT_TRUE(connection->connected());
    ASSERT_EQ(::shutdown(pair.peer.get(), SHUT_WR), 0);
    concrete->handleRead();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(closes, 1);
}

TEST_F(IConnectionTest, IdentityIsStableAndNotReusedAfterDestruction)
{
    const auto firstId = connection->id();
    SocketPair secondPair;
    auto second = std::make_unique<TcpConnection>(
        &loop,
        Socket(std::move(secondPair.connection))
    );
    const auto secondId = second->id();
    EXPECT_NE(firstId, secondId);
    loop.removeChannel(&second->channel());
    second.reset();

    SocketPair thirdPair;
    second = std::make_unique<TcpConnection>(
        &loop,
        Socket(std::move(thirdPair.connection))
    );
    EXPECT_NE(second->id(), firstId);
    EXPECT_NE(second->id(), secondId);
    EXPECT_EQ(connection->id(), firstId);
    loop.removeChannel(&second->channel());
}

TEST(IConnectionOwnershipTest, VirtualDestructionOnOwnerClosesSocket)
{
    SocketPair pair;
    EventLoop loop;
    auto concrete = std::make_unique<TcpConnection>(
        &loop,
        Socket(std::move(pair.connection))
    );
    loop.removeChannel(&concrete->channel());
    std::unique_ptr<IConnection> connection(std::move(concrete));
    connection.reset();
    char byte{};
    EXPECT_EQ(::read(pair.peer.get(), &byte, 1), 0);
}
