#include "net/Socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <thread>

#include <gtest/gtest.h>

#include "net/InetAddress.h"

using namespace service::gateway;

TEST(SocketTest, CreateTcpSocket)
{
    Socket socket = Socket::createTcp();

    EXPECT_TRUE(socket.valid());
    EXPECT_GE(socket.get(), 0);

    // 确认它确实是 SOCK_STREAM
    int type = 0;
    socklen_t len = sizeof(type);

    ASSERT_EQ(
        ::getsockopt(socket.get(), SOL_SOCKET, SO_TYPE, &type, &len),
        0
    );

    EXPECT_EQ(type, SOCK_STREAM);
}

TEST(SocketTest, CreateTcpNonBlocking)
{
    Socket socket = Socket::createTcpNonBlocking();

    ASSERT_TRUE(socket.valid());

    int flags = ::fcntl(socket.get(), F_GETFL, 0);

    ASSERT_NE(flags, -1);

    EXPECT_NE(flags & O_NONBLOCK, 0);
}

TEST(SocketTest, CreateTcpNonBlockingHasCloseOnExec)
{
    Socket socket = Socket::createTcpNonBlocking();

    ASSERT_TRUE(socket.valid());

    int flags = ::fcntl(socket.get(), F_GETFD, 0);

    ASSERT_NE(flags, -1);

    EXPECT_NE(flags & FD_CLOEXEC, 0);
}

TEST(SocketTest, BindToLoopback)
{
    Socket socket = Socket::createTcp();

    InetAddress address(0, true);

    EXPECT_NO_THROW(socket.bind(address));

    sockaddr_in actual{};
    socklen_t len = sizeof(actual);

    ASSERT_EQ(
        ::getsockname(
            socket.get(),
            reinterpret_cast<sockaddr*>(&actual),
            &len
        ),
        0
    );

    EXPECT_EQ(actual.sin_family, AF_INET);

    // port=0 意味着让内核自动分配端口
    EXPECT_NE(ntohs(actual.sin_port), 0);
}

TEST(SocketTest, Listen)
{
    Socket socket = Socket::createTcp();

    InetAddress address(0, true);

    socket.bind(address);

    EXPECT_NO_THROW(socket.listen(SOMAXCONN));

    int acceptConn = 0;
    socklen_t len = sizeof(acceptConn);

    ASSERT_EQ(
        ::getsockopt(
            socket.get(),
            SOL_SOCKET,
            SO_ACCEPTCONN,
            &acceptConn,
            &len
        ),
        0
    );

    EXPECT_EQ(acceptConn, 1);
}

TEST(SocketTest, AcceptConnection)
{
    Socket server = Socket::createTcp();

    InetAddress listenAddress(0, true);

    server.bind(listenAddress);
    server.listen(SOMAXCONN);

    // bind(port=0) 后，查询内核实际分配的端口
    sockaddr_in actual{};
    socklen_t actualLen = sizeof(actual);

    ASSERT_EQ(
        ::getsockname(
            server.get(),
            reinterpret_cast<sockaddr*>(&actual),
            &actualLen
        ),
        0
    );

    const uint16_t port = ntohs(actual.sin_port);

    std::thread clientThread(
        [port]()
        {
            int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

            ASSERT_GE(fd, 0);

            sockaddr_in serverAddr{};
            serverAddr.sin_family = AF_INET;
            serverAddr.sin_port = htons(port);

            ASSERT_EQ(
                ::inet_pton(
                    AF_INET,
                    "127.0.0.1",
                    &serverAddr.sin_addr
                ),
                1
            );

            ASSERT_EQ(
                ::connect(
                    fd,
                    reinterpret_cast<sockaddr*>(&serverAddr),
                    sizeof(serverAddr)
                ),
                0
            );

            ::close(fd);
        }
    );

    InetAddress peer;

    std::optional<Socket> client = server.accept(&peer);

    ASSERT_TRUE(client.has_value());
    EXPECT_TRUE(client->valid());
    EXPECT_GE(client->get(), 0);

    clientThread.join();
}

TEST(SocketTest, DestructorClosesDescriptor)
{
    int fd = -1;

    {
        Socket socket = Socket::createTcp();

        fd = socket.get();

        ASSERT_GE(fd, 0);

        // 此时 fd 应该有效
        EXPECT_NE(::fcntl(fd, F_GETFD), -1);
    }

    // Socket 析构
    // -> FileDescriptor 析构
    // -> close(fd)

    errno = 0;

    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);

    EXPECT_EQ(errno, EBADF);
}