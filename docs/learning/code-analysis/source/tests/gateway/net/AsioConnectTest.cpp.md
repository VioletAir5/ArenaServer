# tests/gateway/net/AsioConnectTest.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；618行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include "net/AsioConnect.h"

#include <fcntl.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"

#include "ConnectionRuntimeTestSupport.h"

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using Tcp = boost::asio::ip::tcp;

class AsioConnectTest : public testing::Test
{
protected:
    boost::asio::io_context context;
    Tcp::acceptor listener{context, Tcp::endpoint(Tcp::v4(), 0)};
    Tcp::socket peer{context};
    std::shared_ptr<AsioConnect> connection;

    void SetUp() override
    {
        peer.connect(Tcp::endpoint(
            boost::asio::ip::address_v4::loopback(),
            listener.local_endpoint().port()
        ));
        Tcp::socket accepted(context);
        listener.accept(accepted);
        connection = std::make_shared<AsioConnect>(
            context,
            std::move(accepted)
        );
        peer.non_blocking(true);
    }
    void TearDown() override
    {
        if (connection)
        {
            connection->setCloseCallback({});
            connection->close();
            connection.reset();
        }
        context.restart();
        context.poll();
    }
    void pump(std::chrono::milliseconds time = 20ms)
    {
        context.restart();
        context.run_for(time);
    }
    void sendPeer(std::string_view bytes)
    {
        boost::system::error_code error;
        boost::asio::write(peer, boost::asio::buffer(bytes), error);
        ASSERT_FALSE(error) << error.message();
    }
    std::string receivePeer()
    {
        std::string result;
        char bytes[8192];
        boost::system::error_code error;
        for (;;)
        {
            auto count = peer.read_some(
                boost::asio::buffer(bytes),
                error
            );
            if (error)
            {
                break;
            }
            result.append(bytes, count);
        }
        return result;
    }
};

TEST_F(AsioConnectTest, CopiesSendInputAndPreservesOrder)
{
    IConnection& transport = *connection;
    connection->start();
    std::string first = "original bytes";
    transport.send(std::as_bytes(std::span(first)));
    first.assign(first.size(), 'x');
    const std::string next = " followed by bytes";
    transport.send(std::as_bytes(std::span(next)));
    EXPECT_EQ(transport.pendingBytes(), first.size() + next.size());
    pump();
    EXPECT_EQ(receivePeer(), "original bytes followed by bytes");
    EXPECT_EQ(transport.pendingBytes(), 0);
}

TEST_F(AsioConnectTest, PausedInFlightReadIsDeliveredOnResume)
{
    std::string received;
    connection->setMessageCallback(
        [&](IConnection&, auto bytes)
        {
            received.append(
                reinterpret_cast<const char*>(bytes.data()),
                bytes.size()
            );
        }
    );
    connection->start();
    connection->pauseRead();
    sendPeer("held bytes");
    pump();
    EXPECT_TRUE(received.empty());
    connection->resumeRead();
    EXPECT_EQ(received, "held bytes");
    sendPeer(" next");
    pump();
    EXPECT_EQ(received, "held bytes next");
}

TEST_F(
    AsioConnectTest, QueuedSendAndShutdownBeforeStartDrainAfterStart
)
{
    const std::string payload = "queued before start";
    connection->send(std::as_bytes(std::span(payload)));
    connection->shutdown();
    EXPECT_EQ(connection->pendingBytes(), payload.size());

    connection->start();
    pump();
    EXPECT_EQ(receivePeer(), payload);
    char byte;
    boost::system::error_code error;
    peer.read_some(boost::asio::buffer(&byte, 1), error);
    EXPECT_EQ(error, boost::asio::error::eof);
    EXPECT_TRUE(connection->connected());

    peer.shutdown(Tcp::socket::shutdown_send);
    pump();
    EXPECT_FALSE(connection->connected());
}

TEST_F(AsioConnectTest, ShutdownDrainsAndStillReceivesUntilPeerFin)
{
    std::string received;
    connection->setMessageCallback(
        [&](IConnection& c, auto bytes)
        {
            received.append(
                reinterpret_cast<const char*>(bytes.data()),
                bytes.size()
            );
            c.send(bytes); // Ignored after shutdown().
        }
    );
    connection->start();
    const std::string payload(128 * 1024, 'a');
    connection->send(std::as_bytes(std::span(payload)));
    connection->shutdown();
    connection->shutdown();
    pump();
    EXPECT_EQ(receivePeer(), payload);
    char byte;
    boost::system::error_code error;
    peer.read_some(boost::asio::buffer(&byte, 1), error);
    EXPECT_EQ(error, boost::asio::error::eof);
    EXPECT_TRUE(connection->connected());
    sendPeer("after server FIN");
    pump();
    EXPECT_EQ(received, "after server FIN");
    peer.shutdown(Tcp::socket::shutdown_send);
    pump();
    EXPECT_FALSE(connection->connected());
}

TEST_F(AsioConnectTest, PartialWritesDrainInOrderBeforeFin)
{
    const int size = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &size,
            sizeof(size)
        ),
        0
    );
    connection->start();
    std::string payload(512 * 1024, '\0');
    for (std::size_t i = 0; i != payload.size(); ++i)
    {
        payload[i] = static_cast<char>(i % 251);
    }
    connection->send(std::as_bytes(std::span(payload)));
    connection->shutdown();
    pump(5ms);
    EXPECT_GT(connection->pendingBytes(), 0);
    std::string received = receivePeer();
    for (int i = 0; i < 400 && received.size() != payload.size(); ++i)
    {
        pump(5ms);
        received += receivePeer();
    }
    EXPECT_EQ(received, payload);
    EXPECT_EQ(connection->pendingBytes(), 0);
    char byte;
    boost::system::error_code error;
    peer.read_some(boost::asio::buffer(&byte, 1), error);
    EXPECT_EQ(error, boost::asio::error::eof);
    EXPECT_TRUE(connection->connected());
}

TEST_F(AsioConnectTest, PeerFinFlushesQueuedEchoBeforeClose)
{
    connection->setMessageCallback([](IConnection& c, auto bytes)
                                   { c.send(bytes); });
    connection->start();
    sendPeer("last message");
    peer.shutdown(Tcp::socket::shutdown_send);
    pump();
    EXPECT_EQ(receivePeer(), "last message");
    EXPECT_FALSE(connection->connected());
}

TEST_F(AsioConnectTest, WatermarksAndHardLimitBoundOutput)
{
    const int sendBufferSize = 4096;
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
        connectionScheduler(context.get_executor()),
        [&](auto) { return [&](IConnection&, auto) { ++messages; }; }
    );
    int closed = 0;
    connection->setCloseCallback(
        [&, weak = std::weak_ptr<ConnectionRuntime>(runtime)](auto*)
        {
            if (auto r = weak.lock())
                r->transportClosed();
            ++closed;
        }
    );
    connection->start();
    const std::string payload(
        BackpressureFeature::kHighWaterMark,
        'z'
    );
    connection->send(std::as_bytes(std::span(payload)));
    sendPeer("held");
    pump(5ms);
    EXPECT_EQ(messages, 0);
    for (int i = 0; i < 20 && connection->pendingBytes() != 0; ++i)
    {
        pump(5ms);
        receivePeer();
    }
    pump();
    EXPECT_EQ(messages, 1);
    const std::string excessive(
        AsioConnect::kHardOutputLimit + 1,
        'z'
    );
    connection->send(std::as_bytes(std::span(excessive)));
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(connection->pendingBytes(), 0);
    connection->close();
    EXPECT_EQ(closed, 1);
}

TEST_F(AsioConnectTest, OutputCallbackCanCloseAndReleaseOwner)
{
    std::weak_ptr<AsioConnect> weak = connection;
    connection->setOutputCallback(
        [&](std::size_t)
        {
            connection->close();
            connection.reset();
        }
    );
    connection->start();
    const std::string payload(
        BackpressureFeature::kHighWaterMark,
        'w'
    );
    connection->send(std::as_bytes(std::span(payload)));
    EXPECT_TRUE(weak.expired());
    pump();
}

TEST_F(AsioConnectTest, ConcurrentSendsKeepOneOrderedWriteStream)
{
    const int socketBuffer = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &socketBuffer,
            sizeof(socketBuffer)
        ),
        0
    );
    connection->start();
    const std::string first(256 * 1024, 'I');
    connection->send(std::as_bytes(std::span(first)));
    pump(5ms);
    ASSERT_GT(connection->pendingBytes(), 0);

    constexpr int producers = 4;
    constexpr int recordsPerProducer = 8;
    constexpr int recordSize = 4096;
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (int producer = 0; producer < producers; ++producer)
    {
        threads.emplace_back(
            [&, producer]
            {
                ++ready;
                while (!go.load())
                {
                    std::this_thread::yield();
                }
                for (int record = 0; record < recordsPerProducer;
                     ++record)
                {
                    const char value = static_cast<char>(
                        33 + producer * recordsPerProducer + record
                    );
                    const std::string payload(recordSize, value);
                    connection->send(std::as_bytes(std::span(payload))
                    );
                }
            }
        );
    }
    while (ready.load() != producers)
    {
        std::this_thread::yield();
    }
    go = true;
    for (auto& thread : threads)
    {
        thread.join();
    }
    EXPECT_LE(
        connection->pendingBytes(),
        AsioConnect::kHardOutputLimit
    );

    const auto expected = first.size() +
                          producers * recordsPerProducer * recordSize;
    std::string received;
    for (int i = 0; i < 500 && received.size() < expected; ++i)
    {
        pump(5ms);
        received += receivePeer();
    }
    ASSERT_EQ(received.size(), expected);
    EXPECT_EQ(received.substr(0, first.size()), first);
    std::array<int, producers * recordsPerProducer> seen{};
    for (std::size_t offset = first.size(); offset < expected;
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
    for (const int count : seen)
    {
        EXPECT_EQ(count, 1);
    }
    EXPECT_EQ(connection->pendingBytes(), 0);
}

TEST_F(AsioConnectTest, ForeignQueuedSendDrainsBeforeShutdownFin)
{
    connection->start();
    const std::string payload(64 * 1024, 'f');
    std::thread sender(
        [&] { connection->send(std::as_bytes(std::span(payload))); }
    );
    sender.join();
    EXPECT_EQ(connection->pendingBytes(), payload.size());
    connection->shutdown();
    pump();
    EXPECT_EQ(receivePeer(), payload);
    char byte;
    boost::system::error_code error;
    peer.read_some(boost::asio::buffer(&byte, 1), error);
    EXPECT_EQ(error, boost::asio::error::eof);
    EXPECT_EQ(connection->pendingBytes(), 0);
}

TEST_F(AsioConnectTest, ConcurrentOutputLimitClosesOnce)
{
    int closes = 0;
    connection->setCloseCallback([&](auto*) { ++closes; });
    connection->start();
    const std::string payload(600 * 1024, 'x');
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
    {
        threads.emplace_back(
            [&]
            {
                ++ready;
                while (!go.load())
                {
                    std::this_thread::yield();
                }
                connection->send(std::as_bytes(std::span(payload)));
            }
        );
    }
    while (ready.load() != 4)
    {
        std::this_thread::yield();
    }
    go = true;
    for (auto& thread : threads)
    {
        thread.join();
    }
    EXPECT_LE(
        connection->pendingBytes(),
        AsioConnect::kHardOutputLimit
    );
    pump();
    EXPECT_FALSE(connection->connected());
    EXPECT_EQ(connection->pendingBytes(), 0);
    EXPECT_EQ(closes, 1);
    pump();
    EXPECT_EQ(closes, 1);
}

TEST_F(AsioConnectTest, CloseOnceDiscardsQueuedAndCancelledOperations)
{
    int closes = 0;
    int messages = 0;
    connection->setCloseCallback([&](auto*) { ++closes; });
    connection->setMessageCallback([&](IConnection&, auto)
                                   { ++messages; });
    const int socketBuffer = 4096;
    ASSERT_EQ(
        ::setsockopt(
            connection->fd(),
            SOL_SOCKET,
            SO_SNDBUF,
            &socketBuffer,
            sizeof(socketBuffer)
        ),
        0
    );
    connection->start();
    const std::string active(
        BackpressureFeature::kHighWaterMark,
        'a'
    );
    connection->send(std::as_bytes(std::span(active)));
    const std::string queued(64 * 1024, 'q');
    std::thread sender(
        [&] { connection->send(std::as_bytes(std::span(queued))); }
    );
    sender.join();
    ASSERT_GT(connection->pendingBytes(), 0);

    connection->close();
    connection->close();
    std::thread lateSender(
        [&] { connection->send(std::as_bytes(std::span(queued))); }
    );
    lateSender.join();
    EXPECT_EQ(connection->pendingBytes(), 0);
    EXPECT_EQ(closes, 1);
    pump();
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(messages, 0);
    EXPECT_EQ(connection->pendingBytes(), 0);

    std::weak_ptr<AsioConnect> weak = connection;
    connection.reset();
    EXPECT_TRUE(weak.expired());
    pump();
}

TEST_F(AsioConnectTest, CancelledIoDoesNotRetainConnection)
{
    connection->start();
    const std::string payload(64 * 1024, 'x');
    connection->send(std::as_bytes(std::span(payload)));
    std::weak_ptr<AsioConnect> weak = connection;
    connection->close();
    connection.reset();
    EXPECT_TRUE(weak.expired());
    pump(
    ); // Cancellation handlers still require their captured buffers.
}

TEST_F(
    AsioConnectTest, MessageCallbackCanCloseAndReleaseExternalOwner
)
{
    std::weak_ptr<AsioConnect> weak = connection;
    connection->setMessageCallback(
        [&](IConnection& c, auto bytes)
        {
            c.close();
            connection.reset();
            EXPECT_EQ(
                std::string(
                    reinterpret_cast<const char*>(bytes.data()),
                    bytes.size()
                ),
                "close in callback"
            );
        }
    );
    connection->start();
    sendPeer("close in callback");
    pump();
    EXPECT_TRUE(weak.expired());
}

TEST_F(AsioConnectTest, CallbackExceptionClosesOnlyThisConnection)
{
    connection->setMessageCallback(
        [](IConnection&, auto)
        { throw std::runtime_error("application failure"); }
    );
    connection->start();
    sendPeer("trigger");
    EXPECT_NO_THROW(pump());
    EXPECT_FALSE(connection->connected());
}

TEST_F(AsioConnectTest, ForeignThreadMutationIsRejected)
{
    bool rejected = false;
    const auto id = connection->id();
    std::thread other(
        [&]
        {
            EXPECT_EQ(connection->id(), id);
            try
            {
                connection->close();
            }
            catch (const std::logic_error&)
            {
                rejected = true;
            }
        }
    );
    other.join();
    EXPECT_TRUE(rejected);
    EXPECT_TRUE(connection->connected());
}

TEST_F(AsioConnectTest, IdentityIsUniqueAcrossBackends)
{
    EventLoop loop;
    int sockets[2];
    ASSERT_EQ(
        ::socketpair(
            AF_UNIX,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            sockets
        ),
        0
    );
    Socket peerSocket{FileDescriptor(sockets[1])};
    TcpConnection native(&loop, Socket(FileDescriptor(sockets[0])));
    EXPECT_NE(native.id(), connection->id());
    EXPECT_NE(connection->id(), 0);
    loop.removeChannel(&native.channel());
}

TEST_F(AsioConnectTest, SocketIsNonblockingAndCloseOnExec)
{
    EXPECT_NE(::fcntl(connection->fd(), F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(connection->fd(), F_GETFD) & FD_CLOEXEC, 0);
}
} // namespace
} // namespace service::gateway
~~~
