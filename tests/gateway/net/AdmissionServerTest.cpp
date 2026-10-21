#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "net/AdmissionController.h"
#include "net/AsioTcpServer.h"
#include "net/EventLoop.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using Tcp = boost::asio::ip::tcp;

std::uint16_t unusedPort(boost::asio::io_context& context)
{
    Tcp::acceptor probe(context, Tcp::endpoint(Tcp::v4(), 0));
    return probe.local_endpoint().port();
}

bool pumpUntil(
    boost::asio::io_context& context,
    const std::function<bool()>& complete
)
{
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!complete() && std::chrono::steady_clock::now() < deadline)
    {
        context.restart();
        context.run_for(2ms);
    }
    return complete();
}

Tcp::socket connectPeer(
    boost::asio::io_context& clients,
    std::uint16_t port,
    const char* source = "127.0.0.1"
)
{
    Tcp::socket peer(clients);
    peer.open(Tcp::v4());
    peer.bind(
        Tcp::endpoint(boost::asio::ip::make_address_v4(source), 0)
    );
    peer.connect(
        Tcp::endpoint(boost::asio::ip::address_v4::loopback(), port)
    );
    return peer;
}

ConnectionHandlerFactory inertHandler()
{
    return [](ConnectionContext::Ptr) -> ConnectionMessageCallback
    { return [](auto&, auto) {}; };
}

ConnectionHandlerFactory
throwOnce(const std::shared_ptr<std::atomic<std::size_t>>& attempts)
{
    return [attempts](
               ConnectionContext::Ptr
           ) -> ConnectionMessageCallback
    {
        if (attempts->fetch_add(1) == 0)
        {
            throw 42;
        }
        return [](auto& channel, auto bytes) { channel.send(bytes); };
    };
}

TEST(AdmissionServerTest, TcpAndWebSocketShareLimitsBeforeUpgrade)
{
    for (const auto workers : {0u, 2u})
    {
        for (const bool sameIp : {true, false})
        {
            boost::asio::io_context context;
            boost::asio::io_context clients;
            auto controller = std::make_shared<AdmissionController>(
                AdmissionController::Limits{
                    sameIp ? 2u : 1u,
                    sameIp ? 1u : 2u
                }
            );
            const auto rawPort = unusedPort(clients);
            AsioTcpServer raw(
                context,
                InetAddress("127.0.0.1", rawPort),
                8,
                60s,
                workers,
                {},
                {},
                controller
            );
            const auto wsPort = unusedPort(clients);
            WebSocketServer ws(
                context,
                InetAddress("127.0.0.1", wsPort),
                "https://arena.test",
                inertHandler(),
                8,
                {},
                controller
            );
            auto first = connectPeer(clients, rawPort);
            ASSERT_TRUE(pumpUntil(
                context,
                [&] { return controller->snapshot().active == 1; }
            ));
            auto rejected = connectPeer(
                clients,
                wsPort,
                sameIp ? "127.0.0.1" : "127.0.0.2"
            );
            ASSERT_TRUE(pumpUntil(
                context,
                [&]
                { return controller->snapshot().rejectedTotal == 1; }
            ));
            EXPECT_EQ(ws.connectionCount(), 0u);
            EXPECT_EQ(
                controller->snapshot().rejectedPerIp,
                sameIp ? 1u : 0u
            );
            EXPECT_EQ(
                controller->snapshot().rejectedGlobal,
                sameIp ? 0u : 1u
            );
            first.close();
            ASSERT_TRUE(pumpUntil(
                context,
                [&] { return controller->snapshot().active == 0; }
            ));

            auto upgrade = connectPeer(clients, wsPort);
            ASSERT_TRUE(pumpUntil(
                context,
                [&] { return ws.connectionCount() == 1; }
            ));
            EXPECT_EQ(controller->snapshot().active, 1u);
            // An invalid HTTP upgrade returns its reservation after
            // close.
            const std::string
                invalid = "GET /ws HTTP/1.1\r\nHost: "
                          "localhost\r\nConnection: close\r\n\r\n";
            boost::asio::write(upgrade, boost::asio::buffer(invalid));
            ASSERT_TRUE(pumpUntil(
                context,
                [&] { return controller->snapshot().active == 0; }
            ));
            EXPECT_EQ(ws.connectionCount(), 0u);

            auto finalPeer = connectPeer(clients, rawPort);
            ASSERT_TRUE(pumpUntil(
                context,
                [&] { return controller->snapshot().active == 1; }
            ));
            raw.stop(0ms);
            ws.stop();
            ASSERT_TRUE(pumpUntil(
                context,
                [&]
                {
                    return raw.stopped() &&
                           controller->snapshot().active == 0;
                }
            ));
            EXPECT_EQ(controller->snapshot().trackedIps, 0u);
        }
    }
}

TEST(
    AdmissionServerTest,
    SetupFailureReturnsTcpAndWebSocketReservations
)
{
    for (const auto workers : {0u, 2u})
    {
        boost::asio::io_context context;
        boost::asio::io_context clients;
        auto controller = std::make_shared<AdmissionController>(
            AdmissionController::Limits{1, 1}
        );
        std::atomic<std::size_t> attempts{0};
        ConnectionHandlerFactory throwing =
            [&attempts](
                ConnectionContext::Ptr
            ) -> ConnectionMessageCallback
        {
            ++attempts;
            throw std::runtime_error(
                "injected handler setup failure"
            );
        };
        const auto port = unusedPort(clients);
        AsioTcpServer tcp(
            context,
            InetAddress("127.0.0.1", port),
            8,
            60s,
            workers,
            throwing,
            {},
            controller
        );
        auto peer = connectPeer(clients, port);
        ASSERT_TRUE(pumpUntil(
            context,
            [&]
            {
                return controller->snapshot().peak == 1 &&
                       tcp.connectionCount() == 0;
            }
        ));
        EXPECT_EQ(controller->snapshot().active, 0u);
        const auto wsPort = unusedPort(clients);
        WebSocketServer ws(
            context,
            InetAddress("127.0.0.1", wsPort),
            "https://arena.test",
            throwing,
            8,
            {},
            controller
        );
        auto wsPeer = connectPeer(clients, wsPort);
        ASSERT_TRUE(pumpUntil(
            context,
            [&]
            {
                return attempts == 2 &&
                       controller->snapshot().active == 0;
            }
        ));
        EXPECT_EQ(ws.connectionCount(), 0u);
        EXPECT_EQ(controller->snapshot().active, 0u);
        EXPECT_EQ(controller->snapshot().rejectedTotal, 0u);
        auto retry = connectPeer(clients, port);
        ASSERT_TRUE(pumpUntil(
            context,
            [&]
            { return attempts == 3 && tcp.connectionCount() == 0; }
        ));
        EXPECT_EQ(controller->snapshot().active, 0u);
        ws.stop();
        tcp.stop(0ms);
    }
}

TEST(
    AdmissionServerTest, EpollRejectsPerIpAndReclaimsAfterOwnerCleanup
)
{
    for (const auto workers : {0u, 2u})
    {
        boost::asio::io_context clients;
        EventLoop base;
        auto controller = std::make_shared<AdmissionController>(
            AdmissionController::Limits{8, 1}
        );
        const auto port = unusedPort(clients);
        TcpServer server(
            &base,
            InetAddress("127.0.0.1", port),
            8,
            60s,
            workers,
            {},
            {},
            controller
        );
        std::vector<Tcp::socket> peers;
        peers.push_back(connectPeer(clients, port));
        int stage = 0;
        bool completed = false;
        const auto watchdog = base.runAfter(3s, [&] { base.quit(); });
        base.runEvery(
            1ms,
            [&]
            {
                const auto snapshot = controller->snapshot();
                if (stage == 0 && snapshot.active == 1)
                {
                    peers.push_back(connectPeer(clients, port));
                    ++stage;
                }
                else if (stage == 1 && snapshot.rejectedPerIp == 1)
                {
                    peers.front().close();
                    ++stage;
                }
                else if (stage == 2 && snapshot.active == 0)
                {
                    peers.push_back(connectPeer(clients, port));
                    ++stage;
                }
                else if (stage == 3 && snapshot.active == 1)
                {
                    server.stop(0ms);
                    ++stage;
                }
                else if (stage == 4 && server.stopped())
                {
                    completed = true;
                    base.cancelTimer(watchdog);
                    base.quit();
                }
            }
        );
        base.loop();
        EXPECT_TRUE(completed);
        EXPECT_EQ(controller->snapshot().active, 0u);
        EXPECT_EQ(controller->snapshot().trackedIps, 0u);
        EXPECT_EQ(controller->snapshot().rejectedPerIp, 1u);
    }
}

TEST(AdmissionServerTest, AsioIsolatesNonStandardFactoryFailure)
{
    for (const auto workers : {0u, 2u})
    {
        boost::asio::io_context context;
        boost::asio::io_context clients;
        auto controller = std::make_shared<AdmissionController>(
            AdmissionController::Limits{1, 1}
        );
        auto attempts = std::make_shared<std::atomic<std::size_t>>(0);
        const auto port = unusedPort(clients);
        AsioTcpServer server(
            context,
            InetAddress("127.0.0.1", port),
            8,
            60s,
            workers,
            throwOnce(attempts),
            {},
            controller
        );
        auto rejected = connectPeer(clients, port);
        ASSERT_TRUE(pumpUntil(
            context,
            [&]
            {
                return attempts->load() == 1 &&
                       server.connectionCount() == 0 &&
                       controller->snapshot().active == 0;
            }
        ));
        auto accepted = connectPeer(clients, port);
        ASSERT_TRUE(pumpUntil(
            context,
            [&]
            {
                return attempts->load() == 2 &&
                       server.connectionCount() == 1;
            }
        ));
        const std::string payload = "ok";
        boost::asio::write(accepted, boost::asio::buffer(payload));
        ASSERT_TRUE(pumpUntil(
            context,
            [&] { return accepted.available() == payload.size(); }
        ));
        std::array<char, 2> response{};
        EXPECT_EQ(
            accepted.read_some(boost::asio::buffer(response)),
            2u
        );
        EXPECT_EQ(
            std::string(response.data(), response.size()),
            payload
        );
        server.stop(0ms);
        ASSERT_TRUE(
            pumpUntil(context, [&] { return server.stopped(); })
        );
        EXPECT_EQ(controller->snapshot().active, 0u);
        EXPECT_EQ(controller->snapshot().rejectedTotal, 0u);
    }
}

TEST(AdmissionServerTest, WebSocketIsolatesNonStandardFactoryFailure)
{
    boost::asio::io_context context;
    boost::asio::io_context clients;
    auto controller = std::make_shared<AdmissionController>(
        AdmissionController::Limits{1, 1}
    );
    auto attempts = std::make_shared<std::atomic<std::size_t>>(0);
    const auto port = unusedPort(clients);
    WebSocketServer server(
        context,
        InetAddress("127.0.0.1", port),
        "https://arena.test",
        throwOnce(attempts),
        8,
        {},
        controller
    );
    auto rejected = connectPeer(clients, port);
    ASSERT_TRUE(pumpUntil(
        context,
        [&]
        {
            return attempts->load() == 1 &&
                   server.connectionCount() == 0 &&
                   controller->snapshot().active == 0;
        }
    ));
    auto accepted = connectPeer(clients, port);
    const std::string
        request = "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: "
                  "websocket\r\n"
                  "Connection: Upgrade\r\nSec-WebSocket-Key: "
                  "dGhlIHNhbXBsZSBub25jZQ==\r\n"
                  "Sec-WebSocket-Version: 13\r\nOrigin: "
                  "https://arena.test\r\n\r\n";
    boost::asio::write(accepted, boost::asio::buffer(request));
    ASSERT_TRUE(
        pumpUntil(context, [&] { return accepted.available() >= 12; })
    );
    std::array<char, 2048> response{};
    const auto size = accepted.read_some(
        boost::asio::buffer(response)
    );
    EXPECT_NE(
        std::string(response.data(), size).find(" 101 "),
        std::string::npos
    );
    EXPECT_EQ(attempts->load(), 2u);
    EXPECT_EQ(server.connectionCount(), 1u);
    EXPECT_EQ(controller->snapshot().active, 1u);
    server.stop();
    EXPECT_EQ(controller->snapshot().active, 0u);
    EXPECT_EQ(controller->snapshot().rejectedTotal, 0u);
}

TEST(AdmissionServerTest, EpollIsolatesNonStandardFactoryFailure)
{
    for (const auto workers : {0u, 2u})
    {
        boost::asio::io_context clients;
        EventLoop base;
        auto controller = std::make_shared<AdmissionController>(
            AdmissionController::Limits{1, 1}
        );
        auto attempts = std::make_shared<std::atomic<std::size_t>>(0);
        const auto port = unusedPort(clients);
        TcpServer server(
            &base,
            InetAddress("127.0.0.1", port),
            8,
            60s,
            workers,
            throwOnce(attempts),
            {},
            controller
        );
        std::vector<Tcp::socket> peers;
        peers.push_back(connectPeer(clients, port));
        int stage = 0;
        bool completed = false;
        const auto watchdog = base.runAfter(3s, [&] { base.quit(); });
        base.runEvery(
            1ms,
            [&]
            {
                if (stage == 0 && attempts->load() == 1 &&
                    server.connectionCount() == 0 &&
                    controller->snapshot().active == 0)
                {
                    peers.push_back(connectPeer(clients, port));
                    ++stage;
                }
                else if (
                    stage == 1 && attempts->load() == 2 &&
                    server.connectionCount() == 1
                )
                {
                    boost::asio::write(
                        peers.back(),
                        boost::asio::buffer("ok", 2)
                    );
                    ++stage;
                }
                else if (stage == 2 && peers.back().available() == 2)
                {
                    std::array<char, 2> response{};
                    EXPECT_EQ(
                        peers.back().read_some(
                            boost::asio::buffer(response)
                        ),
                        2u
                    );
                    EXPECT_EQ(
                        std::string(response.data(), response.size()),
                        "ok"
                    );
                    server.stop(0ms);
                    ++stage;
                }
                else if (stage == 3 && server.stopped())
                {
                    completed = true;
                    base.cancelTimer(watchdog);
                    base.quit();
                }
            }
        );
        base.loop();
        EXPECT_TRUE(completed);
        EXPECT_EQ(attempts->load(), 2u);
        EXPECT_EQ(controller->snapshot().active, 0u);
        EXPECT_EQ(controller->snapshot().rejectedTotal, 0u);
    }
}
} // namespace
} // namespace service::gateway
