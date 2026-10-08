#include <chrono>
#include <csignal>
#include <functional>
#include <iostream>
#include <memory>
#include <string_view>

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AsioTcpServer.h"
#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/PacketEcho.h"
#include "net/PreLoginHandshake.h"
#include "net/TcpServer.h"
#include "net/WebSocketServer.h"

using namespace service::gateway;

int main(int argc, char** argv)
{
    const std::string_view backend = argc >= 2 ? argv[1] : "epoll";
    const std::string_view protocol = argc >= 3 ? argv[2] : "raw";
    if (argc > 4 || (backend != "epoll" && backend != "asio") ||
        (protocol != "raw" && protocol != "packet" &&
         protocol != "handshake") ||
        (protocol == "handshake" && backend != "asio") ||
        (argc == 4 && protocol != "handshake"))
    {
        std::cerr << "Usage: ArenaServer [epoll|asio] "
                     "[raw|packet|handshake] [ws-origin]\n";
        return 2;
    }
    ConnectionHandlerFactory handlerFactory;
    if (protocol == "packet")
    {
        handlerFactory = [] { return makePacketEchoHandler(); };
    }
    if (protocol == "handshake")
    {
        handlerFactory = []
        { return makePreLoginHandshakeHandler(); };
    }
    const InetAddress address("127.0.0.1", 8888);
    if (backend == "epoll")
    {
        EventLoop loop;
        std::unique_ptr<ITcpServer>
            server = std::make_unique<TcpServer>(
                &loop,
                address,
                1000,
                std::chrono::seconds(60),
                4,
                handlerFactory
            );
        loop.loop();
        return 0;
    }

    boost::asio::io_context context;
    boost::asio::signal_set signals(context, SIGINT, SIGTERM);
    boost::asio::steady_timer poll(context);
    std::unique_ptr<ITcpServer>
        server = std::make_unique<AsioTcpServer>(
            context,
            address,
            1000,
            std::chrono::seconds(60),
            4,
            handlerFactory
        );
    std::unique_ptr<WebSocketServer> wsServer;
    if (protocol == "handshake")
    {
        const std::string origin = argc == 4
                                       ? argv[3]
                                       : "http://localhost:3000";
        wsServer = std::make_unique<WebSocketServer>(
            context,
            InetAddress("127.0.0.1", 8889),
            origin,
            handlerFactory
        );
    }
    std::function<void()> waitStopped;
    waitStopped = [&]
    {
        if (server->stopped())
        {
            return;
        }
        poll.expires_after(std::chrono::milliseconds(10));
        poll.async_wait(
            [&](auto error)
            {
                if (!error)
                {
                    waitStopped();
                }
            }
        );
    };
    signals.async_wait(
        [&](auto error, int)
        {
            if (!error)
            {
                server->stop();
                if (wsServer)
                {
                    wsServer->stop();
                }
                waitStopped();
            }
        }
    );
    context.run();
}
