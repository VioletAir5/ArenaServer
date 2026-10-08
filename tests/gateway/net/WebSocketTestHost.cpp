#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <span>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include "net/AsioTcpServer.h"
#include "net/ConnectionHandler.h"
#include "net/InetAddress.h"
#include "net/PreLoginHandshake.h"
#include "net/WebSocketServer.h"

using namespace service::gateway;

int main(int argc, char** argv)
{
    if (argc != 5)
        return 2;
    const auto tcpPort = static_cast<std::uint16_t>(std::stoi(argv[1])
    );
    const auto wsPort = static_cast<std::uint16_t>(std::stoi(argv[2])
    );
    const std::string origin = argv[3];
    const std::string mode = argv[4];
    boost::asio::io_context context;
    ConnectionHandlerFactory handshake = []
    { return makePreLoginHandshakeHandler(); };
    AsioTcpServer tcp(
        context,
        InetAddress("127.0.0.1", tcpPort),
        100,
        std::chrono::seconds(60),
        0,
        handshake
    );
    ConnectionHandlerFactory wsFactory = handshake;
    if (mode == "flood")
    {
        wsFactory = []
        {
            return [](IConnection& connection,
                      std::span<const std::byte>)
            {
                const std::string chunk(128 * 1024, 'x');
                for (int i = 0; i < 16; ++i)
                    connection.send(std::as_bytes(std::span(chunk)));
            };
        };
    }
    WebSocketServer ws(
        context,
        InetAddress("127.0.0.1", wsPort),
        origin,
        wsFactory,
        100
    );
    boost::asio::signal_set signals(context, SIGUSR1, SIGTERM);
    signals.async_wait(
        [&](auto error, int)
        {
            if (!error)
            {
                ws.stop();
                tcp.stop(std::chrono::milliseconds(0));
            }
        }
    );
    std::cout << "WS_TEST_READY" << std::endl;
    context.run();
    return 0;
}