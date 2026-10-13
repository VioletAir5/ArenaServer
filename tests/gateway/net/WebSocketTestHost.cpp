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
#include "net/PacketEcho.h"
#include "net/PreLoginHandshake.h"
#include "net/WebSocketServer.h"

#include "AuthTestProfile.h"
#include "ChaosTestProfile.h"
#include "ConnectionContextTestHandler.h"
#include "ConnectionFeatureTestProfile.h"
#include "MessageDispatchTestProfile.h"
#include "SecurityPolicyTestProfile.h"

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
    ConnectionHandlerFactory handshake =
        [](ConnectionContext::Ptr connection)
    { return makePreLoginHandshakeHandler(std::move(connection)); };
    ConnectionFeatureFactory features;
    if (mode == "features")
    {
        features = makeFeatureTestProfile();
        handshake = [](ConnectionContext::Ptr)
        { return makePacketEchoHandler(); };
    }
    if (mode == "chaos" || mode == "chaos_packet")
    {
        features = makeChaosTestFeatures();
        if (mode == "chaos_packet")
            handshake = [](ConnectionContext::Ptr)
            { return makePacketEchoHandler(); };
    }
    if (mode == "auth")
        handshake = makeAuthTestFactory();
    if (mode == "rpc")
        handshake = makeRpcTestFactory();
    if (mode == "security")
    {
        handshake = makeSecurityTestFactory();
        features = makeSecurityTestFeatures();
    }
    if (mode == "context")
        handshake = makeContextTestFactory();
    AsioTcpServer tcp(
        context,
        InetAddress("127.0.0.1", tcpPort),
        100,
        std::chrono::seconds(60),
        0,
        handshake,
        features
    );
    ConnectionHandlerFactory wsFactory = handshake;
    if (mode == "flood")
    {
        wsFactory = [](ConnectionContext::Ptr)
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
        100,
        features
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
    if (mode == "chaos" || mode == "chaos_packet")
        printChaosTestCounters();
    if (mode == "security")
        printSecurityTestCounters();
    return 0;
}