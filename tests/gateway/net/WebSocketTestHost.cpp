#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <span>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include "net/AsioTcpServer.h"
#include "net/ConnectionHandler.h"
#include "net/ConnectionSchedulers.h"
#include "net/InetAddress.h"
#include "net/OnlineRegistryMaintenance.h"
#include "net/PacketEcho.h"
#include "net/PreLoginHandshake.h"
#include "net/WebSocketServer.h"

#include "TransportTestProfile.h"

using namespace service::gateway;

int main(int argc, char** argv)
{
    if (argc != 5)
        return 2;
    const auto tcpPort = static_cast<std::uint16_t>(
        std::stoi(argv[1])
    );
    const auto wsPort = static_cast<std::uint16_t>(
        std::stoi(argv[2])
    );
    const std::string origin = argv[3];
    const std::string mode = argv[4];
    boost::asio::io_context context;
    const bool lifecycle = mode == "drain" || mode == "stop_drain" ||
                           mode == "deadline" ||
                           mode == "destroy_drain";
    std::shared_ptr<OnlineRegistry> online;
    std::shared_ptr<MatchmakingService> matcher;
    std::shared_ptr<RoomService> rooms;
    ConnectionHandlerFactory handshake = transportTestProfile(
        mode == "flood" || lifecycle ? "handshake" : mode,
        &online,
        &matcher,
        &rooms
    );
    if (matcher)
        matcher->start(connectionScheduler(context.get_executor()));
    if (rooms)
        rooms->start(connectionScheduler(context.get_executor()));
    std::unique_ptr<OnlineRegistryMaintenance> upkeep;
    if (online)
        upkeep = std::make_unique<OnlineRegistryMaintenance>(
            online,
            connectionScheduler(context.get_executor())
        );
    auto admission = std::make_shared<AdmissionController>(
        AdmissionController::Limits{100, 100}
    );
    AsioTcpServer tcp(
        context,
        InetAddress("127.0.0.1", tcpPort),
        100,
        std::chrono::seconds(60),
        0,
        handshake,
        {},
        admission
    );
    ConnectionHandlerFactory wsFactory = handshake;
    auto requestStop = std::make_shared<std::function<void()>>();
    if (mode == "flood")
    {
        wsFactory = [](ConnectionContext::Ptr)
        {
            return [](ConnectionChannel& connection,
                      std::span<const std::byte>)
            {
                const std::string chunk(128 * 1024, 'x');
                for (int i = 0; i < 16; ++i)
                    connection.send(std::as_bytes(std::span(chunk)));
            };
        };
    }
    if (lifecycle)
    {
        wsFactory = [mode, requestStop](ConnectionContext::Ptr)
        {
            return [mode, requestStop](
                       ConnectionChannel& channel,
                       std::span<const std::byte>
                   )
            {
                if (mode != "drain" && mode != "stop_drain")
                    return;
                for (int i = 0; i != 6; ++i)
                {
                    const std::string chunk(
                        96 * 1024,
                        static_cast<char>('a' + i)
                    );
                    channel.send(std::as_bytes(std::span(chunk)));
                }
                if (mode == "drain")
                {
                    channel.shutdown();
                    const std::string ignored = "after shutdown";
                    channel.send(std::as_bytes(std::span(ignored)));
                }
                else
                    (*requestStop)();
            };
        };
    }
    auto ws = std::make_unique<WebSocketServer>(
        context,
        InetAddress("127.0.0.1", wsPort),
        origin,
        wsFactory,
        100,
        ConnectionFeatureFactory{},
        admission
    );
    *requestStop = [&] { ws->stop(std::chrono::seconds(2)); };
    boost::asio::signal_set signals(context, SIGUSR1, SIGTERM);
    signals.async_wait(
        [&](auto error, int)
        {
            if (!error)
            {
                if (upkeep)
                    upkeep->stop();
                if (matcher)
                    matcher->stop();
                if (rooms)
                    rooms->stop();
                const auto
                    grace = mode == "rpc"
                                ? std::chrono::milliseconds(300)
                                : std::chrono::milliseconds::zero();
                if (mode == "deadline")
                    ws->stop(std::chrono::milliseconds(250));
                else if (mode == "destroy_drain")
                {
                    ws->stop(std::chrono::seconds(5));
                    ws.reset();
                }
                else
                    ws->stop(grace);
                tcp.stop(grace);
            }
        }
    );
    std::cout << "WS_TEST_READY" << std::endl;
    context.run();
    if (upkeep)
        upkeep->shutdown();
    if (rooms)
    {
        rooms->stop();
        printRoomTestCounts(*rooms);
    }
    if (matcher)
    {
        matcher->stop();
        printMatchTestCounts(*matcher);
    }
    if (online)
        printOnlineTestCounts(*online);
    if (lifecycle)
        std::cout << "WS_TEST_STOPPED connections="
                  << (ws ? ws->connectionCount() : 0)
                  << " active=" << admission->snapshot().active
                  << std::endl;
    if (mode == "features")
        printPacketPolicyTestCounters();
    if (mode == "chaos" || mode == "chaos_packet")
        printChaosTestCounters();
    if (mode == "security")
        printSecurityTestCounters();
    return 0;
}
