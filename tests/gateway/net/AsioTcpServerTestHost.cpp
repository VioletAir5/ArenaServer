#include <csignal>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string_view>

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AsioTcpServer.h"
#include "net/ConnectionSchedulers.h"
#include "net/OnlineRegistryMaintenance.h"
#include "net/PacketEcho.h"

#include "TransportTestProfile.h"

int main(int argc, char** argv)
{
    if (argc < 2 || argc > 5)
    {
        return 2;
    }
    std::shared_ptr<service::gateway::OnlineRegistry> online;
    service::gateway::ConnectionHandlerFactory handlerFactory;
    if (argc == 5)
    {
        try
        {
            handlerFactory = service::gateway::transportTestProfile(
                argv[4],
                &online
            );
        }
        catch (const std::invalid_argument&)
        {
            return 2;
        }
    }
    using namespace service::gateway;
    boost::asio::io_context context;
    std::unique_ptr<OnlineRegistryMaintenance> upkeep;
    if (online)
        upkeep = std::make_unique<OnlineRegistryMaintenance>(
            online,
            connectionScheduler(context.get_executor())
        );
    boost::asio::signal_set signals(context, SIGTERM, SIGUSR1);
    boost::asio::steady_timer poll(context);
    const auto port = static_cast<std::uint16_t>(
        std::strtoul(argv[1], nullptr, 10)
    );
    const auto idle = std::chrono::milliseconds(
        argc >= 3 ? std::strtoul(argv[2], nullptr, 10) : 60000
    );
    const auto workers = argc >= 4
                             ? std::strtoul(argv[3], nullptr, 10)
                             : 0;
    AsioTcpServer server(
        context,
        InetAddress("127.0.0.1", port),
        1000,
        idle,
        workers,
        handlerFactory
    );
    std::function<void()> waitStopped;
    waitStopped = [&]
    {
        if (server.stopped())
        {
            boost::system::error_code ignored;
            signals.cancel(ignored);
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
        [&](auto error, int signal)
        {
            if (error)
            {
                return;
            }
            if (upkeep)
                upkeep->stop();
            server.stop(
                signal == SIGTERM ? std::chrono::milliseconds(0)
                                  : std::chrono::milliseconds(300)
            );
            waitStopped();
        }
    );
    std::cerr << "TEST_HOST_READY" << std::endl;
    context.run();
    if (upkeep)
        upkeep->shutdown();
    if (online)
        printOnlineTestCounts(*online);
    if (argc == 5 && (std::string_view(argv[4]) == "chaos" ||
                      std::string_view(argv[4]) == "chaos_packet"))
        service::gateway::printChaosTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "security")
        printSecurityTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "features")
        printPacketPolicyTestCounters();
}
