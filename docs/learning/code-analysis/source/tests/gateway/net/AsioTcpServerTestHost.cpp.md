# tests/gateway/net/AsioTcpServerTestHost.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；134行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#include <csignal>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string_view>

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AsioTcpServer.h"
#include "net/PacketEcho.h"

#include "ChaosTestProfile.h"
#include "ConnectionContextTestHandler.h"
#include "ConnectionFeatureTestProfile.h"
#include "MessageDispatchTestProfile.h"
#include "SecurityPolicyTestProfile.h"

int main(int argc, char** argv)
{
    if (argc < 2 || argc > 5)
    {
        return 2;
    }
    service::gateway::ConnectionHandlerFactory handlerFactory;
    service::gateway::ConnectionFeatureFactory featureFactory;
    if (argc == 5)
    {
        if (std::string_view(argv[4]) == "context")
        {
            handlerFactory = makeContextTestFactory();
        }
        else if (std::string_view(argv[4]) == "chaos" || std::string_view(argv[4]) == "chaos_packet")
        {
            featureFactory = service::gateway::makeChaosTestFeatures(
            );
            if (std::string_view(argv[4]) == "chaos")
                handlerFactory =
                    [](service::gateway::ConnectionContext::Ptr ctx)
                {
                    return service::gateway::
                        makePreLoginHandshakeHandler(std::move(ctx));
                };
        }
        else if (std::string_view(argv[4]) == "rpc")
        {
            handlerFactory = service::gateway::makeRpcTestFactory();
        }
        else if (std::string_view(argv[4]) == "security")
        {
            handlerFactory = service::gateway::
                makeSecurityTestFactory();
            featureFactory = service::gateway::
                makeSecurityTestFeatures();
        }
        else if (std::string_view(argv[4]) == "features")
        {
            featureFactory = service::gateway::makeFeatureTestProfile(
            );
        }
        else if (std::string_view(argv[4]) != "packet")
        {
            return 2;
        }
        if (!handlerFactory)
            handlerFactory =
                [](service::gateway::ConnectionContext::Ptr)
            { return service::gateway::makePacketEchoHandler(); };
    }

    using namespace service::gateway;
    boost::asio::io_context context;
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
        handlerFactory,
        featureFactory
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
            server.stop(
                signal == SIGTERM ? std::chrono::milliseconds(0)
                                  : std::chrono::milliseconds(300)
            );
            waitStopped();
        }
    );
    std::cerr << "TEST_HOST_READY" << std::endl;
    context.run();
    if (argc == 5 && (std::string_view(argv[4]) == "chaos" ||
                      std::string_view(argv[4]) == "chaos_packet"))
        service::gateway::printChaosTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "security")
        printSecurityTestCounters();
}
~~~
