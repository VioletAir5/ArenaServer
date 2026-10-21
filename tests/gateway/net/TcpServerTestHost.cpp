#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/PacketEcho.h"
#include "net/TcpServer.h"
#include "os/FileDescriptor.h"

#include "AuthTestProfile.h"
#include "ChaosTestProfile.h"
#include "ConnectionContextTestHandler.h"
#include "ConnectionFeatureTestProfile.h"
#include "MessageDispatchTestProfile.h"
#include "SecurityPolicyTestProfile.h"

// A separate process lets the integration test inspect /proc/<pid>/fd
// and stop the event loop cleanly, including under LeakSanitizer.
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
        else if (
            std::string_view(argv[4]) == "chaos" ||
            std::string_view(argv[4]) == "chaos_packet"
        )
        {
            featureFactory = service::gateway::
                makeChaosTestFeatures();
            if (std::string_view(argv[4]) == "chaos")
                handlerFactory =
                    [](service::gateway::ConnectionContext::Ptr ctx)
                {
                    return service::gateway::
                        makePreLoginHandshakeHandler(std::move(ctx));
                };
        }
        else if (std::string_view(argv[4]) == "auth")
        {
            handlerFactory = service::gateway::makeAuthTestFactory();
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
            featureFactory = service::gateway::
                makeFeatureTestProfile();
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

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGUSR1);
    if (::sigprocmask(SIG_BLOCK, &signals, nullptr) != 0)
    {
        return 3;
    }

    service::FileDescriptor stopFd(
        ::signalfd(-1, &signals, SFD_CLOEXEC | SFD_NONBLOCK)
    );
    if (!stopFd.valid())
    {
        return 4;
    }

    service::gateway::EventLoop loop;
    service::gateway::ITcpServer* serverPtr = nullptr;
    service::gateway::TimerId stopPollId = 0;
    service::gateway::Channel stopChannel(stopFd.get());
    stopChannel.setEvents(EPOLLIN);
    stopChannel.setReadCallback(
        [&]()
        {
            signalfd_siginfo event{};
            if (::read(stopFd.get(), &event, sizeof(event)) !=
                sizeof(event))
            {
                return;
            }
            if (event.ssi_signo == SIGTERM)
            {
                loop.quit();
                return;
            }
            if (event.ssi_signo == SIGUSR1 && serverPtr != nullptr &&
                stopPollId == 0)
            {
                serverPtr->stop(std::chrono::milliseconds(300));
                if (serverPtr->stopped())
                {
                    loop.quit();
                    return;
                }
                stopPollId = loop.runEvery(
                    std::chrono::milliseconds(10),
                    [&]()
                    {
                        if (serverPtr->stopped())
                        {
                            loop.cancelTimer(stopPollId);
                            stopPollId = 0;
                            loop.quit();
                        }
                    }
                );
            }
        }
    );
    loop.addChannel(&stopChannel);

    const auto port = static_cast<std::uint16_t>(
        std::strtoul(argv[1], nullptr, 10)
    );
    {
        service::gateway::InetAddress address("127.0.0.1", port);
        const auto
            idleTimeout = argc >= 3
                              ? std::chrono::milliseconds(
                                    std::strtoul(argv[2], nullptr, 10)
                                )
                              : std::chrono::seconds(60);
        const auto workerCount = argc >= 4 ? std::strtoul(
                                                 argv[3],
                                                 nullptr,
                                                 10
                                             )
                                           : 0;
        service::gateway::TcpServer server(
            &loop,
            address,
            1000,
            idleTimeout,
            workerCount,
            handlerFactory,
            featureFactory
        );
        serverPtr = &server;
        std::cerr << "TEST_HOST_READY" << std::endl;
        loop.loop();
    }

    loop.removeChannel(&stopChannel);
    if (argc == 5 && (std::string_view(argv[4]) == "chaos" ||
                      std::string_view(argv[4]) == "chaos_packet"))
        service::gateway::printChaosTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "security")
        service::gateway::printSecurityTestCounters();
    return 0;
}
