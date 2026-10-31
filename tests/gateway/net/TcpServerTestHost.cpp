#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string_view>
#include <system_error>

#include "net/Channel.h"
#include "net/ConnectionSchedulers.h"
#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/OnlineRegistryMaintenance.h"
#include "net/PacketEcho.h"
#include "net/TcpServer.h"
#include "os/FileDescriptor.h"

#include "TransportTestProfile.h"

// A separate process lets the integration test inspect /proc/<pid>/fd
// and stop the event loop cleanly, including under LeakSanitizer.
int main(int argc, char** argv)
{
    if (argc < 2 || argc > 5)
    {
        return 2;
    }
    std::shared_ptr<service::gateway::OnlineRegistry> online;
    std::shared_ptr<service::gateway::MatchmakingService> matcher;
    std::shared_ptr<service::gateway::RoomService> rooms;
    service::gateway::ConnectionHandlerFactory handlerFactory;
    if (argc == 5)
    {
        try
        {
            handlerFactory = service::gateway::transportTestProfile(
                argv[4],
                &online,
                &matcher,
                &rooms
            );
        }
        catch (const std::invalid_argument&)
        {
            return 2;
        }
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
    if (matcher)
        matcher->start(service::gateway::connectionScheduler(loop));
    if (rooms)
        rooms->start(service::gateway::connectionScheduler(loop));
    std::unique_ptr<service::gateway::OnlineRegistryMaintenance>
        upkeep;
    if (online)
        upkeep = std::make_unique<
            service::gateway::OnlineRegistryMaintenance>(
            online,
            service::gateway::connectionScheduler(loop)
        );
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
            if (upkeep)
                upkeep->stop();
            if (matcher)
                matcher->stop();
            if (rooms)
                rooms->stop();
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
            handlerFactory
        );
        serverPtr = &server;
        if (std::getenv("ARENA_TEST_WARM_RESOURCE_ERROR"))
        {
            // UBSan checks a new dynamic type using a pipe on GCC.
            // Warm the valid error path while descriptors are free,
            // before real EMFILE.
            try
            {
                throw std::system_error(
                    EMFILE,
                    std::generic_category(),
                    "resource-test warmup"
                );
            }
            catch (const std::system_error& error)
            {
                if (error.code().value() != EMFILE ||
                    !error.what()[0])
                    return 5;
            }
            // The first stop also checks this base-interface dynamic
            // type. A read-only query primes it without stopping the
            // real listener.
            if (serverPtr->stopped())
                return 6;
            std::cerr << "TEST_HOST_RESOURCE_ERROR_WARMED"
                      << std::endl;
        }
        std::cerr << "TEST_HOST_READY" << std::endl;
        loop.loop();
    }

    if (upkeep)
        upkeep->shutdown();
    if (rooms)
    {
        rooms->stop();
        service::gateway::printRoomTestCounts(*rooms);
    }
    if (matcher)
    {
        matcher->stop();
        service::gateway::printMatchTestCounts(*matcher);
    }
    if (online)
        service::gateway::printOnlineTestCounts(*online);
    loop.removeChannel(&stopChannel);
    if (argc == 5 && (std::string_view(argv[4]) == "chaos" ||
                      std::string_view(argv[4]) == "chaos_packet"))
        service::gateway::printChaosTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "security")
        service::gateway::printSecurityTestCounters();
    if (argc == 5 && std::string_view(argv[4]) == "features")
        service::gateway::printPacketPolicyTestCounters();
    return 0;
}
