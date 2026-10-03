#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"
#include "os/FileDescriptor.h"

// A separate process lets the integration test inspect /proc/<pid>/fd
// and stop the event loop cleanly, including under LeakSanitizer.
int main(int argc, char** argv)
{
    if (argc != 2 && argc != 3)
    {
        return 2;
    }

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGTERM);
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
    service::gateway::Channel stopChannel(stopFd.get());
    stopChannel.setEvents(EPOLLIN);
    stopChannel.setReadCallback(
        [&]()
        {
            signalfd_siginfo event{};
            if (::read(stopFd.get(), &event, sizeof(event)) ==
                sizeof(event))
            {
                loop.quit();
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
            idleTimeout = argc == 3
                              ? std::chrono::milliseconds(
                                    std::strtoul(argv[2], nullptr, 10)
                                )
                              : std::chrono::seconds(60);
        service::gateway::TcpServer
            server(&loop, address, 1000, idleTimeout);
        std::cerr << "TEST_HOST_READY" << std::endl;
        loop.loop();
    }

    loop.removeChannel(&stopChannel);
    return 0;
}
