#include <csignal>
#include <cstdlib>
#include <functional>
#include <iostream>

#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/AsioTcpServer.h"

int main(int argc, char** argv)
{
    if (argc < 2 || argc > 4)
    {
        return 2;
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
    const auto workers = argc == 4
                             ? std::strtoul(argv[3], nullptr, 10)
                             : 0;
    AsioTcpServer server(
        context,
        InetAddress("127.0.0.1", port),
        1000,
        idle,
        workers
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
}
