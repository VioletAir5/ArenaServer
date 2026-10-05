#include <chrono>
#include <memory>

#include "net/EventLoop.h"
#include "net/ITcpServer.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"

using namespace service::gateway;

int main()
{
    EventLoop loop;

    InetAddress addr("127.0.0.1", 8888);
    std::unique_ptr<ITcpServer> server = std::make_unique<TcpServer>(
        &loop, addr, 1000, std::chrono::seconds(60), 4);

    loop.loop();
}
