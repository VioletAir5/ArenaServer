#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"

using namespace service::gateway;

int main()
{
    EventLoop loop;

    InetAddress addr("127.0.0.1", 8888);
    TcpServer server(&loop, addr, 1000);

    loop.loop();
}
