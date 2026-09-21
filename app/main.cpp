#include<net/Socket.h>
#include<unistd.h>

int main()
{
    using namespace service;

    Socket server = Socket::createTcp();

    server.setReuseAddr(true);

    InetAddress addr(8888);

    server.bind(addr);
    server.listen();

    while (true) {
        Socket client = server.accept(nullptr);

        char buf[4096];

        while (true) {
            ssize_t n = ::read(
                client.get(),
                buf,
                sizeof(buf)
            );

            if (n <= 0) {
                break;
            }

            ::write(client.get(), buf, n);
        }
    }
}