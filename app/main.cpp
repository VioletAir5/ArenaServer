#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <memory>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "net/InetAddress.h"
#include "net/Socket.h"

using namespace service;

int main()
{
    Socket listenSocket = Socket::createTcpNonBlocking();

    InetAddress listenAddr(8888, true);

    listenSocket.bind(listenAddr);
    listenSocket.listen(SOMAXCONN);

    EpollPoller epollPoller;

    std::unordered_map<int, std::unique_ptr<Channel>> clientChannels;
    std::unordered_set<int> pendingClose;

    Channel listenChannel(listenSocket.get());

    listenChannel.setEvents(EPOLLIN);

    listenChannel.setReadCallback(
        [&listenChannel,
         &epollPoller,
         &clientChannels,
         &pendingClose]()
        {
            while (true)
            {
                sockaddr_in peer{};
                socklen_t len = sizeof(peer);

                int connfd = ::accept4(
                    listenChannel.fd(),
                    reinterpret_cast<sockaddr*>(&peer),
                    &len,
                    SOCK_CLOEXEC | SOCK_NONBLOCK
                );

                if (connfd < 0)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        break;
                    }

                    if (errno == EINTR)
                    {
                        continue;
                    }

                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "accept4"
                    );
                }

                auto channel = std::make_unique<Channel>(connfd);

                channel->setEvents(EPOLLIN | EPOLLRDHUP);

                channel->setReadCallback(
                    [connfd, &pendingClose]()
                    {
                        char buffer[4096];

                        while (true)
                        {
                            ssize_t n = ::read(
                                connfd,
                                buffer,
                                sizeof(buffer)
                            );

                            if (n > 0)
                            {
                                ssize_t written = 0;

                                while (written < n)
                                {
                                    ssize_t ret = ::write(
                                        connfd,
                                        buffer + written,
                                        n - written
                                    );

                                    if (ret > 0)
                                    {
                                        written += ret;
                                        continue;
                                    }

                                    if (ret < 0 && errno == EINTR)
                                    {
                                        continue;
                                    }

                                    if (ret < 0 &&
                                        (errno == EAGAIN ||
                                         errno == EWOULDBLOCK))
                                    {
                                        std::cerr << "write would "
                                                     "block, fd="
                                                  << connfd << '\n';

                                        // 当前版本没有 outputBuffer，
                                        // 剩余数据暂时无法保存。
                                        return;
                                    }

                                    std::cerr
                                        << "write error fd=" << connfd
                                        << ", errno=" << errno
                                        << '\n';

                                    pendingClose.insert(connfd);
                                    return;
                                }

                                continue;
                            }

                            if (n == 0)
                            {
                                std::cout << "peer EOF fd=" << connfd
                                          << '\n';

                                pendingClose.insert(connfd);
                                return;
                            }

                            if (errno == EINTR)
                            {
                                continue;
                            }

                            if (errno == EAGAIN ||
                                errno == EWOULDBLOCK)
                            {
                                return;
                            }

                            std::cerr << "read error fd=" << connfd
                                      << ", errno=" << errno << '\n';

                            pendingClose.insert(connfd);
                            return;
                        }
                    }
                );

                channel->setCloseCallback(
                    [connfd, &pendingClose]()
                    {
                        std::cout << "close event fd=" << connfd
                                  << '\n';

                        pendingClose.insert(connfd);
                    }
                );

                channel->setErrorCallback(
                    [connfd, &pendingClose]()
                    {
                        int error = 0;
                        socklen_t len = sizeof(error);

                        if (::getsockopt(
                                connfd,
                                SOL_SOCKET,
                                SO_ERROR,
                                &error,
                                &len
                            ) == 0)
                        {
                            std::cerr << "socket error fd=" << connfd
                                      << ", error=" << error << '\n';
                        }
                        else
                        {
                            std::cerr
                                << "getsockopt(SO_ERROR) failed fd="
                                << connfd << ", errno=" << errno
                                << '\n';
                        }

                        pendingClose.insert(connfd);
                    }
                );

                channel->setWriteCallback(
                    [connfd]() {
                        std::cout << "socket writable fd=" << connfd
                                  << '\n';
                    }
                );

                epollPoller.addChannel(channel.get());

                clientChannels.emplace(connfd, std::move(channel));

                std::cout << "new connection fd=" << connfd << '\n';
            }
        }
    );

    epollPoller.addChannel(&listenChannel);

    std::cout << "listen on 127.0.0.1:8888\n";

    std::vector<Channel*> activeChannels;

    while (true)
    {
        epollPoller.poll(1000, activeChannels);

        for (Channel* channel : activeChannels)
        {
            channel->handleEvent();
        }

        for (int fd : pendingClose)
        {
            auto it = clientChannels.find(fd);

            if (it == clientChannels.end())
            {
                continue;
            }

            try
            {
                epollPoller.removeChannel(it->second.get());
            }
            catch (const std::system_error& e)
            {
                std::cerr << "removeChannel failed fd=" << fd << ": "
                          << e.what() << '\n';
            }

            ::close(fd);

            clientChannels.erase(it);

            std::cout << "connection removed fd=" << fd << '\n';
        }

        pendingClose.clear();
    }

    return 0;
}