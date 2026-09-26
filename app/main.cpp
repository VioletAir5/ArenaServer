#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <iostream>
#include <memory>
#include <system_error>
#include <unordered_map>
#include <unordered_set>

#include "net/Channel.h"
#include "net/InetAddress.h"
#include "net/Socket.h"

using namespace service;

int main()
{
    Socket listenSocket = Socket::createTcpNonBlocking();

    InetAddress listenAddr(8888, true);

    listenSocket.bind(listenAddr);
    listenSocket.listen(SOMAXCONN);

    int epollFd = ::epoll_create1(EPOLL_CLOEXEC);

    if (epollFd < 0)
    {
        throw std::system_error(errno, std::generic_category(), "epoll_create1");
    }

    std::unordered_map<int, std::unique_ptr<Channel>> channels;
    std::unordered_set<int> pendingClose;

    Channel listenChannel(listenSocket.get());

    listenChannel.setReadCallback(
        [&listenChannel, &epollFd, &channels, &pendingClose]()
        {
            while (true)
            {
                sockaddr_in peer{};
                socklen_t len = sizeof(peer);

                int connfd = ::accept4(listenChannel.fd(),
                                       reinterpret_cast<sockaddr*>(&peer),
                                       &len,
                                       SOCK_CLOEXEC | SOCK_NONBLOCK);

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

                    throw std::system_error(errno, std::generic_category(), "accept4");
                }

                auto channel = std::make_unique<Channel>(connfd);

                channel->setReadCallback(
                    [connfd, &pendingClose]()
                    {
                        char buffer[4096];

                        while (true)
                        {
                            ssize_t n = ::read(connfd, buffer, sizeof(buffer));

                            if (n > 0)
                            {
                                ssize_t written = 0;

                                while (written < n)
                                {
                                    ssize_t ret = ::write(connfd, buffer + written, n - written);

                                    if (ret > 0)
                                    {
                                        written += ret;
                                        continue;
                                    }

                                    if (ret < 0 && errno == EINTR)
                                    {
                                        continue;
                                    }

                                    if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                                    {
                                        std::cerr << "write would block, fd=" << connfd << '\n';

                                        // 当前版本没有 outputBuffer，
                                        // 剩余数据暂时无法保存。
                                        return;
                                    }

                                    pendingClose.insert(connfd);
                                    return;
                                }

                                continue;
                            }

                            if (n == 0)
                            {
                                std::cout << "peer EOF fd=" << connfd << '\n';

                                pendingClose.insert(connfd);
                                return;
                            }

                            if (errno == EINTR)
                            {
                                continue;
                            }

                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                            {
                                return;
                            }

                            std::cerr << "read error fd=" << connfd << ", errno=" << errno << '\n';

                            pendingClose.insert(connfd);
                            return;
                        }
                    });

                channel->setCloseCallback(
                    [connfd, &pendingClose]()
                    {
                        std::cout << "close event fd=" << connfd << '\n';

                        pendingClose.insert(connfd);
                    });

                channel->setErrorCallback(
                    [connfd, &pendingClose]()
                    {
                        int error = 0;
                        socklen_t len = sizeof(error);

                        if (::getsockopt(connfd, SOL_SOCKET, SO_ERROR, &error, &len) == 0)
                        {
                            std::cerr << "socket error fd=" << connfd << ", error=" << error
                                      << '\n';
                        }
                        else
                        {
                            std::cerr << "getsockopt(SO_ERROR) failed fd=" << connfd << '\n';
                        }

                        pendingClose.insert(connfd);
                    });

                channel->setWriteCallback(
                    [connfd]() { std::cout << "socket writable fd=" << connfd << '\n'; });

                epoll_event event{};

                event.events = EPOLLIN | EPOLLRDHUP;

                event.data.ptr = channel.get();

                if (::epoll_ctl(epollFd, EPOLL_CTL_ADD, connfd, &event) < 0)
                {
                    ::close(connfd);

                    throw std::system_error(errno, std::generic_category(), "epoll_ctl ADD client");
                }

                channels.emplace(connfd, std::move(channel));

                std::cout << "new connection fd=" << connfd << '\n';
            }
        });

    epoll_event listenEvent{};

    listenEvent.events = EPOLLIN;
    listenEvent.data.ptr = &listenChannel;

    if (::epoll_ctl(epollFd, EPOLL_CTL_ADD, listenSocket.get(), &listenEvent) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "epoll_ctl ADD listen socket");
    }

    std::array<epoll_event, 64> events{};

    std::cout << "listen on 127.0.0.1:8888\n";

    while (true)
    {
        int ready = ::epoll_wait(epollFd, events.data(), static_cast<int>(events.size()), -1);

        if (ready < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            throw std::system_error(errno, std::generic_category(), "epoll_wait");
        }

        for (int i = 0; i < ready; ++i)
        {
            Channel* channel = static_cast<Channel*>(events[i].data.ptr);

            channel->setRevents(events[i].events);

            channel->handleEvent();
        }

        // 一轮事件分发完成后，再统一销毁 Channel。
        for (int fd : pendingClose)
        {
            auto it = channels.find(fd);

            if (it == channels.end())
            {
                continue;
            }

            if (::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr) < 0)
            {
                std::cerr << "epoll_ctl DEL failed fd=" << fd << ", errno=" << errno << '\n';
            }

            ::close(fd);

            channels.erase(it);

            std::cout << "connection removed fd=" << fd << '\n';
        }

        pendingClose.clear();
    }

    ::close(epollFd);

    return 0;
}