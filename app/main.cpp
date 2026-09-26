#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <system_error>

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

    epoll_event listenEvent{};
    listenEvent.events = EPOLLIN;
    listenEvent.data.fd = listenSocket.get();

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
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            if (fd == listenSocket.get())
            {

                while (true)
                {
                    sockaddr_in peerAddr{};
                    socklen_t peerLen = sizeof(peerAddr);

                    int connfd = ::accept4(listenSocket.get(),
                                           reinterpret_cast<sockaddr*>(&peerAddr),
                                           &peerLen,
                                           SOCK_NONBLOCK | SOCK_CLOEXEC);

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

                    epoll_event clientEvent{};
                    clientEvent.events = EPOLLIN | EPOLLRDHUP;

                    clientEvent.data.fd = connfd;

                    if (::epoll_ctl(epollFd, EPOLL_CTL_ADD, connfd, &clientEvent) < 0)
                    {

                        ::close(connfd);

                        throw std::system_error(
                            errno, std::generic_category(), "epoll_ctl ADD client");
                    }

                    std::cout << "new connection fd=" << connfd << '\n';
                }

                continue;
            }

            if (revents & (EPOLLERR | EPOLLHUP))
            {
                ::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);

                ::close(fd);

                std::cout << "connection error/closed fd=" << fd << '\n';

                continue;
            }

            if (revents & EPOLLRDHUP)
            {
                ::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);

                ::close(fd);

                std::cout << "peer closed fd=" << fd << '\n';

                continue;
            }

            if (revents & EPOLLIN)
            {
                char buffer[4096];

                while (true)
                {
                    ssize_t n = ::read(fd, buffer, sizeof(buffer));

                    if (n > 0)
                    {
                        ssize_t written = 0;

                        while (written < n)
                        {
                            ssize_t ret = ::write(fd, buffer + written, n - written);

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

                                std::cerr << "write would block, "
                                          << "data may remain unsent\n";

                                break;
                            }

                            ::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);

                            ::close(fd);

                            break;
                        }

                        continue;
                    }

                    if (n == 0)
                    {
                        ::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);

                        ::close(fd);

                        std::cout << "peer EOF fd=" << fd << '\n';

                        break;
                    }

                    if (errno == EINTR)
                    {
                        continue;
                    }

                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        break;
                    }

                    ::epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);

                    ::close(fd);

                    break;
                }
            }
        }
    }
}