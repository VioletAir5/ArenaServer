#include "net/Socket.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <cerrno>
#include <system_error>
namespace service
{

Socket::Socket(FileDescriptor fd) noexcept
    : fd_(std::move(fd))
{
}

Socket Socket::createTcp()
{

    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if (fd < 0)
    {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    return Socket(FileDescriptor(fd));
}

Socket Socket::createTcpNonBlocking()
{
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

    if (fd < 0)
    {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    return Socket(FileDescriptor(fd));
}

int Socket::get() const noexcept
{
    return fd_.get();
}

bool Socket::valid() const noexcept
{
    return fd_.valid();
}

void Socket::bind(const InetAddress& addr)
{
    if (::bind(fd_.get(), addr.sockaddrPtr(), addr.length()) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "bind");
    }
}

void Socket::listen(int backlog)
{
    if (::listen(fd_.get(), backlog) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "listen");
    }
}

Socket Socket::accept(InetAddress* peer)
{
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);

    int connfd = ::accept(fd_.get(), reinterpret_cast<sockaddr*>(&addr), &len);

    if (connfd < 0)
    {
        throw std::system_error(errno, std::generic_category(), "accept");
    }

    if (peer)
    {
        *peer = InetAddress(addr);
    }

    return Socket(FileDescriptor(connfd));
}

void Socket::setReuseAddr(bool on)
{
    int opt = on ? 1 : 0;

    if (::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "setsockopt SO_REUSEADDR");
    }
}

void Socket::setReusePort(bool on)
{
    int opt = on ? 1 : 0;

    if (::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "setsockopt SO_REUSEPORT");
    }
}

void Socket::setTcpNoDelay(bool on)
{
    int opt = on ? 1 : 0;

    if (::setsockopt(fd_.get(), IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "setsockopt TCP_NODELAY");
    }
}

} // namespace service
