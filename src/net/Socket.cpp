#include "net/Socket.h"

#include <sys/socket.h>
#include <utility>
namespace service
{

Socket::Socket(FileDescriptor fd) noexcept
    : fd_(std::move(fd))
{
}

Socket Socket::createTcp()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);

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

void Socket::shutdown()
{
    if (fd_.valid()) {
        ::shutdown(fd_.get(), SHUT_RDWR);
    }
}

} // namespace service
