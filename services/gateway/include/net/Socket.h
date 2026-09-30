#pragma once

#include "net/InetAddress.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{

class Socket
{
public:
    explicit Socket(FileDescriptor fd) noexcept;

    static Socket createTcp();
    static Socket createTcpNonBlocking();

    int get() const noexcept;
    bool valid() const noexcept;

    void bind(const InetAddress& addr);
    void listen(int backlog = SOMAXCONN);

    Socket accept(InetAddress* peer = nullptr);

    void shutdownWrite();

    void setReuseAddr(bool on);
    void setReusePort(bool on);
    void setTcpNoDelay(bool on);

private:
    FileDescriptor fd_;
};

} // namespace service::gateway
