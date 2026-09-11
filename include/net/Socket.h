#pragma once

#include "os/FileDescriptor.h"

namespace service
{

class Socket
{
private:
    FileDescriptor fd_;

public:
    Socket() = default;

    explicit Socket(FileDescriptor fd) noexcept;

    ~Socket() = default;

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&&) noexcept = default;
    Socket& operator=(Socket&&) noexcept = default;

    static Socket createTcp();

    int get() const noexcept;
    bool valid() const noexcept;

    void shutdown();
};

} // namespace service
