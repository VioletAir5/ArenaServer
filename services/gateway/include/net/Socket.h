/** @file
 * @brief Owns a TCP socket and its system-call operations.
 */
#pragma once

#include <optional>

#include "net/InetAddress.h"
#include "os/FileDescriptor.h"

namespace service::gateway
{

/** @brief Owns a TCP socket and its system-call operations. */
class Socket
{
public:
    /** @brief Constructs the socket. */
    explicit Socket(FileDescriptor fd) noexcept;

    /** @brief Creates tcp. */
    static Socket createTcp();
    /** @brief Creates tcp non blocking. */
    static Socket createTcpNonBlocking();

    int get() const noexcept;
    bool valid() const noexcept;

    void bind(const InetAddress& addr);
    void listen(int backlog = SOMAXCONN);

    std::optional<Socket> accept(InetAddress* peer = nullptr);

    /** @brief Flushes output before sending the local FIN. */
    void shutdownWrite();
    void close() noexcept;

    /** @brief Sets reuse addr. */
    void setReuseAddr(bool on);
    /** @brief Sets reuse port. */
    void setReusePort(bool on);
    /** @brief Sets tcp no delay. */
    void setTcpNoDelay(bool on);

private:
    FileDescriptor fd_;
};

} // namespace service::gateway
