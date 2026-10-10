/** @file
 * @brief Wraps a socket address for binding and connection setup.
 */
#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>

#include <cstdint>
#include <string>

namespace service::gateway
{
/** @brief Wraps a socket address for binding and connection setup. */
class InetAddress
{
private:
    /// Stores the addr.
    sockaddr_in m_addr{};

public:
    /** @brief Constructs the inet address. */
    InetAddress();
    /** @brief Constructs the inet address. */
    InetAddress(const std::string& ip, uint16_t port);
    /** @brief Constructs the inet address. */
    InetAddress(const sockaddr_in& addr);

    /** @brief Constructs the inet address. */
    explicit InetAddress(uint16_t port, bool loopbackOnly = false);

    const sockaddr* sockaddrPtr() const noexcept;
    sockaddr* sockaddrPtr() noexcept;

    socklen_t length() const noexcept;

    std::string ip() const;
    uint16_t port() const noexcept;
    std::string toString() const;
};

} // namespace service::gateway
