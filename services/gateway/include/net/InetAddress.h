#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>

#include <cstdint>
#include <string>

namespace service::gateway
{
class InetAddress
{
private:
    sockaddr_in m_addr{};

public:
    InetAddress();
    InetAddress(const std::string& ip, uint16_t port);
    InetAddress(const sockaddr_in& addr);

    explicit InetAddress(uint16_t port, bool loopbackOnly = false);

    const sockaddr* sockaddrPtr() const noexcept;
    sockaddr* sockaddrPtr() noexcept;

    socklen_t length() const noexcept;

    std::string ip() const;
    uint16_t port() const noexcept;
    std::string toString() const;
};

} // namespace service::gateway
