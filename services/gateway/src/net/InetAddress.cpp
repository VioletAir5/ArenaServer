/** @file
 * @brief Wraps a socket address for binding and connection setup.
 */
#include <net/InetAddress.h>

#include <stdexcept>

namespace service::gateway
{
InetAddress::InetAddress()
{
}

InetAddress::InetAddress(const std::string& ip, uint16_t port)
{
    m_addr.sin_family = AF_INET;
    m_addr.sin_port = htons(port);

    if (::inet_pton(AF_INET, ip.c_str(), &m_addr.sin_addr) != 1)
    {
        throw std::invalid_argument("invalid IPv4 address: " + ip);
    }
}

InetAddress::InetAddress(const sockaddr_in& addr)
    : m_addr(addr)
{
}

InetAddress::InetAddress(uint16_t port, bool loopbackOnly)
{
    m_addr.sin_family = AF_INET;
    m_addr.sin_port = htons(port);

    m_addr.sin_addr.s_addr = htonl(
        loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY
    );
}

const sockaddr* InetAddress::sockaddrPtr() const noexcept
{
    return reinterpret_cast<const sockaddr*>(&m_addr);
}

sockaddr* InetAddress::sockaddrPtr() noexcept
{
    return reinterpret_cast<sockaddr*>(&m_addr);
}

socklen_t InetAddress::length() const noexcept
{
    return sizeof(m_addr);
}

std::string InetAddress::ip() const
{
    char buf[INET_ADDRSTRLEN]{};

    if (::inet_ntop(AF_INET, &m_addr.sin_addr, buf, sizeof(buf)) ==
        nullptr)
    {
        return {};
    }

    return buf;
}

uint16_t InetAddress::port() const noexcept
{
    return ntohs(m_addr.sin_port);
}

std::string InetAddress::toString() const
{
    return ip() + ":" + std::to_string(port());
}

} // namespace service::gateway
