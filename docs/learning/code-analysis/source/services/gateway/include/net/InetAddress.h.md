# services/gateway/include/net/InetAddress.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；42行。

[对应分析](../../../../../02-reactor.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
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
~~~
