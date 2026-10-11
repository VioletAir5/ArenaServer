# services/gateway/include/net/ConnectionContext.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；80行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Carries immutable connection identity and session context.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include "net/InetAddress.h"
#include "net/RejectionCounters.h"

namespace service::gateway
{
// Authentication state belongs to the connection owner. Shared
// ownership extends lifetime; it does not permit access from a
// different thread. This passive object owns no socket, timer, or
// connection.
/** @brief Represents connection session. */
class ConnectionSession final
{
public:
    /** @brief Tracks a session through authentication and expiry. */
    enum class LoginState
    {
        Pending,
        Authenticated,
        Expired
    };

    ConnectionSession();
    ConnectionSession(const ConnectionSession&) = delete;
    ConnectionSession& operator=(const ConnectionSession&) = delete;

    LoginState loginState() const;
    // Return true only when Pending transitions to the requested
    // state. Call markAuthenticated only after successful
    // authentication.
    bool markAuthenticated();
    bool expireLogin();

private:
    void requireOwner() const;

    /// Thread that owns mutable connection state.
    const std::thread::id m_owner;
    /// Authentication state mutated only by the owner thread.
    LoginState m_loginState{LoginState::Pending};
};

// Immutable admission metadata, created on the connection owner using
// values captured by accept. acceptedAt precedes worker handoff and
// WS upgrade. IPv4 peer is taken from the socket, never HTTP/client
// input.
/** @brief Carries immutable connection identity and session context. */
struct ConnectionContext final
{
    using Clock = std::chrono::steady_clock;
    using Ptr = std::shared_ptr<const ConnectionContext>;

    ConnectionContext(
        std::uint64_t id,
        InetAddress peerAddress,
        Clock::time_point accepted,
        RejectionCounters::Ptr counters = defaultRejectionCounters()
    );

    /// Process-wide connection identifier captured at admission.
    const std::uint64_t connectionId;
    /// Peer address captured from the accepted socket.
    const InetAddress peer;
    /// Monotonic time when the socket was accepted.
    const Clock::time_point acceptedAt;
    /// Owner-thread authentication state shared with handlers.
    const std::shared_ptr<ConnectionSession> session;
    // Shared atomic counters may be read from monitoring/other workers.
    const RejectionCounters::Ptr rejections;
};
} // namespace service::gateway
~~~
