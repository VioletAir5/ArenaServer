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
class ConnectionSession final
{
public:
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

    const std::thread::id m_owner;
    LoginState m_loginState{LoginState::Pending};
};

// Immutable admission metadata, created on the connection owner using
// values captured by accept. acceptedAt precedes worker handoff and
// WS upgrade. IPv4 peer is taken from the socket, never HTTP/client
// input.
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

    const std::uint64_t connectionId;
    const InetAddress peer;
    const Clock::time_point acceptedAt;
    const std::shared_ptr<ConnectionSession> session;
    // Shared atomic counters may be read from monitoring/other workers.
    const RejectionCounters::Ptr rejections;
};
} // namespace service::gateway
