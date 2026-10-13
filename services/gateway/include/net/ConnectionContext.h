/** @file
 * @brief Carries immutable connection identity and session context.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>

#include "net/InetAddress.h"
#include "net/RejectionCounters.h"

namespace service::gateway
{
class SessionLease;
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
    ~ConnectionSession();
    ConnectionSession(const ConnectionSession&) = delete;
    ConnectionSession& operator=(const ConnectionSession&) = delete;

    LoginState loginState() const;
    // Return true only when Pending transitions to the requested
    // state. Call markAuthenticated only after successful
    // authentication.
    bool markAuthenticated();
    bool expireLogin();
    void setCloseRequest(std::function<void()>);
    std::function<void()> closeRequest() const;
    bool bind(std::shared_ptr<SessionLease>);
    bool superseded() const;

private:
    friend class ConnectionRuntime;
    void disconnect() noexcept; // Runtime lifecycle only; never a foreign control API.
    void requireOwner() const;

    /// Thread that owns mutable connection state.
    const std::thread::id m_owner;
    /// Authentication state mutated only by the owner thread.
    LoginState m_loginState{LoginState::Pending};
    std::shared_ptr<SessionLease> m_binding;
    struct CloseEndpoint;
    std::shared_ptr<CloseEndpoint> m_closeEndpoint;
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
