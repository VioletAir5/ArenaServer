/** @file
 * @brief Carries immutable connection identity and session context.
 */
#include "net/ConnectionContext.h"
#include "net/AccountSessions.h"

#include <stdexcept>
#include <mutex>
#include <utility>

namespace service::gateway
{
struct ConnectionSession::CloseEndpoint
{
    std::mutex mutex;
    std::function<void()> post;
};

ConnectionSession::ConnectionSession()
    : m_owner(std::this_thread::get_id())
    , m_closeEndpoint(std::make_shared<CloseEndpoint>())
{
}

void ConnectionSession::requireOwner() const
{
    if (std::this_thread::get_id() != m_owner)
    {
        throw std::logic_error(
            "ConnectionSession requires connection owner"
        );
    }
}

ConnectionSession::LoginState ConnectionSession::loginState() const
{
    requireOwner();
    return superseded() ? LoginState::Expired : m_loginState;
}

bool ConnectionSession::markAuthenticated()
{
    requireOwner();
    if (m_loginState != LoginState::Pending)
    {
        return false;
    }
    m_loginState = LoginState::Authenticated;
    return true;
}

bool ConnectionSession::expireLogin()
{
    requireOwner();
    if (m_loginState != LoginState::Pending)
    {
        return false;
    }
    m_loginState = LoginState::Expired;
    return true;
}

ConnectionSession::~ConnectionSession()
{
    disconnect();
}

void ConnectionSession::setCloseRequest(std::function<void()> close)
{
    requireOwner();
    std::lock_guard lock(m_closeEndpoint->mutex);
    if (!close || m_closeEndpoint->post)
        throw std::logic_error("session closer must be installed once");
    m_closeEndpoint->post = std::move(close);
}

std::function<void()> ConnectionSession::closeRequest() const
{
    requireOwner();
    std::lock_guard lock(m_closeEndpoint->mutex);
    if (!m_closeEndpoint->post)
        throw std::logic_error("authenticated session requires owner close scheduler");
    return [endpoint = m_closeEndpoint]
    {
        std::lock_guard gate(endpoint->mutex);
        if (endpoint->post)
            endpoint->post(); // defer must enqueue, never invoke the action inline.
    };
}

bool ConnectionSession::bind(std::shared_ptr<SessionLease> binding)
{
    requireOwner();
    if (m_loginState != LoginState::Pending || !binding)
        return false;
    m_binding = std::move(binding);
    if (!m_binding->current())
    {
        m_loginState = LoginState::Expired;
        return false;
    }
    m_loginState = LoginState::Authenticated;
    return true;
}

bool ConnectionSession::superseded() const
{
    requireOwner();
    return m_binding && !m_binding->current();
}

void ConnectionSession::disconnect() noexcept
{
    {
        std::lock_guard lock(m_closeEndpoint->mutex);
        m_closeEndpoint->post = {}; // Wait for concurrent posts before owner context can die.
    }
    if (m_binding)
        m_binding->release();
    m_loginState = LoginState::Expired;
}

ConnectionContext::ConnectionContext(
    std::uint64_t id,
    InetAddress peerAddress,
    Clock::time_point accepted,
    RejectionCounters::Ptr counters
)
    : connectionId(id)
    , peer(std::move(peerAddress))
    , acceptedAt(accepted)
    , session(std::make_shared<ConnectionSession>())
    , rejections(std::move(counters))
{
    if (id == 0 || !rejections)
    {
        throw std::invalid_argument(
            "connection context requires nonzero id and rejection counters"
        );
    }
}
} // namespace service::gateway
