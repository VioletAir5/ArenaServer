/** @file
 * @brief Carries immutable connection identity and session context.
 */
#include "net/ConnectionContext.h"

#include <mutex>
#include <stdexcept>
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

ConnectionSession::~ConnectionSession() noexcept
{
    cleanup();
}

void ConnectionSession::setCloseRequest(std::function<void()> close)
{
    requireOwner();
    std::lock_guard lock(m_closeEndpoint->mutex);
    if (!close || m_closeEndpoint->post)
        throw std::logic_error(
            "session closer must be installed once"
        );
    m_closeEndpoint->post = std::move(close);
}

std::function<void()> ConnectionSession::closeRequest() const
{
    requireOwner();
    std::lock_guard lock(m_closeEndpoint->mutex);
    if (!m_closeEndpoint->post)
        throw std::logic_error(
            "authenticated session requires owner close scheduler"
        );
    return [endpoint = m_closeEndpoint]
    {
        std::function<void()> post;
        {
            std::lock_guard gate(endpoint->mutex);
            post = endpoint->post;
        }
        // The durable mailbox handle supplies executor lifetime
        // safety. Invoke unlocked: owner dispatch may close and
        // disconnect inline.
        if (post)
            post();
    };
}

namespace
{
bool projectionCurrent(
    const ConnectionSession::Projection& projection
) noexcept
{
    try
    {
        return projection.current && projection.current();
    }
    catch (...)
    {
        return false;
    }
}
void releaseProjection(
    ConnectionSession::Projection projection
) noexcept
{
    try
    {
        if (projection.release)
            projection.release();
    }
    catch (...)
    { /* Cleanup cannot unwind through transport shutdown. */
    }
}
} // namespace

bool ConnectionSession::bind(Projection projection)
{
    requireOwner();
    if (m_loginState != LoginState::Pending || !projection.current ||
        !projection.release || !projection.binding.connectionId ||
        !projection.binding.epoch)
    {
        releaseProjection(std::move(projection));
        return false;
    }
    if (!projectionCurrent(projection))
    {
        m_loginState = LoginState::Expired;
        releaseProjection(std::move(projection));
        return false;
    }
    m_binding.emplace(std::move(projection));
    m_loginState = LoginState::Authenticated;
    return true;
}

bool ConnectionSession::superseded() const
{
    requireOwner();
    return m_binding && !projectionCurrent(*m_binding);
}

std::optional<service::lobby::SessionBinding>
ConnectionSession::binding() const
{
    requireOwner();
    return m_binding && projectionCurrent(*m_binding)
               ? std::optional(m_binding->binding)
               : std::nullopt;
}
void ConnectionSession::disconnect()
{
    requireOwner();
    cleanup();
}

void ConnectionSession::cleanup() noexcept
{
    {
        std::lock_guard lock(m_closeEndpoint->mutex);
        m_closeEndpoint->post = {};
    }
    if (auto binding = std::exchange(m_binding, std::nullopt))
        releaseProjection(std::move(*binding));
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
            "connection context requires nonzero id and rejection "
            "counters"
        );
    }
}
} // namespace service::gateway
