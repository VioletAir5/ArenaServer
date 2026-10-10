#include "net/ConnectionContext.h"

#include <stdexcept>
#include <utility>

namespace service::gateway
{
ConnectionSession::ConnectionSession()
    : m_owner(std::this_thread::get_id())
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
    return m_loginState;
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
