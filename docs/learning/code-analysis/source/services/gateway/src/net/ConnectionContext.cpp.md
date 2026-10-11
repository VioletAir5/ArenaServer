# services/gateway/src/net/ConnectionContext.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；73行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Carries immutable connection identity and session context.
 */
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
~~~
