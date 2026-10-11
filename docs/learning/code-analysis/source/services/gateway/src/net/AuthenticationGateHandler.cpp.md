# services/gateway/src/net/AuthenticationGateHandler.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；49行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Provides the authentication gate handler implementation.
 */
#include <iostream>
#include <stdexcept>
#include <utility>

#include "net/PacketPipeline.h"

namespace service::gateway
{
AuthenticationGateHandler::AuthenticationGateHandler(
    std::shared_ptr<ConnectionSession> session
)
    : m_session(std::move(session))
{
    if (!m_session)
        throw std::invalid_argument(
            "authentication gate requires session"
        );
}
PacketHandleResult
AuthenticationGateHandler::handle(PacketContext& context)
{
    // Fail closed if this node was installed before
    // EnvelopeDecodeHandler.
    if (!context.envelope)
    {
        context.rejectionReason = RejectionReason::InvalidEnvelope;
        return PacketHandleResult::Reject;
    }
    const auto state = m_session->loginState();
    const bool
        preLoginRequest = context.header.m_type ==
                              protocol::PacketHeader::kEchoRequest &&
                          (context.envelope->has_login_request() ||
                           context.envelope->has_ping());
    if (state == ConnectionSession::LoginState::Authenticated ||
        (state == ConnectionSession::LoginState::Pending &&
         preLoginRequest))
        return PacketHandler::handle(context);

    context.rejectionReason = RejectionReason::Unauthenticated;
    std::cerr << "connection unauthenticated packet id="
              << context.connection.id()
              << " type=" << context.header.m_type << '\n';
    return PacketHandleResult::Reject;
}
} // namespace service::gateway
~~~
