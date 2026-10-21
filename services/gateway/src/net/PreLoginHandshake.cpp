/** @file
 * @brief Registers default public routes without a second handler
 * chain.
 */
#include "net/PreLoginHandshake.h"

namespace service::gateway
{
std::shared_ptr<const MessageRegistry> defaultMessageRegistry()
{
    static const auto registry = []
    {
        auto routes = std::make_shared<MessageRegistry>();
        routes->registerHandler(
            arena::protocol::MSG_PING,
            [](auto request, auto complete)
            {
                arena::protocol::Envelope reply;
                reply.mutable_pong()->set_nonce(
                    request->envelope.ping().nonce()
                );
                complete(std::move(reply));
            },
            MessageRouteOptions{false}
        );
        routes->registerHandler(
            arena::protocol::MSG_LOGIN_REQUEST,
            [](auto, auto complete)
            {
                complete(
                    std::unexpected(arena::protocol::ERR_UNAVAILABLE)
                );
            },
            MessageRouteOptions{false}
        );
        routes->seal();
        return std::shared_ptr<const MessageRegistry>(
            std::move(routes)
        );
    }();
    return registry;
}

ConnectionHandler makePreLoginHandshakeHandler(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    PacketPolicyFactory policies
)
{
    return makeArenaProtocolHandler(
        std::move(context),
        registry ? std::move(registry) : defaultMessageRegistry(),
        std::move(policies)
    );
}
} // namespace service::gateway
