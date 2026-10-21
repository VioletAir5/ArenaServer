#pragma once

#include "net/PreLoginHandshake.h"

namespace service::gateway
{
inline std::shared_ptr<const MessageRegistry> rpcTestRegistry()
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
            [](auto request, auto complete)
            {
                const auto& action = request->envelope.login_request()
                                         .account();
                if (action == "wait")
                    return; // Deliberate async omission: exercise the
                            // real deadline.
                if (action == "business")
                    throw BusinessError(
                        arena::protocol::ERR_BUSINESS_FAILURE
                    );
                if (action == "crash")
                    throw std::runtime_error(
                        "do-not-log-private-business-data"
                    );
                if (action == "auth")
                    request->context->session->markAuthenticated();
                arena::protocol::Envelope reply;
                reply.mutable_login_response()->set_accepted(true);
                complete(std::move(reply));
            },
            MessageRouteOptions{false, std::chrono::milliseconds(100)}
        );
        // Synthetic authenticated business MsgId exists only in this
        // host.
        routes->registerHandler(
            static_cast<MsgId>(100),
            [](auto, auto complete)
            {
                arena::protocol::Envelope reply;
                reply.mutable_pong()->set_nonce(42);
                complete(std::move(reply));
            }
        );
        routes->seal();
        return std::shared_ptr<const MessageRegistry>(
            std::move(routes)
        );
    }();
    return registry;
}
inline ConnectionHandlerFactory makeRpcTestFactory()
{
    const auto registry = rpcTestRegistry();
    return [registry](ConnectionContext::Ptr context)
    {
        return makePreLoginHandshakeHandler(
            std::move(context),
            registry
        );
    };
}
} // namespace service::gateway
