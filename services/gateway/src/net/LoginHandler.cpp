#include "net/LoginHandler.h"

namespace service::gateway
{
std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>
        passwords,
    std::shared_ptr<const service::auth::JwtTokens> tokens,
    std::shared_ptr<AccountSessions> sessions
)
{
    if (!passwords || !tokens || !sessions)
        throw std::invalid_argument(
            "login routes require authentication dependencies"
        );
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
        [passwords, tokens, sessions](auto request, auto complete)
        {
            using service::auth::AuthError;
            const auto fail = [&](AuthError error)
            {
                complete(
                    std::unexpected(
                        error == AuthError::Unavailable
                            ? arena::protocol::ERR_UNAVAILABLE
                            : arena::protocol::ERR_UNAUTHENTICATED
                    )
                );
            };
            const auto session = request->context->session;
            if (session->loginState() !=
                ConnectionSession::LoginState::Pending)
                return fail(AuthError::Superseded);
            const auto& login = request->envelope.login_request();
            std::optional<service::auth::TokenClaims> resume;
            std::expected<arena::domain::PlayerId, AuthError>
                player = std::unexpected(
                    AuthError::InvalidCredentials
                );
            if (login.credential_kind() ==
                arena::protocol::CREDENTIAL_PASSWORD)
                player = passwords->authenticate(
                    login.account(),
                    login.credential()
                );
            else if (
                login.credential_kind() ==
                arena::protocol::CREDENTIAL_JWT
            )
            {
                // JWT subject is authoritative; a client cannot
                // substitute an account.
                if (!login.account().empty())
                    return fail(AuthError::InvalidToken);
                auto claims = tokens->verify(login.credential());
                if (!claims)
                    return fail(claims.error());
                resume = *claims;
                player = claims->playerId;
            }
            if (!player)
                return fail(player.error());
            auto grant = sessions->install(
                *player,
                request->connectionId,
                resume,
                *tokens,
                session->closeRequest()
            );
            if (!grant)
                return fail(grant.error());
            if (!session->bind(grant->lease))
            {
                grant->lease->release();
                return fail(AuthError::Superseded);
            }
            arena::protocol::Envelope reply;
            auto* response = reply.mutable_login_response();
            response->set_accepted(true);
            response->set_session_token(std::move(grant->token));
            response->set_player_id(player->value());
            response->set_session_epoch(grant->lease->binding.epoch);
            complete(std::move(reply));
        },
        MessageRouteOptions{false}
    );
    routes->seal();
    return routes;
}
} // namespace service::gateway
