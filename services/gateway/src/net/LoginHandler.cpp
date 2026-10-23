/** @file
 * @brief Registers authentication, online state and bounded room
 * roster routes.
 */
#include "net/LoginHandler.h"

#include "net/MatchHandler.h"
#include "net/RoomHandler.h"

namespace service::gateway
{
namespace
{
arena::protocol::Presence wirePresence(Presence presence)
{
    return presence == Presence::Online
               ? arena::protocol::PRESENCE_ONLINE
               : arena::protocol::PRESENCE_DETACHED;
}

/** @brief Copies one consistent registry transaction into an owning
 * wire reply. */
void encodeState(
    const OnlineState& state,
    arena::protocol::SessionStateResponse& response
)
{
    response.set_session_id(state.session.sessionId.value());
    response.set_player_id(state.session.playerId.value());
    response.set_room_id(
        state.session.roomId ? state.session.roomId->value() : 0
    );
    response.set_presence(wirePresence(state.session.presence));
    for (const auto& member : state.members)
    {
        auto* entry = response.add_members();
        entry->set_player_id(member.playerId.value());
        entry->set_session_id(member.sessionId.value());
        entry->set_presence(wirePresence(member.presence));
    }
}

ErrorCode routeError(OnlineError error)
{
    switch (error)
    {
    case OnlineError::NotCurrent:
        return arena::protocol::ERR_UNAUTHENTICATED;
    case OnlineError::InvalidRoom:
        return arena::protocol::ERR_INVALID_REQUEST;
    case OnlineError::RoomFull:
        return arena::protocol::ERR_BUSINESS_FAILURE;
    case OnlineError::Capacity:
        return arena::protocol::ERR_UNAVAILABLE;
    }
    return arena::protocol::ERR_INTERNAL;
}
} // namespace

std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>
        passwords,
    std::shared_ptr<const service::auth::JwtTokens> tokens,
    std::shared_ptr<OnlineRegistry> sessions,
    std::shared_ptr<MatchmakingService> matchmaking,
    std::shared_ptr<RoomService> rooms
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
            response->set_session_id(
                grant->snapshot.sessionId.value()
            );
            response->set_room_id(
                grant->snapshot.roomId
                    ? grant->snapshot.roomId->value()
                    : 0
            );
            response->set_resumed(grant->resumed);
            complete(std::move(reply));
        },
        MessageRouteOptions{false}
    );
    routes->registerHandler(
        arena::protocol::MSG_SESSION_STATE_REQUEST,
        [sessions](auto request, auto complete)
        {
            const auto binding = request->context->session->binding();
            if (!binding)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_UNAUTHENTICATED
                    )
                );
            const auto state = sessions->state(*binding);
            if (!state)
                return complete(
                    std::unexpected(routeError(state.error()))
                );
            arena::protocol::Envelope reply;
            encodeState(
                *state,
                *reply.mutable_session_state_response()
            );
            complete(std::move(reply));
        }
    );
    routes->registerHandler(
        arena::protocol::MSG_ROOM_JOIN_REQUEST,
        [sessions](auto request, auto complete)
        {
            const auto binding = request->context->session->binding();
            if (!binding)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_UNAUTHENTICATED
                    )
                );
            const auto room = request->envelope.room_join_request()
                                  .room_id();
            if (!room)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_INVALID_REQUEST
                    )
                );
            const auto state = sessions->joinRoom(
                *binding,
                arena::domain::RoomId{room}
            );
            if (!state)
                return complete(
                    std::unexpected(routeError(state.error()))
                );
            arena::protocol::Envelope reply;
            encodeState(
                *state,
                *reply.mutable_room_join_response()->mutable_state()
            );
            complete(std::move(reply));
        }
    );
    routes->registerHandler(
        arena::protocol::MSG_ROOM_LEAVE_REQUEST,
        [sessions](auto request, auto complete)
        {
            const auto binding = request->context->session->binding();
            if (!binding)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_UNAUTHENTICATED
                    )
                );
            const auto state = sessions->leaveRoom(*binding);
            if (!state)
                return complete(
                    std::unexpected(routeError(state.error()))
                );
            arena::protocol::Envelope reply;
            encodeState(
                *state,
                *reply.mutable_room_leave_response()->mutable_state()
            );
            complete(std::move(reply));
        }
    );
    if (matchmaking)
        registerMatchRoutes(
            *routes,
            sessions,
            std::move(matchmaking)
        );
    if (rooms)
        registerRoomRoutes(*routes, sessions, std::move(rooms));
    routes->seal();
    return routes;
}
} // namespace service::gateway
