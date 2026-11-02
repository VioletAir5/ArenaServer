/** @file
 * @brief Marshals remote Lobby replies through the live
 * connection-owner fence.
 */
#include "net/RemoteLobbyRoutes.h"

#include <array>
#include <chrono>
#include <optional>
#include <utility>

#include "net/RequestExecution.h"

#include "security/SecureErase.h"

namespace service::gateway
{
namespace
{
using Binding = service::lobby::SessionBinding;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
ErrorCode publicError(arena::rpc::Status status)
{
    switch (status)
    {
    case arena::rpc::INVALID_ARGUMENT:
        return arena::protocol::ERR_INVALID_REQUEST;
    case arena::rpc::UNAUTHENTICATED:
        return arena::protocol::ERR_UNAUTHENTICATED;
    case arena::rpc::OVERLOADED:
        return arena::protocol::ERR_TOO_MANY_REQUESTS;
    case arena::rpc::DEADLINE_EXCEEDED:
    case arena::rpc::CANCELLED:
        return arena::protocol::ERR_TIMEOUT;
    case arena::rpc::CONFLICT:
    case arena::rpc::NOT_FOUND:
        return arena::protocol::ERR_BUSINESS_FAILURE;
    case arena::rpc::INTERNAL:
        return arena::protocol::ERR_INTERNAL;
    default:
        return arena::protocol::ERR_UNAVAILABLE;
    }
}
std::int64_t unixSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
    )
        .count();
}

/** @brief Cleans a grant whose owner fence disappeared before
 * installation. */
struct PreparedReply
{
    PreparedReply(
        LobbyClient::ExecuteResult value,
        std::weak_ptr<LobbyClient> remote,
        std::uint64_t id,
        bool authenticating
    )
        : result(std::move(value))
        , client(std::move(remote))
        , connection(id)
        , login(authenticating)
    {
    }
    PreparedReply(const PreparedReply&) = delete;
    PreparedReply& operator=(const PreparedReply&) = delete;
    LobbyClient::ExecuteResult result;
    std::weak_ptr<LobbyClient> client;
    std::uint64_t connection;
    bool login, transferred{false};
    ~PreparedReply()
    {
        if (!result)
            return;
        auto& reply = *result;
        if (login && !transferred && reply.has_session() &&
            reply.has_message() &&
            reply.message().has_login_response())
            try
            {
                if (const auto remote = client.lock())
                {
                    arena::lobby::DetachRequest detach;
                    detach.set_connection_id(connection);
                    detach.set_player_id(reply.session().player_id());
                    detach.set_session_epoch(reply.session().epoch());
                    remote->detach(
                        std::move(detach),
                        LobbyClient::Context{
                            Clock::now() + 1000ms,
                            "unbound_" + std::to_string(connection),
                            {"gateway"}
                        },
                        [](auto) {}
                    );
                }
            }
            catch (...)
            {
            }
        if (reply.has_message() &&
            reply.message().has_login_response())
            arena::security::eraseSecret(
                *reply.mutable_message()
                     ->mutable_login_response()
                     ->mutable_session_token()
            );
    }
};

MessageHandler
remoteHandler(std::shared_ptr<LobbyClient> client, bool login)
{
    return [client = std::move(client),
            login](auto request, auto complete)
    {
        const auto execution = request->execution;
        if (!execution)
            return complete(
                std::unexpected(arena::protocol::ERR_UNAVAILABLE)
            );
        const auto session = request->context->session;
        if (login && session->loginState() !=
                         ConnectionSession::LoginState::Pending)
            return complete(
                std::unexpected(arena::protocol::ERR_UNAUTHENTICATED)
            );
        const auto binding = session->binding();
        if (!login && !binding)
            return complete(
                std::unexpected(arena::protocol::ERR_UNAUTHENTICATED)
            );
        arena::lobby::ExecuteRequest command;
        command.set_connection_id(request->connectionId);
        *command.mutable_message() = request->envelope;
        // Dispatcher supplies a nonzero fallback identity for legacy
        // request_id=0.
        command.mutable_message()->set_request_id(request->requestId);
        if (binding)
        {
            command.set_player_id(binding->playerId.value());
            command.set_session_epoch(binding->epoch);
        }
        const auto connection = request->connectionId;
        const auto weakSession = std::weak_ptr<ConnectionSession>(
            session
        );
        auto accepted = client->execute(
            std::move(command),
            LobbyClient::Context{
                execution->deadline(),
                execution->correlationId(),
                {"gateway"}
            },
            [client,
             execution,
             weakSession,
             binding,
             connection,
             login](LobbyClient::ExecuteResult result) mutable
            {
                // This callback never dereferences owner-affine
                // session metadata.
                auto reply = std::make_shared<PreparedReply>(
                    std::move(result),
                    client,
                    connection,
                    login
                );
                execution->completeOnOwner(
                    [client,
                     weakSession,
                     binding,
                     connection,
                     login,
                     reply]() mutable -> MessageReply
                    {
                        const auto owner = weakSession.lock();
                        if (!owner)
                            return std::unexpected(
                                arena::protocol::ERR_UNAUTHENTICATED
                            );
                        if (!reply->result)
                            return std::unexpected(
                                publicError(reply->result.error())
                            );
                        auto& result = *reply->result;
                        if (result.message().has_error())
                        {
                            if (!login && binding &&
                                result.message().error().code() ==
                                    arena::protocol::
                                        ERR_UNAUTHENTICATED)
                                client->invalidate(*binding);
                            return std::unexpected(
                                static_cast<ErrorCode>(
                                    result.message().error().code()
                                )
                            );
                        }
                        if (login)
                        {
                            if (owner->loginState() !=
                                ConnectionSession::LoginState::
                                    Pending)
                                return std::unexpected(
                                    arena::protocol::
                                        ERR_UNAUTHENTICATED
                                );
                            const auto& projection = result.session();
                            const Binding next{
                                arena::domain::PlayerId{
                                    projection.player_id()
                                },
                                connection,
                                projection.epoch()
                            };
                            auto tracked = client->track(
                                next,
                                projection.expires_at_unix(),
                                owner->closeRequest()
                            );
                            if (!tracked)
                                return std::unexpected(
                                    publicError(tracked.error())
                                );
                            const auto weakClient = std::weak_ptr<
                                LobbyClient>(client);
                            const auto
                                expires = projection
                                              .expires_at_unix();
                            // bind() consumes the release handoff on
                            // every outcome.
                            reply->transferred = true;
                            if (!owner->bind(
                                    ConnectionSession::Projection{
                                        next,
                                        [valid = *tracked, expires]
                                        {
                                            return valid->load(
                                                       std::
                                                           memory_order_acquire
                                                   ) &&
                                                   unixSeconds() <
                                                       expires;
                                        },
                                        [weakClient, next]
                                        {
                                            if (const auto remote =
                                                    weakClient.lock())
                                                remote->untrack(next);
                                        }
                                    }
                                ))
                                return std::unexpected(
                                    arena::protocol::
                                        ERR_UNAUTHENTICATED
                                );
                        }
                        return std::move(*result.mutable_message());
                    }
                );
            }
        );
        if (!accepted)
            complete(std::unexpected(publicError(accepted.error())));
        else
            execution->setCancellation([call = *accepted]
                                       { call.cancel(); });
    };
}
} // namespace

std::shared_ptr<const MessageRegistry>
makeRemoteLobbyMessageRegistry(std::shared_ptr<LobbyClient> client)
{
    if (!client)
        throw std::invalid_argument(
            "remote routes require Lobby client"
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
    constexpr std::array requests{
        arena::protocol::MSG_LOGIN_REQUEST,
        arena::protocol::MSG_SESSION_STATE_REQUEST,
        arena::protocol::MSG_ROOM_JOIN_REQUEST,
        arena::protocol::MSG_ROOM_LEAVE_REQUEST,
        arena::protocol::MSG_MATCH_JOIN_REQUEST,
        arena::protocol::MSG_MATCH_CANCEL_REQUEST,
        arena::protocol::MSG_MATCH_STATUS_REQUEST,
        arena::protocol::MSG_ROOM_INPUT_REQUEST,
        arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST,
        arena::protocol::MSG_ROOM_READY_REQUEST,
        arena::protocol::MSG_ROOM_STATUS_REQUEST,
        arena::protocol::MSG_SETTLEMENT_REQUEST
    };
    for (const auto message : requests)
        routes->registerHandler(
            message,
            remoteHandler(
                client,
                message == arena::protocol::MSG_LOGIN_REQUEST
            ),
            MessageRouteOptions{
                message != arena::protocol::MSG_LOGIN_REQUEST,
                1000ms
            }
        );
    routes->seal();
    return routes;
}
} // namespace service::gateway
