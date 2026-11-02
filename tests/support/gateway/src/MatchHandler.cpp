/** @file
 * @brief Converts authenticated owner requests to deferred
 * matchmaking commands.
 */
#include "gateway_test/MatchHandler.h"

#include <stdexcept>
#include <utility>

#include "net/MessageRegistry.h"

#include "lobby/MatchmakingService.h"
#include "lobby/Protocol.h"

namespace service::gateway
{
namespace
{
enum class Operation
{
    Join,
    Cancel,
    Status
};

void registerRoute(
    MessageRegistry& routes,
    MsgId id,
    Operation operation,
    const std::shared_ptr<service::lobby::OnlineRegistry>& sessions,
    const std::shared_ptr<service::lobby::MatchmakingService>&
        matchmaking
)
{
    routes.registerHandler(
        id,
        [sessions,
         matchmaking,
         operation](auto request, auto complete)
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
                    std::unexpected(
                        arena::protocol::ERR_UNAUTHENTICATED
                    )
                );
            const auto value = operation == Operation::Join
                                   ? request->envelope
                                         .match_join_request()
                                         .join_key()
                               : operation == Operation::Cancel
                                   ? request->envelope
                                         .match_cancel_request()
                                         .ticket_id()
                                   : request->envelope
                                         .match_status_request()
                                         .ticket_id();
            if (!value)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_INVALID_REQUEST
                    )
                );
            // Capture owner-only authentication as values before
            // crossing to the management executor. The service
            // revalidates the binding.
            const auto sessionId = state->session.sessionId;
            auto reply =
                [complete, operation](
                    service::lobby::MatchmakingService::Result result
                )
            {
                if (!result)
                    return complete(std::unexpected(result.error()));
                arena::protocol::Envelope envelope;
                auto* ticket =
                    operation == Operation::Join
                        ? envelope.mutable_match_join_response()
                              ->mutable_state()
                    : operation == Operation::Cancel
                        ? envelope.mutable_match_cancel_response()
                              ->mutable_state()
                        : envelope.mutable_match_status_response()
                              ->mutable_state();
                service::lobby::encodeTicket(*result, *ticket);
                complete(std::move(envelope));
            };
            bool admitted = false;
            switch (operation)
            {
            case Operation::Join:
                admitted = matchmaking->join(
                    *binding,
                    sessionId,
                    value,
                    std::move(reply)
                );
                break;
            case Operation::Cancel:
                admitted = matchmaking->cancel(
                    *binding,
                    sessionId,
                    arena::domain::TicketId{value},
                    std::move(reply)
                );
                break;
            case Operation::Status:
                admitted = matchmaking->status(
                    *binding,
                    sessionId,
                    arena::domain::TicketId{value},
                    std::move(reply)
                );
                break;
            }
            if (!admitted)
                complete(
                    std::unexpected(arena::protocol::ERR_UNAVAILABLE)
                );
        }
    );
}
} // namespace

void registerMatchRoutes(
    MessageRegistry& routes,
    std::shared_ptr<service::lobby::OnlineRegistry> sessions,
    std::shared_ptr<service::lobby::MatchmakingService> matchmaking
)
{
    if (!sessions || !matchmaking)
        throw std::invalid_argument(
            "match routes require online and matching authorities"
        );
    registerRoute(
        routes,
        arena::protocol::MSG_MATCH_JOIN_REQUEST,
        Operation::Join,
        sessions,
        matchmaking
    );
    registerRoute(
        routes,
        arena::protocol::MSG_MATCH_CANCEL_REQUEST,
        Operation::Cancel,
        sessions,
        matchmaking
    );
    registerRoute(
        routes,
        arena::protocol::MSG_MATCH_STATUS_REQUEST,
        Operation::Status,
        sessions,
        matchmaking
    );
}
} // namespace service::gateway
