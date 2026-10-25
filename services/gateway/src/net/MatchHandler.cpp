/** @file
 * @brief Converts authenticated owner requests to deferred
 * matchmaking commands.
 */
#include "net/MatchHandler.h"

#include <stdexcept>
#include <utility>

#include "net/MatchmakingService.h"

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

arena::protocol::MatchTicketState
wireState(service::match::TicketState state)
{
    using State = service::match::TicketState;
    switch (state)
    {
    case State::Queued:
        return arena::protocol::MATCH_TICKET_QUEUED;
    case State::Reserved:
        return arena::protocol::MATCH_TICKET_RESERVED;
    case State::Assigned:
        return arena::protocol::MATCH_TICKET_ASSIGNED;
    case State::Cancelled:
        return arena::protocol::MATCH_TICKET_CANCELLED;
    case State::Expired:
        return arena::protocol::MATCH_TICKET_EXPIRED;
    case State::Failed:
        return arena::protocol::MATCH_TICKET_FAILED;
    }
    return arena::protocol::MATCH_TICKET_UNSPECIFIED;
}

/** @brief Copies only public ticket values; logical session IDs stay
 * private. */
void encodeTicket(
    const service::match::TicketSnapshot& snapshot,
    arena::protocol::MatchTicket& ticket
)
{
    ticket.set_player_id(snapshot.playerId.value());
    ticket.set_ticket_id(snapshot.ticketId.value());
    ticket.set_join_key(snapshot.joinKey);
    ticket.set_state(wireState(snapshot.state));
    ticket.set_mmr(snapshot.mmr);
    ticket.set_window(snapshot.window);
    ticket.set_waited_ms(snapshot.waitedMs);
    ticket.set_remaining_ms(snapshot.remainingMs);
    ticket.set_match_id(snapshot.matchId);
    ticket.set_opponent_player_id(
        snapshot.opponent ? snapshot.opponent->value() : 0
    );
    ticket.set_opponent_ticket_id(
        snapshot.opponentTicket ? snapshot.opponentTicket->value() : 0
    );
    ticket.set_active(snapshot.active);
    ticket.set_room_id(
        snapshot.roomId ? snapshot.roomId->value() : 0
    );
}

void registerRoute(
    MessageRegistry& routes,
    MsgId id,
    Operation operation,
    const std::shared_ptr<OnlineRegistry>& sessions,
    const std::shared_ptr<MatchmakingService>& matchmaking
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
                [complete,
                 operation](MatchmakingService::Result result)
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
                encodeTicket(*result, *ticket);
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
    std::shared_ptr<OnlineRegistry> sessions,
    std::shared_ptr<MatchmakingService> matchmaking
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
