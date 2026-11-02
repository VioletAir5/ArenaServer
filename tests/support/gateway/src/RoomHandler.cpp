/** @file
 * @brief Converts owner-authorized room requests into tick mailbox
 * commands.
 */
#include "gateway_test/RoomHandler.h"

#include <stdexcept>
#include <utility>

#include "net/MessageRegistry.h"
#include "net/RequestExecution.h"

#include "lobby/Protocol.h"
#include "lobby/RoomService.h"
#include "runtime/OwnerScheduler.h"

namespace service::gateway
{
namespace
{
void registerRoute(
    MessageRegistry& routes,
    bool input,
    const std::shared_ptr<service::lobby::OnlineRegistry>& sessions,
    const std::shared_ptr<service::lobby::RoomService>& rooms
)
{
    routes.registerHandler(
        input ? arena::protocol::MSG_ROOM_INPUT_REQUEST
              : arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST,
        [sessions, rooms, input](auto request, auto complete)
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
            if (!state->session.roomId)
                return complete(
                    std::unexpected(
                        arena::protocol::ERR_BUSINESS_FAILURE
                    )
                );
            // Capture values on the connection owner. The room owner
            // rechecks transport/session/membership before applying
            // the tick batch.
            const auto session = state->session.sessionId;
            const auto room = *state->session.roomId;
            const auto positionLimit = rooms->positionLimit();
            bool admitted;
            if (input)
            {
                const auto& control = request->envelope
                                          .room_input_request();
                admitted = rooms->input(
                    *binding,
                    session,
                    room,
                    control.actor_incarnation(),
                    control.membership_epoch(),
                    control.seq(),
                    control.dx(),
                    control.dy(),
                    [complete, positionLimit](
                        service::lobby::RoomService::InputResult
                            result
                    )
                    {
                        if (!result)
                            return complete(
                                std::unexpected(result.error())
                            );
                        arena::protocol::Envelope reply;
                        auto* response =
                            reply.mutable_room_input_response();
                        service::lobby::encodeSnapshot(
                            result->snapshot,
                            positionLimit,
                            *response->mutable_state()
                        );
                        response->set_result(
                            service::lobby::wireStatus(result->status)
                        );
                        complete(std::move(reply));
                    }
                );
            }
            else
                admitted = rooms->snapshot(
                    *binding,
                    session,
                    room,
                    [complete, positionLimit](
                        service::lobby::RoomService::SnapshotResult
                            result
                    )
                    {
                        if (!result)
                            return complete(
                                std::unexpected(result.error())
                            );
                        arena::protocol::Envelope reply;
                        service::lobby::encodeSnapshot(
                            *result,
                            positionLimit,
                            *reply.mutable_room_snapshot_response()
                                 ->mutable_state()
                        );
                        complete(std::move(reply));
                    }
                );
            if (!admitted)
                complete(
                    std::unexpected(arena::protocol::ERR_UNAVAILABLE)
                );
        }
    );
}

enum class LifecycleOperation
{
    Ready,
    Status,
    Settlement
};

void registerLifecycleRoute(
    MessageRegistry& routes,
    MsgId id,
    LifecycleOperation operation,
    const std::shared_ptr<service::lobby::OnlineRegistry>& sessions,
    const std::shared_ptr<service::lobby::RoomService>& rooms,
    const DurableSettlementLookup& lookup
)
{
    routes.registerHandler(
        id,
        [sessions,
         rooms,
         operation,
         lookup](auto request, auto complete)
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
            const auto session = state->session.sessionId;
            bool admitted = false;
            if (operation == LifecycleOperation::Settlement)
            {
                const auto match = request->envelope
                                       .settlement_request()
                                       .match_id();
                if (!match)
                    return complete(
                        std::unexpected(
                            arena::protocol::ERR_INVALID_REQUEST
                        )
                    );
                // Finished results remain session-scoped after
                // registry room release.
                admitted = rooms->settlement(
                    *binding,
                    session,
                    match,
                    [complete,
                     lookup,
                     request,
                     player = binding->playerId,
                     match](
                        service::lobby::RoomService::SettlementResult
                            result
                    )
                    {
                        const auto respond =
                            [complete](
                                service::lobby::RoomService::
                                    SettlementResult value
                            )
                        {
                            if (!value)
                                return complete(
                                    std::unexpected(value.error())
                                );
                            arena::protocol::Envelope reply;
                            service::lobby::encodeSettlement(
                                *value,
                                *reply.mutable_settlement_response()
                                     ->mutable_state()
                            );
                            complete(std::move(reply));
                        };
                        if (result ||
                            result.error() !=
                                arena::protocol::
                                    ERR_BUSINESS_FAILURE ||
                            !lookup)
                            return respond(std::move(result));
                        const auto execution = request->execution;
                        if (!execution)
                            return complete(
                                std::unexpected(
                                    arena::protocol::ERR_UNAVAILABLE
                                )
                            );
                        if (arena::runtime::OwnerClock::now() >=
                            execution->deadline())
                            return complete(
                                std::unexpected(
                                    arena::protocol::ERR_TIMEOUT
                                )
                            );
                        // Only values captured on the connection
                        // owner cross this boundary.
                        auto admitted = lookup(
                            player,
                            match,
                            execution->deadline(),
                            execution->correlationId(),
                            respond
                        );
                        if (!admitted)
                            return complete(
                                std::unexpected(admitted.error())
                            );
                        execution->setCancellation(
                            std::move(*admitted)
                        );
                    }
                );
            }
            else
            {
                auto reply =
                    [complete,
                     operation,
                     positionLimit = rooms->positionLimit()](
                        service::lobby::RoomService::StatusResult
                            result
                    )
                {
                    if (!result)
                        return complete(
                            std::unexpected(result.error())
                        );
                    arena::protocol::Envelope envelope;
                    auto* status =
                        operation == LifecycleOperation::Ready
                            ? envelope.mutable_room_ready_response()
                                  ->mutable_state()
                            : envelope.mutable_room_status_response()
                                  ->mutable_state();
                    service::lobby::encodeRoomStatus(
                        *result,
                        positionLimit,
                        *status
                    );
                    complete(std::move(envelope));
                };
                if (operation == LifecycleOperation::Ready)
                {
                    const auto& ready = request->envelope
                                            .room_ready_request();
                    if (!ready.actor_incarnation() ||
                        !ready.membership_epoch())
                        return complete(
                            std::unexpected(
                                arena::protocol::ERR_INVALID_REQUEST
                            )
                        );
                    if (!state->session.roomId)
                        return complete(
                            std::unexpected(
                                arena::protocol::ERR_BUSINESS_FAILURE
                            )
                        );
                    admitted = rooms->ready(
                        *binding,
                        session,
                        *state->session.roomId,
                        ready.actor_incarnation(),
                        ready.membership_epoch(),
                        ready.ready(),
                        std::move(reply)
                    );
                }
                else
                    admitted = rooms->status(
                        *binding,
                        session,
                        std::move(reply)
                    );
            }
            if (!admitted)
                complete(
                    std::unexpected(arena::protocol::ERR_UNAVAILABLE)
                );
        }
    );
}
} // namespace

void registerRoomRoutes(
    MessageRegistry& routes,
    std::shared_ptr<service::lobby::OnlineRegistry> sessions,
    std::shared_ptr<service::lobby::RoomService> rooms,
    DurableSettlementLookup lookup
)
{
    if (!sessions || !rooms)
        throw std::invalid_argument(
            "room routes require online and room authorities"
        );
    registerRoute(routes, true, sessions, rooms);
    registerRoute(routes, false, sessions, rooms);
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_ROOM_READY_REQUEST,
        LifecycleOperation::Ready,
        sessions,
        rooms,
        lookup
    );
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_ROOM_STATUS_REQUEST,
        LifecycleOperation::Status,
        sessions,
        rooms,
        lookup
    );
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_SETTLEMENT_REQUEST,
        LifecycleOperation::Settlement,
        sessions,
        rooms,
        lookup
    );
}
} // namespace service::gateway
