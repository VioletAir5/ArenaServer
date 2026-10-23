/** @file
 * @brief Converts owner-authorized room requests into tick mailbox
 * commands.
 */
#include "net/RoomHandler.h"

#include <stdexcept>
#include <utility>

#include "net/RoomService.h"

namespace service::gateway
{
namespace
{
/** @brief Copies a committed canonical tick without retaining actor
 * state. */
void encodeSnapshot(
    const service::room::Snapshot& snapshot,
    std::int32_t positionLimit,
    arena::protocol::RoomSnapshot& response
)
{
    response.set_room_id(snapshot.roomId.value());
    response.set_incarnation(snapshot.incarnation);
    response.set_tick(snapshot.tick);
    response.set_checksum(snapshot.checksum);
    response.set_checksum_version(
        service::room::RoomActor::checksumVersion
    );
    response.set_position_limit(positionLimit);
    for (const auto& player : snapshot.players)
    {
        auto* member = response.add_players();
        member->set_player_id(player.member.playerId.value());
        member->set_session_id(player.member.sessionId.value());
        member->set_member_epoch(player.member.epoch);
        member->set_x(player.x);
        member->set_y(player.y);
        member->set_last_seq(player.sequence);
        member->set_last_dx(player.lastDx);
        member->set_last_dy(player.lastDy);
    }
}

arena::protocol::RoomInputStatus
wireStatus(service::room::InputStatus status)
{
    using Status = service::room::InputStatus;
    switch (status)
    {
    case Status::Applied:
        return arena::protocol::ROOM_INPUT_APPLIED;
    case Status::Duplicate:
        return arena::protocol::ROOM_INPUT_DUPLICATE;
    case Status::Gap:
        return arena::protocol::ROOM_INPUT_GAP;
    case Status::Stale:
        return arena::protocol::ROOM_INPUT_STALE;
    case Status::Conflict:
        return arena::protocol::ROOM_INPUT_CONFLICT;
    case Status::NotMember:
        return arena::protocol::ROOM_INPUT_NOT_MEMBER;
    case Status::Invalid:
        return arena::protocol::ROOM_INPUT_INVALID;
    }
    return arena::protocol::ROOM_INPUT_UNSPECIFIED;
}

void registerRoute(
    MessageRegistry& routes,
    bool input,
    const std::shared_ptr<OnlineRegistry>& sessions,
    const std::shared_ptr<RoomService>& rooms
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
                    [complete,
                     positionLimit](RoomService::InputResult result)
                    {
                        if (!result)
                            return complete(
                                std::unexpected(result.error())
                            );
                        arena::protocol::Envelope reply;
                        auto* response =
                            reply.mutable_room_input_response();
                        encodeSnapshot(
                            result->snapshot,
                            positionLimit,
                            *response->mutable_state()
                        );
                        response->set_result(
                            wireStatus(result->status)
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
                        RoomService::SnapshotResult result
                    )
                    {
                        if (!result)
                            return complete(
                                std::unexpected(result.error())
                            );
                        arena::protocol::Envelope reply;
                        encodeSnapshot(
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
} // namespace

void registerRoomRoutes(
    MessageRegistry& routes,
    std::shared_ptr<OnlineRegistry> sessions,
    std::shared_ptr<RoomService> rooms
)
{
    if (!sessions || !rooms)
        throw std::invalid_argument(
            "room routes require online and room authorities"
        );
    registerRoute(routes, true, sessions, rooms);
    registerRoute(routes, false, sessions, rooms);
}
} // namespace service::gateway
