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
/** @brief Copies deterministic player state shared by snapshots and
 * results. */
void encodePlayer(
    const service::room::PlayerState& player,
    arena::protocol::RoomPlayerState& member
)
{
    member.set_player_id(player.member.playerId.value());
    member.set_session_id(player.member.sessionId.value());
    member.set_member_epoch(player.member.epoch);
    member.set_x(player.x);
    member.set_y(player.y);
    member.set_last_seq(player.sequence);
    member.set_last_dx(player.lastDx);
    member.set_last_dy(player.lastDy);
}

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
        encodePlayer(player, *response.add_players());
}

arena::protocol::RoomPhase wirePhase(RoomPhase phase)
{
    switch (phase)
    {
    case RoomPhase::Forming:
        return arena::protocol::ROOM_PHASE_FORMING;
    case RoomPhase::Ready:
        return arena::protocol::ROOM_PHASE_READY;
    case RoomPhase::Playing:
        return arena::protocol::ROOM_PHASE_PLAYING;
    case RoomPhase::Settling:
        return arena::protocol::ROOM_PHASE_SETTLING;
    case RoomPhase::Finished:
        return arena::protocol::ROOM_PHASE_FINISHED;
    }
    return arena::protocol::ROOM_PHASE_UNSPECIFIED;
}

arena::protocol::SettlementReason wireReason(SettlementReason reason)
{
    switch (reason)
    {
    case SettlementReason::Completed:
        return arena::protocol::SETTLEMENT_REASON_COMPLETED;
    case SettlementReason::MemberUnavailable:
        return arena::protocol::SETTLEMENT_REASON_MEMBER_UNAVAILABLE;
    case SettlementReason::ReadyTimeout:
        return arena::protocol::SETTLEMENT_REASON_READY_TIMEOUT;
    }
    return arena::protocol::SETTLEMENT_REASON_UNSPECIFIED;
}

void encodeSettlement(
    const RoomSettlement& result,
    arena::protocol::SettlementResult& response
)
{
    response.set_result_id(result.resultId);
    response.set_match_id(result.matchId);
    response.set_room_id(result.roomId.value());
    response.set_final_tick(result.finalTick);
    response.set_final_checksum(result.finalChecksum);
    response.set_reason(wireReason(result.reason));
    response.set_winner_player_id(result.winnerPlayerId);
    for (const auto& player : result.players)
        encodePlayer(player, *response.add_players());
}

/** @brief Encodes one owner-consistent recovery/lifecycle projection.
 */
void encodeRoomStatus(
    const RoomStatus& status,
    std::int32_t positionLimit,
    arena::protocol::RoomStatus& response
)
{
    encodeSnapshot(
        status.snapshot,
        positionLimit,
        *response.mutable_snapshot()
    );
    response.set_match_id(status.matchId);
    response.set_phase(wirePhase(status.phase));
    response.set_started_tick(status.startedTick);
    response.set_finished_tick(status.finishedTick);
    response.set_played_ticks(status.playedTicks);
    response.set_duration_ticks(status.durationTicks);
    for (const auto& member : status.members)
    {
        auto* entry = response.add_members();
        entry->set_player_id(member.member.playerId.value());
        entry->set_session_id(member.member.sessionId.value());
        entry->set_member_epoch(member.member.epoch);
        entry->set_ready(member.ready);
        entry->set_presence(
            member.presence == Presence::Online
                ? arena::protocol::PRESENCE_ONLINE
                : arena::protocol::PRESENCE_DETACHED
        );
    }
    if (status.settlement)
        encodeSettlement(
            *status.settlement,
            *response.mutable_settlement()
        );
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
    const std::shared_ptr<OnlineRegistry>& sessions,
    const std::shared_ptr<RoomService>& rooms
)
{
    routes.registerHandler(
        id,
        [sessions, rooms, operation](auto request, auto complete)
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
                    [complete](RoomService::SettlementResult result)
                    {
                        if (!result)
                            return complete(
                                std::unexpected(result.error())
                            );
                        arena::protocol::Envelope reply;
                        encodeSettlement(
                            *result,
                            *reply.mutable_settlement_response()
                                 ->mutable_state()
                        );
                        complete(std::move(reply));
                    }
                );
            }
            else
            {
                auto reply = [complete,
                              operation,
                              positionLimit = rooms->positionLimit()](
                                 RoomService::StatusResult result
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
                    encodeRoomStatus(*result, positionLimit, *status);
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
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_ROOM_READY_REQUEST,
        LifecycleOperation::Ready,
        sessions,
        rooms
    );
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_ROOM_STATUS_REQUEST,
        LifecycleOperation::Status,
        sessions,
        rooms
    );
    registerLifecycleRoute(
        routes,
        arena::protocol::MSG_SETTLEMENT_REQUEST,
        LifecycleOperation::Settlement,
        sessions,
        rooms
    );
}
} // namespace service::gateway
