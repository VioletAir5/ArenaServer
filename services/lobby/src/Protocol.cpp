/** @file
 * @brief Encodes owner-consistent business projections for Gateway
 * replies.
 */
#include "lobby/Protocol.h"
namespace service::lobby
{
arena::protocol::Presence
wirePresence(service::lobby::Presence presence)
{
    return presence == service::lobby::Presence::Online
               ? arena::protocol::PRESENCE_ONLINE
               : arena::protocol::PRESENCE_DETACHED;
}

void encodeState(
    const service::lobby::OnlineState& state,
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

arena::protocol::ErrorCode
routeError(service::lobby::OnlineError error)
{
    switch (error)
    {
    case service::lobby::OnlineError::NotCurrent:
        return arena::protocol::ERR_UNAUTHENTICATED;
    case service::lobby::OnlineError::InvalidRoom:
        return arena::protocol::ERR_INVALID_REQUEST;
    case service::lobby::OnlineError::RoomFull:
        return arena::protocol::ERR_BUSINESS_FAILURE;
    case service::lobby::OnlineError::Capacity:
        return arena::protocol::ERR_UNAVAILABLE;
    }
    return arena::protocol::ERR_INTERNAL;
}

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

arena::protocol::RoomPhase wirePhase(service::lobby::RoomPhase phase)
{
    switch (phase)
    {
    case service::lobby::RoomPhase::Forming:
        return arena::protocol::ROOM_PHASE_FORMING;
    case service::lobby::RoomPhase::Ready:
        return arena::protocol::ROOM_PHASE_READY;
    case service::lobby::RoomPhase::Playing:
        return arena::protocol::ROOM_PHASE_PLAYING;
    case service::lobby::RoomPhase::Settling:
        return arena::protocol::ROOM_PHASE_SETTLING;
    case service::lobby::RoomPhase::Finished:
        return arena::protocol::ROOM_PHASE_FINISHED;
    }
    return arena::protocol::ROOM_PHASE_UNSPECIFIED;
}

arena::protocol::SettlementReason
wireReason(service::lobby::SettlementReason reason)
{
    switch (reason)
    {
    case service::lobby::SettlementReason::Completed:
        return arena::protocol::SETTLEMENT_REASON_COMPLETED;
    case service::lobby::SettlementReason::MemberUnavailable:
        return arena::protocol::SETTLEMENT_REASON_MEMBER_UNAVAILABLE;
    case service::lobby::SettlementReason::ReadyTimeout:
        return arena::protocol::SETTLEMENT_REASON_READY_TIMEOUT;
    }
    return arena::protocol::SETTLEMENT_REASON_UNSPECIFIED;
}

void encodeSettlement(
    const service::lobby::RoomSettlement& result,
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

void encodeRoomStatus(
    const service::lobby::RoomStatus& status,
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
            member.presence == service::lobby::Presence::Online
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

arena::protocol::ErrorCode
queueError(service::match::QueueError error)
{
    using service::match::QueueError;
    switch (error)
    {
    case QueueError::InvalidRequest:
        return arena::protocol::ERR_INVALID_REQUEST;
    case QueueError::Capacity:
        return arena::protocol::ERR_UNAVAILABLE;
    case QueueError::Conflict:
    case QueueError::NotFound:
    case QueueError::AlreadyAssigned:
        return arena::protocol::ERR_BUSINESS_FAILURE;
    }
    return arena::protocol::ERR_INTERNAL;
}
} // namespace service::lobby
