/** @file
 * @brief Projects committed business values into the unchanged public
 * protocol.
 */
#pragma once
#include "envelope.pb.h"
#include "lobby/OnlineRegistry.h"
#include "lobby/RoomService.h"
#include "match/MatchQueue.h"
#include "room.pb.h"
#include "room/RoomRuntime.h"
namespace service::lobby
{
void encodeState(
    const OnlineState&, arena::protocol::SessionStateResponse&
);
void encodeTicket(
    const service::match::TicketSnapshot&,
    arena::protocol::MatchTicket&
);
void encodeSnapshot(
    const service::room::Snapshot&,
    std::int32_t,
    arena::protocol::RoomSnapshot&
);
void encodeRoomStatus(
    const RoomStatus&, std::int32_t, arena::protocol::RoomStatus&
);
void encodeSettlement(
    const RoomSettlement&, arena::protocol::SettlementResult&
);
/** @brief Projects immutable worker state without constructing a room
 * kernel. */
void encodeRoomStatus(
    const service::room::RuntimeImage&, arena::protocol::RoomStatus&
);
/** @brief Projects one complete immutable worker settlement intent.
 */
void encodeSettlement(
    const service::room::RuntimeSettlement&,
    arena::protocol::SettlementResult&
);
/** @brief Builds a request-independent durable public receipt for one
 * command. */
std::expected<arena::protocol::Envelope, arena::protocol::ErrorCode>
encodeRoomReply(
    const service::room::RuntimeCommand&,
    const arena::room::CommandResult&,
    const service::room::RuntimeImage&
);
arena::protocol::RoomInputStatus
    wireStatus(service::room::InputStatus);
arena::protocol::ErrorCode routeError(OnlineError);
arena::protocol::ErrorCode queueError(service::match::QueueError);
} // namespace service::lobby
