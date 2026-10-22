# Gateway request protocol

Current implementation: 2026-10-22. TCP and binary WebSocket use the same
PacketCodec frame and Envelope. The application entry is
`ArenaServer asio handshake [ws-origin]`. Without authentication configuration,
the default routes provide Ping and an unavailable Login. `ARENA_JWT_KEY` selects
the authentication and online registry assembly. Production password-account
and KDF adapters remain unavailable in main; fixed test accounts are test-only.

## Packet and Envelope

The Packet header is 15 bytes, with multibyte integers in network byte order:
magic4=`0x4152454E`, body length4, type2 (request1/response2), request ID4 and
version1=`1`. The body length includes seven bytes of metadata and the protobuf
payload, with a 256KiB body limit. TCP reads and WS messages do not replace Packet
framing. Invalid magic, length, version or protobuf closes the connection.

Envelope retains `request_id=1`, reserved `2/legacy_client_tag`,
`sent_at_unix_ms=3` and `msg_id=4`. A nonzero Envelope request ID takes precedence
over the Packet ID; both zero returns 1002. Responses preserve the Packet ID and
use the selected logical ID in Envelope. With `msg_id=0`, the server infers a
known payload and leaves the response msg_id zero. Explicit requests receive an
explicit response ID. Unknown explicit IDs return 1001; known ID/payload mismatch
returns 1002. Existing fields and retired tags must never be renumbered or reused.

## Message table

MsgId equals the corresponding Envelope oneof tag.

| Request / response | Payload | Authorization | Behavior |
| --- | --- | --- | --- |
| 10 / 11 | LoginRequest / LoginResponse | Pending connection | Fresh password login or JWT recovery |
| 12 / 13 | Ping / Pong | Public; revoked bindings still rejected | Echo nonce |
| — / 14 | Error | — | Stable typed error |
| 20 / 21 | SessionStateRequest / SessionStateResponse | Current authenticated binding | Read own session and room roster |
| 30 / 31 | RoomJoinRequest / RoomJoinResponse | Current authenticated binding | Atomically join or move rooms |
| 32 / 33 | RoomLeaveRequest / RoomLeaveResponse | Current authenticated binding | Leave room and retain session |
| 40 / 41 | MatchJoinRequest / MatchJoinResponse | Current authenticated binding | Admit or replay one matching ticket |
| 42 / 43 | MatchCancelRequest / MatchCancelResponse | Current authenticated binding | Cancel the caller's waiting ticket |
| 44 / 45 | MatchStatusRequest / MatchStatusResponse | Current authenticated binding | Query a caller-owned ticket |

`makeLoginMessageRegistry` registers 20/30/32 and optionally 40/42/44 when supplied
a MatchmakingService. The default unauthenticated registry omits these routes.
Response IDs cannot register as requests. All built-in requests support explicit
and inferred IDs. Registries are sealed before publication; each connection has
at most 64 pending requests and routes default to a five-second reply deadline.

## Login and recovery

LoginRequest fields are `account1`, `credential2(bytes)`, `client_version3` and
`credential_kind4`: password0 or JWT1. JWT login requires an empty account;
verified JWT subject establishes PlayerId.

LoginResponse retains `accepted1`, `session_token2`, `error3`, `player_id4` and
`session_epoch5`, and adds `session_id6`, `room_id7` and `resumed8`. Room0 means no
membership. Password login returns a fresh logical SessionId and resumed=false;
JWT recovery preserves that logical identity and returns resumed=true. Epoch is
the current transport authorization fence. JWT's exact seven claims and
signature/issuer/audience/expiry validation remain unchanged.

After disconnect, presence becomes Detached while the logical session and room
slot remain during grace. A new connection may recover with the old token before
the exclusive grace deadline. Recovery rotates epoch/token; the old connection
and token lose authority. At or after the deadline, even an otherwise valid JWT
cannot recover the removed record. A new password login creates a fresh session
and removes the old room membership. Recovery does not replay cancelled RPCs;
business operations need their own retry contract, such as the matching join key.

## Presence and room roster

Presence is UNSPECIFIED0, ONLINE1 or DETACHED2. SessionStateRequest is empty.
SessionStateResponse contains:

| Field | Tag | Meaning |
| --- | ---: | --- |
| session_id | 1 | Stable logical identity |
| player_id | 2 | Authenticated account identity |
| room_id | 3 | Current room, zero outside a room |
| presence | 4 | ONLINE for a successful current-connection query |
| members | 5 | Repeated RoomMember from one registry transaction |

RoomMember is `player_id1/session_id2/presence3`. Members may be Online or
Detached; no room produces an empty roster. This API cannot enumerate unrelated
players. RoomJoinRequest requires nonzero `room_id1`. Join/Leave responses wrap
SessionStateResponse in `state1`; RoomLeaveRequest is empty. Repeated leave and
joining the same room are idempotent.

The default registry retains at most 100000 players, 10000 rooms and 64 members per
room. A nonzero ID can create a bounded gateway roster. This is not a battle
RoomActor, invitation system or access-control policy. Move, leave, replacement
and expiry update both indexes atomically; failure preserves existing membership.
Detached players retain their slots until grace expires; empty rooms are removed.

`ARENA_DISCONNECT_GRACE_MS` defaults to 30000 and accepts 1..3600000ms. The first
exact-binding disconnect establishes a steady-clock deadline; repeated cleanup
cannot extend it. An independent management timer sweeps every 100ms by default.
Owner scheduling or lock contention may delay physical removal, but checks still
reject recovery when now>=deadline. A stale connection/epoch cannot detach its
replacement. Normal player/room paths prune only their target records; full scans
are reserved for upkeep, monitoring and capacity-pressure reclamation.

## Matchmaking

MatchJoinRequest contains only nonzero `join_key1`; MatchCancelRequest and
MatchStatusRequest contain only nonzero `ticket_id1`. Player/session identity
comes from the current authenticated binding. MMR comes from a trusted server
RatingProvider (the application uses provisional 1500 until a rating adapter is
provided); clients cannot choose MMR or a different player's ticket.

All three responses wrap MatchTicket in `state1`:

| Field | Tag | Meaning |
| --- | ---: | --- |
| player_id | 1 | Ticket owner |
| ticket_id | 2 | Stable nonzero ticket identity |
| join_key | 3 | Client operation key scoped to the authenticated player |
| state | 4 | MatchTicketState below |
| mmr | 5 | Server rating captured for this ticket |
| window | 6 | Current inclusive MMR difference allowed by this ticket |
| waited_ms | 7 | Elapsed wait from first admission |
| remaining_ms | 8 | Time remaining before the queue deadline |
| match_id | 9 | Shared pair identity, zero before assignment |
| opponent_player_id | 10 | Opponent identity, zero before assignment |
| opponent_ticket_id | 11 | Opponent ticket, zero before assignment |
| active | 12 | Ticket still prevents another active admission |

MatchTicketState is UNSPECIFIED0, QUEUED1, RESERVED2, ASSIGNED3, CANCELLED4,
EXPIRED5 or FAILED6. No logical SessionId is exposed in this payload. Detached
does not introduce a ticket state: a queued ticket remains queued and pauses
pairing eligibility. JWT recovery into the same SessionId restores eligibility
without restarting its wait or deadline. Replacement, expiry or room membership
makes a waiting logical session ineligible and removes it from matching.

Join acknowledges queue admission rather than waiting for a pair. Poll status
using the returned ticket ID. Cancel is ticket-specific and terminal retries are
safe; an assigned ticket cannot be cancelled through this endpoint. Assignment
exposes a pair only: it does not allocate a gateway room or battle actor. The
allocation owner explicitly releases the assignment fence through the internal
service API. There is no client release endpoint.

The queue is deterministic 1v1. Defaults: initial window 50, expansion 25 each full
1000ms, maximum window 500, queue timeout 30000ms, retained retry history 60000ms,
maxQueued 1024, total retained maxTickets 4096 and 8192 comparisons per tick. Both
participants must accept the MMR gap. Periodic matching uses the management owner
and does not create a dedicated thread. Queue limits and the service command
mailbox are separate from per-connection pending limits.

**Uncertain outcomes:** an RPC timeout or disconnect stops reply delivery, not
necessarily an already admitted matchmaking command. Retry the same join key
after recovery to obtain the original ticket. A different key cannot bypass the
single-active-ticket constraint. Retry history is bounded, not durable or
permanent. Never assume that a missing
reply means no ticket was created. Replays and recovery do not extend a ticket's
original queue deadline. Cancel/status must use the caller's logical session.
A new password session cannot query the old session's tickets. A prior waiting
ticket is invalidated when eligibility is next refreshed; an assigned ticket
keeps its player fence until the allocation owner explicitly releases the pair.

The connection owner copies the current binding and logical SessionId into an
owned command. The management owner rechecks exact connection/epoch/session
before mutation. Completion returns through the connection's bounded poster;
the service retains neither transport pointers nor owner-only ConnectionSession.
Stopping the service rejects new commands and fails pending commands before
server drain and management executor retirement.

## Errors and request lifetime

Error retains `code1(uint32)`, `message2(string)` and `causes3(repeated Error)`.

| Code | Fixed message | Typical condition |
| ---: | --- | --- |
| 1001 | unknown message | Unregistered message or response sent as a request |
| 1002 | invalid request | Zero ID/key/ticket/room, known payload mismatch |
| 1003 | authentication required | Missing, stale or expired authority |
| 1004 | request timed out | Deadline or actual completion time exceeded |
| 1005 | too many requests | Per-connection 64 pending limit |
| 1006 | duplicate request id | End the existing active invocation once |
| 2001 | business failure | Full room, ticket conflict/not found/already assigned, room-bound match join |
| 2002 | service unavailable | Missing adapter, capacity, stopped/full matchmaking mailbox |
| 9000 | internal error | Unexpected exception, wrong reply type or encoding budget failure |

Clients branch on code, not message. Exception text, credentials and tokens stay
out of replies and logs. Success payloads must match the request direction; the
protocol fills correlation fields. Typed errors normally keep the connection
open; framing, policy, output hard bounds or irrecoverable encoding can close it.

RequestDispatcher owns pending generations/deadlines; ArenaProtocol owns wire
conversion. RequestMessage owns Envelope/context. Foreign completion travels
through a mailbox of at most 256 outstanding actions; saturation leaves the
original deadline to finish the request. Late, duplicate and post-close replies
are ignored. A timer cannot preempt a blocked handler. Shutdown stops fresh input,
waits for admitted requests/deadlines, then drains output with a server deadline.

Request logs retain request_start/request_handler/request_end with connection,
request, message, code and outcome; reply means submission to output, not peer
receipt. session_install records identity/epoch/resumed without tokens.
online_sweep records active/retained/rooms only when reclamation occurs.

## Verification entry points

- `tests/auth/OnlineRegistryTest.cpp`: clocks, fences, indexes, limits and cleanup.
- `tests/auth/OnlineRegistryMaintenanceTest.cpp`: independent upkeep and teardown.
- `tests/auth/OnlineProtocolTest.cpp`: production authentication/presence routes.
- `tests/auth/OnlinePresenceIntegrationTest.py`: real recovery and idle expiry.
- `tests/auth/AccountLoginIntegrationTest.py`: concurrent login and old authority.
- `tests/shared/protocol/EnvelopeCodecTest.cpp`: frozen schema compatibility,
  additive matchmaking tags and forwarding through old protobuf peers.
- `tests/match/`: queue/service/route and transport matching acceptance tests.
