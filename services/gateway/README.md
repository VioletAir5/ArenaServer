# Gateway

The gateway is the implemented Linux networking service. CMake builds
`arena_gateway` and the `ArenaServer` executable. Source is under `include/net`,
`src/net` and `app`; other service scopes must not be inferred from target-design
documents.

## Current modules

| Module | Responsibility |
| --- | --- |
| `TcpServer`, `AsioTcpServer`, `WebSocketServer` | Listening, accepted descriptor handoff, owner registries and stop completion |
| `AdmissionController` | Shared process-local total/IP admission leases and bounded-cardinality counters |
| `ITransport` | Concrete byte I/O, buffers, partial reads/writes, half-close and physical close |
| `ConnectionHost` | One connection's lifetime, read pause composition, transport pressure and bounded owner mailbox |
| `ConnectionChannel` | Explicit owner-thread interface provided to protocol handlers, separate from concrete transport |
| `FrameProtocol` | Bounded framing and complete-frame packet policy checks |
| `ArenaProtocol` | Envelope decoding, request conversion and reply encoding |
| `RequestDispatcher` | Request admission, authentication requirements, 64 pending requests, deadlines and once-only completion |
| `MessageRegistry` | A sealed route table shared by connection owners |
| `OnlineRegistry` | Stable logical sessions, current auth leases, presence and bounded room rosters in one synchronized authority |
| `OnlineRegistryMaintenance` | Management-owner sweeps and shutdown, independent of a disconnected transport |
| `MatchmakingService` | Bounded cross-thread commands, current-session fencing and management-owner matching/expiry |
| `MatchHandler` | Authenticated join/cancel/status wire adapters; MMR comes from the server |
| `RoomService` | Bounded tick-cutoff mailboxes and fixed-owner 20 Hz room actors |
| `RoomHandler` | Authenticated input/snapshot adapters with actor and membership fences |

The input path is transport -> Host -> per-connection protocol -> packet
policies -> Envelope -> Dispatcher -> registered handler. The reply path runs
through protocol encoding and the same channel back to the transport. There
are no RTTI capability casts between these modules.

`ConnectionContext` retains immutable connection ID, trusted socket peer and
steady-clock acceptance time, plus owner-thread `ConnectionSession`
authentication state and shared rejection counters. That authentication state
is distinct from the domain's recoverable `Session` model. The old unused
network `Session`/`SessionManager` and generic `PacketHandler` chain are removed.

## Application profiles

```bash
ArenaServer epoll raw
ArenaServer asio packet
ArenaServer asio handshake http://localhost:3000
```

`raw` echoes bytes. `packet` echoes binary request frames. `handshake` uses the
Arena request protocol and remains available in main with the Asio backend.
TCP listens on `127.0.0.1:8888`; handshake additionally listens on
`127.0.0.1:8889/ws`. Both carry the existing PacketCodec binary frame. WebSocket
requires binary messages and the exact configured Origin (default
`http://localhost:3000`). Missing/different Origin is rejected before upgrade.

The default route table handles Ping and returns unavailable for Login. With
`ARENA_JWT_KEY`, main installs JWT/account-session login routes. Password
account/digest adapters in main remain unavailable; this is not a configured
password database or KDF. Online/session authority is process-local.

## Online sessions and reconnect

The authenticated registry exposes SessionState (20/21), RoomJoin (30/31) and
RoomLeave (32/33) request/response routes. A nonzero room ID creates a bounded
gateway roster on first join. This is the current membership store; battle
lifecycle, invitations and game admission rules remain planned.
The defaults are 100000 retained players, 10000 rooms and 64 members per room.
Room moves, leaving, session replacement and expiry update both indexes in
one mutex transaction; snapshots own their data and include Online/Detached
member presence. The owner-affine domain Session/Room models are not shared
across worker threads.

LoginResponse adds `session_id`, `room_id` (zero if unbound) and `resumed`.
The logical SessionId stays stable across authorized JWT recovery; the
connection ID, epoch and token change. Password login starts a fresh logical
session and removes previous room membership. Existing JWT LoginRequest
(`credential_kind=CREDENTIAL_JWT`, empty account) is the reconnect entry.
Its signed token must still match the current registry epoch/expiry.

The first matching disconnect changes presence to Detached and keeps the
logical session and room slot for grace. It retains no Host or socket.
Repeated or old disconnects cannot extend grace or remove a replacement.
Recovery at or after the monotonic deadline is rejected, even before the
next maintenance tick. A management timer sweeps every 100 ms without login
traffic, removing expired sessions/members and pruning empty rooms. JWT expiry
also reclaims records. Application shutdown stops maintenance before draining
owners, then clears registry state after their retirement. Reconnect restores
session/room state; it does not replay requests cancelled by the old connection.

Handshake packet policy is a 200-packet burst, replenished at 100 packets/s,
and an outer request-type allowlist `{1}`. It applies once per complete frame
before protobuf decoding. Malformed framing/payload closes the connection;
typed request errors are encoded as replies.

## Match tickets

With the authenticated route table, MatchJoin (40/41), MatchCancel (42/43)
and MatchStatus (44/45) use the process-local `arena_match` queue. Join takes
a nonzero client join key, returns promptly, and can be retried after an
uncertain RPC outcome. Cancel/status target a ticket ID and are restricted to
the authenticated logical session. A connection request deadline cancels reply
delivery; an already admitted business command may still commit. Retry the
same join key to recover its outcome. No client-supplied rating is accepted.

Matching runs every 100 ms on the management owner, separately from connection
owners. Defaults: 1v1, window 50 +25 per second up to 500, both windows must
accept, timeout 30 seconds. Detached sessions pause pairing while their original
waiting time/deadline continue. JWT recovery retains the ticket; fresh password
sessions cannot steal it. Expired/replaced/room-bound sessions cancel waiting
tickets on upkeep. Authorization is rechecked when each deferred command runs.

Main currently supplies a provisional rating of 1500 for every player; a real
rating store plugs into `RatingProvider`. This produces an assignment with
opponent/ticket identities, not a battle room. Assigned players remain fenced
until a trusted allocation owner calls `release`; that future integration has
no public route. See [match queue contract](../match/README.md).

The service mailbox admits at most 1024 pending commands. Capacity/history
limits fail closed. Shutdown closes command admission, completes accepted
pending commands with unavailable, cancels the timer, and destroys domain
tickets on the management owner before the executor retires. `event=match_tick`
reports changed counters; final `MATCH_COUNTS` includes disjoint wait/gap
histograms and cumulative sums, with no per-player metric labels.

## Capacity and shutdown

Authenticated RoomSnapshot (52/53) lazily initializes an `arena_room` actor for
an existing roster. RoomInput (50/51) carries its incarnation, membership epoch,
contiguous sequence and integer controls. Actors run on the management owner
at an absolute 50 ms cadence. Connection workers enqueue bounded value commands;
only a tick cutoff changes room state. JWT recovery preserves member sequence;
leaving and rejoining increments a separate membership fence. Every committed
tick has a canonical checksum and a bounded replay record. See the
[room runtime contract](../room/README.md) for input ordering, catch-up,
reclamation, replay schema and the reference integer update rule.

All rooms currently share one management owner. This implements room execution
in process; match-to-room allocation and Battle simulation remain planned.
Shutdown stops room ticking and command admission before connection drain,
then prints final `ROOM_COUNTS` with zero actors after owner reclamation.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `ARENA_MAX_CONNECTIONS` | `1000` | Shared total TCP/WS admitted connections |
| `ARENA_MAX_CONNECTIONS_PER_IP` | `100` | Shared reservations for a real peer IPv4, excluding source port |
| `ARENA_TCP_PORT` | `8888` | TCP port, 1..65535 |
| `ARENA_WS_PORT` | `8889` | WebSocket port, 1..65535 |
| `ARENA_DISCONNECT_GRACE_MS` | `30000` | Detached session retention, 1..3600000 milliseconds |
| `ARENA_MATCH_MAX_QUEUED` | `1024` | Maximum waiting players in the process-local queue |
| `ARENA_MATCH_MAX_TICKETS` | `4096` | Waiting, pinned assignment and retry-history records; at least max queued |
| `ARENA_MATCH_TIMEOUT_MS` | `30000` | Queue deadline, 1..3600000 milliseconds |

Limits must be positive representable integers. Listener constructors still
accept an optional shared controller; omitting it creates a listener-local
controller with the old max/max limits. Application assembly explicitly shares
one controller. Reservations include pending worker handoff and incomplete WS
upgrades, and retire after setup failure or connection cleanup. Shutdown prints
active/peak/global/IP/local admission counters without per-peer metric labels.

SIGINT/SIGTERM initiate owner-thread shutdown. Host first quiesces input,
including later frames in a coalesced batch. ArenaProtocol waits for accepted
requests to complete or time out, enqueues their replies, then allows transport
write shutdown. TCP drains queued bytes, half-closes and waits for peer EOF.
WS drains queued binary messages before normal Close. The server grace deadline
bounds the whole sequence and forces retirement if necessary. Native reads
continue during drain to observe EOF; application bytes are discarded. An
immediate/policy close may discard pending output and requests. Stop completion
and owner I/O retirement precede executor destruction.

Gateway frame bodies are limited to 256 KiB; transport output has a 1 MiB hard
limit. Host backpressure pauses at the high watermark and resumes only after
all read pause reasons clear. Hard transport bounds remain active regardless
of optional policies.

Successful maintenance reclamation logs `event=online_sweep` with active,
retained and room counts. These are process-local counts, without token or
credential values.

## Extending a protocol or route

`ConnectionHandlerFactory` runs once per accepted connection on its owner and
returns fresh `ConnectionHandler` state: `onMessage`, optional `onStarted`,
`onDraining` and `onClosed`. A drain hook calls its supplied owner-thread
completion once accepted protocol work has finished; protocols without that
hook proceed directly to transport drain. Input spans are borrowed for the callback. Lifecycle hooks are
retired once, after active callbacks unwind. Register transport policies before
Host start, and packet policies before framing begins.

```cpp
PacketPolicyFactory policies = [](PacketPolicyChain& chain,
                                 const ConnectionContext&) {
    chain.add<PacketRatePolicy>(200.0, 100.0);
    chain.add<PacketTypePolicy>(std::vector<std::uint16_t>{1});
};
ConnectionHandlerFactory handlers = [routes, policies](auto context) {
    return makeArenaProtocolHandler(std::move(context), routes, policies);
};
```

Register messages with `MessageRegistry::registerHandler` and then `seal()`.
Handlers run synchronously on the connection owner; a slow handler must submit
its own work to an appropriate executor. `RequestMessage` owns its Envelope.
Completion may run on any thread and posts through a weak, bounded owner
mailbox. Repeated, stale-generation and closed-connection results have no
effect. If the mailbox's 256 outstanding-action capacity is full, a reply
cannot enqueue and the existing request deadline remains responsible for its
terminal response. The count bound does not constrain arbitrary allocations
made by business code.

`ConnectionChannel` operations require the owner except immutable `id()` and
thread-safe `poster()`. Retain `poster()` for foreign work, never a raw
channel reference. Concrete transport cross-thread send capabilities do not
change this protocol-facing contract. Final Host/transport release occurs on
owner; shared pointers alone do not make connection methods thread-safe.

Protocols and connection policies share this one flat `ConnectionChannel`
interface. Read control uses `setReadPaused(reason, paused)`; immediate logical
closure uses `close()`. Timer callbacks receive the same channel on its owner,
so request deadlines finish directly without posting again. The host owns its
context and protocol callbacks, including through closure and callback unwind;
handler factories return their callbacks directly.

Concrete TCP byte operations use the `ITransport` names and byte spans:
`send`, `shutdown`, `close`, `pauseRead`, `resumeRead`, `pendingBytes` and
`connected`. Shutdown still drains output and half-closes; close still notifies
owner cleanup exactly once. The low-level `Socket::shutdownWrite` remains the
actual write-half shutdown operation.

## Verification

`cmake --build build -j4` builds all migrated targets. `ctest --preset gcc15`
runs the normal matrix excluding performance suites. Gateway tests include
transport lifecycle, all real TCP/WS policy/request profiles, shared admission,
factory exception isolation, foreign replies, stale posters and bounded queues.
`ApplicationLifecycleIntegrationTest.py` exercises real executable configuration
and signals. ASan/UBSan are separate build checks; two legacy epoll FD-exhaustion
cases can conflict with the installed UBSan runtime's own pipe allocation and
must be reported separately from a passing non-UBSan verification.

Multi-instance session authority, load-balancer readiness integration, a
connection login deadline and distributed business idempotency remain planned.
This architecture supports their placement but does not claim deployed high
availability or automatic recovery of in-flight business operations.
