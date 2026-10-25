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
| `OnlineRegistry` | Logical sessions, auth leases, presence, manual rosters and atomic two-player managed allocation |
| `OnlineRegistryMaintenance` | Management-owner sweeps and shutdown, independent of a disconnected transport |
| `MatchmakingService` | Bounded commands, session fencing, matching/expiry and trusted owner-side room allocation |
| `MatchHandler` | Authenticated join/cancel/status wire adapters; MMR comes from the server |
| `RoomService` | Tick mailboxes, domain Room/Battle lifecycle, 20 Hz simulation and retained result stubs |
| `RoomHandler` | Authenticated controls, readiness, recovery and result adapters with actor/membership fences |

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
`ARENA_JWT_KEY`, main installs JWT/session, matching and room routes. Adding
`ARENA_ACCOUNTS_FILE` installs the file-backed password account adapter and
OpenSSL PBKDF2 verifier; without it password login remains unavailable. See the
[authentication contract](../auth/README.md) for the account file and KDF limits.
Online/session authority remains process-local.

## Online sessions and reconnect

The authenticated registry exposes SessionState (20/21), RoomJoin (30/31) and
RoomLeave (32/33) request/response routes. A nonzero room ID creates a bounded
manual roster on first join. Match-created rooms use server-managed allocation:
direct RoomJoin cannot enter that namespace or move active participants out;
RoomLeave cannot bypass its lifecycle. Invitations and distributed membership
authority remain planned.
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
traffic, removing expired sessions/members and empty manual rosters. Managed
metadata remains until its room owner releases the allocation. JWT expiry
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

Main supplies a provisional rating of 1500; a rating store plugs into
`RatingProvider`. Each new pair is exported once from the queue. The configured
owner atomically binds both logical sessions to one server room in
`OnlineRegistry`, then opens its `RoomService` lifecycle. Failed hosting rolls
back allocation. Success releases the queue assignment fences and saves the
same `room_id` in both retained tickets; the managed room prevents another
admission. Allocation failure returns unavailable. Assemblies without an
allocator keep the explicit assignment-release contract. See the
[match queue contract](../match/README.md).

## Managed room protocol and recovery

Existing IDs 10–53 and their fields remain stable. The lobby uses authenticated
SessionState: no room means lobby; LoginResponse, SessionState or assigned
MatchTicket.room_id discovers an allocated room.

| Request / response | IDs | Contract |
| --- | --- | --- |
| RoomInput | 50 / 51 | Existing actor/member fences, contiguous sequence and integer control |
| RoomSnapshot | 52 / 53 | Existing committed kernel snapshot; manual rooms may initialize lazily |
| RoomReady | 54 / 55 | bool ready, actor incarnation and membership epoch; returns managed status |
| RoomStatus | 56 / 57 | Current managed status or this logical session's latest retained result |
| Settlement | 58 / 59 | Nonzero match ID; immutable result for the same participant/session |

RoomStatus contains the kernel snapshot, match ID, phase, both members' readiness
and presence, start/finish tick, played/duration ticks and optional result.
Incarnation, membership epoch and last applied input sequence allow authorized
JWT recovery to continue with the next sequence. The room owner rechecks the
transport generation and logical session after admission; callbacks retain
values and completion handles rather than a raw connection.

Managed phases are Forming → Ready → Playing → Settling → Finished. Both ready
flags produce Ready at a cutoff; a following cutoff starts Playing. Input
applies only during Playing. After the configured successful Playing ticks,
the owner creates one draw result stub and freezes simulation; the next cutoff
releases registry membership. Result ID equals match ID; both participants see
the same final tick/checksum and winner zero. Missing/replaced members, grace
expiry and readiness timeout settle with a typed reason. Clients cannot finish
a match or choose a winner.

Detached members remain in the fixed cohort during grace. JWT recovery retains
actor incarnation, membership, readiness and input sequence. Fresh or expired
logical sessions cannot query old results. Finished status and settlement remain
queryable after room unbinding for bounded retention. These are in-memory
stubs without economy, ranking or restart recovery; general combat rules remain
future work.

The service mailbox admits at most 1024 pending commands. Capacity/history
limits fail closed. Shutdown closes command admission, completes accepted
pending commands with unavailable, cancels the timer, and destroys domain
tickets on the management owner before the executor retires. `event=match_tick`
reports changed counters; final `MATCH_COUNTS` includes disjoint wait/gap
histograms and cumulative sums, with no per-player metric labels.

## Capacity and shutdown

For a manual roster, authenticated RoomSnapshot (52/53) lazily initializes an
`arena_room` actor. RoomInput (50/51) carries its incarnation, membership epoch,
contiguous sequence and integer controls. Actors run on the management owner
at an absolute 50 ms cadence. Connection workers enqueue bounded value commands;
only a tick cutoff changes room state. JWT recovery preserves member sequence;
leaving and rejoining increments a separate membership fence. Every committed
tick has a canonical checksum and a bounded replay record. See the
[room runtime contract](../room/README.md) for input ordering, catch-up,
reclamation, replay schema and the reference integer update rule.

All rooms share one management owner. Managed matches create actors during
allocation; manual rosters still initialize lazily. The service owns domain
Room/Battle lifecycle entities; `RoomActor` owns deterministic reference
simulation and replay state. Finished actors retain their last snapshot/result
until bounded retention expires. General combat rules and a standalone Room
network process remain planned.
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
| `ARENA_JWT_KEY` | unset | Enables authenticated session/match/room assembly with an explicit signing key |
| `ARENA_ACCOUNTS_FILE` | unset | File-backed password accounts, used with authenticated assembly |
| `ARENA_BATTLE_TICKS` | `100` | Successful Playing ticks before the result stub, 1..72000 |
| `ARENA_READY_TIMEOUT_TICKS` | `600` | Managed pre-start tick limit, 1..72000 |
| `ARENA_RESULT_RETENTION_MS` | `60000` | Finished status/result retention, 1..3600000 milliseconds |

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

The real-main journey harness creates temporary PBKDF2 account files and runs
TCP/WebSocket bots through login/lobby/match/automatic allocation, readiness,
fixed-tick input, result lookup and replay, with RST recovery and grace expiry.
After rebuilding both executables, run:

```bash
python3 tests/room/FullJourneyIntegrationTest.py build/services/gateway/app/ArenaServer build/services/room/arena_replay --report-dir /tmp/arena-journey
```

This is an acceptance procedure, not a claim that a particular run passed.
It uses the production application/account adapter; reports and logs go to the
supplied directory.

Multi-instance session authority, load-balancer readiness integration, a
connection login deadline and distributed business idempotency remain planned.
This architecture supports their placement but does not claim deployed high
availability or automatic recovery of in-flight business operations.

## Persistent room replay

Set `ARENA_REPLAY_DIR=/safe/local/replays` when starting the authenticated
handshake configuration to archive active rooms. The management owner copies
an initial snapshot and every committed canonical tick to `ReplayRecorder`;
a bounded private disk worker writes the complete stream independently of the
actor's rolling in-memory history. Empty ticks and rejected simulation input
attempts remain in the archive.

Each run uses its own directory. Explicit room retirement/service stop seals
accepted jobs; only a flushed, closed archive with its footer is renamed to
`.areplay`. Queue/storage failures invalidate that recording and leave any
partial file as `.part`, while simulation continues. Shutdown reports
`REPLAY_COUNTS` with completed, failed and rejected counts. Recording does not
promise fsync-level durability, unlimited disk retention or crash recovery.

Run `build/services/room/arena_replay <file.areplay> --report <desync.json>`.
Exit 0 means every recorded tick matched; 1 reports the first absolute desync
tick and expected/actual checksum; 2 means malformed, unsupported, bounded-out
or unreadable input/report output. The JSON also exposes every successful
replay tick, with 64-bit integers as decimal strings. See
[room replay format and tools](../room/README.md) for limits and examples.
