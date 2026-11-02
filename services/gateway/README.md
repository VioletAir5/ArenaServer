# Gateway

CMake builds `arena_gateway` and `services/gateway/app/ArenaServer`. Production
handshake mode serves TCP/WebSocket transports and forwards application requests
to the independent `ArenaLobby` process. Gateway owns no authoritative online
registry, matching queue, room actor, JWT signing key, password KDF or persistence
worker. Embedded adapters live under `tests/support/gateway` for test fixtures
and are excluded from the production Gateway link image. Main uses
`makeRemoteLobbyMessageRegistry` and `LobbyClient`.

```text
TCP / WebSocket -> Gateway -> Lobby -> Auth -> Storage -> PostgreSQL
                                +--------------^
```

Lobby owns session/grace state, matching, room simulation and the local durable
journal. Auth performs credential verification and JWT signing; Storage owns SQL
and database transactions. See the [Lobby runtime](../lobby/README.md) for startup,
recovery, settlement and offline replay export.

## Connection modules and ownership

| Module | Responsibility |
| --- | --- |
| `TcpServer`, `AsioTcpServer`, `WebSocketServer` | Listener descriptors, accepted handoff, connection owners and finite stop |
| `AdmissionController` | Shared process-local total/IP reservation leases and counters |
| `ITransport` | Concrete nonblocking byte I/O, buffers, partial reads/writes and physical close |
| `ConnectionHost` / `ConnectionChannel` | Owner-thread lifetime, read pauses, backpressure and bounded completion mailbox |
| `FrameProtocol` / `ArenaProtocol` | Packet framing, policies, Envelope decoding and canonical reply encoding |
| `RequestDispatcher` / `MessageRegistry` | Sealed routes, authentication requirements, 64 pending requests, deadlines and once-only completion |
| `RemoteLobbyRoutes` / `LobbyClient` | Typed v1 RPC, captured Lobby route, cancellation, transport heartbeats and readiness probes |
| `ConnectionSession` | Passive binding projection, validity check and exactly-once release; no Lobby authority object |

The input path is transport -> Host -> framing/policies -> Envelope -> Dispatcher
-> remote route. Ping stays local; Login, session/room operations, matching,
inputs and settlement go to Lobby using the existing public message IDs.
Business errors enter the same canonical frontend error encoder and rejection
counters as local errors. Typed successful replies must still match their request.

`ConnectionContext` owns immutable physical connection ID, trusted socket peer,
acceptance time and rejection counters. Session mutation belongs to the connection
owner. A foreign RPC completion prepares owning values and crosses the weak owner
mailbox; only a live, unexpired request may install a login projection. Late
successful logins trigger conditional remote detach rather than binding a closed
connection. RPC cancellation does not prove a remote durable mutation rolled back.

Gateway's dedicated RPC executor is separate from connection owners. Finite
business calls, one heartbeat slot and one independent readiness slot drain before
that executor retires. Heartbeats run in bounded batches of at most 512 current
connections. Only a successful qualified revocation can invalidate the matching
projection; dependency failure cannot revive a projection or create a new session.
Reconnect/grace and stable logical session/room identity belong to Lobby.

## Executable profiles

```sh
build/services/gateway/app/ArenaServer epoll raw
build/services/gateway/app/ArenaServer asio packet
build/services/gateway/app/ArenaServer asio handshake http://localhost:3000
```

`raw` echoes bytes; `packet` echoes binary frames. Production `handshake` requires
Asio and an explicit `ARENA_LOBBY_TOKEN`; Auth, Lobby and Storage must be running.
It does not enable embedded business by setting `ARENA_JWT_KEY` or an account file.
Use the [unified launcher](../lobby/README.md#unified-startup-and-offline-replay)
for the four-process stack.

TCP defaults to `127.0.0.1:8888`; handshake also serves `127.0.0.1:8889/ws`.
Both carry PacketCodec binary frames. WS accepts binary messages and the exact
configured Origin, default `http://localhost:3000`; missing/different Origin is
rejected before upgrade. Handshake packet policies allow outer type `{1}` with a
200-packet burst replenished at 100 packets/s, once per complete frame before
protobuf decoding. Malformed framing/payload closes the connection; typed
business errors produce replies.

| Gateway setting | Default | Meaning |
| --- | --- | --- |
| `ARENA_LOBBY_TOKEN` | required | Explicit Gateway-to-Lobby credential; no generated/default token |
| `ARENA_LOBBY_PORT` | `7102` | Loopback dependency port when runtime discovery is absent |
| `ARENA_LOBBY_MAX_INFLIGHT` | `64` | Business RPC bound, 1..1024; heartbeat/probe have separate finite slots |
| `ARENA_LOBBY_TIMEOUT_MS` | `1000` | RPC deadline cap, 1..1000 ms, also clipped to the parent request |
| `ARENA_TCP_PORT` / `ARENA_WS_PORT` | `8888` / `8889` | Distinct nonzero business ports |
| `ARENA_MAX_CONNECTIONS` | `1000` | Shared TCP/WS admitted connection bound; handshake accepts at most 65536 |
| `ARENA_MAX_CONNECTIONS_PER_IP` | `100` | Reservations for the real peer IPv4, excluding source port |
| `ARENA_HEALTH_PORT` | unset | Separate loopback HTTP listener; direct application startup permits `0` |
| `ARENA_RUNTIME_CONFIG` / `ARENA_NODE_ID` | unset / `gateway` | Optional versioned discovery file and operator node identity |

Only total/IP admission is process-local. Pending worker handoff and incomplete
WS upgrades retain their reservations. Setup failure and owner cleanup release
leases. TCP/WS share one controller in main; independently assembled listeners
can have their own controller. Counters use fixed labels rather than peer/player
metric labels.

## Runtime discovery and health

An optional `ARENA_RUNTIME_CONFIG` file uses the shared runtime schema:

```json
{
  "version": "1",
  "desiredReady": true,
  "drainGraceMs": 5000,
  "nodes": [
    {"service": "lobby", "nodeId": "lobby-a", "host": "127.0.0.1", "port": 7102, "ready": true}
  ]
}
```

Gateway selects the first ready Lobby node in canonical service/node order. Each
admitted call retains one route snapshot; reload cannot retarget it. A configured
store without a ready Lobby node fails closed, with no fixed-endpoint fallback,
transport retry or local-business fallback. This is static service discovery;
Room sharding, owner epochs and replicated routing are not implemented.

Publish a complete regular file by atomic rename, then send `SIGHUP` to configured
processes. Unknown/duplicate fields, malformed values, files over 64 KiB and stale
versions are rejected. Versions must strictly increase, including when restoring
older contents. Publication is atomic within each process, not across the stack.
One loader and one coalesced pending reload bound repeated signals. Startup
listener/key configuration does not change through discovery reload.

- `/health/alive` reports the control owner's liveness, including drain.
- `/health/ready` requires bound listeners, operator/node intent, no drain and a
  recent authenticated **Lobby** `health.ready` result for the current route.
- `/health/config` exposes version, readiness, inflight and reload counters.

Providers read cached state rather than issuing SQL/RPC. Health has at most 16
peers, 4 KiB request headers and 250 ms I/O deadlines. Lobby probes run before any
login and recur on the 250 ms control timer, with a 300 ms maximum RPC timeout.
Route changes and probe errors cannot leave a stale ready result authoritative.
New connections fail readiness admission when the dependency is unavailable.
With runtime control, existing connections can still request room input/status
while unrelated Auth/Storage dependencies are down; login, match join and
settlement require dependency readiness. Explicit operator quiescence gates all
new business requests.

## Capacity and shutdown

Frame bodies are limited to 256 KiB; transport output has a 1 MiB hard bound.
Host backpressure composes read-pause reasons, resuming only when all clear.
The owner mailbox accepts at most 256 outstanding foreign actions. Queue and
transport bounds remain active regardless of optional policies.

SIGINT/SIGTERM close admission and begin owner-thread drain. Host quiesces input,
including later frames in a coalesced batch. ArenaProtocol waits for accepted
requests to finish or time out and enqueues their replies before transport write
shutdown. TCP drains output, half-closes and waits for EOF; WS drains messages
before normal Close. A finite grace deadline forces retirement when needed.
Native reads continue to observe EOF while application bytes are discarded.
Immediate/policy closure may discard pending output and requests.

Connection owners and physical I/O retire before their executors. The remote
client then cancels/drains actual calls and timer deliveries. Final
`LOBBY_COUNTS` reports tracked/business/heartbeat/probe/owner-post work at zero;
`connection_admission active=0` reports transport reservations. These physical
cleanup counters do not require Lobby's durable sessions or retained results to
be cleared at process shutdown.

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

## Verification and replay

Focused executable checks include:

```sh
ctest --test-dir build -R 'RemoteLobby|ConnectionContext|ServiceSupervisorAcceptance' --output-on-failure
ctest --test-dir build -R '^(LobbyProcessAcceptance|FullJourneyProductionGateway|RuntimeConfigDiscovery|SupervisedProcessAcceptance)$' --output-on-failure
```

PostgreSQL-backed checks require the configured private PostgreSQL test tools and
Python dependencies; see [Storage](../storage/README.md). The complete journey
uses real Gateway/Auth/Lobby/Storage processes, TCP/WS bots, password/JWT recovery,
matching, room input, settlement and offline durable replay. It creates no
Gateway-local account file or persistence assembly. `SupervisedProcessAcceptance`
uses the production launcher and verifies automatic Auth recovery with unchanged
bystander PIDs, session/room/input continuity and final process/FD/pool cleanup.

CTest direct-stack journey/discovery checks use `RunWithClock.py` for the process
tree when the test clock target is enabled. The production launcher deliberately
filters test clock injection; its real acceptance uses the host clock. Production
JWT verification is unchanged. Test fixtures and focused passes are not evidence
that the separate 90-restart/99.9% gate or the full normal matrix has finished;
final run reports provide that evidence.

Production replay comes from Lobby's synced journal. Stop Lobby, run
`arena_replay_export --state-dir ... --output-dir ...`, then verify the exported
`.arena` with `build/services/room/arena_replay FILE --report REPORT.json`.
See [offline export and local-durability limits](../lobby/README.md#unified-startup-and-offline-replay).
