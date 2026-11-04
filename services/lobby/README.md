# Durable Lobby service

`ArenaLobby` owns online sessions, reconnect grace, tickets, room reservations,
explicit owner routes and the canonical journal. Independent `ArenaRoom` processes
compute room state at durable 50 ms cutoffs. Production `ArenaServer ... handshake`
forwards the existing public envelopes to this independent process. Gateway has no password verifier,
JWT key, online registry, matching queue or room actor instance.

The current request path is one way:

```text
TCP / WebSocket -> Gateway -> Lobby -> Auth -> Storage -> PostgreSQL
                                |                  ^
                                +------------------+
                                |
                                +-> Room-a / Room-b (isolated room proposals)
```

Auth returns a signed identity candidate. Lobby alone installs that identity.
MatchQueue remains local to Lobby. RoomRuntime executes only in Room workers;
Lobby retains immutable worker values rather than a local RoomService instance.
A worker cannot call Lobby or Storage. Tick computation itself performs no RPC or
disk I/O; orchestration installs committed images and submits isolated proposals.
Canonical MMR comes from Storage, not from the public match request.
The source-level contracts are `contract.json` and `shared/proto/lobby.proto`.
All internal calls use role-bound v1 metadata, a captured route, inherited
correlation/deadline, finite admission and no automatic transport retry.
Positive Auth, Storage and Room readiness observations qualify only their captured
discovery version. A publication cannot reuse an older positive probe; accepted
business calls still finish against the route captured at admission.

## Transaction and recovery boundary

`LobbyAuthority` serializes owner commands. Each candidate restores isolated
OnlineRegistry and MatchQueue models from the latest immutable checkpoint.
RoomDirectory candidates compose with those models through the same journal
writer; no worker owns a second copy of session authority. Authority freezes clocks
once, gathers results and callback effects, then writes registry, queue, rooms, metadata and ordered replay segments in one CAS
journal batch. Only `fdatasync` acknowledgment permits pointer publication,
responses and revocation effects. A rejected candidate changes no committed state.

Control commands run promptly. A room mutation first enters the durable ordered
inbox. After lease expiry and Registry maintenance, Authority appends a bounded
50 ms cutoff containing the exact roster and relevant current authorization tuples.
RoomCoordinator sends a prefix ending at that cutoff to its exact qualified owner.
The worker returns one complete image, compact command results and a replay delta.
Authority publishes the image, immutable responses, membership effects and replay
extent in one journal CAS. Public replies wait for this final acknowledgment.
Cancellation after ingress acknowledgment does not delete the accepted command.
Snapshot, Status and Settlement queries read one already committed immutable
image and its revision. They share the worker's pure query policy, including
cohort, phase and result-retention checks. Each read also verifies the current
physical transport, authorization epoch, token expiry and lease. A new roster or
presence change waits for a real committed cutoff within the original request
deadline. Reads do not enqueue operations, advance ticks or renew transport leases;
the Gateway's independent heartbeat owns lease renewal. Input and Ready replies
still wait for their worker proposal and final journal acknowledgment.
Each cutoff reserves mailbox capacity and never exceeds the worker command bound.
The complete checkpoint retains room incarnation, roster,
input sequence fences, controls, logical tick/checksum, lifecycle and settlement
budget. Replay segments include room/incarnation/first-tick identity; recovery
checks structural continuity and final image identity without running a local
simulation. The offline replay tool recomputes every recorded physical checksum.

`LobbyService` performs Auth/Storage RPC and RoomCoordinator performs Room RPC
outside candidates and return to the owner for the final generation check. Epoch reservation is durable before Auth
signs. A failed or stale candidate cannot replace a newer installed session.
A login `operation_key` is optional and additive: clients that need to resolve an
unknown login acknowledgment across physical reconnects retain one random key
for that logical operation. Replays still require actual credential verification,
the same player and current session generation, and an unexpired durable receipt.
Legacy clients retain their ordinary password/JWT login flow.

Gateway process identity plus physical connection ID map to a durable internal
handle. Heartbeats refresh only exact current transports. A missed detach is
reclaimed by the transport lease, followed by the original reconnect grace;
reopening does not renew that grace. Gateway retains only passive projections.
An explicit unknown/revoked heartbeat closes the matching owner connection;
a failed heartbeat cannot invent a replacement authority or revive a projection.

Terminal match data first becomes an immutable durable settlement intent.
Each external attempt and its finite monotonic deadline are persisted before RPC.
An uncertain outcome reconciles the same match/settlement identity. Only the
PostgreSQL acknowledgment is recorded with an ordered trusted SettlementAck in
the same Lobby journal transaction. The next worker cutoff proposes Finished;
its final Lobby journal acknowledgment exposes Finished and releases membership. Exhausted/permanent failures remain blocked;
restarting the process cannot reset their attempt or elapsed budget.

## Process settings

Each service requires explicit incoming/outgoing credentials. The unified
launcher constructs a separate environment for each role:

| Process | Private settings received |
| --- | --- |
| Gateway | `ARENA_LOBBY_TOKEN` |
| Lobby | `ARENA_LOBBY_TOKEN`, `ARENA_AUTH_TOKEN`, `ARENA_STORAGE_LOBBY_TOKEN`, `ARENA_ROOM_TOKEN`, `ARENA_ROOM_NODES`, `ARENA_LOBBY_STATE_DIR` |
| Each Room | `ARENA_ROOM_TOKEN`, exact `ARENA_NODE_ID`, `ARENA_ROOM_STATE_DIR` |
| Auth | `ARENA_AUTH_TOKEN`, `ARENA_STORAGE_AUTH_TOKEN`, `ARENA_JWT_KEY` |
| Storage | Generated role map `ARENA_STORAGE_CALLER_TOKENS` containing the supplied Auth/Lobby tokens, plus explicit PG settings |

The map is serialization of operator-supplied tokens, not generated credentials.
Only Auth receives the signing key; Gateway receives no Storage credential or
password verifier. Direct startup must preserve the same role boundaries.

Default loopback RPC ports are Storage 7105, Auth 7101, Lobby 7102 and the two
Room nodes 7103/7104. Gateway uses
TCP 8888 and WS 8889 in authenticated handshake mode. `ARENA_HEALTH_PORT=0` selects
an ephemeral separate health listener; an explicit runtime file can supply atomic
versioned discovery. Missing/unready discovery entries have no endpoint fallback.
The Gateway health cache probes actual Lobby readiness even before any login.

Lobby startup requires an explicit private journal directory and the current
Linux boot identity. It preserves absolute monotonic deadlines only on the same
boot; a boot mismatch fails closed rather than silently starting fresh sessions.
Only an incomplete final journal frame is truncated. CRC/version/revision errors
preserve the journal and prevent readiness. An uncertain write quarantines new
writes until a real reopen reconciles its durable prefix.

The local journal defaults to 256 MiB. When the next append would exceed its
bound, the disk owner writes a checksummed, chunked checkpoint of the acknowledged
state to a private temporary file, syncs it, atomically replaces the journal and
syncs its directory before appending the new revision. Checkpointing preserves
the revision, does not discard live values or replay history, and rejects capacity
when the live checkpoint plus the next batch cannot fit. The temporary checkpoint
can coexist with the old file during replacement. This is local durability;
replicated failover is not implemented.

Recovery reads original version-one journals and compacted version-two journals.
Only an incomplete append tail can be truncated; an incomplete mandatory
checkpoint fails closed. A compacted journal requires a version-two reader, so
rolling back only the executable cannot reopen that directory with an old reader.
Offline export assembles complete replay histories and can use more peak memory
than individual-segment validation; file capacity is not a RAM requirement estimate.
Legacy Lobby checkpoints retain passive room values until a worker has validated
and staged the exact converted image, then the single Lobby CAS adopts it. A worker
is activated only after that ACK. Startup never
restores a local RoomService or evaluates room physics.

## Room ownership and handoff

RoomDirectory stores `(room_id, incarnation)`, exact `node:instance` owner, owner
epoch, ordered ingress, immutable receipts and the latest complete worker image.
Every newly started worker has a new random instance ID. Discovery captures one
exact node; a failed existing owner is never replaced by a transport fallback.
Initial placement uses rendezvous hashing over qualified healthy workers.

RoomCoordinator drives durable Prepare -> Freeze -> Transfer -> Commit. Freeze
fences proposals at the last acknowledged image and preserves every unapplied
inbox record. Transfer requires the exact target's durable Stage receipt. Commit
increments the canonical owner epoch; only then may the target install and process
the remaining ordered prefix. Aborting uses source epoch + 2, fencing both prior
workers. The handoff's fixed eight-second deadline leaves compensation time within
the separate ten-second recovery acceptance limit; measured recovery is still
required. A crash does not renew the saved deadline.

Initial creation first persists the worker-produced image and route in Lobby WAL.
Only after that ACK does an empty-transfer activation install the private worker
cache. Expired reservations and rejected initial publication never create a private
Stage. Legacy adoption retains a pre-adoption Stage for semantic kernel validation;
its existing canonical legacy actor remains the durable intent until adoption ACK.
An exact current-instance initial Stage can be consumed by empty-transfer activation.
Migration Transfer still requires its exact durable Stage receipt.

Optional `ARENA_OPERATIONS_TOKEN` enables operations-only `room.migration.start`
and `room.migration.status`. Start acknowledges durable Prepare, not completed
migration. Status reports canonical phase, owner epoch, cursor, ticks and checksum.
The key `(room_id, incarnation, migration_id, source_epoch, target_node)` is
idempotent. A changed tuple conflicts. Operations credentials cannot execute public
business requests; Gateway credentials cannot start migration. Direct Lobby startup
requires `ARENA_ROOM_NODES`, for example:

```json
[{"nodeId":"room-a","host":"127.0.0.1","port":7103},
 {"nodeId":"room-b","host":"127.0.0.1","port":7104}]
```

The normal process tests exercise the complete production path with independent
Gateway, Auth, Lobby, Storage and two Room processes. `RoomMigrationProcessAcceptance`
migrates 1000 distinct room incarnations in both directions, verifies all ordered
inputs with independent checksum recomputation and the native replay tool, and
requires migration P99 <= 2 seconds. Its latency starts at the first migration
request and ends at the public target-owner durable-tick acknowledgment.

`RoomMigrationFaultAcceptance` kills source, target and coordinator at each of the
four durable phases and requires rollback or takeover within 10 seconds. Its
partition case lets an isolated old worker compute, then proves the late result
cannot roll back or double-apply canonical history. Repeated Commit remains
idempotent; healing rejects the old execution epoch. The generated phase diagram
shows sampled observations and the first post-fault committed tick, without
inventing unobserved intermediate phases.

On worker restart, the private journal is fully validated before bounded cleanup
batches release previous-instance images. Readiness and business admission wait
for every physical cleanup ACK. Lightweight epoch fences remain within the
existing route/stage limits; the canonical Lobby image drives recovery. Initial
creation therefore needs no orphan-prone private Stage before the canonical ACK.

`ServiceRestartAcceptance` retains its fixed 90 restarts, 75 fault windows, 1666
logical operations, 6000 Playing ticks and 540-second total budget. TOKEN renewal
uses a real TCP/WebSocket handoff and preserves logical session, room, membership
and acknowledged input sequences. Settling hides unacknowledged public results;
Finished must preserve the frozen tick/checksum and have real PostgreSQL match,
unique settlement/outbox and reward records. Raw connection and business errors
remain recorded even when the bounded logical operation later succeeds.

The generated reports are `build/tests/lobby/room-migration-process-report.json`,
`room-migration-fault-report.json` and `service-restart-report.json`. They retain
histories, checksum/replay verification, refusal counts, sampled RSS/socket-FD
peaks and physical shutdown evidence. Socket-FD samples include listeners and
internal probes; they are not a public-client connection gauge. The same-rate
civil clock used by the normal process-test wrapper reaches every service child;
production startup filters this test-only environment.

A Debug/UBSan WSL run on 2026-11-04 verified all 1000 histories with 4000 applied
controls and 2000 preserved duplicate attempts, zero lost/double-applied controls,
migration P99 815.6 ms and maximum 880.1 ms. All 12 phase/victim pairs recovered
within 8193.3 ms. The complete restart gate finished in 480.7 seconds with all
1666 logical contracts successful; every parent FD returned from 5 to 5 and no
owned child process remained. These are recorded local acceptance measurements,
not replicated failover or a capacity guarantee for another deployment.

## Unified startup and offline replay

Prepare the database and migrations as described in Storage's README. Configure
explicit `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and six distinct credentials:
`ARENA_JWT_KEY`, `ARENA_AUTH_TOKEN`, `ARENA_LOBBY_TOKEN`,
`ARENA_STORAGE_AUTH_TOKEN`, `ARENA_STORAGE_LOBBY_TOKEN`, `ARENA_ROOM_TOKEN`. Tokens are 32..256 bytes;
the signing key is 32..4096 bytes. No credential is generated or silently replaced.
Optional `PGPASSWORD`, private owned `PGPASSFILE` and `PGSSLMODE` reach Storage only;
ambient `PGSERVICE`, legacy account settings and test clock injection are filtered.
Build `ArenaServer`, `ArenaAuth`, `ArenaLobby`, `ArenaRoom` and the shared RPC
descriptor first.
Then run the real built processes with the prepared Python environment:

```sh
python3 tools/runtime/RunServices.py --repo-root "$PWD" --build-dir "$PWD/build" \
  --python "$PWD/.cache/storage-venv/bin/python" \
  --state-dir "$PWD/.cache/dev-stack-state" --log-dir "$PWD/.cache/dev-stack-logs"
```

The required paths are absolute. `--build-dir` selects the real service artifacts;
`--descriptor` can override `build/shared/generated/rpc.desc`. `--python` preserves
a virtualenv interpreter symlink. State/log directories must be private and owned;
new files use mode 0600. An exclusive state lock prevents simultaneous supervision.
Gateway is started in its supported `asio handshake` profile.

| Process | Business port | Separate HTTP health port |
| --- | --- | --- |
| Storage | 7105 | 17105 |
| Auth | 7101 | 17101 |
| Room-a | 7103 | 17103 |
| Room-b | 7104 | 17104 |
| Lobby | 7102 | 17102 |
| Gateway | TCP 8888, WS 8889 | 18888 |

Override them with `--storage-port`, `--auth-port`, `--lobby-port`, `--gateway-port`,
`--gateway-ws-port` and each `--ROLE-health-port` option. Repeat `--room-node
NODE_ID:PORT` to configure 1..16 actual Room children and `--room-health-port
NODE_ID:PORT` for separate health ports. `--room-bin` overrides the built worker;
`--room-runtime-config` supplies their shared discovery file. All thirteen default
launcher ports (nine plus two per Room) must be nonzero, available and distinct.
Startup checks bounded HTTP `/health/ready` responses in
Storage -> Auth -> Rooms -> Lobby -> Gateway order; a marker
line alone never establishes readiness. Initial startup failure stops and joins
the admitted stack in reverse dependency order.

After `stack_ready`, only the failed role restarts; other processes retain their
PIDs. `--restart-attempts` defaults to three per role for the entire supervisor
lifetime, including failed recovery starts. Healthy recovery does not reset it;
zero disables recovery. Backoff uses exponential full jitter with defaults
`--restart-backoff-base .1 --restart-backoff-max 2`. Recovery probes are bounded,
nonblocking and do not prevent other roles or stop signals from progressing.
Exhaustion leaves that role degraded without terminating bystanders. Explicit
TERM/INT stops and joins the whole stack. Degradation or forced kill produces a
nonzero final exit; successful recovery followed by graceful stop can exit zero.

Default per-service startup/stop/probe/post-kill budgets are 15/10/0.25/2 seconds;
`--startup-timeout`, `--stop-timeout`, `--probe-timeout`, `--kill-timeout` have finite
validated maxima of 120/30/1/5 seconds. Child stdout/stderr goes directly to a new
private per-attempt file; lifecycle JSON contains no credentials. No log-reader
thread or unbounded restart queue is created.

HUP reloads only roles with an explicit `--ROLE-runtime-config` file, after their
first ready response. It changes discovery/operator intent, not listener ports,
startup signing keys or restart budgets. The launcher does not start PostgreSQL,
apply migrations, create accounts or retry business operations. Process recovery
is not replicated Lobby/Storage failover.

After stopping Lobby, export its synced replay segments under the actual exclusive
journal lock. Existing outputs are never overwritten:

```sh
build/services/lobby/arena_replay_export \
  --state-dir "$PWD/.cache/dev-stack-state/lobby" \
  --output-dir "$PWD/.cache/exported-replays"
```

Verify an exported file with
`build/services/room/arena_replay FILE.arena --report REPORT.json`.
Each `.arena` file is readable by the original `arena_replay` tool. Export validates
the full checkpoint/config, ordered ticks and every stored checksum before file
publication. A live Lobby lock, corrupt/gapped history or capacity violation is a
failure. The tool joins its real writer; its five-second polling budget does not
place a hard upper bound on a blocked filesystem syscall or the final destructor
join.

## Normal verification

Enable the PostgreSQL test dependencies described in [Storage](../storage/README.md)
and build the real service, replay and test targets. The process checks use an
isolated harness-owned PostgreSQL cluster; they never substitute a developer's
running database.

```sh
ctest --test-dir build -R 'LobbyAuthorityTest|LobbyServiceTest|AuthClientTest|RemoteLobby' --output-on-failure
ctest --test-dir build -R '^(LobbyProcessAcceptance|FullJourneyProductionGateway|RuntimeConfigDiscovery|SupervisedProcessAcceptance)$' --output-on-failure
ctest --test-dir build -R '^(ServiceSupervisorAcceptance|ServiceRestartAcceptance|ServiceBoundaryProcessAcceptance)$' --output-on-failure
```

| Check | Evidence scope |
| --- | --- |
| Authority/codec/recovery tests | Actual journal sync/failure/reopen, typed checkpoints, deterministic inputs and replay continuity |
| `LobbyProcessAcceptance` | Real six-process journey, Gateway/Lobby SIGKILL continuity, independent Auth restart, PostgreSQL settlement and offline replay |
| `RoomMigrationProcessAcceptance` | 1000 distinct real Room lifetimes, both migration directions, every acknowledged input checked against independent and native offline replay, stale epoch/idempotency checks, P99 at most two seconds |
| `RoomMigrationFaultAcceptance` | Four published migration phases crossed with source/target/Lobby SIGKILL, real pre-sync and transport gates, delayed old proposals, exact epoch rejection and ten-second recovery budget |
| `FullJourneyProductionGateway` | Two real TCP/WS bots; recover/expired grace cases, original session/room/control fences, committed results and every live tick checked against exported replay |
| `RuntimeConfigDiscovery` | Real services and PG with captured routes, readiness/outage behavior, reload and finite cleanup |
| `ServiceSupervisorAcceptance` | Executable fixtures for launcher ordering, role isolation, cumulative restart exhaustion, signals and FD cleanup; not a real business acceptance |
| `SupervisedProcessAcceptance` | Production launcher with real service ELFs/private PG, Auth SIGKILL recovery, unchanged bystander PIDs, JWT/session/room/input continuity and actual stop/FD/pool closure |
| `ServiceRestartAcceptance` | Separate repeated process restart/latency gate; its final report determines the 90-restart/99.9% result |
| `ServiceBoundaryProcessAcceptance` | Actual listener version/role/deadline/cycle rejection and TCP connect-delay boundaries |

Direct-stack CTest journey/discovery/restart checks use the optional test-only
`RunWithClock.py` process-tree wrapper. `SupervisedProcessAcceptance` intentionally
uses the host clock and the production launcher's environment isolation. Neither
mode changes production JWT verification or bypasses a failed assertion.

These are implemented verification paths; their actual reports determine which
acceptance gates passed. Focused smoke results and fixture-only tests cannot
replace the full migration count or phase-fault matrix. Room computation runs in
independent workers selected by explicit routes and owner epochs. One Lobby WAL
remains the authoritative publisher of images, ordered inputs and ownership
changes; replicated Lobby durability and consensus remain planned work.
