# Durable Lobby service

`ArenaLobby` owns online sessions, reconnect grace, tickets, room allocation and
20 Hz room state. Production `ArenaServer ... handshake` forwards the existing
public envelopes to this independent process. Gateway has no password verifier,
JWT key, online registry, matching queue or room actor instance.

The current request path is one way:

```text
TCP / WebSocket -> Gateway -> Lobby -> Auth -> Storage -> PostgreSQL
                                |                  ^
                                +------------------+
```

Auth returns a signed identity candidate. Lobby alone installs that identity.
MatchQueue and RoomService remain local Lobby owners; tick computation performs
no RPC or disk I/O. Canonical MMR comes from Storage, not from the public match
request.
The source-level contracts are `contract.json` and `shared/proto/lobby.proto`.
All internal calls use role-bound v1 metadata, a captured route, inherited
correlation/deadline, finite admission and no automatic transport retry.

## Transaction and recovery boundary

`LobbyAuthority` serializes owner commands. Each candidate restores isolated
OnlineRegistry, MatchQueue and RoomService models from the latest immutable
checkpoint. It freezes clocks once, gathers results and callback effects, then
writes registry, queue, rooms, metadata and ordered replay segments in one CAS
journal batch. Only `fdatasync` acknowledgment permits pointer publication,
responses and revocation effects. A rejected candidate changes no committed state.

Control commands run promptly. Room commands enter the same bounded mailbox and
share one 50 ms cutoff. The complete checkpoint retains room incarnation, roster,
input sequence fences, controls, logical tick/checksum, lifecycle and settlement
budget. Replay segments include room/incarnation/first-tick identity; recovery
checks their checksums and complete continuity against the committed actor.

`LobbyService` performs Auth/Storage RPC outside candidates and returns to the
owner for the final generation check. Epoch reservation is durable before Auth
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
PostgreSQL acknowledgment followed by a new Lobby journal acknowledgment exposes
Finished and releases membership. Exhausted/permanent failures remain blocked;
restarting the process cannot reset their attempt or elapsed budget.

## Process settings

Each service requires explicit incoming/outgoing credentials. The unified
launcher constructs a separate environment for each role:

| Process | Private settings received |
| --- | --- |
| Gateway | `ARENA_LOBBY_TOKEN` |
| Lobby | `ARENA_LOBBY_TOKEN`, `ARENA_AUTH_TOKEN`, `ARENA_STORAGE_LOBBY_TOKEN`, `ARENA_LOBBY_STATE_DIR` |
| Auth | `ARENA_AUTH_TOKEN`, `ARENA_STORAGE_AUTH_TOKEN`, `ARENA_JWT_KEY` |
| Storage | Generated role map `ARENA_STORAGE_CALLER_TOKENS` containing the supplied Auth/Lobby tokens, plus explicit PG settings |

The map is serialization of operator-supplied tokens, not generated credentials.
Only Auth receives the signing key; Gateway receives no Storage credential or
password verifier. Direct startup must preserve the same role boundaries.

Default loopback RPC ports are Storage 7105, Auth 7101 and Lobby 7102. Gateway uses
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

The journal is local, defaults to 256 MiB, and has no automatic compaction yet.
This is not replicated failover durability. Recovery assembles complete replay
histories and can use more peak memory than individual-segment validation; the
journal capacity is not a promise that the same amount of RAM is sufficient.

## Unified startup and offline replay

Prepare the database and migrations as described in Storage's README. Configure
explicit `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and five distinct credentials:
`ARENA_JWT_KEY`, `ARENA_AUTH_TOKEN`, `ARENA_LOBBY_TOKEN`,
`ARENA_STORAGE_AUTH_TOKEN`, `ARENA_STORAGE_LOBBY_TOKEN`. Tokens are 32..256 bytes;
the signing key is 32..4096 bytes. No credential is generated or silently replaced.
Optional `PGPASSWORD`, private owned `PGPASSFILE` and `PGSSLMODE` reach Storage only;
ambient `PGSERVICE`, legacy account settings and test clock injection are filtered.
Build `ArenaServer`, `ArenaAuth`, `ArenaLobby` and the shared RPC descriptor first.
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
| Lobby | 7102 | 17102 |
| Gateway | TCP 8888, WS 8889 | 18888 |

Override them with `--storage-port`, `--auth-port`, `--lobby-port`, `--gateway-port`,
`--gateway-ws-port` and each `--ROLE-health-port` option. All nine launcher ports
must be nonzero, available and distinct. Startup checks bounded HTTP
`/health/ready` responses in Storage -> Auth -> Lobby -> Gateway order; a marker
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
| `LobbyProcessAcceptance` | Real four-process journey, Gateway/Lobby SIGKILL continuity, independent Auth restart, PostgreSQL settlement and offline replay |
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

These are implemented verification paths, not a claim that Day33, the full normal
matrix, the 90-restart gate or the 64-day goal has completed. Focused smoke results
and fixture-only tests cannot replace the final real-process reports. The current
architecture still has one Lobby room owner and finite local durability; Room
sharding, distributed ownership epochs and replicated HA remain outside it.
