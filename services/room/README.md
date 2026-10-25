# Room runtime

`arena_room` implements `service::room::RoomActor`, an owner-affine, deterministic
integer room-state kernel. The gateway's `RoomService` hosts actors in process.
This is an implemented library, with no independent Room process yet.

## Ownership and mailbox

An actor and its domain `Room` are constructed, stepped and destroyed on one
fixed management owner. Foreign access to mutable actor state throws before
reading it; foreign destruction terminates. An actor needs no simulation mutex.
The gateway currently uses one base owner for all rooms, separately from its
connection workers. Moving a live actor between threads is unsupported.

Connection owners capture authenticated values and submit to `RoomService`.
Its mutex protects admission and copied commands only. Producers never post a
world mutation or hold a pointer to a player, actor, connection or input buffer.
The owner swaps out the mailbox at a tick cutoff, releases the mutex, checks
authority, and calls each actor's `step`. Callbacks run after room iteration,
outside the lock; their dispatcher completion posts to the original connection.

Default limits are 128 initialized actors, 1024 pending commands globally,
256 per room, 64 members per actor and 256 input attempts per tick. Snapshot
requests consume mailbox slots too. Rejected admission returns `false` and
does not call the completion. Stop closes admission under the same mutex,
cancels the timer, destroys actors on owner and settles remaining commands
once with unavailable. A ready timer from before cancellation has no effect.

## 20 Hz scheduling

The interval is exactly 50 ms. The next deadline is advanced from the previous
absolute deadline, so callback duration does not accumulate as timer drift.
Every active room advances even without client traffic. Each tick uses the
same integer update rule; the clock determines only when to run it.

After a stall, the service runs at most four catch-up steps per timer callback.
If still behind, it advances to the next future wall slot and increments
`skippedWallSlots`. Logical tick numbers remain contiguous; skipped wall slots
do not imply skipped logical tick IDs. This is bounded catch-up, not a hard
real-time guarantee or unlimited simulation of elapsed wall time. A timer
failure stops the service and closes admission.

## Membership and recovery

RoomJoin/RoomLeave still manipulate the bounded gateway roster. An authenticated
RoomSnapshot request (52/53) lazily initializes an actor for that existing room;
it can fail when the runtime actor limit is reached. Input (50/51) requires a
snapshot's actor incarnation and membership epoch, and never recreates an actor.
No client identity or target room is trusted from an input packet.

At each cutoff the owner copies the complete `OnlineRegistry::roomMembers`
roster, including Detached members in grace. Applied commands must match the
current PlayerId, logical SessionId, connection ID, transport epoch, room and
membership epoch. JWT recovery changes the transport fence but preserves the
logical member and input sequence. A real move/leave increments `roomEpoch`;
rejoining the same room after leaving resets that player's simulation state.
An idempotent same-room join does not reset it. Fresh password login also resets
the logical membership. When a room's roster becomes empty, its actor and
journal are reclaimed on the next tick without requiring new traffic.

A recreated actor receives a process-local, nonwrapping incarnation. Old inputs
cannot address its replacement. This fence is not persistent across process
restarts; process-local sessions and room state are not durable recovery.

## Input and reference state

Each member starts at `(0, 0)` with last sequence zero. An input contains a
nonzero uint64 sequence and signed `dx`, `dy`, each in `[-1, 1]`. Each Applied
input moves one integer grid step, clamped to `[-positionLimit, positionLimit]`
using an int64 intermediate; the default limit is 1000000.

The actor reconciles the entire roster, then canonically sorts attempts by
PlayerId, SessionId, membership epoch, sequence, dx and dy. Thus sequences 2
and 1 arriving in one cutoff execute as 1 then 2. Different cutoffs remain
different logs; a gap is rejected rather than buffered indefinitely.

| Result | Meaning |
| --- | --- |
| Applied | Exactly the next contiguous sequence; changes position and last input |
| Duplicate | Same last sequence and controls; no second movement |
| Gap | Greater than the next contiguous sequence; retry after filling the gap |
| Stale | Older than the retained last sequence; no state change |
| Conflict | Different controls for the same member/sequence |
| NotMember | Input does not belong to a member of this cutoff roster |
| Invalid | Zero sequence/epoch or controls outside the permitted range |

All conflicting controls for one member/sequence **within the same batch**
receive Conflict, including otherwise invalid/nonmember attempts. They do not
advance that sequence. Other attempts check validity, membership, then sequence.
Exact same-batch duplicates apply once. Results map back to caller input order.
Sequence and tick identities never wrap. A rejection does not consume sequence.

The domain Room remains `Forming`. Preparation/start/battle/settlement protocols,
CombatEngine, room allocation from a match assignment and autonomous movement
from held controls are future work. Integer movement here is an explicit
reference update rule for verifying ownership and deterministic execution.

## Checksums and replay

Every successful step, including an empty-input tick, publishes a checksum.
Version 1 uses FNV-1a64 with offset 14695981039346656037 and prime 1099511628211.
The byte schema is explicit little endian:

```text
u64 version, roomId, incarnation, tick
u32 positionLimit
u64 playerCount
for each player in ascending PlayerId:
  u64 playerId, sessionId, membershipEpoch
  u32 x, y
  u64 lastSequence
  u32 lastDx, lastDy
```

Signed values use their modulo-2^32 unsigned conversion. The hash excludes
timestamps, addresses, padding, STL container order and the checksum field.
It is a deterministic diagnostic checksum, not cryptographic integrity.

The bounded journal retains 200 ticks (10 seconds at an unstalled 20 Hz),
including the full canonical roster, **all** attempted inputs and checksum
for each tick. A rolling checkpoint contains the state before the oldest
retained tick. `replayLog()` exports owned values on owner. `RoomActor::replay`
validates the checkpoint and canonical contiguous records, reexecutes every
step, and checks every checksum. Same initial checkpoint, configuration,
incarnation and tick input records produce the same checksum sequence.
This does not promise the same cutoffs for arbitrary network arrival timing.

The rolling journal remains an in-memory diagnostic window. Each successful
`TickResult` also owns its complete canonical `record`; optional archive output
retains the initial checkpoint and every committed record independently of
`historyTicks`. Offline replay accepts archives longer than that rolling limit,
retains only one working journal tick, and reports the first failing absolute
tick with recorded and computed checksums. A later malformed record does not
hide an earlier deterministic mismatch in an already decoded log.

Tick commit prepares throwing allocations, including the exported record,
before changing live state; a failed core step leaves state/journal unchanged.
A reply allocation failure or connection timeout can happen after commit:
retry the same sequence and inspect the returned state. A later input may make
an older retry Stale. Archive admission/storage failure likewise does not roll
back a simulation commit.

## Persistent archive and offline CLI

`ReplayWriter::create(path, config, checkpoint)` writes an initial header;
`append(record)` writes the next complete cutoff, including empty ticks and
rejected attempts. `finish()` writes the required completion footer, flushes
and closes. Destruction alone never writes a footer. A failed append/write is
terminal; an interrupted or failed stream must not be advertised as complete.
`writeReplayFile` and `readReplayFile` provide owned-log convenience APIs.

The version-one `.areplay` format has no native structs, host endianness,
timestamps or external JSON/protobuf dependency. Every integer is little
endian; signed 32-bit fields use their exact two's-complement bit pattern.

| Section | Fields in order |
| --- | --- |
| File identity | 8 bytes `AREPLAY\0`; u32 format version `1`; u32 checksum version `1` |
| ActorConfig | u64 maxMembers, maxInputsPerTick, historyTicks; i32 positionLimit |
| Initial checkpoint | u64 roomId, incarnation, tick, checksum; u32 player count; complete canonical players |
| Checkpoint player | u64 playerId, sessionId, membershipEpoch; i32 x, y; u64 lastSequence; i32 lastDx, lastDy |
| Tick frame | u32 tag `0x4b434954` (`TICK`), u32 payload bytes; u64 tick, checksum; u32 member count, input count; full members then ordered inputs |
| Member | u64 playerId, sessionId, membershipEpoch |
| Input attempt | member; u64 sequence; i32 dx, dy, including values rejected by simulation |
| Completion frame | u32 tag `0x21444e45` (`END!`), u32 payload bytes `16`; u64 record count, final absolute tick |

The fixed header is 80 bytes before 48-byte checkpoint players. Tick payload
length is exactly `24 + 24*memberCount + 40*inputCount`; the completion frame
is 24 bytes. The reader checks versions, frame lengths and bounded counts
before allocation, requires the footer, and rejects trailing bytes. Defaults
bound a file to 64 MiB, 1000000 ticks, 4096 configured members, 65536 attempts
per tick and 1000000 configured rolling-history ticks. Actual cutoffs must also
fit the recorded ActorConfig. Callers may supply stricter `ReplayFileLimits`.
Decoded ownership containers and the checksum report remain bounded by these
limits; this is a diagnostic archive rather than a cryptographic audit log.

```bash
cmake --build build --target arena_replay -j2
./build/services/room/arena_replay /safe/local/path/session.areplay
./build/services/room/arena_replay /safe/local/path/session.areplay --report /safe/local/path/report.json
```

The tool always writes one valid JSON object to stdout and optionally the same
report to the requested file. Exit `0` means validated; `1` means the first
checkpoint/tick checksum mismatch; `2` means invalid input/arguments, a resource
bound or I/O failure. A report path aliasing the input is rejected. Report-file
write failure also returns `2` with an `io` report on stdout. Fields are:

```json
{"status":"ok","reason":"replay validated","failed_tick":null,"expected_checksum":null,"actual_checksum":null,"checkpoint_tick":"0","final_tick":"1","final_checksum":"123","checksums":[{"tick":"1","checksum":"123"}]}
```

The numeric values above only illustrate the JSON schema. All uint64 values
are decimal strings, including zero, so JavaScript consumers do not lose
precision. `checksums` contains every successfully replayed tick, excluding
the checkpoint; on failure it is empty. `failed_tick`, `expected_checksum` and
`actual_checksum` identify the first semantic failure where available; file
header/framing failures may have no tick or state hashes. `status` is `ok`,
`desync`, `invalid` or `io`.

## Optional live recording

Set `ARENA_REPLAY_DIR=/safe/local/path` before starting the gateway to enable
recording for its authenticated `RoomService`. Each run creates a separate
subdirectory. The management owner submits owned initial state and full
committed records to `ReplayRecorder`; one private writer thread performs disk
I/O without accessing actors or network connections. Queue count/bytes, active
streams and per-file limits are bounded. Room reclamation and service stop seal
the accepted stream; recorder stop drains accepted work and joins outside the
latency-sensitive simulation step.

Only streams successfully finished and closed are renamed from `.part` to
`.areplay`. Queue rejection or disk error invalidates that archive, retains a
failure indication and does not roll back the room. Inspect recorder completed,
failed, rejected, active/queued bytes and last-error metrics; a missing archive
is not proof that no simulation ran. An incomplete `.part` is never accepted
as a complete recording merely because its filename exists. Flush/close do
not issue `fsync`, so this does not promise power-loss durability or persistent
room/session recovery after a server restart.

## Observability and verification

`event=room_tick` records room ID, incarnation, logical tick and checksum after
each commit. Shutdown prints `ROOM_COUNTS`: actors, total successful actor
steps, Applied, actor-rejected, Duplicate and skipped wall-slot counts. Actor
rejections count typed core results, excluding transport/auth/admission errors.

```bash
cmake -S . -B build
cmake --build build --target arena_room_tests -j2
./build/tests/room/arena_room_tests
ctest --test-dir build --output-on-failure -R '^RoomTransport_'
```

Tests cover independent checksum encoding, seeded reference-model logs,
permutation invariance, bounded checkpoint replay, malformed logs, owner checks,
32 simultaneous producers, stop races, deadline/catch-up behavior and real
epoll/Asio timers. Network tests cover TCP with 0/4 workers and WebSocket,
JWT recovery, membership and actor replacement fences and resource reclamation.
