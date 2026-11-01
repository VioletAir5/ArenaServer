# Lobby foundations

The independent Lobby business process is still being integrated. Gateway currently
hosts the session, matching and room-management workflows; this directory already
contains the following implemented dependencies for their migration.

- `arena_lobby_auth` provides the concrete asynchronous `AuthClient`. It calls the
  independent Auth process with role-bound v1 metadata, a clipped parent deadline
  and a route captured at admission. Its typed result is a signed candidate, not
  an installed session. It owns no KDF, signing key or authoritative player state.
- `arena_lobby_state` provides `DurableStateStore`: one bounded local journal writer,
  immutable published snapshots and compare-and-swap mutation batches. Owner calls
  never wait for disk; successful completion follows `fdatasync`. Pending work and
  retained state have independent finite limits. Writer shutdown and owner callback
  delivery are both accounted before `closed` becomes true.

Recovery checks complete frame checksums, versions and revision order under an
exclusive directory lock. Only an incomplete final frame is truncated. An uncertain
write/sync outcome quarantines further writes; reopening and inspecting the durable
state resolves the journal prefix. The default journal limit is 256 MiB and there
is no automatic compaction yet. A local sync acknowledgment is not replicated
failover durability.

The existing business owners now expose plain checkpoints and validated restore:
`net/OnlineRegistry.h`, `match/MatchQueue.h` and `net/RoomService.h`. They preserve
session/room membership, original grace and queue deadlines, logical revisions,
incarnation high-water marks, room input fences, checksums and management lifecycle.
Candidate leases and callback effects remain isolated from the committed registry.
RoomService also supports explicit owner-driven ticks and externally acknowledged
settlement intents. These APIs do not themselves serialize or atomically persist
the complete Lobby authority; that transaction boundary remains the next step.

A Lobby transaction must prepare its registry/queue/room candidate, capture replies
and external effects, persist one corresponding journal batch, and publish only
after acknowledgment. A failed `advance()` or settlement application discards the
candidate. Reads that prune expired state also belong in this boundary. Async Auth
finalization checks the target player's old claims/generation against current state,
while using the current journal revision for its final CAS. No full-session restart
or fresh-login substitute is implied by the standalone checkpoint tests.

The shared `runtime/OwnerScheduler.h` supplies owner posting and timers without a
Gateway dependency. The remaining EventLoop adapter stays in Gateway. New Lobby
runtime code should use this shared interface rather than importing transport
features.

Focused targets are `arena_lobby_auth_tests`, `arena_lobby_state_tests`,
`arena_verified_session_tests`, `arena_match_recovery_tests` and
`arena_room_service_recovery_tests`. They cover real RPC/FD cleanup, actual journal
write/sync faults, candidate isolation, corrupt checkpoint rejection and continued
state equivalence. End-to-end Gateway/Lobby crash recovery and 90 process restarts
must be verified after the actual business process is connected.
