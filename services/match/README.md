# Matchmaking

`arena_match` implements process-local 1v1 ticket queueing and assignment.
The gateway currently embeds this library; a standalone network service,
distributed queue, persistent ratings and room allocation remain planned.

## Interface and ownership

`MatchQueue` owns actual `arena::domain::Ticket` objects on its construction
thread. `join`, `replay`, `cancel`, `status`, `advance`, `release` and `metrics` require
that owner. Callers supply nondecreasing `steady_clock` time, typed PlayerId /
SessionId identities, a nonzero join key and a trusted server MMR (0..10000).
The gateway's `MatchmakingService` provides bounded cross-thread admission and
periodic matching on the management owner; domain objects never cross workers.

Join acknowledges admission without waiting for an opponent. Pairing occurs
on periodic `advance`, with the oldest available anchor and the nearest
compatible candidate (ticket ID breaks ties). Candidate comparisons have a
per-tick budget; unfinished scans continue on later ticks. This bounds work
without repeatedly starving the tail of the queue.

For a waiting ticket, its inclusive MMR window is:

```text
min(maximumWindow,
    initialWindow + floor(wait / expansionInterval) * expansionStep)
```

Both participants must accept the absolute rating difference. Defaults are
50 initial points, +25 each 1000 ms, maximum 500, a 30000 ms exclusive deadline,
1024 queued players and 4096 total retained records. Expiry precedes matching
at the exact deadline. Retries and reconnect do not reset admission time.

## Identity, cancellation and recovery

- One active ticket per player, including an assigned ticket. The same
  `(PlayerId, SessionId, joinKey)` replays the original record; a different key
  while active returns that active ticket without creating another admission.
- A direct core join with a reused key and different input MMR conflicts.
  The gateway calls `replay` before the rating adapter, preserving the admitted
  rating even when ratings later change or the adapter becomes unavailable.
- Cancellation targets the caller's exact ticket and logical session.
  Cancelled/Expired retries return terminal state. Assigned cancellation is
  rejected; it cannot undo a committed pair.
- The availability adapter pauses a Detached session during grace. Missing,
  replaced, expired or room-bound sessions cancel waiting tickets. Logical
  SessionId survives authorized JWT recovery, so the original ticket resumes.
- Assignment reserves both player identities until a trusted allocation
  owner calls `release(matchId, now)`. Retry-history retention never releases
  those fences. Release retires both fences together; repeated release is safe.
  The current gateway exposes no client release route or room allocator.
- Cancelled, expired and released-assignment history is retained for 60000 ms,
  then reclaimed. Idempotency is limited to this process and retention window.
  Assigned fences count toward the total capacity until explicit release.

## Statistics and verification

`QueueMetrics` provides cumulative joins/replays/cancellations/expirations,
pairs, rejections, releases, and current queued/assigned/retained gauges.
Fixed histograms store successful player wait, abandoned wait and pair MMR
gap without retaining an unbounded sample list. Wait bins have upper bounds
100, 500, 1000, 5000, 10000, 30000, 60000 ms, then overflow. Gap bins have
upper bounds 0, 25, 50, 100, 200, 300, 500, then overflow. Bins are disjoint;
cumulative sums support averages, and bins support approximate percentiles.
Terminal waits are frozen, so polling and retries cannot double-count samples.

```bash
cmake --build build --target arena_match_tests -j2
./build/tests/match/arena_match_tests
ctest --test-dir build --output-on-failure -R MatchmakingTransport_
```

Tests include deterministic boundary checks, seeded stateful property tests,
concurrent mailbox requests and real epoll/Asio TCP/WebSocket paths. These
verify process-local safety, not durable or cross-process exactly-once delivery.
