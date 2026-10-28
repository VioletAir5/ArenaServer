# PostgreSQL storage model

This module provides the reviewed PostgreSQL 14+ schema, ordered migrations and a synchronous Python asset-transfer entry point.
It includes transaction-scoped Account/Match repositories and a bounded Python connection pool.
It does not yet provide a C++ database adapter, settlement worker
or outbox publisher. Gateway still uses its file account snapshot and in-memory
session, match and room services. Applying this schema does not make those paths
persistent.

## Files and ownership

- `migrations/001_initial.up.sql`: the `arena` schema, domains, eight business
  tables, indexes and four integrity/state guards.
- `migrations/001_initial.down.sql`: destructive rollback in explicit dependency
  order, including functions, domains and the schema. It uses no `CASCADE`.
- `migrations/002_asset_transfer.up.sql` / `.down.sql`: additive immutable transfer receipts and explicit rollback.
- `assets.py`: atomic transfer SQL; the fresh and pooled paths share one transaction executor.
- `transactions.py`: retry budget, stable storage errors, single COMMIT owner and exact loan release.
- `database.py`: bounded Psycopg pool and transaction-scoped UnitOfWork.
- `repositories.py`: parameterized Account/Player and Match persistence, immutable records.
- `application.py`: SQL-free atomic account/player registration workflow.
- `migrate.py`: Python standard-library orchestration of one `psql` process.
  Only reviewed versions 001 and 002 are supported; it does not discover arbitrary migrations.
- `fixtures/seed.sql`: separate, deterministic test data. It is not run by `up`
  and must not be treated as production accounts or runtime settlement output.

SQL files contain no `BEGIN` or `COMMIT`: the runner owns the transaction. They
are trusted, reviewed artifacts, not user-supplied SQL. Migrations use the Python
standard library and psql. Asset transfers use Python 3.10+, Psycopg and libpq
(install `services/storage/requirements.txt` in a virtual environment).

## Durable model

| Table | Identity and principal constraints |
| --- | --- |
| `account` | Explicit account ID; unique case-sensitive printable ASCII name of 1..128 bytes; bounded versioned PBKDF2 digest; `active`/`disabled`; finite audit timestamps. |
| `player` | Explicit player ID; unique account FK (one character per account); display name of 1..128 bytes; `active`/`suspended`/`retired`; MMR in 0..10000 and nonnegative revision. |
| `item` | Explicit item ID; unique lowercase catalog code of 1..64 bytes; bounded display name. |
| `inventory` | Composite `(player_id, item_id)` PK and both FKs; nonnegative signed-bigint quantity and revision. |
| `game_match` | One authoritative battle ID and room ID; `forming`/`running`/`completed`/`aborted` with consistent nullable result fields and finite timestamps; winner must be a participant. |
| `match_player` | Composite `(match_id, player_id)` PK; unique slot 1 or 2; FKs to match/player; logical session and membership epoch; initial MMR, last input sequence and final integer position. |
| `settlement` | One settlement per terminal match; unique nonzero UUID idempotency key; 32-byte request hash; bounded object payload; consistent attempt/retry/commit/failure fields. |
| `asset_transfer` | Nonzero UUID PK; source/destination player and item FKs; distinct players; positive bigint quantity; finite creation time; immutable successful command identity. |
| `outbox` | Nonzero UUID event ID; FK to a committed settlement; one global `settlement.committed` event per source; bounded object payload; pending/published delivery fields. |

`arena.uint64` is an unconstrained-typmod `numeric` domain whose CHECK requires
an integral value in `0..18446744073709551615`; `arena.entity_id` additionally
requires a nonzero value. This retains the existing uint64 wire/domain range.
`numeric(20,0)` is deliberately avoided because it can round a fractional input
before CHECK evaluates it. IDs are supplied by the caller; this migration creates
no ID sequences. Player and initial match MMR are restricted to `0..10000`, matching the current
match queue admission bound. Quantities/revisions use
signed `bigint`, so callers must check their own arithmetic and conversion bounds.

Names retain `FilePasswordAccounts`' current case-sensitive ASCII semantics;
there is no new lowercasing or Unicode normalization policy. Stored digests use
`$arena-pbkdf2-sha256$<rounds>$<salt hex>$<key hex>` with 100000..2000000 rounds,
a 16-byte salt and a 32-byte key. Schema validation does not verify a password.
`updated_at` remains an explicit write responsibility. Most revisions are also
caller-maintained; the match roster guard additionally increments its parent
`game_match.revision` on each effective roster write to publish an MVCC fence.

### Cross-row boundaries

Insert a `forming` match, add its two distinct players/slots, then transition
it. A non-forming match requires exactly two participants. The parent state guard
and child roster guard serialize on the same parent row. Each effective child
write also increments the parent revision: a lock alone would not invalidate an
older Repeatable Read snapshot, while a conflicting parent version forces that
transaction to retry instead of authorizing a start with a stale roster. Active
membership and `initial_mmr` are immutable; while running, `last_input_seq`
cannot decrease. Terminal participant/result rows cannot be rewritten. These are
immediate guards, not a deferred constraint that permits a partial terminal row.
Match state changes permit `forming -> running/aborted` and
`running -> completed/aborted`. The schema does not implement the runtime
ready/tick state machine.

`settlement(match_id, match_state)` references the match's unique `(match_id,
state)` key and requires `completed` or `aborted`. `outbox(settlement_id,
settlement_state)` similarly references the settlement state and fixes it to
`committed`. A duplicate match/key/source is rejected by a unique constraint;
that alone does not prove a retried payload matches. Settlement inserts start
`pending`, then permit `pending -> applying`, `applying -> retry_wait/committed/
failed` and `retry_wait -> applying/failed`. Starting an application increments
the attempt counter exactly once. A retry's recorded `last_attempt_at` cannot
precede its previous `next_attempt_at` deadline. Identity/request fields and terminal results
are immutable. Terminal settlements cannot be deleted. Outbox identities cannot be deleted in
either delivery state; an archive/retention operation needs a separate policy.
These guards cover ordinary DML, not privileged TRUNCATE/DDL or disabled triggers.
Outbox inserts start pending/unattempted; each update records at
most one attempt, and published events retain immutable identity and payload.
A publication timestamp must not precede the event's `available_at`.
A future write transaction must compare the stored request hash and outcome before reporting idempotent
success. The successful-transfer receipt journal is separate from settlement. There is no reward distribution, durable retry worker,
per-recipient outbox or exactly-once delivery implementation in this change.

The JSONB payload checks require an object of at most 65536 bytes in PostgreSQL's
rendered text representation. They do not validate application-specific fields or
bound the size of an incoming statement before PostgreSQL parses it.

Indexes cover inventory item references, player match history, room match history,
active matches, retryable settlements and pending outbox scans. Their presence is
not a claim about a particular query plan or a million-row performance result.

## Connection and commands

Use an isolated development database when trying `down` or the seed fixture.
Run from the repository root, with Python 3 and matching PostgreSQL 14+ client
binaries installed:

```sh
export PGHOST=/safe/local/postgresql-socket
export PGDATABASE=arena_development
export PGUSER=arena_developer
# Optional when psql is not already on PATH; a psql executable path also works.
export PG_BIN=/usr/lib/postgresql/14/bin
# If authentication needs a password, prefer a private libpq password file.
export PGPASSFILE=/safe/local/arena.pgpass

python3 services/storage/migrate.py status
python3 services/storage/migrate.py up
"$PG_BIN/psql" -X --no-password --set=ON_ERROR_STOP=1 --single-transaction \
  --file=services/storage/fixtures/seed.sql
python3 services/storage/migrate.py down
```

`PGHOST` and `PGDATABASE` are required even for `status`; there is no implicit
current-user/local-database target. Database names must match
`[A-Za-z_][A-Za-z0-9_]{0,62}`; URI/connection-string targets are rejected. `PGHOST`
selects one hostname or socket directory; host lists are not supported. Other
standard libpq authentication/TLS variables are inherited. `PGSERVICE`,
`PGSERVICEFILE` and `PGHOSTADDR` are removed from the child environment to prevent
a service or address override from redirecting the explicit target. The runner
never accepts a password argument, prints the connection environment or forwards
raw libpq/SQL diagnostics. `psql -X` ignores startup files and `--no-password`
disables interactive prompting; authentication may still use libpq's password
file/environment mechanisms. Prefer a password file with the platform's required
private permissions; see the [PostgreSQL password-file documentation](https://www.postgresql.org/docs/14/libpq-pgpass.html).

`PGCONNECT_TIMEOUT` defaults to 5 seconds and may be set to 1..60. Each transaction
uses a 10-second lock timeout and 60-second statement timeout. The subprocess has
a 90-second ceiling. Busy, unavailable or invalid targets fail rather than wait
indefinitely.

## Migration contract

`up`, `down` and `status` each open one Read Committed transaction and acquire
database-scoped `pg_advisory_xact_lock(1095910734, 1296648018)` before reading history. The lock
statement is separate from the subsequent validation so a waiter sees its
predecessor's committed changes under Read Committed. Only cooperating runners
use this advisory protocol; it does not prevent a DBA from issuing unrelated DDL.
The lock releases automatically at transaction end. The reviewed SQL is executed
inside the transaction's anonymous PL/pgSQL block; it is not interpreted as psql
meta-commands and cannot independently commit the migration.

The runner owns `public.arena_schema_migrations` with these five columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `version` | `smallint` PK, positive | Ordered supported versions 1 and 2. |
| `name` | `text NOT NULL` | `001_initial` or `002_asset_transfer`, matching the version. |
| `up_sha256` | `text NOT NULL` | SHA256 of the exact up-file bytes. |
| `down_sha256` | `text NOT NULL` | SHA256 of the exact down-file bytes. |
| `applied_at` | `timestamptz NOT NULL` | Database application time. |

Both checksums must be 64 lowercase hexadecimal characters. Existing column
order/types/NOT NULL properties and the version PK are checked before history is
used. Unknown versions, gaps, renamed migrations and changed up **or down** bytes
fail closed, including for `status`. Accepted history is empty with no `arena`
schema, or a matching ordered prefix `[1]` or `[1,2]` with the schema present.
An existing 001 deployment can be upgraded without rewriting its reviewed files
or data: `up` applies only 002. From an empty database, `up` applies both in one
transaction. `down` rolls every applied migration back in reverse order (002,
then 001), including the migration ledger; it is not a one-version downgrade.
These checks detect migration history/artifact drift; they do not fingerprint
every live business object against arbitrary manual DDL edits. Preserve all
applied migration bytes and add a new reviewed version for future changes.

DDL and the history insert are committed together. An intermediate error leaves
neither a partially applied schema nor a new history table. `status` creates no
persistent objects and changes no history. Repeated `up` is a validated no-op.
`down` removes business objects and then the ledger in the same transaction;
repeated `down` on an empty database is a no-op. Unexpected dependent objects make
the explicit rollback fail and roll back, rather than being swept away by
`CASCADE`. Unrelated objects in `public` are preserved.

**`down` destroys all data in this module.** It is a development rollback, not a
production data-recovery or downgrade strategy. A process/network failure during
COMMIT can leave the caller uncertain even though DDL/history remain atomic in
the database: reconnect and inspect `status`, then retry the same reviewed files.
Do not equate a client timeout with proof that nothing committed.

### Output

Success returns exit code **0** and one JSON object on stdout, after psql confirms
COMMIT. For example:

```json
{"action":"up","state":"applied","version":2,"changed":true,"up_sha256":"<64 hex>","down_sha256":"<64 hex>"}
```

`state` is `applied` or `unapplied`; `version` is 1, 2 or null. `changed` is false for
`status` and repeated no-ops. Hashes identify the applied head; when unapplied (including after `down`), they
identify the latest local supported migration (002), while `version` is null. Failure returns **2**, writes a
short diagnostic to stderr and emits no success JSON. Server errors include a
SQLSTATE when available, without query text, credentials or connection details.

## Runtime integration still required

Before adding an adapter, define durable allocation for IDs that are currently
process-generated, and bind each owner transaction to stable account/player,
match, session and idempotency identities. Local counters/random boot seeds do
not establish durable global uniqueness across restarts. Never hide an ID
collision with `ON CONFLICT DO NOTHING` and call it a successful retry.

Authentication lookups, gateway inventory commands, match result capture, settlement and
outbox delivery remain unwired. The migration CLI performs maintenance DDL only;
it must not be called from a connection owner or the fixed room tick loop.

## Verification

The real PostgreSQL harness creates its own temporary cluster/socket/database;
it does not accept an external database target. CTest registration is opt-in
(default `ARENA_ENABLE_POSTGRES_TESTS=OFF`). Enable it with a complete server/client
bin directory (`PG_BIN` must be a directory for this CMake option):

```sh
cmake -S . -B build -DARENA_ENABLE_POSTGRES_TESTS=ON \
  -DARENA_POSTGRES_BIN="$PG_BIN" \
  -DARENA_POSTGRES_TEST_PYTHON="$PWD/.cache/storage-venv/bin/python"
# Relocated installations may also need -DARENA_POSTGRES_SHARE=/safe/local/pg-share
ctest --test-dir build -R '^(PostgresStorageMigrations|AssetTransferConcurrency|RepositoryTransactionBoundary)$' --output-on-failure
```

Or run the same harness directly:

```sh
python3 tests/storage/StorageMigrationTest.py --repository-root "$PWD" \
  --pg-bin "$PG_BIN" --cycles 20 --report /tmp/arena-storage-migration.json
```

It is responsible for empty/up/seed/down/up cycles, negative PK/FK/UNIQUE/CHECK
cases, uint64/fraction boundaries, concurrent runners, checksum/history refusal
and injected intermediate DDL failures. Catalog/data comparison checks rollback
and cleanup of tables, sequences, functions, triggers and domains while preserving
an unrelated `public` sentinel. Fixture mutation for fault injection occurs only
in a copied storage directory and the isolated cluster. Current measurements and
executed results belong in the task's verification report, not this contract.

## Atomic asset transfers

`assets.transfer(connect, TransferRequest(...), RetryBudget(...))` is a storage
primitive for moving one item quantity between distinct players. The trusted
caller must authorize the source identity. It does not mint rewards, charge
fees, update settlement, or publish events. It has no implicit database target;
`connect()` must return a new, idle, non-autocommit Psycopg connection with the
default tuple row factory and bounded connection establishment. Each worker owns
its connection; no connection or transaction is shared across concurrent calls.
Do not invoke this synchronous path on a network owner or room tick thread.

The transaction inserts a successful-command receipt first using a stable UUID
and `ON CONFLICT DO NOTHING`. A visible existing receipt must match source,
destination, item and quantity exactly before returning `replayed=True`; a
payload mismatch is `idempotency_conflict`. An invisible concurrent conflict at
SERIALIZABLE is retried as a whole transaction. The adapter then ensures both
inventory rows exist and locks them in ascending player-ID order, independent of
transfer direction. It checks source funds, destination signed-bigint capacity
and both revisions before atomically debiting, crediting and incrementing each
revision. No process mutex is needed: PostgreSQL row locks and SERIALIZABLE
isolation protect competing processes as well as threads.

Only acknowledged COMMIT returns a `TransactionResult`. Before COMMIT the runner
requires an active healthy transaction; a callback that swallowed a SQL failure
is rejected as `database_error` / `25P02`, since COMMIT on an aborted transaction
can acknowledge ROLLBACK without raising a driver error. An error rolls back the
receipt, any zero-row creation and both balance changes together. Successful
receipts are immutable, including deletion, so a UUID cannot be reused after a
successful transfer. Rejections are not durably remembered: the same command may
succeed after balances change. A replay returns the original fact of success,
not current balances. The table is a successful-command journal, not a complete
inventory ledger: authorized direct inventory writes are outside this adapter's
conservation guarantee. Direct receipt inserts are trusted maintenance SQL, not
a public business interface. Privileged DDL/TRUNCATE or disabled triggers are
outside the ordinary DML guard boundary.

```python
from uuid import UUID
import psycopg
from services.storage.assets import RetryBudget, TransferRequest, transfer

# Use explicit parameters and a private password file/TLS policy as appropriate.
def connect():
    return psycopg.connect(host="/safe/local/socket", dbname="arena_development",
                           user="arena_worker", connect_timeout=2)

result = transfer(connect, TransferRequest(
    UUID("58393cde-9450-49b4-973d-5b788172ecda"), 42, 43, 1, 10), RetryBudget())
```

### Retry budget

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_attempts` | 8 | Includes the initial execution; supported range 1..64. |
| `max_elapsed` | 15 seconds | Monotonic retry-admission window, including connection, SQL, cleanup and sleeps; maximum 120 seconds. |
| `base_delay` | 10 ms | Exponential full-jitter base. |
| `max_delay` | 500 ms | Per-sleep cap; supported maximum 5 seconds. |
| `lock_timeout_ms` | 1000 | Server lock wait per statement, also clipped to remaining budget. |
| `statement_timeout_ms` | 2000 | Server statement time, also clipped to remaining budget. |

After failed attempt `n` (starting at 1), sleep uniformly in
`[0, min(max_delay, base_delay * 2^(n-1), remaining_budget)]`. Close the failed
connection before sleeping and recheck the monotonic budget before connecting
again. The same immutable request/UUID is reused on every attempt. No nested
retry layer or durable scheduler is provided; callers must not reset this budget
inside an unbounded outer retry loop. This is a per-command budget, not a global
admission/concurrency limiter.

`max_elapsed` prevents new attempts after expiry; it is **not a hard cancellation
deadline** for synchronous libpq, DNS, COMMIT or a callback containing many SQL
statements. The factory must separately bound connection/network waits. Server
lock/statement and idle-in-transaction timeouts are local to each attempt and
must never round down to PostgreSQL's unlimited value zero. An already
acknowledged commit is success even if the admission window has elapsed.

Only `40001` (serialization) and `40P01` (deadlock) retry the complete callback in
a fresh SERIALIZABLE transaction, including errors reported by COMMIT. Never
resume a failed statement or retry external side effects. Internal callbacks may
only execute transactional SQL and must not commit, notify external services or
mutate application state that survives rollback. `run_transaction` exposes this
same boundary for internal storage operations; it is not a sandbox for untrusted
callbacks.

| Failure category | Handling |
| --- | --- |
| `invalid_request` | Bad UUID, IDs, quantity or self-transfer; rejected before connecting. |
| `not_found` | The receipt's player/item FK is missing; unrelated FK failures remain integrity errors. |
| `insufficient_assets`, `quantity_overflow`, `revision_overflow` | Business rejection, entire transaction rolled back; no automatic retry. |
| `idempotency_conflict` | Existing successful UUID has different command fields; no retry. |
| `retry_exhausted` | Attempt limit reached; `last_failure_category` and `sqlstate` retain serialization/deadlock cause. |
| `budget_exhausted` | Admission window elapsed; no new attempt. |
| `lock_timeout` (`55P03`), `statement_timeout` (`57014`) | No automatic retry. The latter also covers server query cancellation. |
| `integrity_error` (class 23), `database_error`, `invalid_connection` | Fail closed; no automatic retry. |
| `connection_error` | Transport/server failure before COMMIT; no automatic retry. |
| `outcome_unknown` | Transport loss while committing; do not assume rollback. Resubmit the same UUID/request to resolve success safely. |

Results and errors expose attempts, monotonic elapsed time, failed SQLSTATEs and
scheduled jitter delays, without forwarding raw SQL/connection diagnostics.
Timeouts and exhausted budgets are observable failures, never hidden success.

`AssetTransferConcurrency` runs real private PostgreSQL transfers, exact balance
oracles and controlled serialization/deadlock sessions. It also tests budget and
jitter boundaries with a deterministic clock. Lost COMMIT acknowledgment is
simulated after a real successful commit, explicitly distinguished from a real
transport fault. See the generated `asset-transfer-report.json` for the executed
counts and failure evidence. Install the pinned driver before enabling storage
tests; missing dependencies cause configure failure rather than a silent skip.

## Repository and pooled transaction interface

Install the pinned `psycopg` and `psycopg_pool` versions in `requirements.txt`.
This is a synchronous Python storage interface; the C++ gateway remains unwired.
Call it from a storage worker, not a connection owner or room tick thread.

```python
from services.storage.database import Database
from services.storage.application import register_player

with Database(kwargs={"host": "/safe/local/socket", "dbname": "arena_development",
                      "user": "arena_worker", "connect_timeout": 2},
              max_size=4, acquire_timeout=.25, max_waiting=16) as database:
    result = register_player(database, account_id=7, account_name="alice7",
                             password_digest=prepared_digest, player_id=47,
                             display_name="Alice", mmr=1500)
    account, player = result.value
    found = database.run(lambda work: work.accounts.find_by_name("alice7")).value
```

`Database.run(callback, budget)` acquires one loan per attempt, starts the same
SERIALIZABLE executor used by asset transfers, invokes the callback with a
UnitOfWork, verifies the transaction is healthy and commits once. Account,
Player, Match and optional `work.transfer(request)` operations within that
callback share one transaction. Repositories never acquire, commit or roll back
independently; business callers do not build SQL or receive a connection/cursor.
Account and Match records are detached frozen dataclasses; match rosters are
ordered immutable tuples. Inputs retain the schema's full uint64 identities.

Repositories are valid only in their callback and owning thread. They are revoked
before COMMIT and on every exception or retry; retained references fail with
`scope_closed`, cross-thread access with `scope_thread`. Nested runs on the same
Database fail with `nested_transaction`, and async/generator callbacks or results
with `invalid_operation`. Python private attributes are an ownership contract,
not a sandbox against hostile introspection. Callback side effects outside SQL
remain prohibited because an entire callback can run again.

AccountRepository supports create/get/find_by_name, state changes, player
creation and lookup. MatchRepository supports create/get/start/finish/abort; it
locks the parent before reading or modifying a roster, uses explicit transitions
and preserves the database guards. Use create's optional `created_at` for a
historical event so a later `started_at` does not precede database creation time.
Missing lookups return None; missing mutation targets raise `not_found`.

The default pool has min_size=0, max_size=4, max_waiting=16 and a 250 ms checkout
queue timeout. Both capacity and queue length are finite. Checkout timeout is
clipped to the transaction's remaining retry-admission budget, which is checked
again after checkout. It bounds waiting for pool capacity, not hard cancellation
of libpq/health-check I/O; connection establishment and server SQL have separate
timeouts. Connections have a checkout health check; a broken idle connection is
discarded. Timeouts/overload do not trigger automatic business transaction retry.

- `pool_timeout`: no healthy loan obtained within the checkout waiting limit.
  It may indicate saturation or an unavailable database; it does not assert the
  database is down.
- `pool_overloaded`: the SDK waiting queue reached max_waiting.
- `pool_closed`: explicit shutdown or a poisoned pool after an SDK return error.
- Other business, SQLSTATE, commit-unknown and retry errors follow the shared
  transaction contract above; `StorageError` is the common error type, also
  exported as the existing `assets.TransferError` name.

After a longer outage Psycopg 3.2.8 can retain expired waiters after its reconnect
attempt gives up. A failed checkout on an empty pool calls public `check()` to
restart background growth, retaining the original failure classification. This
allows later calls to recover with the same Database without manipulating SDK
queue internals or retrying a failed business command invisibly.

Successful loans return IDLE with connection properties reset. Failed attempts
roll back before return; broken connections and commit failures are quarantined.
Each successful getconn has one putconn attempt. Cleanup errors never replace an
acknowledged commit or the original failure. If SDK return itself fails, the
connection is quarantined, `return_failures` increments and the pool closes to
new work instead of silently losing capacity. Normal shutdown wakes waiters and
rejects new requests; active callbacks finish and close their loans on return.
Join those callers before asserting that all resources are gone.

`Database.stats` combines SDK counters with `checked_out`, `return_failures` and
`closed`. Samples during concurrent mutation are observational; assert exact
capacity invariants after work quiesces. `_lease` is for internal infrastructure
and fault tests only. Public pool recovery does not reset the business retry
budget or add a second commit owner.

The `RepositoryTransactionBoundary` CTest target owns a private PostgreSQL
cluster and covers repository roundtrips, cross-repository rollback, scoped
access, bounded pool saturation and shutdown, repeated database stop/restart,
real deferred COMMIT failures, real COMMIT serialization conflict and simulated
lost acknowledgment after a real commit. Cleanup injections are explicitly
labeled. Generated reports live under `build/tests/storage/`; no external
database is accepted by these tests.
