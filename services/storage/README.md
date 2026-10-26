# PostgreSQL storage model

This module provides the reviewed PostgreSQL 14+ schema and one migration CLI.
It does not yet provide a C++ database adapter, connection pool, settlement worker
or outbox publisher. Gateway still uses its file account snapshot and in-memory
session, match and room services. Applying this schema does not make those paths
persistent.

## Files and ownership

- `migrations/001_initial.up.sql`: the `arena` schema, domains, eight business
  tables, indexes and four integrity/state guards.
- `migrations/001_initial.down.sql`: destructive rollback in explicit dependency
  order, including functions, domains and the schema. It uses no `CASCADE`.
- `migrate.py`: Python standard-library orchestration of one `psql` process.
  Only version 001 is supported; it does not discover arbitrary migrations.
- `fixtures/seed.sql`: separate, deterministic test data. It is not run by `up`
  and must not be treated as production accounts or runtime settlement output.

SQL files contain no `BEGIN` or `COMMIT`: the runner owns the transaction. They
are trusted, reviewed artifacts, not user-supplied SQL. No ORM or Python database
package is needed.

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
success. There is no asset ledger, reward distribution, durable retry worker,
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
| `version` | `smallint` PK, positive | The sole supported value is 1. |
| `name` | `text NOT NULL` | Must be `001_initial`. |
| `up_sha256` | `text NOT NULL` | SHA256 of the exact up-file bytes. |
| `down_sha256` | `text NOT NULL` | SHA256 of the exact down-file bytes. |
| `applied_at` | `timestamptz NOT NULL` | Database application time. |

Both checksums must be 64 lowercase hexadecimal characters. Existing column
order/types/NOT NULL properties and the version PK are checked before history is
used. Unknown versions, extra history rows, renamed migrations and changed up
**or down** bytes fail closed, including for `status`. The only accepted history
states are no ledger/empty ledger with no `arena` schema, or one matching 001 row
with the schema present. A schema without matching history is not adopted or
automatically dropped. These checks detect migration history/artifact drift;
they do not fingerprint every business table against arbitrary manual DDL edits.
Once applied, preserve the reviewed 001 files rather than editing them in place.

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
{"action":"up","state":"applied","version":1,"changed":true,"up_sha256":"<64 hex>","down_sha256":"<64 hex>"}
```

`state` is `applied` or `unapplied`; `version` is 1 or null. `changed` is false for
`status` and repeated no-ops. After `down`, the reported hashes still identify the
local reviewed files, while `version` is null. Failure returns **2**, writes a
short diagnostic to stderr and emits no success JSON. Server errors include a
SQLSTATE when available, without query text, credentials or connection details.

## Runtime integration still required

Before adding an adapter, define durable allocation for IDs that are currently
process-generated, and bind each owner transaction to stable account/player,
match, session and idempotency identities. Local counters/random boot seeds do
not establish durable global uniqueness across restarts. Never hide an ID
collision with `ON CONFLICT DO NOTHING` and call it a successful retry.

Authentication lookups, inventory mutation, match result capture, settlement and
outbox delivery remain unwired. The migration CLI performs maintenance DDL only;
it must not be called from a connection owner or the fixed room tick loop.

## Verification

The real PostgreSQL harness creates its own temporary cluster/socket/database;
it does not accept an external database target. CTest registration is opt-in
(default `ARENA_ENABLE_POSTGRES_TESTS=OFF`). Enable it with a complete server/client
bin directory (`PG_BIN` must be a directory for this CMake option):

```sh
cmake -S . -B build -DARENA_ENABLE_POSTGRES_TESTS=ON \
  -DARENA_POSTGRES_BIN="$PG_BIN"
# Relocated installations may also need -DARENA_POSTGRES_SHARE=/safe/local/pg-share
ctest --test-dir build -R '^PostgresStorageMigrations$' --output-on-failure
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
