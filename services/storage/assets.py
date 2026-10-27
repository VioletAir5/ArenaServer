"""Atomic inventory transfers and bounded whole-transaction PostgreSQL retries.

This synchronous storage entry point belongs on a worker, never a room tick or
connection owner. The connection factory must return a fresh, idle Psycopg
connection with bounded connection establishment. Operations passed to
run_transaction must only perform transactional SQL: no COMMIT, external I/O,
notifications, or mutation of process state that would survive a rollback.
"""

from dataclasses import dataclass
import math
import random
import time
from typing import Callable, Generic, TypeVar
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus

MAX_QUANTITY = (1 << 63) - 1
MAX_ENTITY_ID = (1 << 64) - 1
RETRYABLE = {"40001": "serialization", "40P01": "deadlock"}
T = TypeVar("T")


@dataclass(frozen=True)
class RetryBudget:
    """Attempt and elapsed retry-admission limits; delays use full jitter.

    max_attempts includes the first execution. max_elapsed includes connection,
    SQL, cleanup and sleep time, and prevents admission of another attempt after
    expiry. It is not a hard cancellation deadline for synchronous libpq calls.
    Server timeouts bound individual SQL/lock waits, not an entire multi-query
    transaction. The caller must separately bound connection/network waits.
    """

    max_attempts: int = 8
    max_elapsed: float = 15.0
    base_delay: float = 0.01
    max_delay: float = 0.5
    lock_timeout_ms: int = 1000
    statement_timeout_ms: int = 2000

    def __post_init__(self):
        for name, low, high in (("max_attempts", 1, 64), ("lock_timeout_ms", 1, 60000),
                                ("statement_timeout_ms", 1, 60000)):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError("invalid retry budget: " + name)
        for name, upper in (("max_elapsed", 120), ("base_delay", 5), ("max_delay", 5)):
            value = getattr(self, name)
            if type(value) not in (int, float) or not 0 < value <= upper or not math.isfinite(value):
                raise ValueError("invalid retry budget: " + name)
        if self.max_elapsed > 120 or not self.base_delay <= self.max_delay <= 5:
            raise ValueError("retry budget exceeds supported bounds")


@dataclass(frozen=True)
class TransferRequest:
    """A stable command identity and exact, positive integral item quantity."""

    transfer_id: UUID
    source_player_id: int
    destination_player_id: int
    item_id: int
    quantity: int


@dataclass(frozen=True)
class TransferReceipt:
    """Confirmation of a committed transfer, including deduplicated successes."""

    transfer_id: UUID
    replayed: bool


@dataclass(frozen=True)
class TransactionResult(Generic[T]):
    """Only returned after PostgreSQL confirms COMMIT."""

    value: T
    attempts: int
    elapsed: float
    failures: tuple[str, ...]
    delays: tuple[float, ...]


class TransferError(Exception):
    """Stable public classification without raw server diagnostics or SQL."""

    def __init__(self, category, *, sqlstate=None, attempts=0, elapsed=0.0,
                 failures=(), delays=(), last_failure_category=None):
        super().__init__(category)
        self.category = category
        self.sqlstate = sqlstate
        self.attempts = attempts
        self.elapsed = elapsed
        self.failures = tuple(failures)
        self.delays = tuple(delays)
        self.last_failure_category = last_failure_category or category


def classify_sqlstate(state):
    """Only explicit transaction-abort classes are automatically retryable."""
    if state in RETRYABLE:
        return RETRYABLE[state]
    if state == "55P03":
        return "lock_timeout"
    if state == "57014":
        return "statement_timeout"
    if state and state.startswith("23"):
        return "integrity_error"
    if state and (state.startswith("08") or state in ("57P01", "57P02", "57P03")):
        return "connection_error"
    return "database_error"


def run_transaction(connect: Callable[[], psycopg.Connection],
                    operation: Callable[[psycopg.Connection], T],
                    budget: RetryBudget = RetryBudget(), *, clock=time.monotonic,
                    sleep=time.sleep, random_value=random.SystemRandom().random
                    ) -> TransactionResult[T]:
    """Retry the entire operation in a fresh SERIALIZABLE transaction.

    A connection/transport failure after COMMIT is sent is outcome_unknown and
    never retried automatically. Re-submit the same TransferRequest identity to
    resolve it safely. Retries occur only after rollback/close releases locks.
    """
    started = clock()
    attempts, failures, delays = 0, [], []
    last_state, last_category = None, None

    def failure(category):
        return TransferError(category, sqlstate=last_state, attempts=attempts,
                             elapsed=max(0.0, clock() - started), failures=failures,
                             delays=delays, last_failure_category=last_category)

    while True:
        remaining = budget.max_elapsed - (clock() - started)
        if remaining <= 0:
            raise failure("budget_exhausted")
        attempts += 1
        connection, committing, caught = None, False, None
        try:
            connection = connect()
            if connection.closed or connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
                raise TransferError("invalid_connection")
            remaining = budget.max_elapsed - (clock() - started)
            if remaining <= 0:
                raise TransferError("budget_exhausted")
            connection.isolation_level = psycopg.IsolationLevel.SERIALIZABLE
            connection.read_only = False
            connection.deferrable = False
            timeout = max(1, int(remaining * 1000))
            connection.execute("SELECT set_config('lock_timeout', %s, true), "
                               "set_config('statement_timeout', %s, true), "
                               "set_config('idle_in_transaction_session_timeout', %s, true)",
                               (str(min(budget.lock_timeout_ms, timeout)),
                                str(min(budget.statement_timeout_ms, timeout)), str(timeout)))
            value = operation(connection)
            # COMMIT on an aborted PostgreSQL transaction acknowledges ROLLBACK.
            # A callback swallowing a SQL error must never become false success.
            status = connection.info.transaction_status
            if status == TransactionStatus.INERROR:
                raise TransferError("database_error", sqlstate="25P02")
            if status != TransactionStatus.INTRANS:
                raise TransferError("invalid_connection")
            committing = True
            connection.commit()
            # Once commit is acknowledged, even an expired budget means success.
            return TransactionResult(value, attempts, max(0.0, clock() - started),
                                     tuple(failures), tuple(delays))
        except TransferError as error:
            caught = error
            last_state, last_category = error.sqlstate, error.category
        except (psycopg.Error, OSError) as error:
            last_state = getattr(error, "sqlstate", None)
            last_category = classify_sqlstate(last_state)
            if last_state is None and isinstance(error, (psycopg.OperationalError, OSError)):
                last_category = "connection_error"
            if committing and (last_category == "connection_error" or last_state is None):
                last_category = "outcome_unknown"
            caught = TransferError(last_category, sqlstate=last_state)
        finally:
            if connection is not None:
                # Close, rather than reuse, a failed session or an unknown outcome.
                try:
                    connection.close()
                except (psycopg.Error, OSError):
                    pass
        if last_state:
            failures.append(last_state)
        if last_state not in RETRYABLE or caught.category == "outcome_unknown":
            raise failure(caught.category) from None
        if attempts >= budget.max_attempts:
            raise failure("retry_exhausted") from None
        remaining = budget.max_elapsed - (clock() - started)
        if remaining <= 0:
            raise failure("budget_exhausted") from None
        ceiling = min(budget.max_delay, budget.base_delay * (2 ** (attempts - 1)), remaining)
        sample = random_value()
        if not math.isfinite(sample) or not 0 <= sample <= 1:
            raise ValueError("random_value must return a finite value in [0, 1]")
        delay = ceiling * sample
        delays.append(delay)
        sleep(delay)


def _validate(request):
    if not isinstance(request, TransferRequest) or not isinstance(request.transfer_id, UUID) or request.transfer_id.int == 0:
        raise TransferError("invalid_request")
    for value in (request.source_player_id, request.destination_player_id, request.item_id):
        if type(value) is not int or not 1 <= value <= MAX_ENTITY_ID:
            raise TransferError("invalid_request")
    if (type(request.quantity) is not int or not 1 <= request.quantity <= MAX_QUANTITY
            or request.source_player_id == request.destination_player_id):
        raise TransferError("invalid_request")


def _apply(connection, request):
    fields = (request.source_player_id, request.destination_player_id, request.item_id, request.quantity)
    # Reserve the successful command identity first. Any rejection rolls this back.
    try:
        inserted = connection.execute(
            "INSERT INTO arena.asset_transfer "
            "(transfer_id, source_player_id, destination_player_id, item_id, quantity) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (transfer_id) DO NOTHING RETURNING transfer_id",
            (request.transfer_id, *fields)).fetchone()
    except psycopg.errors.ForeignKeyViolation as error:
        if error.diag.constraint_name in ("asset_transfer_source_fk", "asset_transfer_destination_fk",
                                          "asset_transfer_item_fk"):
            raise TransferError("not_found", sqlstate="23503") from None
        raise
    if inserted is None:
        stored = connection.execute(
            "SELECT source_player_id, destination_player_id, item_id, quantity "
            "FROM arena.asset_transfer WHERE transfer_id=%s", (request.transfer_id,)).fetchone()
        if stored is None or tuple(stored) != fields:
            raise TransferError("idempotency_conflict")
        return TransferReceipt(request.transfer_id, True)

    balances = {}
    # Use one order for both missing-row insertion and row locking in either direction.
    for player in sorted((request.source_player_id, request.destination_player_id)):
        connection.execute("INSERT INTO arena.inventory (player_id,item_id) VALUES (%s,%s) "
                           "ON CONFLICT (player_id,item_id) DO NOTHING", (player, request.item_id))
        balances[player] = connection.execute(
            "SELECT quantity, revision FROM arena.inventory WHERE player_id=%s AND item_id=%s FOR UPDATE",
            (player, request.item_id)).fetchone()
    source, destination = balances[request.source_player_id], balances[request.destination_player_id]
    if source[0] < request.quantity:
        raise TransferError("insufficient_assets")
    if destination[0] > MAX_QUANTITY - request.quantity:
        raise TransferError("quantity_overflow")
    if source[1] == MAX_QUANTITY or destination[1] == MAX_QUANTITY:
        raise TransferError("revision_overflow")
    for player, delta in ((request.source_player_id, -request.quantity),
                          (request.destination_player_id, request.quantity)):
        connection.execute("UPDATE arena.inventory SET quantity=quantity+%s, revision=revision+1, "
                           "updated_at=clock_timestamp() WHERE player_id=%s AND item_id=%s",
                           (delta, player, request.item_id))
    return TransferReceipt(request.transfer_id, False)


def transfer(connect: Callable[[], psycopg.Connection], request: TransferRequest,
             budget: RetryBudget = RetryBudget(), **retry_options
             ) -> TransactionResult[TransferReceipt]:
    """Move one item atomically; successful identities are permanently deduplicated.

    Rejections have no durable receipt, so the same request may succeed after
    inventory changes. This primitive does not authorize the source player or
    publish events; the authenticated caller must supply that business boundary.
    """
    _validate(request)
    return run_transaction(connect, lambda connection: _apply(connection, request), budget, **retry_options)
