"""One owner for transaction retries, error classification and connection release."""
from dataclasses import dataclass
import math
import random
import time
from typing import Callable, Generic, TypeVar

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

RETRYABLE = {"40001": "serialization", "40P01": "deadlock"}
T = TypeVar("T")


class _Lease:
    """Own exactly one release action, including early callback exceptions."""
    def __init__(self, connection, release):
        self.connection = connection
        self._release = release
        self._released = False

    def release(self, *, discard=False):
        if self._released:
            return
        self._released = True
        self._release(discard)


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
class TransactionResult(Generic[T]):
    """Only returned after PostgreSQL confirms COMMIT."""

    value: T
    attempts: int
    elapsed: float
    failures: tuple[str, ...]
    delays: tuple[float, ...]


class StorageError(Exception):
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


def _run_transaction(acquire,
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
        return StorageError(category, sqlstate=last_state, attempts=attempts,
                             elapsed=max(0.0, clock() - started), failures=failures,
                             delays=delays, last_failure_category=last_category)

    while True:
        remaining = budget.max_elapsed - (clock() - started)
        if remaining <= 0:
            raise failure("budget_exhausted")
        attempts += 1
        connection, loan, committing, acknowledged, caught = None, None, False, False, None
        try:
            loan = acquire(remaining)
            connection = loan.connection
            if connection.closed or connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
                raise StorageError("invalid_connection")
            remaining = budget.max_elapsed - (clock() - started)
            if remaining <= 0:
                raise StorageError("budget_exhausted")
            connection.row_factory = tuple_row
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
                raise StorageError("database_error", sqlstate="25P02")
            if status != TransactionStatus.INTRANS:
                raise StorageError("invalid_connection")
            committing = True
            connection.commit()
            acknowledged = True
            # Once commit is acknowledged, even an expired budget means success.
            return TransactionResult(value, attempts, max(0.0, clock() - started),
                                     tuple(failures), tuple(delays))
        except StorageError as error:
            caught = error
            last_state, last_category = error.sqlstate, error.category
        except (psycopg.Error, OSError) as error:
            last_state = getattr(error, "sqlstate", None)
            last_category = classify_sqlstate(last_state)
            if last_state is None and isinstance(error, (psycopg.OperationalError, OSError)):
                last_category = "connection_error"
            if committing and (last_category == "connection_error" or last_state is None):
                last_category = "outcome_unknown"
            caught = StorageError(last_category, sqlstate=last_state)
        finally:
            if loan is not None:
                loan.release(discard=committing and not acknowledged)
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


def run_transaction(connect: Callable[[], psycopg.Connection], operation,
                    budget: RetryBudget = RetryBudget(), **options):
    """Run transactional SQL with a fresh connection on every attempt.

    Pooled callers must use Database.run; returning a pool checkout from connect
    would bypass its matching putconn and deplete the pool.
    """
    def acquire(remaining):
        connection = connect()
        def release(discard):
            try:
                connection.close()
            except (psycopg.Error, OSError):
                pass
        return _Lease(connection, release)
    return _run_transaction(acquire, operation, budget, **options)
