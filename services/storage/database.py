"""Bounded PostgreSQL pool and transaction-scoped repository access."""
from contextlib import contextmanager
import inspect
import math
import re
import threading
import time

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg_pool import ConnectionPool, PoolClosed, PoolTimeout, TooManyRequests

from .transactions import RetryBudget, StorageError, _Lease, _run_transaction
from .repositories import AccountRepository, MatchRepository
from .settlement import SettlementRepository
from .outbox import OutboxRepository
from .ids import IdRepository
from .deadlines import RequestGuard, GuardedPool, connection_type, current_guard, guard_scope


class _Session:
    """Revoke repository access before commit or connection reuse."""
    def __init__(self, connection):
        self._connection = connection
        self._owner = threading.get_ident()
        self._active = True

    def connection(self):
        if not self._active:
            raise StorageError("scope_closed")
        if self._owner != threading.get_ident():
            raise StorageError("scope_thread")
        return self._connection

    def revoke(self):
        self._active = False
        self._connection = None


class UnitOfWork:
    """Only repositories and materialized results cross the business interface."""
    def __init__(self, session):
        self._session = session
        self.accounts = AccountRepository(session)
        self.matches = MatchRepository(session)
        self.settlements = SettlementRepository(session)
        self.outbox = OutboxRepository(session)
        self.ids = IdRepository(session)

    def transfer(self, request):
        from .assets import _apply, _validate
        connection = self._session.connection()
        _validate(request)
        return _apply(connection, request)


def _positive_timeout(value, upper=120):
    if type(value) not in (int, float) or not 0 < value <= upper or not math.isfinite(value):
        raise ValueError("timeout must be finite and positive within supported bounds")
    return float(value)


class Database:
    """Own a bounded pool; run one business callback in one atomic transaction.

    Checkout includes a guarded queue and health check. Request-aware I/O,
    commit and cleanup run on the borrower thread. close() rejects new work
    and wakes waiters; outstanding borrowers retain ownership until release.
    """
    def __init__(self, *, kwargs, min_size=0, max_size=4, acquire_timeout=.25,
                 max_waiting=16, connection_class=psycopg.Connection):
        if (type(min_size) is not int or type(max_size) is not int
                or not 0 <= min_size <= max_size <= 128 or max_size < 1
                or type(max_waiting) is not int or not 1 <= max_waiting <= 4096):
            raise ValueError("invalid bounded pool size or waiting limit")
        self.acquire_timeout = _positive_timeout(acquire_timeout)
        parameters = dict(kwargs)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", parameters.get("dbname", "")):
            raise ValueError("an explicit simple database name is required")
        host, user = parameters.get("host"), parameters.get("user")
        if (not isinstance(host, str) or not host or host != host.strip()
                or len(host) > 1024 or any(c in host for c in "\0\r\n=,")
                or not isinstance(user, str) or not user):
            raise ValueError("an explicit single host and user are required")
        connect_timeout = parameters.get("connect_timeout", 5)
        if type(connect_timeout) is not int or not 1 <= connect_timeout <= 60:
            raise ValueError("connect_timeout must be between 1 and 60 seconds")
        if any(k in parameters for k in ("autocommit", "row_factory", "options")):
            raise ValueError("transaction and session defaults are owned by Database")
        parameters.update(connect_timeout=connect_timeout, autocommit=False,
                          row_factory=tuple_row,
                          options="-c statement_timeout=2000 -c lock_timeout=1000")
        parameters.setdefault("application_name", "arena-storage")
        self._lock = threading.Lock()
        self._local = threading.local()
        self._checked_out = 0
        self._return_failures = 0
        self._poisoned = False
        self._pool = GuardedPool(kwargs=parameters, connection_class=connection_type(connection_class),
            min_size=min_size, max_size=max_size, max_waiting=max_waiting,
            timeout=self.acquire_timeout, reconnect_timeout=2, num_workers=2,
            check=None, open=True)

    @property
    def stats(self):
        result = self._pool.get_stats()
        with self._lock:
            result.update(checked_out=self._checked_out, return_failures=self._return_failures)
        result["closed"] = self._pool.closed
        return result

    def _nudge_reconnect(self):
        # Public check() restarts empty-pool growth after reconnect gives up,
        # without admitting a new loan or resetting the request deadline.
        if not self._pool.closed and self._pool.get_stats().get("pool_size") == 0:
            try:
                self._pool.check()
            except (psycopg.Error, OSError):
                pass

    def _borrow(self, timeout):
        timeout = min(self.acquire_timeout, _positive_timeout(timeout))
        if self._poisoned:
            raise StorageError("pool_closed")
        parent = current_guard()
        guard = RequestGuard(time.monotonic()+timeout, parent=parent,
                             expiry_category="pool_timeout")
        with guard_scope(guard):
            while True:
                guard.check()
                try:
                    connection = self._pool.getconn(timeout=max(.000001, guard.deadline-time.monotonic()))
                except PoolTimeout:
                    self._nudge_reconnect()
                    if parent is not None:
                        parent.check_cancelled()
                    raise StorageError("pool_timeout") from None
                except TooManyRequests:
                    self._nudge_reconnect()
                    if parent is not None:
                        parent.check_cancelled()
                    raise StorageError("pool_overloaded") from None
                except PoolClosed:
                    raise StorageError("pool_closed") from None
                except StorageError:
                    self._nudge_reconnect()
                    raise
                loan = self._account_loan(connection)
                try:
                    guard.check()
                    # The SDK swallows health-check exceptions and backs off.
                    # Own this check after accounting so cancellation can return
                    # exactly one loan and cannot enter that unbounded path.
                    ConnectionPool.check_connection(connection)
                    guard.check()
                    return loan
                except StorageError:
                    loan.release(discard=True)
                    raise
                except (psycopg.Error, OSError):
                    loan.release(discard=True)
                    # Replacing a stale idle backend is a read-only checkout
                    # action, bounded by the original checkout deadline.
                    guard.check()
                except BaseException:
                    loan.release(discard=True)
                    raise

    def _account_loan(self, connection):
        with self._lock:
            self._checked_out += 1

        def release(discard):
            def quarantine():
                try:
                    connection.close()
                except Exception:
                    try:
                        connection.pgconn.finish()
                    except Exception:
                        pass
            try:
                try:
                    if discard or connection.closed:
                        quarantine()
                    else:
                        if connection.info.transaction_status != TransactionStatus.IDLE:
                            connection.rollback()
                        if connection.info.transaction_status != TransactionStatus.IDLE:
                            quarantine()
                        else:
                            connection.autocommit = False
                            connection.row_factory = tuple_row
                            connection.isolation_level = None
                            connection.read_only = False
                            connection.deferrable = False
                except Exception:
                    quarantine()
                try:
                    self._pool.putconn(connection)
                except Exception:
                    # Cleanup must never turn an acknowledged commit into a retry.
                    quarantine()
                    with self._lock:
                        self._return_failures += 1
                        self._poisoned = True
                    try:
                        self._pool.close(timeout=0)
                    except Exception:
                        pass
            finally:
                with self._lock:
                    self._checked_out -= 1
        return _Lease(connection, release)

    @contextmanager
    def _lease(self, timeout=None):
        """Infrastructure-only borrowing; business callers use run()."""
        loan = self._borrow(self.acquire_timeout if timeout is None else timeout)
        try:
            yield loan.connection
        finally:
            loan.release()

    def run(self, operation, budget=RetryBudget(), *, _request_guard=None, **retry_options):
        if getattr(self._local, "running", False):
            raise StorageError("nested_transaction")
        if (not callable(operation) or inspect.iscoroutinefunction(operation)
                or inspect.isgeneratorfunction(operation) or inspect.isasyncgenfunction(operation)):
            raise StorageError("invalid_operation")
        def invoke(connection):
            session = _Session(connection)
            try:
                value = operation(UnitOfWork(session))
                if inspect.isawaitable(value) or inspect.isgenerator(value) or inspect.isasyncgen(value):
                    if inspect.iscoroutine(value) or inspect.isgenerator(value):
                        value.close()
                    raise StorageError("invalid_operation")
                return value
            finally:
                session.revoke()
        self._local.running = True
        parent = current_guard()
        guard = _request_guard or RequestGuard(time.monotonic()+budget.max_elapsed,
                                              parent=parent)
        try:
            with guard_scope(guard):
                return _run_transaction(self._borrow, invoke, budget, _guard=guard, **retry_options)
        finally:
            self._local.running = False

    def close(self):
        self._pool.close(timeout=3)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
