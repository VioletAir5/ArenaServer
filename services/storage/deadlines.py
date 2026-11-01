"""Owner-thread cancellation and finite waits for the pinned PostgreSQL driver."""
from contextlib import contextmanager
import selectors
import threading
import time

import psycopg
from psycopg import pq
from psycopg_pool import ConnectionPool, PoolTimeout
from psycopg_pool.pool import WaitingClient

from .transactions import StorageError

POLL_INTERVAL = .005
CANCEL_TIMEOUT = .05
_active = threading.local()


class RequestGuard:
    """Carry one immutable deadline and a cancellation event into sync I/O."""
    def __init__(self, deadline, cancelled=None, *, parent=None,
                 expiry_category="budget_exhausted"):
        self.deadline = min(deadline, parent.deadline) if parent is not None else deadline
        self.cancelled = cancelled if cancelled is not None else threading.Event()
        self.parent = parent
        self.expiry_category = expiry_category

    def check_cancelled(self):
        if self.parent is not None:
            self.parent.check_cancelled()
        if self.cancelled.is_set():
            raise StorageError("request_cancelled")

    def check(self):
        self.check_cancelled()
        if time.monotonic() >= self.deadline:
            raise StorageError(self.expiry_category)

    def sleep(self, delay):
        end = min(self.deadline, time.monotonic()+delay)
        while True:
            self.check()
            remaining = end-time.monotonic()
            if remaining <= 0:
                return
            self.cancelled.wait(min(POLL_INTERVAL, remaining))


def current_guard():
    return getattr(_active, "guard", None)


@contextmanager
def guard_scope(guard):
    """Bind before checkout, including its health check, and through cleanup."""
    previous = current_guard()
    _active.guard = guard
    try:
        yield
    finally:
        _active.guard = previous


def require_bounded_cancel():
    if not psycopg.capabilities.has_cancel_safe():
        raise ValueError("Storage workers require libpq 17+; install pinned psycopg[binary]")


def _cancel_and_close(connection):
    """Only the query owner touches the main PGconn; no cancel task survives."""
    cancel = None
    try:
        if connection.closed:
            return
        # A separate cancellation socket is polled with our own deadline.
        # The pinned driver's cancel_safe uses a 100 ms polling interval.
        if psycopg.capabilities.has_cancel_safe():
            end = time.monotonic()+CANCEL_TIMEOUT
            cancel = connection.pgconn.cancel_conn()
            cancel.start()
            with selectors.DefaultSelector() as selector:
                while time.monotonic() < end:
                    state = cancel.poll()
                    if state in (pq.PollingStatus.OK, pq.PollingStatus.FAILED):
                        break
                    events = selectors.EVENT_READ if state == pq.PollingStatus.READING else selectors.EVENT_WRITE
                    # libpq may replace this socket between poll calls.
                    descriptor = cancel.socket
                    selector.register(descriptor, events)
                    selector.select(min(POLL_INTERVAL, max(0, end-time.monotonic())))
                    selector.unregister(descriptor)
    except Exception:
        # Dispatch success is not proof of server cancellation. Server-side
        # statement timeouts remain independent protection during a partition.
        pass
    finally:
        try:
            if cancel is not None:
                cancel.finish()
        except Exception:
            pass
        finally:
            try:
                connection.close()
            except Exception:
                try:
                    connection.pgconn.finish()
                except Exception:
                    pass


class _OwnerWait:
    """Consume nonblocking driver generators with request-aware polling."""
    def wait(self, gen, interval=.1):
        guard = current_guard()
        if guard is None:
            return super().wait(gen, interval=interval)
        try:
            guard.check()
            state = next(gen)
            descriptor = self.pgconn.socket
            with selectors.DefaultSelector() as selector:
                while True:
                    guard.check()
                    selector.register(descriptor, int(state))
                    events = selector.select(min(POLL_INTERVAL, max(0, guard.deadline-time.monotonic())))
                    selector.unregister(descriptor)
                    guard.check()
                    # A generator which returns has already parsed its result.
                    # In particular, never turn an acknowledged COMMIT into an
                    # error by checking the deadline after StopIteration.
                    state = gen.send(events[0][1] if events else 0)
        except StopIteration as done:
            return done.value
        except StorageError:
            try:
                gen.close()
            finally:
                _cancel_and_close(self)
            raise


def connection_type(base):
    """Preserve supplied fault-test commit/rollback/close overrides."""
    if not isinstance(base, type) or not issubclass(base, psycopg.Connection):
        raise ValueError("connection_class must derive from psycopg.Connection")
    return type("OwnerWait"+base.__name__, (_OwnerWait, base), {})


class _GuardedWaiter(WaitingClient):
    """A single cancellable pool waiter, rather than repeated timed-out loans."""
    def __init__(self, guard):
        super().__init__()
        self.guard = guard

    def wait(self, timeout):
        end = time.monotonic()+timeout
        with self._cond:
            while self.conn is None and self.error is None:
                try:
                    self.guard.check()
                    remaining = end-time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout("pool checkout deadline expired")
                    self._cond.wait(min(POLL_INTERVAL, remaining))
                except Exception as error:
                    self.error = error
            # A concurrently assigned connection must reach the borrower,
            # which accounts and releases it even when cancellation just won.
            if self.conn is not None:
                return self.conn
            raise self.error


class GuardedPool(ConnectionPool):
    """Extend pinned psycopg_pool 3.2.8's queue with owner cancellation.

    Its public getconn timeout cannot wake early for a cancellation event.
    This override retains the SDK's queue, capacity and loan handoff protocol;
    it changes only the waiter and removes our retired queue entry promptly.
    Health checks run in Database after the loan is accounted.
    """
    def _getconn_unchecked(self, timeout):
        guard = current_guard()
        if guard is None:
            return super()._getconn_unchecked(timeout)
        guard.check()
        with self._lock:
            connection = self._get_ready_connection(timeout)
            if connection is None:
                started = time.monotonic()
                waiter = _GuardedWaiter(guard)
                self._waiting.append(waiter)
                self._stats[self._REQUESTS_QUEUED] += 1
                self._maybe_grow_pool()
        if connection is None:
            try:
                connection = waiter.wait(timeout)
            except Exception:
                self._stats[self._REQUESTS_ERRORS] += 1
                raise
            finally:
                self._stats[self._REQUESTS_WAIT_MS] += int(1000*(time.monotonic()-started))
                with self._lock:
                    try:
                        self._waiting.remove(waiter)
                    except ValueError:
                        pass  # The returning connection already removed it.
        connection._pool = self
        return connection