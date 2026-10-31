"""Finite worker admission and cancellation fenced to the owning pool loan."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

from shared.runtime.rpc import RpcError, DEADLINE_EXCEEDED, OVERLOADED, UNAVAILABLE
from .transactions import RetryBudget, StorageError


class _ConnectionObserver:
    """Serialize cancellation with detachment before a pool loan can be reused."""
    def __init__(self, context):
        self.context = context
        self.cancelled = threading.Event()
        self._lock = threading.Lock()
        self._connection = None

    def check(self):
        if self.cancelled.is_set():
            raise StorageError("request_cancelled")
        self.context.check()

    def attach(self, connection):
        with self._lock:
            self.check()
            self._connection = connection

    def detach(self, connection):
        with self._lock:
            if self._connection is connection:
                self._connection = None

    def cancel(self):
        with self._lock:
            if self._connection is not None:
                try:
                    # libpq >=17 bounds this operation. Older libpq falls back
                    # to legacy cancel; it must never run on the asyncio loop.
                    self._connection.cancel_safe(timeout=.5)
                except Exception:
                    pass


class Worker:
    """Run sync transactions off-loop with no unbounded executor submission.

    A cancelled caller releases no capacity until the actual transaction and
    cancellation job finish. PostgreSQL statement/lock timeouts independently
    bound server waits. Older libpq cannot bound remote cancel network I/O.
    """
    def __init__(self, database, *, max_workers=4):
        if type(max_workers) is not int or not 1 <= max_workers <= 128:
            raise ValueError("invalid bounded worker count")
        self.database, self.max_workers = database, max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="arena-storage")
        self._canceller = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="arena-pg-cancel")
        self._jobs = {}
        self._loop, self._closed = None, False
        self.completed = self.rejected = self.cancelled = self.peak = 0

    def _bind(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("worker belongs to another event loop")
        return loop

    @property
    def stats(self):
        return dict(active=len(self._jobs), completed=self.completed, rejected=self.rejected,
                    cancelled=self.cancelled, peak=self.peak, closed=self._closed)

    def _finish(self, future):
        job = self._jobs.get(future)
        if job is not None and future.done() and (job[1] is None or job[1].done()):
            del self._jobs[future]
            self.completed += 1

    def _request_cancel(self, future):
        job = self._jobs.get(future)
        if job is None or job[1] is not None:
            return
        observer = job[0]
        observer.cancelled.set()
        self.cancelled += 1
        cancellation = self._canceller.submit(observer.cancel)
        job[1] = cancellation
        cancellation.add_done_callback(lambda _: self._loop.call_soon_threadsafe(self._finish, future))

    async def run(self, operation, context):
        loop = self._bind()
        context.check()
        if self._closed:
            raise RpcError(UNAVAILABLE, retryable=True)
        if len(self._jobs) >= self.max_workers:
            self.rejected += 1
            raise RpcError(OVERLOADED, retryable=True)
        observer = _ConnectionObserver(context)
        remaining = max(.001, context.remaining())
        budget = RetryBudget(max_attempts=3, max_elapsed=min(30, remaining),
            base_delay=.001, max_delay=.01, lock_timeout_ms=max(1, min(1000, int(remaining*1000))),
            statement_timeout_ms=max(1, min(2000, int(remaining*1000))))
        def execute():
            def invoke(work):
                observer.check()
                value = operation(work)
                observer.check()
                return value
            return self.database.run(invoke, budget=budget, _connection_observer=observer)
        future = self._executor.submit(execute)
        self._jobs[future] = [observer, None]
        self.peak = max(self.peak, len(self._jobs))
        future.add_done_callback(lambda _: loop.call_soon_threadsafe(self._finish, future))
        pending = asyncio.wrap_future(future)
        try:
            done, _ = await asyncio.wait((pending,), timeout=context.remaining())
            if not done:
                self._request_cancel(future)
                raise RpcError(DEADLINE_EXCEEDED)
            return pending.result().value
        except asyncio.CancelledError:
            self._request_cancel(future)
            raise
        finally:
            # Consume eventual errors if the RPC peer vanished while SQL ran.
            pending.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    async def close(self):
        self._bind()
        self._closed = True
        for future in tuple(self._jobs):
            self._request_cancel(future)
        while self._jobs:
            futures = [asyncio.wrap_future(future) for future in self._jobs]
            futures += [asyncio.wrap_future(job[1]) for job in self._jobs.values() if job[1] is not None]
            await asyncio.gather(*futures, return_exceptions=True)
            await asyncio.sleep(0)
        self._executor.shutdown(wait=True)
        self._canceller.shutdown(wait=True)
