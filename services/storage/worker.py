"""Finite worker admission with cancellation observed by the SQL owner."""
import asyncio
from concurrent.futures import ThreadPoolExecutor

from shared.runtime.rpc import RpcError, DEADLINE_EXCEEDED, OVERLOADED, UNAVAILABLE
from .deadlines import RequestGuard, require_bounded_cancel
from .transactions import RetryBudget


class Worker:
    """Run bounded sync work off-loop without an abandoned cancel executor.

    The caller only sets an event. The SQL owner polls it, dispatches a finite
    nonblocking PG cancellation and quarantines its own connection. Capacity
    remains held until that actual worker has completed all cleanup.
    """
    def __init__(self, database, *, max_workers=4):
        if type(max_workers) is not int or not 1 <= max_workers <= 128:
            raise ValueError("invalid bounded worker count")
        require_bounded_cancel()
        self.database, self.max_workers = database, max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="arena-storage")
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
        if future in self._jobs and future.done():
            del self._jobs[future]
            self.completed += 1

    def _request_cancel(self, future):
        guard = self._jobs.get(future)
        if guard is not None and not guard.cancelled.is_set():
            guard.cancelled.set()
            self.cancelled += 1

    async def run(self, operation, context):
        loop = self._bind()
        context.check()
        if self._closed:
            raise RpcError(UNAVAILABLE, retryable=True)
        if len(self._jobs) >= self.max_workers:
            self.rejected += 1
            raise RpcError(OVERLOADED, retryable=True)
        guard = RequestGuard(context.deadline)
        remaining = max(.001, context.remaining())
        budget = RetryBudget(max_attempts=3, max_elapsed=min(30, remaining),
            base_delay=.001, max_delay=.01, lock_timeout_ms=max(1, min(1000, int(remaining*1000))),
            statement_timeout_ms=max(1, min(2000, int(remaining*1000))))
        def execute():
            def invoke(work):
                guard.check()
                value = operation(work)
                guard.check()
                return value
            return self.database.run(invoke, budget=budget, _request_guard=guard)
        future = self._executor.submit(execute)
        self._jobs[future] = guard
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
            # Consume eventual errors when the RPC peer has already vanished.
            pending.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    async def close(self):
        self._bind()
        self._closed = True
        for future in tuple(self._jobs):
            self._request_cancel(future)
        while self._jobs:
            joined = asyncio.gather(*(asyncio.wrap_future(future) for future in self._jobs),
                                    return_exceptions=True)
            # A repeated shutdown cancellation must not abandon running SQL
            # or cancel its Future wrapper before the owner actually exits.
            while not joined.done():
                try:
                    await asyncio.shield(joined)
                except asyncio.CancelledError:
                    continue
        self._executor.shutdown(wait=True)