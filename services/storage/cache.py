"""Async cache-aside with bounded singleflight and origin admission."""
import asyncio
from collections import OrderedDict
from dataclasses import dataclass
import inspect
import math
import random

from .redis_store import CacheError, _timeout


@dataclass
class _Flight:
    task: object
    waiters: int = 0


class CacheAside:
    """Collapse misses on one event loop, preserving authoritative-loader errors.

    Loaders are cooperative async functions accepting an absolute monotonic
    deadline; blocking database work belongs on a bounded storage worker. A
    finite token bucket limits all origin calls, including during Redis outage.
    Neither cached values nor negative entries are valid for asset decisions.
    """
    def __init__(self, store, *, positive_ttl=30, negative_ttl=2, jitter=.2,
                 max_inflight=64, max_waiters=4096, max_local_entries=4096,
                 loads_per_second=100, load_burst=16, load_timeout=.2,
                 cache_timeout=.05, random_source=None):
        for value in (positive_ttl, negative_ttl):
            if type(value) not in (int, float) or not math.isfinite(value) or not .001 <= value <= 2400:
                raise ValueError("TTL must be finite and in [.001,2400]")
        if type(jitter) not in (int, float) or not .2 <= jitter <= .5:
            raise ValueError("TTL jitter must be between 20% and 50%")
        if any(type(value) is not int or not 1 <= value <= 10000
               for value in (max_inflight, max_waiters, max_local_entries, loads_per_second, load_burst)):
            raise ValueError("invalid bounded cache admission configuration")
        self.store = store
        self.positive_ttl, self.negative_ttl, self.jitter = positive_ttl, negative_ttl, jitter
        self.max_inflight, self.max_waiters, self.max_local_entries = max_inflight, max_waiters, max_local_entries
        self.loads_per_second, self.load_burst = loads_per_second, load_burst
        self.load_timeout, self.cache_timeout = _timeout(load_timeout), _timeout(cache_timeout)
        self._random = random_source or random.SystemRandom()
        self._local, self._flights, self._invalidating = OrderedDict(), {}, set()
        self._retired, self._cancelling = {}, set()
        self._loop, self._closed, self._waiters = None, False, 0
        self._tokens, self._refilled = float(load_burst), None
        self._counts = dict(loads=0, cache_hits=0, local_hits=0, negative_hits=0,
            cache_failures=0, rejected=0, coalesced=0, load_failures=0,
            peak_inflight=0, peak_waiters=0)

    def _bind(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop, self._refilled = loop, loop.time()
        elif self._loop is not loop:
            raise CacheError("wrong_event_loop")
        return loop

    @property
    def stats(self):
        return dict(self._counts, inflight=len(self._flights)+len(self._retired),
                    retired=len(self._retired), waiters=self._waiters,
                    local_entries=len(self._local), closed=self._closed)

    def _key(self, key):
        # Use the same bounded key grammar before creating any flight.
        self.store._key("cache", key)
        return key

    def _remember(self, key, value, ttl):
        self._local[key] = (self._loop.time() + ttl, value)
        self._local.move_to_end(key)
        while len(self._local) > self.max_local_entries:
            self._local.popitem(last=False)

    def _local_value(self, key):
        entry = self._local.get(key)
        if entry is not None and entry[0] > self._loop.time():
            self._local.move_to_end(key)
            self._counts["local_hits"] += 1
            if entry[1] is None:
                self._counts["negative_hits"] += 1
            return True, entry[1]
        self._local.pop(key, None)
        return False, None

    def _admit_load(self):
        now = self._loop.time()
        self._tokens = min(self.load_burst, self._tokens + (now-self._refilled)*self.loads_per_second)
        self._refilled = now
        if self._tokens < 1:
            self._counts["rejected"] += 1
            raise CacheError("origin_overloaded")
        self._tokens -= 1

    async def _fetch(self, key, loader):
        try:
            cached = await asyncio.wait_for(self.store.get_cache(key), self.cache_timeout)
            if cached is not None:
                if cached == b"0":
                    self._counts["negative_hits"] += 1
                    self._counts["cache_hits"] += 1
                    return None
                if cached.startswith(b"1"):
                    self._counts["cache_hits"] += 1
                    return cached[1:]
                raise CacheError("invalid_data")
        except (CacheError, asyncio.TimeoutError):
            self._counts["cache_failures"] += 1
        self._admit_load()
        self._counts["loads"] += 1
        try:
            pending = loader(self._loop.time() + self.load_timeout)
            if not inspect.isawaitable(pending):
                raise TypeError("cache loader must be async")
            value = await asyncio.wait_for(pending, self.load_timeout)
        except asyncio.TimeoutError:
            self._counts["load_failures"] += 1
            raise CacheError("origin_timeout") from None
        except BaseException:
            self._counts["load_failures"] += 1
            raise
        if value is not None and (not isinstance(value, bytes) or len(value) >= self.store.max_value_bytes):
            raise ValueError("loader must return bounded bytes or None")
        base_ttl = self.negative_ttl if value is None else self.positive_ttl
        ttl = base_ttl * self._random.uniform(1-self.jitter, 1+self.jitter)
        ttl_ms = max(1, int(ttl*1000))
        try:
            await asyncio.wait_for(self.store.set_cache(key, b"0" if value is None else b"1"+value,
                                                       ttl_ms=ttl_ms), self.cache_timeout)
        except (CacheError, asyncio.TimeoutError):
            self._counts["cache_failures"] += 1
        # Local expiration starts after the write, never extending a remote hit.
        self._remember(key, value, ttl)
        return value

    def _done(self, key, flight):
        if self._flights.get(key) is flight:
            del self._flights[key]
        self._retired.pop(flight.task, None)
        self._cancelling.discard(flight.task)
        # Consume errors even if all callers timed out or cancelled.
        if not flight.task.cancelled():
            flight.task.exception()

    def _cancel(self, task):
        # Python 3.10 has no Task.cancelling(). Track our own cancellation so a
        # second cancel cannot interrupt wait_for's join of loader cleanup.
        if task not in self._cancelling and not task.done():
            self._cancelling.add(task)
            task.cancel()

    async def get(self, key, loader, *, timeout=.5):
        loop = self._bind()
        _timeout(timeout)
        self._key(key)
        if self._closed:
            raise CacheError("closed")
        if key in self._invalidating:
            raise CacheError("invalidating")
        found, value = self._local_value(key)
        if found:
            return value
        if self._waiters >= self.max_waiters:
            self._counts["rejected"] += 1
            raise CacheError("overloaded")
        flight = self._flights.get(key)
        if flight is None:
            if len(self._flights)+len(self._retired) >= self.max_inflight:
                self._counts["rejected"] += 1
                raise CacheError("overloaded")
            flight = _Flight(loop.create_task(self._fetch(key, loader)))
            self._flights[key] = flight
            flight.task.add_done_callback(lambda _: self._done(key, flight))
            self._counts["peak_inflight"] = max(len(self._flights)+len(self._retired), self._counts["peak_inflight"])
        else:
            self._counts["coalesced"] += 1
        flight.waiters += 1
        self._waiters += 1
        self._counts["peak_waiters"] = max(self._waiters, self._counts["peak_waiters"])
        try:
            return await asyncio.wait_for(asyncio.shield(flight.task), timeout)
        except asyncio.TimeoutError:
            raise CacheError("timeout") from None
        finally:
            flight.waiters -= 1
            self._waiters -= 1
            if flight.waiters == 0 and not flight.task.done():
                # A new caller must never join this cancellation. Retain the
                # task for admission, invalidation and shutdown until it exits.
                if self._flights.get(key) is flight:
                    del self._flights[key]
                self._retired[flight.task] = (key, flight)
                self._cancel(flight.task)

    async def invalidate(self, key):
        self._bind()
        self._key(key)
        if self._closed:
            raise CacheError("closed")
        if key in self._invalidating:
            raise CacheError("invalidating")
        self._invalidating.add(key)
        try:
            # Join the old fill before DEL so it cannot repopulate after return.
            flight = self._flights.get(key)
            tasks = [task for task, (old_key, _) in self._retired.items() if old_key == key]
            if flight is not None:
                tasks.append(flight.task)
            if tasks:
                await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
            self._local.pop(key, None)
            await asyncio.wait_for(self.store.delete_cache(key), self.cache_timeout)
        except asyncio.TimeoutError:
            raise CacheError("timeout") from None
        finally:
            self._invalidating.discard(key)

    async def close(self):
        self._bind()
        self._closed = True
        tasks = [flight.task for flight in self._flights.values()] + list(self._retired)
        for task in tasks:
            self._cancel(task)
        await asyncio.gather(*tasks, return_exceptions=True)
        self._local.clear()
