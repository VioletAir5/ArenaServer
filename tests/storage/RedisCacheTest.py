#!/usr/bin/env python3
"""Normal acceptance against a private Redis process and real pooled sockets."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True


def require(value, message):
    if not value:
        raise AssertionError(message)


class RedisProcess:
    def __init__(self, binary):
        self.binary = str(binary)
        self.path = Path(tempfile.mkdtemp(prefix="arena-redis-"))
        self.socket = self.path / "redis.sock"
        self.process = None
        self.log = None

    def start(self):
        self.log = open(self.path / "redis.log", "ab")
        self.process = subprocess.Popen([self.binary, "--port", "0", "--unixsocket",
            str(self.socket), "--unixsocketperm", "700", "--save", "", "--appendonly", "no",
            "--dir", str(self.path), "--maxmemory", "32mb", "--maxmemory-policy", "noeviction"],
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError("private Redis did not start")
            if self.socket.exists():
                return
            time.sleep(.01)
        raise AssertionError("private Redis socket did not appear")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None
        self.socket.unlink(missing_ok=True)

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.path)


class Acceptance:
    def __init__(self, process, modules):
        self.process = process
        self.redis, self.rs, self.cs = modules
        self.stores, self.caches = [], []
        self.report = dict(passed=False, cases=[], redis_client_version=self.redis.__version__,
            acceptance_scope="normal functional and bounded fault integration",
            not_run=["100000-session TTL load", "1000000-member leaderboard latency",
                     "pipeline RTT benchmark", "PostgreSQL asset writes during Redis outage"])
        require(self.redis.__version__ == "5.2.1", "expected pinned redis==5.2.1")

    def store(self, **kwargs):
        store = self.rs.RedisStore(unix_socket_path=self.process.socket, io_timeout=.1, **kwargs)
        self.stores.append(store)
        return store

    def cache(self, store, **kwargs):
        cache = self.cs.CacheAside(store, **kwargs)
        self.caches.append(cache)
        return cache

    async def error(self, action, category):
        try:
            await action()
        except self.rs.CacheError as error:
            require(error.category == category, f"expected {category}, got {error.category}")
            return
        raise AssertionError("expected CacheError " + category)

    async def case(self, name, action):
        self.report["current_case"] = name
        started = time.monotonic()
        details = await action() or {}
        await asyncio.sleep(0)
        require(all(store.stats["active_operations"] == 0 for store in self.stores),
                "case leaked active Redis operations")
        require(all(cache.stats["inflight"] == 0 and cache.stats["waiters"] == 0 for cache in self.caches),
                "case leaked cache flights or waiters")
        # The SDK owns socket reuse; test its actual loan sets after quiescence.
        require(all(not store._pool._in_use_connections for store in self.stores),
                "case leaked SDK pooled connections")
        self.report["cases"].append(dict(name=name, passed=True,
                                        elapsed=time.monotonic()-started, **details))

    async def sessions(self):
        store = self.store(namespace="sessions")
        record = self.rs.SessionProjection((1 << 64)-1, (1 << 64)-2, (1 << 64)-3, (1 << 64)-4)
        key = store._session_key(record.player_id)
        initial_ttl, renewed_ttl, expiry_ttl = 5000, 10000, 120
        require(await store.put_session(record, ttl_ms=initial_ttl), "session write failed")
        require(await store.get_session(record.player_id) == record, "uint64 session changed")
        require(not await store.put_session(self.rs.SessionProjection(record.player_id, 7, 1), ttl_ms=15000),
                "stale epoch overwrote current session")
        require(not await store.delete_session(record.player_id, 7, 1), "stale delete removed current session")
        require(not await store.touch_session(record.player_id, 7, 1, ttl_ms=15000), "stale touch extended session")
        before_renewal = await store._client.pttl(key)
        require(0 < before_renewal <= initial_ttl, "fixture lease expired or stale CAS extended its TTL")
        renewal_started = time.monotonic()
        require(await store.touch_session(record.player_id, record.session_id, record.epoch, ttl_ms=renewed_ttl),
                "current owner did not renew a live fixture lease")
        after_renewal = await store._client.pttl(key)
        require(initial_ttl < after_renewal <= renewed_ttl, "current owner did not extend the actual Redis TTL")
        require(await store.get_session(record.player_id) == record, "renewal changed the session projection")

        renewal_finished = time.monotonic()

        # Expiry is independent of the time spent checking CAS and renewal.
        # Poll read-only state until the physical key is gone: PTTL=0 is still
        # a sub-millisecond live Redis lease, even if get_session returns None.
        require(await store.touch_session(record.player_id, record.session_id, record.epoch, ttl_ms=expiry_ttl),
                "current owner could not set a finite expiry on its live lease")
        expiry_started = time.monotonic()
        deadline = expiry_started + 2
        expiry_polls = 0
        while True:
            observed = await store.get_session(record.player_id)
            remaining = await store._client.pttl(key)
            expiry_polls += 1
            require(observed in (record, None), "expiring session projection changed")
            require(remaining == -2 or 0 <= remaining <= expiry_ttl, "session lost its bounded expiry")
            if observed is None and remaining == -2:
                break
            require(time.monotonic() < deadline, "session did not expire within the bounded observation window")
            await asyncio.sleep(.01)
        expiry_finished = time.monotonic()
        require(not await store.touch_session(record.player_id, record.session_id, record.epoch, ttl_ms=initial_ttl),
                "expired owner resurrected a reclaimed session")
        require(await store.get_session(record.player_id) is None and await store._client.pttl(key) == -2,
                "expired touch recreated a session key")
        for i in range(1, 21):
            await store.put_session(self.rs.SessionProjection(i, i+100, i+200), ttl_ms=1000)
        order = (10, 1, 999, 20, 10)
        rows = await store.get_sessions(order)
        require(tuple(row.player_id if row else None for row in rows) == (10, 1, None, 20, 10),
                "pipeline reordered or conflated duplicate/missing results")
        require(await store.delete_session(1, 101, 201), "current session delete failed")
        require(await store.get_session(1) is None, "deleted session remained")
        return dict(expiry_checked=True, renewal_checked=True, expired_touch_rejected=True,
                    pttl_before_renewal_ms=before_renewal, pttl_after_renewal_ms=after_renewal,
                    renewal_elapsed_ms=(renewal_finished-renewal_started)*1000,
                    expiry_elapsed_ms=(expiry_finished-expiry_started)*1000,
                    expiry_poll_count=expiry_polls, batch_order=list(order))

    async def leaderboard(self):
        store = self.store(namespace="leaderboard", max_members=3, max_batch=4)
        for player, score in ((1, 1500), (2, 1500), ((1 << 64)-1, 1300)):
            await store.put_score("season1", player, score)
        require(await store.top("season1") == ((2, 1500), (1, 1500), ((1 << 64)-1, 1300)),
                "stable tie break or full-width player identity failed")
        require(await store.ranks("season1", (1, 3, 2, 1)) == ((2, 1500), None, (1, 1500), (2, 1500)),
                "pipeline rank results mismatched")
        await self.error(lambda: store.put_score("season1", 4, 2000), "capacity")
        await store.put_score("season1", 1, 1600)
        require((await store.top("season1", offset=0, count=1))[0] == (1, 1600),
                "existing member could not update at capacity")
        for action in (lambda: store.get_sessions(range(1, 6)),
                       lambda: store.top("season1", count=101),
                       lambda: store.set_cache("big", b"x"*65537, ttl_ms=100)):
            try:
                await action()
            except ValueError:
                pass
            else:
                raise AssertionError("resource limit accepted unbounded request")
        await store._client.set(store._key("cache", "external-big"), b"x"*(2*1024*1024))
        await self.error(lambda: store.get_cache("external-big"), "invalid_data")
        await store._client.zadd(store._key("leaderboard", "external-big"), {b"x"*(2*1024*1024): 1})
        await self.error(lambda: store.top("external-big"), "invalid_data")
        # Every public score read uses the same exact-integer contract.
        member = b"00000000000000000042"
        for score in (2**53, -(2**53), 1.5, float("inf"), -float("inf")):
            await store._client.zadd(store._key("leaderboard", "external-score"), {member: score})
            await self.error(lambda: store.top("external-score"), "invalid_data")
            await self.error(lambda: store.ranks("external-score", (42,)), "invalid_data")
        for score in (2**53-1, -(2**53)+1):
            await store.put_score("score-boundary", 42, score)
            require(await store.top("score-boundary") == ((42, score),),
                    "top rejected an allowed exact-integer score")
            require(await store.ranks("score-boundary", (42,)) == ((1, score),),
                    "rank rejected an allowed exact-integer score")
        # Materializing an unbounded iterator must stop at max_batch+1.
        consumed = 0
        def infinite():
            nonlocal consumed
            while True:
                consumed += 1
                yield 1
        try:
            await store.get_sessions(infinite())
        except ValueError:
            pass
        require(consumed == store.max_batch+1, "unbounded input materialized before batch validation")
        return dict(tie_break="score descending, uint64 ID descending", membership_limit=3,
                    rejected_external_value_bytes=2*1024*1024)

    async def cold_singleflight(self):
        store = self.store(namespace="singleflight")
        cache = self.cache(store)
        calls = 0
        release, entered = asyncio.Event(), asyncio.Event()
        async def loader(deadline):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            require(deadline > asyncio.get_running_loop().time(), "loader deadline already expired")
            return b"profile"
        tasks = [asyncio.create_task(cache.get("player:42", loader, timeout=1)) for _ in range(1000)]
        await entered.wait()
        require(cache.stats["waiters"] == 1000, "cold callers did not overlap")
        release.set()
        values = await asyncio.gather(*tasks)
        require(values == [b"profile"]*1000 and calls == 1, "cold miss duplicated origin calls")
        require(await cache.get("player:42", loader) == b"profile" and calls == 1,
                "local hit queried origin")
        return dict(concurrent_callers=1000, origin_loads=calls, **cache.stats)

    async def negative_and_invalidation(self):
        store = self.store(namespace="negative")
        cache = self.cache(store, random_source=random.Random(42), positive_ttl=.1, negative_ttl=.1,
                           load_burst=100, loads_per_second=1000)
        value, calls = None, 0
        async def loader(_):
            nonlocal calls
            calls += 1
            return value
        require(await cache.get("missing", loader) is None, "negative load changed")
        require(await cache.get("missing", loader) is None and calls == 1, "negative cache missed")
        value = b"registered"
        await cache.invalidate("missing")
        require(await cache.get("missing", loader) == value and calls == 2, "negative invalidation failed")
        await asyncio.sleep(.15)
        require(await cache.get("missing", loader) == value and calls == 3, "positive TTL did not expire")
        # A new instance proves the Redis hit path without the local projection.
        fresh = self.cache(store)
        require(await fresh.get("missing", loader) == value and calls == 3, "remote hit queried origin")

        gate, entered = asyncio.Event(), asyncio.Event()
        async def slow(_):
            entered.set()
            await gate.wait()
            return b"old"
        first = asyncio.create_task(cache.get("race", slow))
        await entered.wait()
        invalidation = asyncio.create_task(cache.invalidate("race"))
        await asyncio.sleep(0)
        await self.error(lambda: cache.get("race", slow), "invalidating")
        gate.set()
        await first
        await invalidation
        require(await store.get_cache("race") is None and "race" not in cache._local,
                "completed invalidation was undone by an old fill")
        ttls = []
        async def present(_):
            return b"v"
        for i in range(64):
            await cache.get("jitter:" + str(i), present)
            ttls.append(await store._client.pttl(store._key("cache", "jitter:"+str(i))))
        spread = (max(ttls)-min(ttls))/100
        require(spread >= .2 and min(ttls) > 0, "TTL jitter insufficient")
        return dict(origin_loads=calls, ttl_min_ms=min(ttls), ttl_max_ms=max(ttls), jitter_spread=spread)

    async def leader_failure(self):
        store = self.store(namespace="failure")
        cache = self.cache(store, load_timeout=.05)
        calls = 0
        async def fail(_):
            nonlocal calls
            calls += 1
            await asyncio.sleep(.01)
            raise LookupError("authoritative-not-found")
        values = await asyncio.gather(*(cache.get("fail", fail) for _ in range(100)),
                                      return_exceptions=True)
        require(all(isinstance(value, LookupError) for value in values) and calls == 1,
                "leader error did not wake all followers")
        async def slow(_):
            await asyncio.sleep(5)
        values = await asyncio.gather(*(cache.get("slow", slow) for _ in range(100)),
                                      return_exceptions=True)
        require(all(isinstance(value, self.rs.CacheError) and value.category == "origin_timeout"
                    for value in values), "leader timeout left followers hanging")
        entered = asyncio.Event()
        async def blocked(_):
            entered.set()
            await asyncio.sleep(5)
        callers = [asyncio.create_task(cache.get("cancel", blocked)) for _ in range(10)]
        await entered.wait()
        for task in callers:
            task.cancel()
        await asyncio.gather(*callers, return_exceptions=True)
        await asyncio.sleep(.01)
        require(cache.stats["inflight"] == 0 and cache.stats["waiters"] == 0,
                "cancelled leader/followers leaked")
        async def success(_):
            return b"recovered"
        require(await cache.get("cancel", success) == b"recovered", "cancelled key never recovered")
        return dict(failure_followers=100, timeout_followers=100, cancelled_callers=10)

    async def cancellation_retirement(self):
        store = self.store(namespace="retirement")
        cache = self.cache(store)
        entered = asyncio.Event()
        async def blocked(_):
            entered.set()
            await asyncio.Event().wait()
        first = asyncio.create_task(cache.get("immediate", blocked))
        await entered.wait()
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        async def fresh(_):
            return b"fresh"
        # No scheduling sleep: the cancelled task's done callback can still be
        # queued when an unrelated caller retries this same key.
        require(await cache.get("immediate", fresh) == b"fresh",
                "fresh caller inherited an earlier caller's cancellation")

        bounded = self.cache(store, max_inflight=1)
        entered.clear()
        cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
        async def slow_cleanup(_):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_entered.set()
                await cleanup_release.wait()
                return b"obsolete"
        first = asyncio.create_task(bounded.get("retire", slow_cleanup))
        await entered.wait()
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await cleanup_entered.wait()
        require(bounded.stats["retired"] == bounded.stats["inflight"] == 1,
                "cancelled pending task escaped inflight accounting")
        await self.error(lambda: bounded.get("different", fresh), "overloaded")
        invalidation = asyncio.create_task(bounded.invalidate("retire"))
        await asyncio.sleep(0)
        require(not invalidation.done(), "invalidation skipped an older retiring fill")
        await self.error(lambda: bounded.get("retire", fresh), "invalidating")
        cleanup_release.set()
        await invalidation
        require(bounded.stats["inflight"] == bounded.stats["retired"] == 0,
                "retired task was not reclaimed after invalidation")
        require(await store.get_cache("retire") is None and "retire" not in bounded._local,
                "retired fill survived invalidation")
        require(await bounded.get("retire", fresh) == b"fresh",
                "retired identity cleanup removed a fresh flight")

        # Shutdown also joins retired tasks instead of tracking only current keys.
        closing = self.cache(store, max_inflight=1)
        entered.clear()
        cleanup_entered.clear()
        cleanup_release.clear()
        first = asyncio.create_task(closing.get("close", slow_cleanup))
        await entered.wait()
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await cleanup_entered.wait()
        require(closing.stats["retired"] == 1, "close fixture did not retain pending cancellation")
        shutdown = asyncio.create_task(closing.close())
        await asyncio.sleep(0)
        require(not shutdown.done(), "close skipped pending loader cancellation cleanup")
        cleanup_release.set()
        await shutdown
        require(closing.stats["retired"] == closing.stats["inflight"] == 0,
                "shutdown retained a retired task")
        return dict(immediate_same_key_retry=True, retired_admission_bounded=True,
                    invalidation_joined_retired=True, close_joined_retired=True)

    async def bounded_admission(self):
        store = self.store(namespace="bounded")
        cache = self.cache(store, max_inflight=2, max_waiters=2, load_burst=2, loads_per_second=1)
        entered, gate = asyncio.Event(), asyncio.Event()
        async def loader(_):
            entered.set()
            await gate.wait()
            return b"ok"
        first = asyncio.create_task(cache.get("one", loader))
        await entered.wait()
        second = asyncio.create_task(cache.get("two", loader))
        await asyncio.sleep(.01)
        await self.error(lambda: cache.get("three", loader), "overloaded")
        gate.set()
        await asyncio.gather(first, second)
        await self.error(lambda: cache.get("rate", loader), "origin_overloaded")
        # Same-key follower cancellation must not cancel the remaining owner.
        cache2 = self.cache(store)
        gate.clear()
        entered.clear()
        first = asyncio.create_task(cache2.get("owner", loader))
        second = asyncio.create_task(cache2.get("owner", loader))
        await entered.wait()
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        gate.set()
        require(await second == b"ok", "one caller cancelled another caller's load")
        return dict(inflight_limit=2, waiter_limit=2, origin_burst=2)

    async def redis_outage(self):
        store = self.store(namespace="outage")
        cache = self.cache(store, loads_per_second=1, load_burst=4)
        calls = 0
        async def loader(_):
            nonlocal calls
            calls += 1
            await asyncio.sleep(.01)
            return b"pg-authoritative"
        failures = []
        for cycle in range(3):
            self.process.stop()
            await self.error(lambda: store.get_session(42), "unavailable")
            require(await cache.get("profile:"+str(cycle), loader) == b"pg-authoritative",
                    "Redis failure prevented bounded authoritative fallback")
            results = await asyncio.gather(*(cache.get(f"flood:{cycle}:{i}", loader)
                                             for i in range(100)), return_exceptions=True)
            failures.append(sum(isinstance(value, self.rs.CacheError) for value in results))
            require(failures[-1] > 0, "outage flood bypassed origin admission")
            self.process.start()
            # Same owned pool must reconnect, without constructing a replacement.
            session = self.rs.SessionProjection(42, 1+cycle, 1+cycle)
            require(await store.put_session(session, ttl_ms=1000), "pool did not recover")
            require(await store.get_session(42) == session, "recovered socket returned stale data")
            await asyncio.sleep(1.02)
        require(calls <= 8, "Redis outage amplified origin load")
        return dict(restart_cycles=3, total_origin_loads=calls, flood_rejections=failures,
                    asset_mutation_api=False)

    async def cancelled_socket(self):
        store = self.store(namespace="socket", max_connections=1)
        # SIGSTOP yields a real pending command; cancellation must release and
        # disconnect its half-read socket before the next command reuses it.
        os.kill(self.process.process.pid, signal.SIGSTOP)
        task = asyncio.create_task(store.get_session(7))
        await asyncio.sleep(.02)
        await self.error(lambda: store.get_session(8), "overloaded")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            os.kill(self.process.process.pid, signal.SIGCONT)
        require(store.stats["active_operations"] == 0 and not store._pool._in_use_connections,
                "cancelled socket retained a lease")
        await store.put_session(self.rs.SessionProjection(7, 7, 7), ttl_ms=1000)
        require((await store.get_session(7)).player_id == 7, "cancelled reply poisoned next command")
        return dict(real_socket_cancellation=True)

    async def run(self):
        for name, action in (
            ("session_ttl_epoch_and_pipeline", self.sessions),
            ("leaderboard_and_bounds", self.leaderboard),
            ("1000_concurrent_cold_singleflight", self.cold_singleflight),
            ("negative_cache_jitter_invalidation", self.negative_and_invalidation),
            ("leader_error_timeout_and_cancellation", self.leader_failure),
            ("cancelled_flight_retirement_and_immediate_retry", self.cancellation_retirement),
            ("bounded_origin_and_waiter_admission", self.bounded_admission),
            ("redis_stop_restart_and_bounded_fallback", self.redis_outage),
            ("cancelled_real_socket_reuse", self.cancelled_socket)):
            await self.case(name, action)
        self.report["passed"] = True

    async def close(self):
        for cache in self.caches:
            await cache.close()
        for store in self.stores:
            await store.close()
        require(all(not store._pool._in_use_connections for store in self.stores),
                "shutdown retained a Redis lease")
        require(all(not connection.is_connected for store in self.stores
                    for connection in store._pool._available_connections),
                "shutdown retained a connected Redis socket")
        self.report["final_stores"] = [store.stats for store in self.stores]
        self.report["final_caches"] = [cache.stats for cache in self.caches]
        self.report["all_connections_closed"] = True
        client = self.redis.asyncio.Redis(unix_socket_path=str(self.process.socket))
        try:
            clients = await client.client_list()
            require(len(clients) == 1, "server retained application client connections after close")
            self.report["remaining_application_clients"] = len(clients)-1
            self.report["redis_server_version"] = (await client.info("server"))["redis_version"]
        finally:
            await client.aclose()


async def acceptance(args, process):
    import redis
    from services.storage import redis_store, cache
    test = Acceptance(process, (redis, redis_store, cache))
    try:
        await test.run()
    except BaseException as error:
        test.report["error"] = str(error)
        test.report["error_type"] = type(error).__name__
        raise
    finally:
        try:
            await test.close()
        finally:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(test.report, indent=2)+"\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--redis-server", type=Path, default=Path("/usr/bin/redis-server"))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.repository_root = args.repository_root.resolve()
    args.report = args.report.resolve()
    sys.path.insert(0, str(args.repository_root))
    process = RedisProcess(args.redis_server)
    try:
        process.start()
        asyncio.run(acceptance(args, process))
    finally:
        process.cleanup()
        if args.report.exists():
            report = json.loads(args.report.read_text())
            report["cluster_cleaned"] = not process.path.exists()
            args.report.write_text(json.dumps(report, indent=2)+"\n")
    print("RedisCacheAcceptance: passed")


if __name__ == "__main__":
    main()
