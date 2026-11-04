"""Loopback Storage RPC: SQL-free orchestration over scoped repositories."""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import signal
import re
import time
import sys
import threading
from uuid import UUID, uuid5

from google.protobuf.message import DecodeError
from shared.runtime.protocol import Protocol
from shared.runtime.config import ConfigSnapshot, ConfigStore, ConfigReloader
from shared.runtime.health import HealthServer
from shared.runtime.rpc import (Server, Context, RpcError, INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE,
    OVERLOADED, DEADLINE_EXCEEDED, CONFLICT, INTERNAL, OUTCOME_UNKNOWN, validate_service_credentials)
from .database import Database
from .outbox import Publisher, consume_event
from .repositories import MatchParticipant, _participants
from .redis_store import RedisStore, CacheError
from .settlement import SettlementCommand, Reward
from .transactions import RetryBudget, StorageError
from .worker import Worker

READINESS_PROBE_SECONDS = .25
READINESS_MAX_AGE_SECONDS = .5

_ID_NAMESPACE = UUID("98e4ecb1-df9f-4588-9554-378c79291c25")


def _millis(value):
    if value is None:
        return 0
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def _instant(value, fallback):
    if not value:
        return fallback
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)
    except (ValueError, OverflowError, OSError):
        raise StorageError("invalid_request") from None


def _storage_error(error):
    category = error.category
    if category in ("invalid_request", "invalid_operation"):
        return RpcError(INVALID_ARGUMENT)
    if category == "not_found":
        return RpcError(NOT_FOUND)
    if category in ("idempotency_conflict", "integrity_error", "id_exhausted",
                    "quantity_overflow", "revision_overflow"):
        return RpcError(CONFLICT)
    if category == "outcome_unknown":
        return RpcError(OUTCOME_UNKNOWN)
    if category in ("pool_timeout", "pool_overloaded", "retry_exhausted"):
        return RpcError(OVERLOADED, retryable=True)
    if category in ("statement_timeout", "lock_timeout", "budget_exhausted", "request_cancelled"):
        return RpcError(DEADLINE_EXCEEDED)
    return RpcError(UNAVAILABLE, retryable=category in ("connection_error", "pool_closed"))


class _PublisherTransactions:
    """Apply the same finite transaction policy to claim, consume and ack."""
    def __init__(self, database):
        self.database = database
        self.budget = RetryBudget(max_attempts=3, max_elapsed=1,
            base_delay=.001, max_delay=.01, lock_timeout_ms=100,
            statement_timeout_ms=250)

    def run(self, operation):
        return self.database.run(operation, budget=self.budget)


class OutboxPump:
    """One worker drains at-least-once events without blocking the RPC owner."""
    def __init__(self, database):
        transactions = _PublisherTransactions(database)
        self._transactions = transactions
        self._publisher = Publisher(transactions)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._executor = self._future = None
        self._counts = dict(enabled=True, active=False, closed=False, max_workers=1,
            passes=0, claimed=0, published=0, failed=0, errors=0, consumed=0,
            deduplicated=0, last_error_category=None, last_error_sqlstate=None)

    @property
    def stats(self):
        with self._lock:
            return dict(self._counts)

    def start(self):
        if self._executor is not None or self._stop.is_set():
            raise RuntimeError("outbox pump requires a fresh instance")
        self._executor = ThreadPoolExecutor(max_workers=1,
            thread_name_prefix="arena-outbox")
        self._future = self._executor.submit(self._run)

    def _deliver(self, event):
        replayed = consume_event(self._transactions, event).value
        with self._lock:
            self._counts["deduplicated" if replayed else "consumed"] += 1

    def _run(self):
        with self._lock:
            self._counts["active"] = True
        try:
            while not self._stop.is_set():
                try:
                    # One pass owns SQL resources; shutdown joins it before
                    # pool closure. No executor submission queue accumulates.
                    result = self._publisher.run_once(self._deliver, limit=1)
                except Exception as error:
                    with self._lock:
                        self._counts["errors"] += 1
                        self._counts["last_error_category"] = (
                            error.category if isinstance(error, StorageError) else "internal")
                        self._counts["last_error_sqlstate"] = (
                            error.sqlstate if isinstance(error, StorageError) else None)
                else:
                    with self._lock:
                        self._counts["passes"] += 1
                        for key in ("claimed", "published", "failed"):
                            self._counts[key] += result[key]
                self._stop.wait(.05)
        finally:
            with self._lock:
                self._counts["active"] = False

    async def close(self):
        self._stop.set()
        try:
            if self._future is not None:
                # Shield alone lets caller cancellation escape before the worker
                # joins. Keep joining through repeated cancellation so the enclosing
                # service cannot abandon SQL ownership or close its pool too early.
                pending = asyncio.wrap_future(self._future)
                while not pending.done():
                    try:
                        await asyncio.shield(pending)
                    except asyncio.CancelledError:
                        continue
                pending.result()
        finally:
            if self._executor is not None:
                self._executor.shutdown(wait=True)
            with self._lock:
                self._counts["closed"] = True


class StorageService:
    """Own the worker and optional disposable Redis projections."""
    def __init__(self, protocol, database, *, max_workers=4, redis_store=None,
                 publish=False):
        self.protocol, self.database = protocol, database
        self.worker = Worker(database, max_workers=max_workers)
        self.redis = redis_store
        self.publisher = OutboxPump(database) if publish else None
        self._projections = set()
        self._close_task = None
        self._runtime_readiness = None
        self.projection_rejected = self.projection_failed = 0
        self.handlers = {}
        for method, request, handler in (
            ("account.find", "FindAccountRequest", self._account),
            ("player.get", "PlayerGetRequest", self._player),
            ("match.allocate_ids", "AllocateIdsRequest", self._allocate),
            ("match.save_result", "SaveResultRequest", self._save),
            ("match.get", "MatchGetRequest", self._match),
            ("settlement.get", "SettlementGetRequest", self._settlement)):
            self.handlers[method] = self._handler(request, handler)
        self.handlers["health.ready"] = self._ready

    def start(self):
        if self.publisher is not None:
            self.publisher.start()

    def message(self, name, **values):
        return self.protocol.message("arena.storage."+name)(**values)

    def _handler(self, request_name, handler):
        async def invoke(payload, context):
            try:
                request = self.protocol.message("arena.storage."+request_name).FromString(payload)
                return (await handler(request, context)).SerializeToString()
            except DecodeError:
                raise RpcError(INVALID_ARGUMENT) from None
            except StorageError as error:
                raise _storage_error(error) from None
        return invoke

    async def _ready(self, payload, context):
        if payload:
            raise RpcError(INVALID_ARGUMENT)
        context.check()
        if self._runtime_readiness is not None:
            if not self._runtime_readiness():
                raise RpcError(UNAVAILABLE, retryable=True)
            return b""
        try:
            await self.worker.run(lambda work: work.accounts.get(1), context)
        except StorageError as error:
            raise _storage_error(error) from None
        return b""

    @staticmethod
    def _account_record(account):
        return dict(account_id=account.account_id, account_name=account.account_name,
                    password_digest=account.password_digest, state=account.state)

    @staticmethod
    def _player_record(player):
        return dict(player_id=player.player_id, account_id=player.account_id,
            display_name=player.display_name, state=player.state, mmr=player.mmr, revision=player.revision)

    def _match_record(self, match):
        return self.message("MatchRecord", match_id=match.match_id, room_id=match.room_id,
            state=match.state, result_version=match.result_version,
            winner_player_id=match.winner_player_id or 0, final_tick=match.final_tick or 0,
            final_checksum=match.final_checksum or 0, finish_reason=match.finish_reason or "",
            created_unix_ms=_millis(match.created_at), started_unix_ms=_millis(match.started_at),
            finished_unix_ms=_millis(match.finished_at),
            participants=[dict(player_id=p.player_id, slot=p.slot, session_id=p.session_id,
                membership_epoch=p.membership_epoch, initial_mmr=p.initial_mmr,
                last_input_seq=p.last_input_seq, final_x=p.final_x, final_y=p.final_y,
                last_dx=p.last_dx, last_dy=p.last_dy)
                for p in match.participants])

    @staticmethod
    def _settlement_record(receipt):
        return dict(settlement_id=receipt.settlement_id, match_id=receipt.match_id,
                    event_id=str(receipt.event_id), replayed=receipt.replayed)

    async def _account(self, request, context):
        def lookup(work):
            account = work.accounts.find_by_name(request.account_name)
            if account is None:
                raise StorageError("not_found")
            player = work.accounts.find_player_by_account(account.account_id)
            if player is None:
                raise StorageError("not_found")
            return account, player
        account, player = await self.worker.run(lookup, context)
        return self.message("FindAccountReply", account=self._account_record(account),
                            player=self._player_record(player))

    async def _player(self, request, context):
        player = await self.worker.run(lambda work: work.accounts.get_player(request.player_id), context)
        if player is None:
            raise RpcError(NOT_FOUND)
        return self.message("PlayerGetReply", player=self._player_record(player))

    async def _allocate(self, request, context):
        first, last = await self.worker.run(lambda work: work.ids.reserve(request.count), context)
        return self.message("AllocateIdsReply", first=first, last=last)

    @staticmethod
    def _same_result(current, requested, participants):
        if (current.room_id != requested.room_id or current.state != requested.state
                or current.result_version != requested.result_version
                or (current.winner_player_id or 0) != requested.winner_player_id
                or current.final_tick != requested.final_tick
                or current.final_checksum != requested.final_checksum
                or current.finish_reason != requested.finish_reason
                or current.participants != participants):
            return False
        return all(not value or value == _millis(actual) for value, actual in (
            (requested.created_unix_ms, current.created_at),
            (requested.started_unix_ms, current.started_at),
            (requested.finished_unix_ms, current.finished_at)))

    async def _save(self, request, context):
        if not request.HasField("match"):
            raise RpcError(INVALID_ARGUMENT)
        value = request.match
        if (not 0 < value.match_id < 1 << 63 or value.room_id != ((1 << 63) | value.match_id)
                or value.state not in ("completed", "aborted") or len(value.participants) != 2
                or value.finish_reason not in ("completed", "member_unavailable", "ready_timeout")
                or (value.state == "completed") != (value.finish_reason == "completed")
                or value.state == "aborted" and value.winner_player_id):
            raise RpcError(INVALID_ARGUMENT)
        participants = _participants(tuple(MatchParticipant(
            p.player_id, p.slot, p.session_id, p.membership_epoch, p.initial_mmr,
            p.last_input_seq, p.final_x, p.final_y, p.last_dx, p.last_dy) for p in value.participants))
        # The authoritative policy is stable across retries and restarts.
        rewards = tuple(Reward(p.player_id, 1, 10) for p in participants) if value.state == "completed" else ()
        command = SettlementCommand(value.match_id, value.match_id,
            uuid5(_ID_NAMESPACE, "settlement:"+str(value.match_id)),
            uuid5(_ID_NAMESPACE, "event:"+str(value.match_id)), rewards, value.result_version)
        def persist(work):
            current = work.matches.get_for_result(value.match_id)
            replayed = current is not None
            if current is not None:
                if not self._same_result(current, value, participants):
                    raise StorageError("idempotency_conflict")
            else:
                now = datetime.now(timezone.utc)
                created = _instant(value.created_unix_ms, now)
                started = _instant(value.started_unix_ms, created)
                finished = _instant(value.finished_unix_ms, started)
                assembled = work.matches.create(value.match_id, value.room_id, participants,
                    value.result_version, created_at=created, if_absent=True)
                if assembled.state != "forming":
                    if not self._same_result(assembled, value, participants):
                        raise StorageError("idempotency_conflict")
                    return assembled, work.settlements.commit(command), True
                if value.state == "completed" or value.started_unix_ms:
                    work.matches.start(value.match_id, started)
                if value.state == "completed":
                    current = work.matches.finish(value.match_id, finished, value.final_tick,
                        value.final_checksum, value.winner_player_id or None)
                else:
                    current = work.matches.abort(value.match_id, finished, value.final_tick,
                                                 value.final_checksum, value.finish_reason)
            receipt = work.settlements.commit(command)
            return current, receipt, replayed
        match, receipt, replayed = await self.worker.run(persist, context)
        self._project(match)
        return self.message("SaveResultReply", match=self._match_record(match),
                            settlement=self._settlement_record(receipt), replayed=replayed)

    def _project(self, match):
        if self.redis is None:
            return
        if len(self._projections) >= 8:
            self.projection_rejected += 1
            return
        async def project():
            try:
                for member in match.participants:
                    await self.redis.put_score("current", member.player_id, member.initial_mmr)
            except (CacheError, ValueError):
                self.projection_failed += 1
        task = asyncio.create_task(project())
        self._projections.add(task)
        task.add_done_callback(self._projections.discard)

    async def _match(self, request, context):
        match = await self.worker.run(lambda work: work.matches.get(request.match_id), context)
        if match is None:
            raise RpcError(NOT_FOUND)
        return self.message("MatchGetReply", match=self._match_record(match))

    async def _settlement(self, request, context):
        receipt = await self.worker.run(lambda work: work.settlements.get(request.settlement_id), context)
        if receipt is None:
            raise RpcError(NOT_FOUND)
        return self.message("SettlementGetReply", settlement=self._settlement_record(receipt))

    async def close(self):
        # One owner-loop task owns the whole dependency shutdown. Shielding
        # only the publisher would still let cancellation abandon a later
        # projection/Redis await before pool closure.
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                continue
        self._close_task.result()

    async def _close(self):
        await self.worker.close()
        try:
            if self.publisher is not None:
                await self.publisher.close()
            if self._projections:
                await asyncio.gather(*tuple(self._projections), return_exceptions=True)
            if self.redis is not None:
                await self.redis.close()
        finally:
            # Pool maintenance may be connecting; keep the owner loop pumping
            # while its finite worker shutdown joins those operations.
            await asyncio.to_thread(self.database.close)


class _RuntimeState:
    """Keep operator intent, actual dependency health and terminal drain distinct."""
    def __init__(self, service, store, node_id):
        self.service, self.store, self.node_id = service, store, node_id
        self.server = self.reloader = None
        self.draining = False
        self._dependency = False
        self._dependency_version = -1
        self._dependency_checked_at = None
        self._probe_task = self._close_task = None
        self._sequence = 0
        self._probe_error = None

    def snapshot(self):
        return self.store.snapshot() if self.store is not None else None

    def version(self):
        snapshot = self.snapshot()
        return snapshot.version if snapshot is not None else 0

    def _operator_ready(self, snapshot):
        if self.draining or snapshot is not None and not snapshot.desired_ready:
            return False
        own_node = next((node for node in snapshot.nodes
            if node.service=='storage' and node.id==self.node_id),None) if snapshot is not None else None
        return own_node is None or own_node.ready

    def operator_ready(self):
        return self._operator_ready(self.snapshot())

    def _ready_for(self, snapshot):
        version = snapshot.version if snapshot is not None else 0
        checked = self._dependency_checked_at
        return (self._operator_ready(snapshot) and self._dependency
            and self._dependency_version == version and checked is not None
            and 0 <= time.monotonic()-checked < READINESS_MAX_AGE_SECONDS)

    def ready(self):
        return self._ready_for(self.snapshot())

    def admit(self, method):
        return method == 'health.ready' or self.ready()

    def status(self):
        snapshot = self.snapshot()
        return dict(service='storage',node_id=self.node_id,config_version=snapshot.version if snapshot is not None else 0,
            desired_ready=snapshot.desired_ready if snapshot is not None else True,
            draining=self.draining,ready=self._ready_for(snapshot),alive=True,
            inflight=self.server.inflight,worker=self.service.worker.stats,pool=self.service.database.stats,
            methods=dict(self.server.method_calls),completions=dict(self.server.method_completed),
            admission_rejected=self.server.admission_rejected,
            dependency_ready=self._dependency,probe_version=self._dependency_version,probe_error=self._probe_error,
            reload=self.reloader.stats if self.reloader is not None else
                dict(applied=0,rejected=0,coalesced=0,active=0,peak=0,last_error=None))

    async def _probe(self):
        while not self.draining:
            started = time.monotonic()
            version = self.version()
            self._sequence += 1
            context = Context(self._sequence,'storage-readiness',time.monotonic()+READINESS_PROBE_SECONDS,'')
            healthy = False
            publish_result = True
            try:
                await self.service.worker.run(lambda work:work.accounts.get(1),context)
                healthy = True
            except (StorageError,RpcError) as error:
                self._probe_error = error.category if isinstance(error,StorageError) else str(error.status)
                # Full local admission did not observe a database failure.
                # Keep the last observation's original version and hard expiry.
                publish_result = not (isinstance(error,RpcError) and error.status == OVERLOADED)
            except Exception:
                self._probe_error = 'internal'
            # A result for an old publication cannot make the new version ready.
            if publish_result and version == self.version():
                self._dependency,self._dependency_version = healthy,version
                self._dependency_checked_at = time.monotonic()
                if healthy:
                    self._probe_error = None
            await asyncio.sleep(max(0,READINESS_PROBE_SECONDS-(time.monotonic()-started)))

    def request_reload(self):
        return self.reloader.request_reload() if self.reloader is not None else False

    def start(self):
        self._probe_task = asyncio.create_task(self._probe())

    async def close(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        # A cancelled drain waiter must still reach worker and pool shutdown.
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                continue
        self._close_task.result()

    async def _close(self):
        if self.reloader is not None:
            await self.reloader.close()
        if self._probe_task is not None:
            self._probe_task.cancel()
            await asyncio.gather(self._probe_task,return_exceptions=True)

async def run_server(args):
    if not 0 <= args.port <= 65535:
        raise ValueError("invalid loopback port")
    caller_tokens = None
    if 'ARENA_STORAGE_CALLER_TOKENS' in os.environ:
        try:
            caller_tokens = validate_service_credentials('storage',
                json.loads(os.environ['ARENA_STORAGE_CALLER_TOKENS']))
        except (ValueError, TypeError, UnicodeError):
            raise ValueError('invalid Storage caller credential profile') from None
    token = None if caller_tokens is not None else os.environ.get("ARENA_INTERNAL_TOKEN", "")
    if caller_tokens is None and not 32 <= len(token.encode()) <= 256:
        raise ValueError("an internal service credential is required")
    node_id = os.environ.get('ARENA_NODE_ID','storage')
    if not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}',node_id):
        raise ValueError('invalid service node identity')
    config_path = getattr(args,'runtime_config',None)
    if config_path is None:
        config_path = os.environ.get('ARENA_RUNTIME_CONFIG')
    if config_path is not None and not os.fspath(config_path):
        raise ValueError('runtime configuration path must not be empty')
    health_port = getattr(args,'health_port',None)
    if health_port is None and 'ARENA_HEALTH_PORT' in os.environ:
        health_port = int(os.environ['ARENA_HEALTH_PORT'])
    if health_port is not None and (type(health_port) is not int or not 0 <= health_port <= 65535):
        raise ValueError('invalid loopback health port')
    controlled = config_path is not None or health_port is not None
    parameters = dict(host=os.environ.get("PGHOST", ""), dbname=os.environ.get("PGDATABASE", ""),
                      user=os.environ.get("PGUSER", ""), connect_timeout=1,
                      port=int(os.environ.get("PGPORT", "5432")))
    for name in ("PGSERVICE", "PGSERVICEFILE", "PGHOSTADDR"):
        os.environ.pop(name, None)
    protocol = Protocol(args.descriptor)
    store = ConfigStore(await asyncio.to_thread(ConfigSnapshot.read,protocol,config_path)) if config_path is not None else None
    database = Database(kwargs=parameters, max_size=args.workers, max_waiting=args.workers,
                        acquire_timeout=.2)
    projection = RedisStore(unix_socket_path=args.redis_socket, namespace="arena") if args.redis_socket else None
    service = StorageService(protocol, database, max_workers=args.workers,
                             redis_store=projection, publish=args.publish)
    runtime = _RuntimeState(service,store,node_id) if controlled else None
    server = Server(protocol, service.handlers, service_token=token,
                    service='storage' if caller_tokens is not None else '', caller_tokens=caller_tokens,
                    max_timeout=1 if caller_tokens is not None else 5,
                    admission=runtime.admit if runtime is not None else None)
    health = HealthServer(runtime.status) if runtime is not None else None
    if runtime is not None:
        runtime.server = server
        service._runtime_readiness = runtime.ready
        if config_path is not None:
            runtime.reloader = ConfigReloader(protocol,config_path,store)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopped.set)
    if runtime is not None:
        loop.add_signal_handler(signal.SIGHUP,runtime.request_reload)
    try:
        port = await server.start("127.0.0.1", args.port)
        service.start()
        details = dict(service="storage", port=port, state="listening")
        if runtime is not None:
            runtime.start()
            actual_health_port = await health.start(health_port if health_port is not None else 0)
            details.update(health_port=actual_health_port,node_id=node_id,config_version=runtime.version())
        print(json.dumps(details), flush=True)
        await stopped.wait()
    finally:
        grace = 1
        if runtime is not None:
            runtime.draining = True
            snapshot = runtime.snapshot()
            grace = snapshot.drain_grace_ms/1000 if snapshot is not None else 5
            draining = runtime.status()
            draining['state'] = 'draining'
            print(json.dumps(draining),flush=True)
        try:
            await server.close(grace=grace)
            if runtime is not None:
                await runtime.close()
            await service.close()
        finally:
            if health is not None:
                await health.close()
        print(json.dumps(dict(service="storage", state="stopped", worker=service.worker.stats,
            pool=database.stats, projection_failed=service.projection_failed,
            publisher=service.publisher.stats if service.publisher is not None else
                dict(enabled=False, active=False, closed=True))), flush=True)

def main():
    parser = argparse.ArgumentParser(description="Arena internal Storage service")
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--port", type=int, default=7105)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--redis-socket", type=Path)
    parser.add_argument('--runtime-config',type=Path)
    parser.add_argument('--health-port',type=int)
    parser.add_argument("--publish", action="store_true",
                        help="drain committed outbox events into the deduplicated reward audit")
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    try:
        asyncio.run(run_server(args))
    except (ValueError, OSError, StorageError):
        print(json.dumps(dict(service="storage", state="failed")), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
