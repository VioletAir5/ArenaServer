"""Loopback Storage RPC: SQL-free orchestration over scoped repositories."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys
from uuid import UUID, uuid5

from google.protobuf.message import DecodeError
from shared.runtime.protocol import Protocol
from shared.runtime.rpc import (Server, RpcError, INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE,
    OVERLOADED, DEADLINE_EXCEEDED, CONFLICT, INTERNAL, OUTCOME_UNKNOWN)
from .database import Database
from .repositories import MatchParticipant, _participants
from .redis_store import RedisStore, CacheError
from .settlement import SettlementCommand, Reward
from .transactions import StorageError
from .worker import Worker

_ID_NAMESPACE = UUID("98e4ecb1-df9f-4588-9554-378c79291c25")


def _millis(value):
    return 0 if value is None else int(value.timestamp()*1000)


def _instant(value, fallback):
    if not value:
        return fallback
    try:
        return datetime.fromtimestamp(value/1000, timezone.utc)
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


class StorageService:
    """Own the worker and optional disposable Redis projections."""
    def __init__(self, protocol, database, *, max_workers=4, redis_store=None):
        self.protocol, self.database = protocol, database
        self.worker = Worker(database, max_workers=max_workers)
        self.redis = redis_store
        self._projections = set()
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
        await self.worker.run(lambda work: work.accounts.get(1), context)
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
                last_input_seq=p.last_input_seq, final_x=p.final_x, final_y=p.final_y)
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
            p.last_input_seq, p.final_x, p.final_y) for p in value.participants))
        # The authoritative policy is stable across retries and restarts.
        rewards = tuple(Reward(p.player_id, 1, 10) for p in participants) if value.state == "completed" else ()
        command = SettlementCommand(value.match_id, value.match_id,
            uuid5(_ID_NAMESPACE, "settlement:"+str(value.match_id)),
            uuid5(_ID_NAMESPACE, "event:"+str(value.match_id)), rewards, value.result_version)
        def persist(work):
            current = work.matches.get(value.match_id)
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
        await self.worker.close()
        if self._projections:
            await asyncio.gather(*tuple(self._projections), return_exceptions=True)
        if self.redis is not None:
            await self.redis.close()
        self.database.close()


async def run_server(args):
    if not 0 <= args.port <= 65535:
        raise ValueError("invalid loopback port")
    token = os.environ.get("ARENA_INTERNAL_TOKEN", "")
    if not 32 <= len(token.encode()) <= 256:
        raise ValueError("an internal service credential is required")
    parameters = dict(host=os.environ.get("PGHOST", ""), dbname=os.environ.get("PGDATABASE", ""),
                      user=os.environ.get("PGUSER", ""), connect_timeout=1,
                      port=int(os.environ.get("PGPORT", "5432")))
    for name in ("PGSERVICE", "PGSERVICEFILE", "PGHOSTADDR"):
        os.environ.pop(name, None)
    protocol = Protocol(args.descriptor)
    database = Database(kwargs=parameters, max_size=args.workers, max_waiting=args.workers,
                        acquire_timeout=.2)
    projection = RedisStore(unix_socket_path=args.redis_socket, namespace="arena") if args.redis_socket else None
    service = StorageService(protocol, database, max_workers=args.workers, redis_store=projection)
    server = Server(protocol, service.handlers, service_token=token)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopped.set)
    try:
        port = await server.start("127.0.0.1", args.port)
        print(json.dumps(dict(service="storage", port=port, state="listening")), flush=True)
        await stopped.wait()
    finally:
        await server.close(grace=1)
        await service.close()
        print(json.dumps(dict(service="storage", state="stopped", worker=service.worker.stats,
            pool=database.stats, projection_failed=service.projection_failed)), flush=True)


def main():
    parser = argparse.ArgumentParser(description="Arena internal Storage service")
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--port", type=int, default=7105)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--redis-socket", type=Path)
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
