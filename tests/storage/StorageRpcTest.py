#!/usr/bin/env python3
"""Real private PostgreSQL and actual subprocess Storage RPC acceptance."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
from StorageMigrationTest import Cluster, require, run


class Acceptance:
    def __init__(self, args, cluster):
        self.args, self.cluster = args, cluster
        self.database = "arena_rpc_storage"
        self.process, self.port = None, None
        self.publish_outbox = False
        self.log = []
        self.token = "private-fixture-token-" + "x"*40
        from shared.runtime.protocol import Protocol
        from shared.runtime.rpc import Client
        self.Client = Client
        self.protocol = Protocol(args.descriptor)
        self.report = dict(passed=False,cases=[],clock_fixture=dict(
            common_civil_origin=bool(os.environ.get("ARENA_TEST_CLOCK_REALTIME_NS")),
            inherited_monotonic_origin=bool(os.environ.get("ARENA_TEST_CLOCK_MONOTONIC_NS"))))

    def message(self, name, **values):
        return self.protocol.message("arena.storage."+name)(**values)

    def connection(self):
        import psycopg
        return psycopg.connect(host=str(self.cluster.socket), dbname=self.database,
                              user="arena_test", port=5432, connect_timeout=1, autocommit=True)

    def query(self, sql, values=()):
        with self.connection() as connection:
            cursor = connection.execute(sql, values or None)
            return cursor.fetchall() if cursor.description else []

    def start(self, *, publish=None):
        if publish is not None:
            self.publish_outbox = publish
        environment = self.cluster.environment(self.database)
        environment.update(ARENA_INTERNAL_TOKEN=self.token, PYTHONDONTWRITEBYTECODE="1")
        self.process = subprocess.Popen([sys.executable, "-m", "services.storage.server",
            "--descriptor", str(self.args.descriptor), "--port", "0", "--workers", "2",
            "--redis-socket", str(self.cluster.root/"missing-redis.sock")] +
            (["--publish"] if self.publish_outbox else []),
            cwd=self.args.repository_root, env=environment, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            require(bool(selector.select(8)), "Storage process did not report its bound port")
        line = self.process.stdout.readline()
        if not line:
            raise AssertionError("Storage process exited before listening: "+self.process.stderr.read())
        started = json.loads(line)
        require(started["state"] == "listening", "Storage startup state mismatch")
        self.port = started["port"]
        self.client = self.Client(self.protocol, "127.0.0.1", self.port, service_token=self.token)

    def stop(self):
        if self.process is None:
            return
        process, self.process = self.process, None
        process.terminate()
        output, error = process.communicate(timeout=8)
        require(process.returncode == 0, "Storage process did not drain cleanly: "+error)
        lines = [json.loads(line) for line in output.splitlines() if line]
        require(lines and lines[-1]["state"] == "stopped", "Storage stop report missing")
        stopped = lines[-1]
        require(stopped["worker"]["active"] == 0 and stopped["pool"]["checked_out"] == 0,
                "Storage process leaked worker slots or PostgreSQL loans")
        require(stopped["pool"]["closed"], "Storage pool remained open")
        publisher = stopped["publisher"]
        require(publisher["closed"] and not publisher["active"],
                "outbox worker remained active when the pool closed")
        self.log.append(stopped)

    async def call(self, method, request_name, reply_name, **values):
        payload = self.message(request_name, **values).SerializeToString()
        response = await self.client.call(method, payload, timeout=2)
        return self.protocol.message("arena.storage."+reply_name).FromString(response)

    async def expect(self, action, status):
        from shared.runtime.rpc import RpcError
        try:
            await action()
        except RpcError as error:
            require(error.status == status, f"expected RPC status {status}, got {error.status}")
            return
        raise AssertionError("expected RPC failure")

    async def case(self, name, action):
        self.report["current_case"] = name
        started = time.monotonic()
        details = await action() or {}
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                           "AND state LIKE 'idle in transaction%'")[0][0] == 0,
                "idle or aborted transaction leaked")
        self.report["cases"].append(dict(name=name, passed=True,
            elapsed=time.monotonic()-started, **details))

    def setup(self):
        self.cluster.create(self.database)
        result = run([sys.executable, self.args.repository_root/"services/storage/migrate.py", "up"],
                     self.cluster.environment(self.database), timeout=95)
        require(result.returncode == 0, "Storage migrations failed: "+result.stderr)
        fixture = self.args.repository_root/"services/storage/fixtures/seed.sql"
        self.cluster.sql(fixture.read_text(), self.database, single=True)
        self.start()

    async def auth_and_lookup(self):
        from shared.runtime.rpc import UNAUTHENTICATED, NOT_FOUND, INVALID_ARGUMENT
        unauthenticated = self.Client(self.protocol, "127.0.0.1", self.port)
        await self.expect(lambda: unauthenticated.call("account.find",
            self.message("FindAccountRequest", account_name="alice").SerializeToString()), UNAUTHENTICATED)
        reply = await self.call("account.find", "FindAccountRequest", "FindAccountReply", account_name="alice")
        require(reply.account.account_id == 1 and reply.player.player_id == 42
                and reply.account.password_digest.startswith("$arena-pbkdf2-sha256$")
                and reply.account.state == "active", "canonical account/player lookup changed")
        await self.expect(lambda: self.call("account.find", "FindAccountRequest",
            "FindAccountReply", account_name="does_not_exist"), NOT_FOUND)
        await self.expect(lambda: self.call("match.allocate_ids", "AllocateIdsRequest",
            "AllocateIdsReply", count=0), INVALID_ARGUMENT)
        return dict(auth_required=True, canonical_player=reply.player.player_id)

    async def wire_contract(self):
        from shared.runtime.rpc import NOT_FOUND, INVALID_ARGUMENT
        contract = json.loads((self.args.repository_root/'services/storage/contract.json').read_text())
        declared = {value["name"]: value for value in contract["methods"]}
        require(len(declared) == 7, "all deployed Storage APIs require contracts")
        require(set(declared) == {"account.find", "player.get", "match.allocate_ids",
            "match.save_result", "match.get", "settlement.get", "health.ready"},
            "wire acceptance must cover every registered method")
        request_fields = self.protocol.Request.DESCRIPTOR.fields_by_name
        response_fields = self.protocol.Response.DESCRIPTOR.fields_by_name
        require(all(name in request_fields for name in
            ("deadline_unix_ms", "correlation_id", "idempotency_key", "method")),
            "request envelope lost the deadline/idempotency contract")
        require(all(name in response_fields for name in ("status", "retryable", "correlation_id")),
            "response envelope lost the error contract")
        for name, value in declared.items():
            require(value["owner"] == "storage" and value["logical_owner"]
                and 0 < value["deadline_ms"] <= 5000 and "errors" in value
                and value["idempotency"]["automatic_retries"] == 0,
                "incomplete contract for "+name)
            for direction in ("request", "response"):
                if value[direction] is not None:
                    self.protocol.message("arena.storage."+value[direction])
            # Each actual handler must reject a malformed payload through the
            # same authenticated TCP transport used by production clients.
            await self.expect(lambda name=name: self.client.call(name, b"\xff"), INVALID_ARGUMENT)
        require(await self.client.call("health.ready") == b"", "readiness reply must be empty")
        player = await self.call("player.get", "PlayerGetRequest", "PlayerGetReply", player_id=42)
        require(player.player.player_id == 42 and player.player.account_id == 1,
                "player.get returned another canonical identity")
        for method, request, reply, field in (
            ("player.get", "PlayerGetRequest", "PlayerGetReply", "player_id"),
            ("match.get", "MatchGetRequest", "MatchGetReply", "match_id"),
            ("settlement.get", "SettlementGetRequest", "SettlementGetReply", "settlement_id")):
            await self.expect(lambda: self.call(method, request, reply, **{field: (1 << 63)-1}), NOT_FOUND)
        await self.expect(lambda: self.client.call("missing.method"), NOT_FOUND)
        payload = self.message("AllocateIdsRequest", count=1).SerializeToString()
        ranges = [self.message("AllocateIdsReply").FromString(await self.client.call(
            "match.allocate_ids", payload, idempotency_key="same-key-does-not-deduplicate"))
            for _ in range(2)]
        require(ranges[0].first == ranges[0].last and ranges[1].first == ranges[1].last
                and ranges[0].last < ranges[1].first,
                "non-idempotent allocation must not reuse a block on the same transport key")
        return dict(declared_methods=sorted(declared), malformed_payloads_rejected=7,
                    readiness_empty_reply=True, canonical_player_lookup=True,
                    id_allocation_non_idempotent=True, automatic_retries=0)

    def result(self, match_id, *, state="completed", reason="completed"):
        return self.message("MatchRecord", match_id=match_id, room_id=(1 << 63)|match_id,
            state=state, result_version=1, final_tick=100, final_checksum=(1 << 64)-1,
            finish_reason=reason, participants=[
                dict(player_id=42, slot=1, session_id=501, membership_epoch=7, initial_mmr=1000,
                     last_input_seq=10, final_x=-5, final_y=8),
                dict(player_id=43, slot=2, session_id=502, membership_epoch=8, initial_mmr=1200,
                     last_input_seq=11, final_x=6, final_y=-9)])

    async def persist_restart_replay(self):
        from shared.runtime.rpc import CONFLICT
        ids = await self.call("match.allocate_ids", "AllocateIdsRequest", "AllocateIdsReply", count=8)
        require(ids.first > 1006 and ids.last == ids.first+7, "allocator reused seed identities")
        value = self.result(ids.first)
        reply = await self.call("match.save_result", "SaveResultRequest", "SaveResultReply", match=value)
        require(not reply.replayed and not reply.settlement.replayed, "initial save replayed")
        require(reply.match.final_checksum == (1 << 64)-1
                and reply.match.participants[0].final_x == -5, "result projection lost fields")
        before = self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 "
                            "AND player_id IN (42,43) ORDER BY player_id")
        require([(int(player), quantity) for player, quantity in before] == [(42,110),(43,260)],
                "completed match did not atomically credit fixed rewards")
        self.stop()
        self.start()
        self.saved_match = value
        if self.args.native_probe:
            environment = dict(os.environ, ARENA_INTERNAL_TOKEN=self.token)
            probe = subprocess.run([str(self.args.native_probe), str(self.port), "alice", str(ids.first)],
                env=environment, capture_output=True, text=True, timeout=5)
            require(probe.returncode == 0, "native Storage RPC probe failed: "+probe.stderr)
            require(json.loads(probe.stdout) == dict(player_id="42", match_id=str(ids.first),
                settlement_id=str(ids.first)), "native Storage RPC probe returned wrong durable identities")
            self.report["native_probe_passed"] = True
        for _ in range(5):
            replay = await self.call("match.save_result", "SaveResultRequest", "SaveResultReply", match=value)
            require(replay.replayed and replay.settlement.replayed
                    and replay.settlement.event_id == reply.settlement.event_id, "restart lost idempotency")
        require(self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 "
                           "AND player_id IN (42,43) ORDER BY player_id") == before,
                "result retries credited inventory again")
        found = await self.call("match.get", "MatchGetRequest", "MatchGetReply", match_id=ids.first)
        receipt = await self.call("settlement.get", "SettlementGetRequest",
                                 "SettlementGetReply", settlement_id=ids.first)
        require(found.match == reply.match and receipt.settlement.event_id == reply.settlement.event_id,
                "durable reads differ after restart")
        changed = self.result(ids.first)
        changed.final_checksum = 7
        await self.expect(lambda: self.call("match.save_result", "SaveResultRequest",
            "SaveResultReply", match=changed), CONFLICT)
        later = await self.call("match.allocate_ids", "AllocateIdsRequest", "AllocateIdsReply", count=2)
        require(later.first > ids.last, "process restart reset reserved IDs")
        require(self.query("SELECT count(*) FROM arena.asset_ledger WHERE settlement_id=%s",
                           (ids.first,))[0][0] == 2, "reward ledger duplicated")
        require(self.query("SELECT count(*) FROM arena.outbox WHERE settlement_id=%s",
                           (ids.first,))[0][0] == 1, "outbox duplicated")
        return dict(match_id=ids.first, restart_replays=5, inventory=list(map(lambda r:[int(r[0]),r[1]],before)),
                    redis_available=False, durable_ids_after_restart=later.first)

    async def abort_and_rollback(self):
        from shared.runtime.rpc import CONFLICT
        ids = await self.call("match.allocate_ids", "AllocateIdsRequest", "AllocateIdsReply", count=2)
        balances = self.query("SELECT sum(quantity) FROM arena.inventory WHERE item_id=1")[0][0]
        aborted = self.result(ids.first, state="aborted", reason="ready_timeout")
        reply = await self.call("match.save_result", "SaveResultRequest", "SaveResultReply", match=aborted)
        require(reply.match.state == "aborted", "abort was promoted to completed")
        require(self.query("SELECT sum(quantity) FROM arena.inventory WHERE item_id=1")[0][0] == balances,
                "aborted match awarded assets")
        invalid = self.result(ids.last)
        invalid.participants[0].player_id = 999999
        await self.expect(lambda: self.call("match.save_result", "SaveResultRequest",
            "SaveResultReply", match=invalid), CONFLICT)
        require(self.query("SELECT count(*) FROM arena.game_match WHERE match_id=%s",
                           (ids.last,))[0][0] == 0, "failed roster left partial match")
        require(self.query("SELECT count(*) FROM arena.settlement WHERE match_id=%s",
                           (ids.last,))[0][0] == 0, "failed roster left partial settlement")
        return dict(abort_no_reward=True, failed_roster_rolled_back=True)

    async def concurrent_id_and_result(self):
        from shared.runtime.rpc import RpcError, CONFLICT
        def outcome(reply):
            if isinstance(reply, RpcError):
                return dict(kind="RpcError", status=reply.status, retryable=reply.retryable)
            if isinstance(reply, BaseException):
                return dict(kind=type(reply).__name__)
            return dict(kind="SaveResultReply", checksum=reply.match.final_checksum,
                        replayed=reply.replayed, event_id=reply.settlement.event_id)
        ranges = await asyncio.gather(*(self.call("match.allocate_ids", "AllocateIdsRequest",
            "AllocateIdsReply", count=3) for _ in range(2)))
        identities = [identity for value in ranges for identity in range(value.first, value.last+1)]
        require(len(set(identities)) == 6, "concurrent allocator returned overlapping ranges")
        balances = self.query("SELECT player_id,quantity,revision FROM arena.inventory WHERE item_id=1 "
                              "AND player_id IN (42,43) ORDER BY player_id")
        value = self.result(identities[0])
        # A private trigger widens the actual unique-key race, without mocking PG.
        self.query("CREATE FUNCTION arena.rpc_insert_barrier() RETURNS trigger LANGUAGE plpgsql AS "
                   "'BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END'")
        self.query("CREATE TRIGGER rpc_insert_barrier BEFORE INSERT ON arena.game_match "
                   "FOR EACH ROW EXECUTE FUNCTION arena.rpc_insert_barrier()")
        try:
            replies = await asyncio.gather(*(self.call("match.save_result", "SaveResultRequest",
                "SaveResultReply", match=value) for _ in range(2)), return_exceptions=True)
            self.report["concurrent_duplicate_outcomes"] = [outcome(reply) for reply in replies]
            require(all(not isinstance(reply, BaseException) for reply in replies)
                    and sum(not reply.replayed for reply in replies) == 1,
                    "concurrent duplicate results did not produce one original commit: "
                    + json.dumps(self.report["concurrent_duplicate_outcomes"]))
            require(replies[0].match == replies[1].match
                    and replies[0].settlement.event_id == replies[1].settlement.event_id
                    and sum(not reply.settlement.replayed for reply in replies) == 1,
                    "concurrent duplicate results did not retain one canonical match/receipt")
            conflict_a = self.result(identities[1])
            conflict_b = self.result(identities[1])
            conflict_b.final_checksum = 42
            commands = (conflict_a, conflict_b)
            contested = await asyncio.gather(*(self.call("match.save_result", "SaveResultRequest",
                "SaveResultReply", match=command) for command in commands), return_exceptions=True)
            self.report["concurrent_result_outcomes"] = [outcome(reply) for reply in contested]
            self.report["concurrent_result_database"] = dict(
                terminal=[(state, int(checksum)) for state, checksum in self.query(
                    "SELECT state,final_checksum FROM arena.game_match WHERE match_id=%s",
                    (identities[1],))],
                ledger=self.query("SELECT count(*) FROM arena.asset_ledger WHERE settlement_id=%s",
                                  (identities[1],))[0][0],
                receipt=self.query("SELECT count(*) FROM arena.settlement WHERE match_id=%s",
                                   (identities[1],))[0][0])
            require(sum(isinstance(reply, RpcError) and reply.status == CONFLICT
                        for reply in contested) == 1,
                    "concurrent mismatched result did not reject exactly one payload: "
                    + json.dumps(self.report["concurrent_result_outcomes"]))
            winners = [(index, reply) for index, reply in enumerate(contested)
                       if not isinstance(reply, BaseException)]
            require(len(winners) == 1 and not winners[0][1].replayed,
                    "concurrent mismatched result did not acknowledge exactly one original winner")
            winner_index, winner = winners[0]
            canonical = self.message("MatchRecord")
            canonical.CopyFrom(winner.match)
            for field in ("created_unix_ms", "started_unix_ms", "finished_unix_ms"):
                canonical.ClearField(field)
            require(canonical == commands[winner_index],
                    "acknowledged winner does not match its full original payload")
            found = await self.call("match.get", "MatchGetRequest", "MatchGetReply",
                                    match_id=identities[1])
            require(found.match == winner.match, "mismatched loser overwrote the canonical result")
            for identity in identities[:2]:
                counts = self.query("SELECT "
                    "(SELECT count(*) FROM arena.asset_ledger WHERE settlement_id=%s),"
                    "(SELECT count(*) FROM arena.settlement WHERE match_id=%s),"
                    "(SELECT count(*) FROM arena.outbox WHERE settlement_id=%s)",
                    (identity, identity, identity))[0]
                require(counts == (2,1,1), "concurrent result left duplicate/orphan credits or receipts")
            require(self.query("SELECT player_id,quantity,revision FROM arena.inventory WHERE item_id=1 "
                               "AND player_id IN (42,43) ORDER BY player_id")
                    == [(player, quantity+20, revision+2) for player, quantity, revision in balances],
                    "concurrent result commands changed assets beyond two authoritative rewards")
        finally:
            self.query("DROP TRIGGER rpc_insert_barrier ON arena.game_match")
            self.query("DROP FUNCTION arena.rpc_insert_barrier()")
        return dict(concurrent_id_ranges=2, distinct_ids=6, concurrent_duplicate_requests=2,
                    concurrent_result_replays=1, original_commits=2, exact_inventory_once=True,
                    concurrent_conflict_rejected=True)

    async def database_outage_recovery(self):
        from shared.runtime.rpc import RpcError, OVERLOADED, UNAVAILABLE
        stopped = run([self.cluster.tool("pg_ctl"), "-D", self.cluster.data, "-m", "fast",
                       "-w", "-t", "15", "stop"], self.cluster.base, timeout=20)
        require(stopped.returncode == 0, "could not stop private PostgreSQL")
        try:
            try:
                await self.call("account.find", "FindAccountRequest", "FindAccountReply", account_name="alice")
            except RpcError as error:
                require(error.status in (OVERLOADED, UNAVAILABLE), "database outage returned wrong classification")
            else:
                raise AssertionError("database outage silently served authoritative lookup")
        finally:
            options = (f"-c listen_addresses='' -c unix_socket_directories='{self.cluster.socket}' "
                       "-c unix_socket_permissions=0700 -c port=5432 -c max_connections=16 "
                       "-c log_min_messages=warning -c timezone=UTC")
            restarted = run([self.cluster.tool("pg_ctl"), "-D", self.cluster.data, "-l", self.cluster.log,
                             "-w", "-t", "15", "-o", options, "start"], self.cluster.base, timeout=20)
            if restarted.returncode:
                self.report["pg_restart_failure"] = dict(
                    returncode=restarted.returncode, stdout=restarted.stdout,
                    stderr=restarted.stderr, server_log=(
                        self.cluster.log.read_text(errors="replace")[-12000:]
                        if self.cluster.log.exists() else "missing"))
            require(restarted.returncode == 0, "could not restart private PostgreSQL: "
                    + restarted.stdout + restarted.stderr)
        deadline = time.monotonic()+5
        while True:
            try:
                await self.client.call("health.ready", timeout=1)
                break
            except RpcError:
                require(time.monotonic() < deadline, "same Storage process failed to recover its database pool")
                await asyncio.sleep(.05)
        reply = await self.call("match.save_result", "SaveResultRequest", "SaveResultReply", match=self.saved_match)
        require(reply.replayed and reply.settlement.replayed, "database restart lost committed receipt")
        return dict(same_process_recovered=True, post_restart_result_replayed=True)

    async def worker_cancellation(self):
        from shared.runtime.rpc import Context, OVERLOADED
        from services.storage.database import Database
        from services.storage.worker import Worker
        database = Database(kwargs=dict(host=str(self.cluster.socket), dbname=self.database,
            user="arena_test", connect_timeout=1), max_size=1, max_waiting=1)
        worker = Worker(database, max_workers=1)
        entered = threading.Event()
        def slow(work):
            entered.set()
            work._session.connection().execute("SELECT pg_sleep(5)")
        context = Context(1, "cancel-test", time.monotonic()+2, "")
        task = asyncio.create_task(worker.run(slow, context))
        try:
            deadline = time.monotonic()+2
            while not entered.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(.005)
            require(entered.is_set(), "worker did not enter real PostgreSQL query")
            await self.expect(lambda: worker.run(lambda _: None, context), OVERLOADED)
            deadline = time.monotonic()+1
            while not self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                                 "AND state='active' AND query='SELECT pg_sleep(5)'")[0][0]:
                require(time.monotonic() < deadline, "real worker query never became active")
                await asyncio.sleep(.005)
            cancellation_started = time.monotonic()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            # The caller has gone; capacity is still held until SQL and cleanup finish.
            deadline = cancellation_started+.1
            while worker.stats["active"] and time.monotonic() < deadline:
                await asyncio.sleep(.001)
            require(worker.stats["active"] == 0 and database.stats["checked_out"] == 0,
                    "cancellation released caller but leaked SQL worker/loan")
            cancellation_elapsed = time.monotonic()-cancellation_started
            require(cancellation_elapsed <= .1, "actual PG worker/loan cancellation exceeded100ms")
            result = await worker.run(lambda work: work.accounts.get_player(42),
                Context(2, "reuse-test", time.monotonic()+1, ""))
            require(result.player_id == 42, "cancelled old request poisoned next borrower")
            return dict(real_pg_sleep_cancelled=True, worker_peak=worker.stats["peak"],
                        final_checked_out=database.stats["checked_out"],
                        cancellation_elapsed=cancellation_elapsed)
        finally:
            await worker.close()
            database.close()

    async def wait_for(self, check, message, *, timeout=8):
        deadline = time.monotonic()+timeout
        while True:
            value = check()
            if value:
                return value
            require(time.monotonic() < deadline, message)
            await asyncio.sleep(.005)

    async def pump_close_cancellation(self, *, service_close=True):
        from shared.runtime.rpc import Context
        from services.storage.database import Database
        from services.storage.server import StorageService
        database = Database(kwargs=dict(host=str(self.cluster.socket),dbname=self.database,
            user="arena_test",port=5432,connect_timeout=1,
            application_name="arena-pump-cancel-close"),max_size=1,max_waiting=1)
        service = StorageService(self.protocol,database,max_workers=1,publish=True)
        closing = None
        trigger_installed = False
        try:
            service.start()
            # This publisher claims the oldest eligible event. Drain fixture
            # backlog before creating the one identity whose shutdown we test.
            await self.wait_for(lambda:self.query("SELECT count(*) FROM arena.outbox "
                "WHERE state='pending'")[0][0]==0,"local publisher did not drain fixture backlog")
            first,last = await service.worker.run(lambda work:work.ids.reserve(1),
                Context(71,"pump-close-reserve",time.monotonic()+2,""))
            self.query("CREATE FUNCTION arena.rpc_cancel_close_consumer() RETURNS trigger LANGUAGE plpgsql AS "
                "$body$ BEGIN PERFORM pg_sleep(0.15); RETURN NEW; END $body$")
            self.query("CREATE TRIGGER rpc_cancel_close_consumer BEFORE INSERT ON arena.consumer_receipt "
                "FOR EACH ROW EXECUTE FUNCTION arena.rpc_cancel_close_consumer()")
            trigger_installed = True
            response = await service._save(self.message("SaveResultRequest",match=self.result(first)),
                Context(72,"pump-close-save",time.monotonic()+2,""))
            event = response.settlement.event_id
            await self.wait_for(lambda:self.query("SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND application_name='arena-pump-cancel-close' "
                "AND state='active' AND query LIKE 'INSERT INTO arena.consumer_receipt(consumer,event_id,payload_hash)%'")[0][0],
                "local publisher did not enter its actual PostgreSQL consume barrier")
            closing = asyncio.create_task(service.close() if service_close else service.publisher.close())
            await asyncio.sleep(.005)
            closing.cancel()
            await asyncio.sleep(.005)
            require(not closing.done(),"cancelled close returned before its actual SQL publisher joined")
            require(not database.stats["closed"],"cancelled close closed the pool while SQL was running")
            closing.cancel()
            await asyncio.wait_for(asyncio.shield(closing),1.5)
            require(service.publisher.stats["closed"] and not service.publisher.stats["active"]
                    and database.stats["checked_out"]==0,
                    "cancelled publisher close leaked its actual SQL worker or loan")
            require(database.stats["closed"] == service_close,
                    "publisher and service pool ownership boundaries were confused")
            require(self.query("SELECT state FROM arena.outbox WHERE event_id=%s",(event,))[0][0]=="published",
                    "cancelled close abandoned the actual in-flight acknowledgement")
            require(self.query("SELECT count(*) FROM arena.consumer_receipt "
                "WHERE consumer='reward_audit' AND event_id=%s",(event,))[0][0]==1,
                "cancelled close lost or duplicated its consumer receipt")
            await service.close()
            return dict(actual_inflight_sql=True,close_cancel_requests=2,published=True,
                scope="service" if service_close else "standalone_publisher",
                worker_joined=True,pool_closed=database.stats["closed"],checked_out=database.stats["checked_out"])
        finally:
            if not database.stats["closed"]:
                await service.close()
            if closing is not None:
                await asyncio.gather(closing,return_exceptions=True)
            if trigger_installed:
                self.query("DROP TRIGGER rpc_cancel_close_consumer ON arena.consumer_receipt")
                self.query("DROP FUNCTION arena.rpc_cancel_close_consumer()")

    async def cancelled_close_lifetimes(self):
        return dict(service=await self.pump_close_cancellation(),
                    publisher=await self.pump_close_cancellation(service_close=False))

    async def runtime_outbox(self):
        pending = self.query("SELECT count(*) FROM arena.outbox WHERE state='pending'")[0][0]
        require(pending > 0, "default-off publisher unexpectedly drained fixture events")
        balances = self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 "
                              "AND player_id IN (42,43) ORDER BY player_id")
        target = int(self.saved_match.match_id)
        event = self.query("SELECT event_id FROM arena.outbox WHERE settlement_id=%s", (target,))[0][0]
        self.stop()
        require(not self.log[-1]["publisher"]["enabled"], "publisher default changed")
        # Sequence increments survive transaction rollback, proving every ack
        # retry failed after the consumer had already committed its receipt.
        self.query("CREATE SEQUENCE arena.rpc_ack_failures")
        self.query("CREATE FUNCTION arena.rpc_ack_failure() RETURNS trigger LANGUAGE plpgsql AS "
                   "$body$ BEGIN PERFORM nextval('arena.rpc_ack_failures'); "
                   "RAISE EXCEPTION 'private acknowledgement barrier' USING ERRCODE='40001'; END $body$")
        self.query("CREATE TRIGGER rpc_ack_failure BEFORE UPDATE ON arena.outbox FOR EACH ROW "
                   f"WHEN (NEW.state='published' AND NEW.settlement_id={target}) "
                   "EXECUTE FUNCTION arena.rpc_ack_failure()")
        self.start(publish=True)
        try:
            await self.wait_for(lambda: self.query("SELECT is_called AND last_value>=3 "
                "FROM arena.rpc_ack_failures")[0][0], "runtime publisher did not exhaust the injected ack retries")
            require(self.query("SELECT count(*) FROM arena.consumer_receipt "
                               "WHERE consumer='reward_audit' AND event_id=%s", (event,))[0][0] == 1,
                    "consumer did not commit before acknowledgement failure")
            require(self.query("SELECT state FROM arena.outbox WHERE event_id=%s",
                               (event,))[0][0] == "pending", "failed acknowledgement marked the event published")
        finally:
            self.query("DROP TRIGGER rpc_ack_failure ON arena.outbox")
            self.query("DROP FUNCTION arena.rpc_ack_failure()")
        await self.wait_for(lambda: self.query("SELECT count(*) FROM arena.outbox "
            "WHERE state='pending'")[0][0] == 0, "runtime publisher did not drain the durable backlog")
        attempts = self.query("SELECT attempt_count FROM arena.outbox WHERE event_id=%s", (event,))[0][0]
        require(attempts >= 2, "post-delivery acknowledgement failure was not redelivered")
        require(self.query("SELECT count(*) FROM arena.consumer_receipt "
                           "WHERE consumer='reward_audit' AND event_id=%s", (event,))[0][0] == 1,
                "runtime redelivery duplicated the consumer receipt")
        require(self.query("SELECT count(*),sum(quantity) FROM arena.settlement_reward_projection "
                           "WHERE consumer='reward_audit' AND event_id=%s", (event,))[0] == (2,20),
                "runtime redelivery duplicated or lost the ledger projection")
        require(self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 "
                           "AND player_id IN (42,43) ORDER BY player_id") == balances,
                "consumer redelivery minted producer rewards again")
        self.query("DROP SEQUENCE arena.rpc_ack_failures")

        # Stop during actual SQL, not merely between polling iterations. The
        # pump must finish consume+ack before StorageService closes the pool.
        self.query("CREATE FUNCTION arena.rpc_slow_consumer() RETURNS trigger LANGUAGE plpgsql AS "
                   "$body$ BEGIN PERFORM pg_sleep(0.15); RETURN NEW; END $body$")
        self.query("CREATE TRIGGER rpc_slow_consumer BEFORE INSERT ON arena.consumer_receipt "
                   "FOR EACH ROW EXECUTE FUNCTION arena.rpc_slow_consumer()")
        try:
            allocated = await self.call("match.allocate_ids", "AllocateIdsRequest", "AllocateIdsReply", count=1)
            value = self.result(allocated.first)
            reply = await self.call("match.save_result", "SaveResultRequest", "SaveResultReply", match=value)
            live_event = reply.settlement.event_id
            await self.wait_for(lambda: self.query("SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' "
                "AND query LIKE 'INSERT INTO arena.consumer_receipt(consumer,event_id,payload_hash)%'")[0][0],
                "runtime consumer did not enter the private PostgreSQL barrier")
            self.stop()
        finally:
            self.query("DROP TRIGGER rpc_slow_consumer ON arena.consumer_receipt")
            self.query("DROP FUNCTION arena.rpc_slow_consumer()")
        stopped = self.log[-1]
        require(stopped["publisher"]["enabled"] and stopped["publisher"]["max_workers"] == 1
                and stopped["publisher"]["deduplicated"] >= 1 and stopped["publisher"]["errors"] >= 1,
                "runtime publisher did not report its bounded retry/dedup lifecycle")
        facts = self.query("SELECT state,attempt_count,created_at,updated_at,available_at,"
            "last_attempt_at,published_at,clock_timestamp() FROM arena.outbox WHERE event_id=%s",
            (live_event,))[0]
        self.report["outbox_shutdown"] = dict(event_id=live_event,
            publisher=stopped["publisher"],row=dict(zip(
                ("state","attempt_count","created_at","updated_at","available_at",
                 "last_attempt_at","published_at","database_now"),
                (value.isoformat() if hasattr(value,"isoformat") else value for value in facts))))
        require(facts[0] == "published", "shutdown abandoned an in-flight acknowledgement")
        require(self.query("SELECT count(*) FROM arena.consumer_receipt "
                           "WHERE consumer='reward_audit' AND event_id=%s", (live_event,))[0][0] == 1,
                "shutdown lost or duplicated the in-flight consumer receipt")
        after = self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 "
                           "AND player_id IN (42,43) ORDER BY player_id")
        require(after == [(player, quantity+10) for player, quantity in balances],
                "runtime consumption changed assets beyond the new producer reward")
        return dict(default_off_preserved=True, pending_drained=pending,
            redelivery_attempts=attempts, consumer_receipts=1, reward_projection_rows=2,
            shutdown_during_consume=True, publisher=stopped["publisher"])

    async def run(self):
        for name, action in (
            ("authenticated_canonical_account_and_validation", self.auth_and_lookup),
            ("all_registered_rpc_wire_contracts", self.wire_contract),
            ("persist_restart_idempotent_result_with_redis_down", self.persist_restart_replay),
            ("abort_and_atomic_fk_failure", self.abort_and_rollback),
            ("concurrent_durable_ids_and_idempotent_result", self.concurrent_id_and_result),
            ("private_pg_outage_same_process_recovery", self.database_outage_recovery),
            ("bounded_worker_actual_pg_cancellation_and_reuse", self.worker_cancellation),
            ("opt_in_runtime_outbox_drain_dedup_and_inflight_shutdown", self.runtime_outbox),
            ("cancelled_publisher_close_joins_actual_sql_and_pool", self.cancelled_close_lifetimes)):
            await self.case(name, action)
        self.stop()
        self.report["service_shutdowns"] = self.log
        self.report["passed"] = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--pg-share", type=Path)
    parser.add_argument("--pg-lib", type=Path)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--native-probe", type=Path)
    args = parser.parse_args()
    args.repository_root, args.descriptor, args.report = (
        value.resolve() for value in (args.repository_root, args.descriptor, args.report))
    sys.path.insert(0, str(args.repository_root))
    cluster = Cluster(args)
    test = Acceptance(args, cluster)
    try:
        cluster.start()
        test.setup()
        asyncio.run(test.run())
    except BaseException as error:
        test.report["error"], test.report["error_type"] = str(error), type(error).__name__
        raise
    finally:
        try:
            test.stop()
        finally:
            test.report["service_shutdowns"] = test.log
            if not test.report["passed"] and cluster.log.exists():
                test.report["server_log_tail"] = cluster.log.read_text(errors="replace")[-8000:]
            cluster.close()
            test.report["cluster_cleaned"] = not cluster.root.exists()
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(test.report, indent=2)+"\n")
    print("StorageRpcAcceptance: passed")


if __name__ == "__main__":
    main()
