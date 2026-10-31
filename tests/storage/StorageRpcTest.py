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
        self.log = []
        self.token = "private-fixture-token-" + "x"*40
        from shared.runtime.protocol import Protocol
        from shared.runtime.rpc import Client
        self.Client = Client
        self.protocol = Protocol(args.descriptor)
        self.report = dict(passed=False, cases=[])

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

    def start(self):
        environment = self.cluster.environment(self.database)
        environment.update(ARENA_INTERNAL_TOKEN=self.token, PYTHONDONTWRITEBYTECODE="1")
        self.process = subprocess.Popen([sys.executable, "-m", "services.storage.server",
            "--descriptor", str(self.args.descriptor), "--port", "0", "--workers", "2",
            "--redis-socket", str(self.cluster.root/"missing-redis.sock")],
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
        ranges = await asyncio.gather(*(self.call("match.allocate_ids", "AllocateIdsRequest",
            "AllocateIdsReply", count=3) for _ in range(2)))
        identities = [identity for value in ranges for identity in range(value.first, value.last+1)]
        require(len(set(identities)) == 6, "concurrent allocator returned overlapping ranges")
        value = self.result(identities[0])
        # A private trigger widens the actual unique-key race, without mocking PG.
        self.query("CREATE FUNCTION arena.rpc_insert_barrier() RETURNS trigger LANGUAGE plpgsql AS "
                   "'BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END'")
        self.query("CREATE TRIGGER rpc_insert_barrier BEFORE INSERT ON arena.game_match "
                   "FOR EACH ROW EXECUTE FUNCTION arena.rpc_insert_barrier()")
        try:
            replies = await asyncio.gather(*(self.call("match.save_result", "SaveResultRequest",
                "SaveResultReply", match=value) for _ in range(2)))
            require(sum(not reply.replayed for reply in replies) == 1,
                    "concurrent duplicate results did not produce one original commit")
            from shared.runtime.rpc import RpcError, CONFLICT
            conflict_a = self.result(identities[1])
            conflict_b = self.result(identities[1])
            conflict_b.final_checksum = 42
            contested = await asyncio.gather(*(self.call("match.save_result", "SaveResultRequest",
                "SaveResultReply", match=command) for command in (conflict_a, conflict_b)),
                return_exceptions=True)
            require(sum(isinstance(reply, RpcError) and reply.status == CONFLICT
                        for reply in contested) == 1,
                    "concurrent mismatched result did not reject exactly one payload")
            require(self.query("SELECT count(*) FROM arena.asset_ledger WHERE settlement_id IN (%s,%s)",
                               (identities[0], identities[1]))[0][0] == 4,
                    "concurrent result commands duplicated or orphaned reward credits")
        finally:
            self.query("DROP TRIGGER rpc_insert_barrier ON arena.game_match")
            self.query("DROP FUNCTION arena.rpc_insert_barrier()")
        return dict(concurrent_id_ranges=2, distinct_ids=6, concurrent_result_replays=2,
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
            require(restarted.returncode == 0, "could not restart private PostgreSQL")
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
            deadline = time.monotonic()+3
            while worker.stats["active"] and time.monotonic() < deadline:
                await asyncio.sleep(.005)
            require(worker.stats["active"] == 0 and database.stats["checked_out"] == 0,
                    "cancellation released caller but leaked SQL worker/loan")
            require(time.monotonic()-cancellation_started < .75, "PG cancellation waited for statement timeout")
            result = await worker.run(lambda work: work.accounts.get_player(42),
                Context(2, "reuse-test", time.monotonic()+1, ""))
            require(result.player_id == 42, "cancelled old request poisoned next borrower")
            return dict(real_pg_sleep_cancelled=True, worker_peak=worker.stats["peak"],
                        final_checked_out=database.stats["checked_out"],
                        cancellation_elapsed=time.monotonic()-cancellation_started)
        finally:
            await worker.close()
            database.close()

    async def run(self):
        for name, action in (
            ("authenticated_canonical_account_and_validation", self.auth_and_lookup),
            ("persist_restart_idempotent_result_with_redis_down", self.persist_restart_replay),
            ("abort_and_atomic_fk_failure", self.abort_and_rollback),
            ("concurrent_durable_ids_and_idempotent_result", self.concurrent_id_and_result),
            ("private_pg_outage_same_process_recovery", self.database_outage_recovery),
            ("bounded_worker_actual_pg_cancellation_and_reuse", self.worker_cancellation)):
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
            cluster.close()
            test.report["cluster_cleaned"] = not cluster.root.exists()
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(test.report, indent=2)+"\n")
    print("StorageRpcAcceptance: passed")


if __name__ == "__main__":
    main()
