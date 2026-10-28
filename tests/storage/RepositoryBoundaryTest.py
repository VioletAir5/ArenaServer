#!/usr/bin/env python3
"""Real PostgreSQL repository, transaction scope and bounded pool acceptance."""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
sys.dont_write_bytecode = True
from StorageMigrationTest import Cluster, Failure, require, run

MAX_ID = (1 << 64) - 1
DIGEST = "$arena-pbkdf2-sha256$100000$" + "00"*16 + "$" + "11"*32


class RepositoryCluster(Cluster):
    max_connections = 32

    def stop_server(self):
        result = run([self.tool("pg_ctl"), "-D", self.data, "-m", "fast",
                      "-w", "-t", "15", "stop"], self.base, timeout=20)
        require(result.returncode == 0, "cannot stop private test database")

    def restart_server(self):
        options=(f"-c listen_addresses='' -c unix_socket_directories='{self.socket}' "
                 f"-c unix_socket_permissions=0700 -c port=5432 -c max_connections={self.max_connections} "
                 "-c log_min_messages=warning -c timezone=UTC")
        result = run([self.tool("pg_ctl"), "-D", self.data, "-l", self.log,
                      "-w", "-t", "15", "-o", options, "start"], self.base, timeout=20)
        require(result.returncode == 0, "cannot restart private test database")


class Acceptance:
    def __init__(self, args, cluster, pg, database_module, transactions, repositories, application):
        self.args, self.cluster, self.pg = args, cluster, pg
        self.db_module, self.tx, self.repos, self.application = database_module, transactions, repositories, application
        self.database = "arena_repository_boundary"
        self.sequence = 50000
        self.pools = []
        self.report = dict(passed=False, cases=[], psycopg_version=pg.__version__,
                           libpq_version=pg.pq.version(), psycopg_pq_implementation=pg.pq.__impl__)
        self.budget = transactions.RetryBudget(max_attempts=8, max_elapsed=10,
            base_delay=.001, max_delay=.02, lock_timeout_ms=1000, statement_timeout_ms=2000)

    def identity(self):
        self.sequence += 1
        return self.sequence

    def connection_kwargs(self):
        return dict(host=str(self.cluster.socket), port=5432, dbname=self.database,
                    user="arena_test", connect_timeout=1, application_name="arena-repository-boundary")

    def connect(self):
        return self.pg.connect(**self.connection_kwargs(), autocommit=True)

    def query(self, sql, params=None):
        with self.connect() as connection:
            cursor = connection.execute(sql, params)
            return cursor.fetchall() if cursor.description else []

    def pool(self, **options):
        config = dict(kwargs=self.connection_kwargs(), min_size=0, max_size=1,
                      acquire_timeout=.15, max_waiting=1)
        config.update(options)
        database = self.db_module.Database(**config)
        self.pools.append(database)
        return database

    def error(self, action, category, state=None, attempts=None):
        try:
            action()
        except self.tx.StorageError as error:
            require(error.category == category,
                    f"expected {category}, got {error.category}/{error.sqlstate}")
            if state is not None:
                require(error.sqlstate == state, f"expected SQLSTATE {state}, got {error.sqlstate}")
            if attempts is not None:
                require(error.attempts == attempts, f"expected {attempts} attempts, got {error.attempts}")
            return error
        raise Failure("expected StorageError " + category)

    def until(self, predicate, message, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(.01)
        raise Failure(message)

    def released(self, database, *, replenish=False):
        def clean():
            stats = database.stats
            return (stats["checked_out"] == 0 and stats.get("requests_waiting", 0) == 0
                    and 0 <= stats.get("pool_available", 0) <= stats.get("pool_size", 0)
                    and (not replenish or stats.get("pool_available", 0) >= 1))
        self.until(clean, "pool did not release its checked out connections")
        return dict(database.stats)

    def case(self, name, action):
        self.report["current_case"] = name
        started = time.monotonic()
        details = action() or {}
        for database in self.pools:
            require(database.stats["checked_out"] == 0, "case leaked a checked out connection")
        idle = self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                          "AND state LIKE 'idle in transaction%'")[0][0]
        require(idle == 0, "case leaked an idle or aborted PostgreSQL transaction")
        self.report["cases"].append(dict(name=name, passed=True,
            elapsed=time.monotonic()-started, **details))

    def setup(self):
        self.cluster.create(self.database)
        storage = self.args.repository_root/"services"/"storage"
        result = run([sys.executable, storage/"migrate.py", "up"],
                     self.cluster.environment(self.database), timeout=95)
        require(result.returncode == 0, "repository migration failed: " + result.stderr)
        self.cluster.sql((storage/"fixtures"/"seed.sql").read_text(), self.database, single=True)
        self.report["server_version"] = self.query("SHOW server_version")[0][0]
        import psycopg_pool
        self.report["psycopg_pool_version"] = psycopg_pool.__version__

    def participants(self, *, first=42, second=43, session=900):
        return (self.repos.MatchParticipant(first, 1, session, 1, 1000),
                self.repos.MatchParticipant(second, 2, session+1, 2, 1200))

    def repository_roundtrips(self):
        database = self.pool()
        account_id, player_id, match_id, room_id = (self.identity() for _ in range(4))
        name = "Boundary'--"
        def create(uow):
            account = uow.accounts.create(account_id, name, DIGEST)
            player = uow.accounts.create_player(player_id, account_id, "Boundary player", 10000)
            require(account.account_id == account_id and account.password_digest == DIGEST,
                    "account create changed fields")
            require("arena-pbkdf2" not in repr(account), "account repr exposes password digest")
            require(player.mmr == 10000 and player.revision == 0, "player create changed fields")
            require(uow.accounts.find_by_name(name) == account, "name lookup changed account")
            require(uow.accounts.find_by_name(name.lower()) is None, "name lookup lost case sensitivity")
            require(uow.accounts.get(account_id) == account and uow.accounts.get_player(player_id) == player,
                    "same transaction read did not observe creates")
            disabled = uow.accounts.set_state(account_id, "disabled")
            require(disabled.state == "disabled", "account state update was not persisted")
            match = uow.matches.create(match_id, room_id, self.participants(first=player_id))
            require(match.state == "forming" and match.revision == 2
                    and tuple(p.slot for p in match.participants) == (1, 2),
                    "match create did not insert a canonical two-member roster")
            return disabled, player, match
        account, player, forming = database.run(create, self.budget).value
        require(account.updated_at >= account.created_at and player.created_at.tzinfo is not None,
                "repository lost timezone or timestamps")
        try:
            account.state = "active"
        except FrozenInstanceError:
            pass
        else:
            raise Failure("account record is mutable")
        started = max(datetime.now(timezone.utc), forming.created_at)
        running = database.run(lambda u: u.matches.start(match_id, started), self.budget).value
        require(running.state == "running" and running.started_at == started
                and running.revision == 3, "match start roundtrip failed")
        finished = database.run(lambda u: u.matches.finish(
            match_id, started+timedelta(seconds=1), MAX_ID, MAX_ID, player_id), self.budget).value
        require(finished.state == "completed" and finished.final_tick == MAX_ID
                and finished.final_checksum == MAX_ID and finished.winner_player_id == player_id,
                "match finish narrowed uint64 or lost winner")
        recovered = database.run(lambda u: u.matches.get(match_id), self.budget).value
        require(recovered == finished, "terminal match roundtrip changed immutable value")
        abort_id = self.identity()
        created = database.run(lambda u: u.matches.create(
            abort_id, self.identity(), self.participants()), self.budget).value
        aborted = database.run(lambda u: u.matches.abort(
            abort_id, created.created_at+timedelta(seconds=1), 0, MAX_ID, "ready_timeout"),
            self.budget).value
        require(aborted.state == "aborted" and aborted.started_at is None
                and aborted.finish_reason == "ready_timeout", "forming abort roundtrip failed")
        def maximums(uow):
            account = uow.accounts.create(MAX_ID-1, "BoundaryMax", DIGEST)
            player = uow.accounts.create_player(MAX_ID-1, account.account_id, "Maximum boundary", 0)
            peers = (self.repos.MatchParticipant(player.player_id, 1, MAX_ID, MAX_ID, 0),
                     self.repos.MatchParticipant(MAX_ID, 2, MAX_ID-1, MAX_ID-1, 10000))
            return uow.matches.create(MAX_ID-1, MAX_ID, peers), uow.accounts.get(MAX_ID)
        maximum, seeded_maximum = database.run(maximums, self.budget).value
        require(type(maximum.match_id) is int and maximum.match_id == MAX_ID-1
                and maximum.room_id == MAX_ID and seeded_maximum.account_id == MAX_ID,
                "repository narrowed unsigned identifiers")
        require(all(type(p.session_id) is int and type(p.membership_epoch) is int
                    for p in maximum.participants), "participant IDs escaped as numeric wrappers")
        missing = MAX_ID-100
        require(database.run(lambda u:(u.accounts.get(missing),u.accounts.get_player(missing),
                    u.matches.get(missing)), self.budget).value == (None,None,None),
                "missing reads did not return None")
        self.error(lambda: database.run(lambda u:u.accounts.set_state(missing,"active"), self.budget),
                   "not_found", attempts=1)
        self.error(lambda: database.run(lambda u:u.matches.start(missing,started), self.budget),
                   "not_found", attempts=1)
        self.released(database, replenish=True)
        return dict(account_case_sensitive=True, parameterized_name=name, full_uint64_roundtrip=True,
                    forming_running_completed=True, forming_aborted=True)

    def atomicity_and_scope(self):
        database = self.pool()
        retained = []
        successful_id = self.identity()
        def success(uow):
            retained.append((uow, uow.accounts, uow.matches))
            return uow.accounts.create(successful_id, "scope_success", DIGEST)
        database.run(success, self.budget)
        self.error(lambda:retained[0][1].get(successful_id), "scope_closed")
        for outcome in ("business", "sql"):
            account_id, player_id, match_id = (self.identity() for _ in range(3))
            def operation(uow):
                retained.append((uow,uow.accounts,uow.matches))
                uow.accounts.create(account_id, "scope_"+outcome, DIGEST)
                uow.accounts.create_player(player_id,account_id,"Atomic player")
                uow.matches.create(match_id,self.identity(),self.participants(first=player_id))
                if outcome == "business":
                    raise self.tx.StorageError("business_rejected")
                uow.accounts.create(1,"duplicate_primary",DIGEST)
            self.error(lambda:database.run(operation,self.budget),
                       "business_rejected" if outcome=="business" else "integrity_error",
                       None if outcome=="business" else "23505", attempts=1)
            require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(account_id,))[0][0]==0
                    and self.query("SELECT count(*) FROM arena.player WHERE player_id=%s",(player_id,))[0][0]==0
                    and self.query("SELECT count(*) FROM arena.game_match WHERE match_id=%s",(match_id,))[0][0]==0,
                    "cross-repository transaction left partial rows")
            self.error(lambda:retained[-1][1].get(account_id),"scope_closed")
        def foreign(uow):
            with ThreadPoolExecutor(max_workers=1) as workers:
                error = workers.submit(lambda:self.error(lambda:uow.accounts.get(1),"scope_thread")).result(3)
                require(error.category=="scope_thread", "foreign scope operation was admitted")
            for value in (uow,uow.accounts,uow.matches):
                require(not any(hasattr(value,name) for name in
                    ("execute","cursor","connection","commit","rollback","pool","lease")),
                    "public business object exposes raw SQL or connection lifetime")
            return uow.accounts.get(1)
        require(database.run(foreign,self.budget).value.account_id==1, "owner scope failed after foreign rejection")
        arbitrary = self.identity()
        def throw_callback(uow):
            retained.append((uow,uow.accounts,uow.matches))
            uow.accounts.create(arbitrary,"scope_runtime",DIGEST)
            raise RuntimeError("test business callback")
        try:
            database.run(throw_callback,self.budget)
        except RuntimeError:
            pass
        else:
            raise Failure("arbitrary business exception was swallowed")
        require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(arbitrary,))[0][0]==0,
                "arbitrary callback exception committed data")
        self.error(lambda:retained[-1][2].get(1001),"scope_closed")
        self.released(database,replenish=True)
        return dict(commit_rollback_scope_invalidated=True, foreign_thread_rejected=True,
                    arbitrary_callback_rollback=True, account_player_match_atomic=True)

    def sql_free_application(self):
        database = self.pool()
        source = self.args.repository_root/"services"/"storage"/"application.py"
        tree = ast.parse(source.read_text(), filename=str(source))
        forbidden_methods = {"execute","executemany","cursor","commit","rollback",
                             "connect","getconn","putconn","_lease"}
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                modules = [a.name for a in node.names] if isinstance(node,ast.Import) else [node.module or ""]
                require(not any("psycopg" in name or name.endswith("repositories") for name in modules),
                        "application workflow imports a SQL driver or repository implementation")
            if isinstance(node,ast.Attribute):
                require(node.attr not in forbidden_methods, "application workflow calls raw SQL or connection API")
            if isinstance(node,ast.Constant) and isinstance(node.value,str):
                first = node.value.lstrip().upper()
                require(not first.startswith(("SELECT ","INSERT ","UPDATE ","DELETE ","BEGIN","COMMIT")),
                        "application workflow embeds SQL")
        account_id, player_id = self.identity(),self.identity()
        result = self.application.register_player(database,account_id=account_id,
            account_name="application_ok",password_digest=DIGEST,player_id=player_id,
            display_name="SQL free workflow",mmr=2300)
        require(result.value[0].account_id==account_id and result.value[1].player_id==player_id,
                "real application registration failed")
        rollback_id = self.identity()
        self.error(lambda:self.application.register_player(database,account_id=rollback_id,
            account_name="application_rollback",password_digest=DIGEST,player_id=42,
            display_name="Duplicate player"),"integrity_error","23505",attempts=1)
        require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(rollback_id,))[0][0]==0,
                "application player failure left an account")
        self.released(database,replenish=True)
        return dict(ast_sql_boundary=True, real_workflow=True, failed_second_write_atomic=True)

    def invalid_and_nested_operations(self):
        database = self.pool()
        async def coroutine(_):
            return 1
        def generator(_):
            yield 1
        def coroutine_result(_):
            return coroutine(None)
        for operation in (None,coroutine,generator,coroutine_result):
            self.error(lambda:database.run(operation,self.budget),"invalid_operation")
        self.error(lambda:database.run(lambda _:database.run(lambda u:u.accounts.get(1)),self.budget),
                   "nested_transaction",attempts=1)
        require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                "invalid operation left the owner thread marked active")
        for invalid in (0,-1,True,MAX_ID+1):
            self.error(lambda:database.run(lambda u:u.accounts.get(invalid),self.budget),
                       "invalid_request",attempts=1)
        self.released(database,replenish=True)
        return dict(async_generator_rejected=True,nested_rejected=True, invalid_ids_rejected=True)

    def pool_full_and_shutdown(self):
        rounds = []
        for index in range(3):
            database = self.pool(acquire_timeout=.12,max_waiting=1)
            with database._lease(timeout=1) as held:
                pid = held.info.backend_pid
                require(database.stats["checked_out"]==1,"held lease was not accounted")
                started = time.monotonic()
                error = self.error(lambda:database.run(lambda u:u.accounts.get(1),self.budget),
                                   "pool_timeout",attempts=1)
                elapsed = time.monotonic()-started
                require(.07 <= elapsed <= 2 and not error.delays,
                        "pool checkout timeout was unbounded or retried")
            require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                    "pool did not recover after holder returned")
            with database._lease() as held:
                require(held.info.backend_pid==pid,"healthy holder unnecessarily lost pool capacity")
                clipped = self.tx.RetryBudget(max_attempts=1,max_elapsed=.04,
                    base_delay=.001,max_delay=.001)
                started = time.monotonic()
                self.error(lambda:database.run(lambda u:u.accounts.get(1),clipped),
                           "pool_timeout",attempts=1)
                require(time.monotonic()-started < 1,"checkout ignored the shorter transaction budget")
            with ThreadPoolExecutor(max_workers=1) as workers:
                with database._lease() as held:
                    waiting = workers.submit(lambda:self.error(
                        lambda:database.run(lambda u:u.accounts.get(1),self.budget),"pool_closed",attempts=1))
                    self.until(lambda:database.stats.get("requests_waiting",0)==1,
                               "pool waiter did not enter bounded queue",timeout=2)
                    started = time.monotonic()
                    self.error(lambda:database.run(lambda u:u.accounts.get(1),self.budget),
                               "pool_overloaded",attempts=1)
                    require(time.monotonic()-started<1,"overloaded pool waited instead of rejecting")
                    database.close()
                    waiting.result(timeout=5)
                    self.error(lambda:database.run(lambda u:u.accounts.get(1),self.budget),
                               "pool_closed",attempts=1)
                require(held.closed,"outstanding holder was not closed on return to a closed pool")
            self.released(database)
            self.until(lambda:self.query("SELECT count(*) FROM pg_stat_activity WHERE pid=%s",(pid,))[0][0]==0,
                       "closed pool retained its backend")
            rounds.append(dict(round=index+1,checkout_timeout=elapsed,queue_bound=1,closed_waiter=True,
                               final_stats=dict(database.stats)))
        return dict(repetitions=3,rounds=rounds)

    def database_restart_and_backend_loss(self):
        database = self.pool(acquire_timeout=.2,min_size=1)
        rounds = []
        for index in range(3):
            require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                    "pool initial checkout failed")
            with database._lease() as connection:
                dead_idle_pid = connection.info.backend_pid
            require(self.query("SELECT pg_terminate_backend(%s)",(dead_idle_pid,))[0][0],
                    "could not terminate own idle backend")
            require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                    "checkout delivered a dead idle connection to business code")
            with database._lease() as connection:
                require(connection.info.backend_pid!=dead_idle_pid,"pool recycled terminated backend")
            owner_entered, proceed = threading.Event(),threading.Event()
            retained, holder_pid = [],[]
            def operation(uow):
                retained.append(uow.accounts)
                uow.accounts.get(1)
                owner_entered.set()
                require(proceed.wait(4),"test did not release terminated owner")
                return uow.accounts.get(1)
            with ThreadPoolExecutor(max_workers=1) as workers:
                pending = workers.submit(lambda:self.error(
                    lambda:database.run(operation,self.budget),"connection_error",attempts=1))
                require(owner_entered.wait(4),"business owner did not enter transaction")
                pid_rows = self.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                    "AND application_name='arena-repository-boundary' AND state='idle in transaction'")
                require(len(pid_rows)==1,"cannot identify own checked out backend")
                holder_pid.append(pid_rows[0][0])
                require(self.query("SELECT pg_terminate_backend(%s)",(holder_pid[0],))[0][0],
                        "could not terminate checked out backend")
                proceed.set()
                active_error = pending.result(timeout=6)
            require(not active_error.delays,"connection loss was automatically retried")
            self.error(lambda:retained[0].get(1),"scope_closed")
            require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                    "pool did not recover from checked out backend termination")
            self.cluster.stop_server()
            try:
                started = time.monotonic()
                error = self.error(lambda:database.run(lambda u:u.accounts.get(1),self.budget),
                                   "pool_timeout",attempts=1)
                elapsed = time.monotonic()-started
                require(elapsed<2 and not error.delays,"database down admission was unbounded or retried")
                require(database.stats["checked_out"]==0,"database down left a borrowed connection")
            finally:
                self.cluster.restart_server()
            self.until(lambda:database.stats.get("pool_available",0)>=1,
                       "pool did not replenish after database restart",timeout=8)
            require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                    "replenished pool did not recover after database restart")
            rounds.append(dict(round=index+1,database_down_elapsed=elapsed,
                               dead_idle_replaced=True,active_backend_failure=active_error.category,
                               recovered_stats=self.released(database,replenish=True)))
        # Let the SDK exhaust background reconnects, then prove the same pool can grow again.
        self.cluster.stop_server()
        try:
            self.error(lambda:database.run(lambda u:u.accounts.get(1),self.budget),
                       "pool_timeout",attempts=1)
            self.until(lambda:database.stats.get("pool_size",-1)==0,
                       "SDK did not reach its exhausted reconnect state",timeout=8)
            exhausted = dict(database.stats)
            require(exhausted["checked_out"]==0,"long outage retained a borrowed connection")
        finally:
            self.cluster.restart_server()
        probes = []
        recovered = False
        end = time.monotonic()+8
        while time.monotonic()<end:
            try:
                value = database.run(lambda u:u.accounts.get(1),self.budget).value
                require(value.account_id==1,"long outage recovery changed account")
                recovered=True
                break
            except self.tx.StorageError as error:
                require(error.category in ("pool_timeout","pool_overloaded") and error.attempts==1
                        and not error.delays,"recovery probe retried business work or hid another error")
                probes.append(error.category)
                time.sleep(.02)
        require(recovered,"exhausted pool never recovered without a new Database")
        stats = self.released(database,replenish=True)
        database.close()
        return dict(repetitions=3,rounds=rounds,long_outage_exhausted_stats=exhausted,
                    recovery_probe_failures=probes,same_database_recovered=True,recovered_stats=stats)

    def deferred_commit_failures(self):
        self.query("CREATE TABLE public.boundary_fk_parent(id bigint PRIMARY KEY);"
                   "CREATE TABLE public.boundary_fk_child(id bigint REFERENCES public.boundary_fk_parent(id) "
                   "DEFERRABLE INITIALLY DEFERRED)")
        self.query("""CREATE FUNCTION public.boundary_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN INSERT INTO public.boundary_fk_child VALUES(-1); RETURN NEW; END $$;
          CREATE CONSTRAINT TRIGGER boundary_deferred_account AFTER INSERT ON arena.account
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          WHEN (NEW.account_name LIKE 'boundary_deferred_%')
          EXECUTE FUNCTION public.boundary_commit_failure()""")
        parent = self
        class ObserveCommit(self.pg.Connection):
            failures = []
            retained = []
            def commit(connection):
                if ObserveCommit.retained:
                    parent.error(lambda:ObserveCommit.retained[-1].get(1),"scope_closed")
                try:
                    return super().commit()
                except parent.pg.Error as error:
                    ObserveCommit.failures.append(error.sqlstate)
                    raise
        database = self.pool(connection_class=ObserveCommit)
        rounds = []
        try:
            for index in range(3):
                account_id = self.identity()
                made, pids = [],[]
                def operation(uow):
                    ObserveCommit.retained.append(uow.accounts)
                    made.append(uow.accounts.create(account_id,"boundary_deferred_"+str(index),DIGEST))
                    return made[-1]
                with database._lease(timeout=1) as connection:
                    pids.append(connection.info.backend_pid)
                error = self.error(lambda:database.run(operation,self.budget),
                                   "integrity_error","23503",attempts=1)
                require(len(made)==1 and not error.delays,"deferred violation did not fail only at commit")
                require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(account_id,))[0][0]==0
                        and self.query("SELECT count(*) FROM public.boundary_fk_child")[0][0]==0,
                        "real COMMIT failure left partial rows")
                self.error(lambda:ObserveCommit.retained[-1].get(account_id),"scope_closed")
                require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                        "pool lost capacity after commit failure")
                with database._lease() as connection:
                    require(connection.info.backend_pid!=pids[0],"failed commit connection was not discarded")
                rounds.append(dict(round=index+1,sqlstate=error.sqlstate,attempts=error.attempts,
                                   stats=self.released(database,replenish=True)))
            require(ObserveCommit.failures==["23503"]*3,"failures did not originate at actual commit")
        finally:
            database.close()
            self.query("DROP TRIGGER boundary_deferred_account ON arena.account;"
                       "DROP FUNCTION public.boundary_commit_failure();"
                       "DROP TABLE public.boundary_fk_child;DROP TABLE public.boundary_fk_parent")
        return dict(repetitions=3,actual_commit_failures=ObserveCommit.failures,rounds=rounds)

    def serialization_commit_retry(self):
        setup = self.pool()
        left,right = self.identity(),self.identity()
        setup.run(lambda u:(u.accounts.create(left,"serialization_left",DIGEST),
                            u.accounts.create(right,"serialization_right",DIGEST)),self.budget)
        setup.close()
        parent = self
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        seen, failures, saved = set(),[],[]
        class CommitBarrier(self.pg.Connection):
            armed = True
            def commit(connection):
                identity = threading.get_ident()
                with lock:
                    first = CommitBarrier.armed and identity not in seen
                    seen.add(identity)
                if first:
                    barrier.wait(timeout=5)
                try:
                    return super().commit()
                except parent.pg.Error as error:
                    with lock:
                        failures.append(error.sqlstate)
                    raise
        database = self.pool(max_size=2,max_waiting=4,acquire_timeout=1,connection_class=CommitBarrier)
        entered = threading.Barrier(2)
        calls = {left:0,right:0}
        def operation(target):
            def apply(uow):
                with lock:
                    calls[target] += 1
                    first = calls[target]==1
                    old = [repo for player,repo in saved if player==target]
                    saved.append((target,uow.accounts))
                if old:
                    parent.error(lambda:old[0].get(left),"scope_closed")
                values = (uow.accounts.get(left),uow.accounts.get(right))
                require(all(value.state=="active" for value in values) if first else True,
                        "first attempts did not share an initial snapshot")
                if first:
                    entered.wait(timeout=5)
                return uow.accounts.set_state(target,"disabled")
            return apply
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda target:database.run(operation(target),self.budget),(left,right)))
        CommitBarrier.armed=False
        require(failures==["40001"],"serialization failure did not originate at actual COMMIT")
        require(sorted(result.attempts for result in results)==[1,2]
                and sum(len(result.failures) for result in results)==1,
                "COMMIT serialization did not retry exactly the whole transaction")
        require(database.run(lambda u:(u.accounts.get(left).state,u.accounts.get(right).state),
                             self.budget).value==("disabled","disabled"),
                "committed serialization retry lost a repository write")
        for _,repo in saved:
            self.error(lambda:repo.get(left),"scope_closed")
        self.released(database,replenish=True)
        return dict(real_commit_sqlstates=failures,attempts=sorted(r.attempts for r in results),
                    callback_calls=sorted(calls.values()),first_attempt_scope_revoked=True)

    def unknown_commit_outcome(self):
        parent = self
        observations = []
        class LostAcknowledgement(self.pg.Connection):
            armed = True
            def commit(connection):
                if LostAcknowledgement.armed:
                    LostAcknowledgement.armed=False
                    observations.append(connection.info.backend_pid)
                    super().commit()
                    raise parent.pg.OperationalError("injected acknowledgement loss after real COMMIT")
                return super().commit()
        database = self.pool(connection_class=LostAcknowledgement)
        account_id, callbacks, saved = self.identity(),[],[]
        def operation(uow):
            callbacks.append(True)
            saved.append(uow.accounts)
            return uow.accounts.create(account_id,"lost_ack_repository",DIGEST)
        error = self.error(lambda:database.run(operation,self.budget),"outcome_unknown",attempts=1)
        require(len(callbacks)==1 and not error.delays,"unknown commit was automatically retried")
        self.error(lambda:saved[0].get(account_id),"scope_closed")
        recovered = database.run(lambda u:u.accounts.find_by_name("lost_ack_repository"),self.budget).value
        require(recovered is not None and recovered.account_id==account_id,
                "read did not resolve the real committed result")
        with database._lease() as connection:
            require(connection.info.backend_pid!=observations[0],
                    "unknown-outcome connection returned to pool without quarantine")
        require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(account_id,))[0][0]==1,
                "unknown acknowledgement duplicated account")
        self.released(database,replenish=True)
        return dict(real_commit=True,ack_failure_injected=True,automatic_retries=0,
                    discarded_backend=observations[0],resolved_by_read=True)

    def pooled_transfer_atomicity(self):
        from services.storage.assets import TransferRequest
        from uuid import UUID
        database = self.pool()
        identity = UUID(int=0xB0A0D)
        account_id = self.identity()
        def apply(uow):
            uow.accounts.create(account_id,"transfer_atomic_rollback",DIGEST)
            uow.transfer(TransferRequest(identity,42,43,1,3))
            raise self.tx.StorageError("business_rejected")
        before = self.query("SELECT player_id,quantity,revision FROM arena.inventory WHERE item_id=1 ORDER BY player_id")
        self.error(lambda:database.run(apply,self.budget),"business_rejected",attempts=1)
        require(self.query("SELECT player_id,quantity,revision FROM arena.inventory WHERE item_id=1 ORDER BY player_id")==before,
                "pooled transfer escaped shared transaction rollback")
        require(self.query("SELECT count(*) FROM arena.asset_transfer WHERE transfer_id=%s",(identity,))[0][0]==0
                and self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(account_id,))[0][0]==0,
                "pooled transfer receipt or account escaped rollback")
        result = database.run(lambda u:u.transfer(TransferRequest(identity,42,43,1,3)),self.budget)
        require(not result.value.replayed,"rolled back transfer was incorrectly remembered")
        replay = database.run(lambda u:u.transfer(TransferRequest(identity,42,43,1,3)),self.budget)
        require(replay.value.replayed,"pooled transfer did not retain successful receipt")
        self.released(database,replenish=True)
        return dict(account_inventory_receipt_atomic=True,success_replay=True)

    def cleanup_fault_quarantine(self):
        parent = self
        rollback_calls,close_calls = [],[]
        class CleanupFailure(self.pg.Connection):
            def rollback(connection):
                if getattr(connection,"_inject_rollback",False):
                    connection._inject_rollback=False
                    rollback_calls.append(connection.info.backend_pid)
                    raise parent.pg.OperationalError("injected rollback cleanup failure")
                return super().rollback()
            def close(connection):
                if getattr(connection,"_inject_close",False):
                    connection._inject_close=False
                    close_calls.append(connection.info.backend_pid)
                    raise parent.pg.OperationalError("injected close cleanup failure")
                return super().close()
        database = self.pool(connection_class=CleanupFailure)
        account_id = self.identity()
        with database._lease(timeout=1) as connection:
            old_pid = connection.info.backend_pid
            connection._inject_rollback=True
            connection._inject_close=True
        original = database._pool.putconn
        returns = []
        def observe_put(connection):
            returns.append(True)
            return original(connection)
        database._pool.putconn=observe_put
        try:
            def fail(uow):
                uow.accounts.create(account_id,"cleanup_primary_error",DIGEST)
                raise self.tx.StorageError("business_rejected")
            self.error(lambda:database.run(fail,self.budget),"business_rejected",attempts=1)
        finally:
            database._pool.putconn=original
        require(rollback_calls==[old_pid] and close_calls==[old_pid] and len(returns)==1,
                "cleanup fault did not preserve exactly one matching pool return")
        require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(account_id,))[0][0]==0,
                "cleanup quarantine failed to roll back uncommitted data")
        require(database.run(lambda u:u.accounts.get(1),self.budget).value.account_id==1,
                "pool did not replenish after physical quarantine")
        with database._lease() as connection:
            require(connection.info.backend_pid!=old_pid,"cleanup fault reused a failed backend")
        self.released(database,replenish=True)
        database.close()

        # SDK-return failure is a labeled infrastructure injection after a real COMMIT.
        poisoned = self.pool()
        successful_id = self.identity()
        with poisoned._lease(timeout=1) as connection:
            acknowledged_pid = connection.info.backend_pid
        original = poisoned._pool.putconn
        returns = []
        def broken_put(connection):
            returns.append(True)
            raise RuntimeError("injected SDK pool return failure")
        poisoned._pool.putconn=broken_put
        try:
            result = poisoned.run(lambda u:u.accounts.create(
                successful_id,"cleanup_acknowledged",DIGEST),self.budget)
        finally:
            poisoned._pool.putconn=original
        require(result.value.account_id==successful_id and result.attempts==1 and len(returns)==1,
                "cleanup exception replaced an acknowledged commit or retried it")
        require(self.query("SELECT count(*) FROM arena.account WHERE account_id=%s",(successful_id,))[0][0]==1,
                "acknowledged commit did not persist")
        require(poisoned.stats["return_failures"]==1 and poisoned.stats["closed"],
                "SDK return failure did not poison the bounded pool")
        self.error(lambda:poisoned.run(lambda u:u.accounts.get(1),self.budget),"pool_closed",attempts=1)
        self.released(poisoned)
        self.until(lambda:self.query("SELECT count(*) FROM pg_stat_activity WHERE pid=ANY(%s)",
                     ([old_pid,acknowledged_pid],))[0][0]==0,
                   "cleanup faults retained physical PostgreSQL sessions")
        return dict(rollback_and_close_failures_injected=True,pgconn_fallback_closed=True,
                    sdk_put_after_real_ack_injected=True,acknowledged_success_preserved=True,
                    exactly_once_matching_put=True,poisoned_pool_closed=True)

    def execute(self):
        self.setup()
        for name,action in (
            ("repository_roundtrips_and_full_uint64",self.repository_roundtrips),
            ("atomicity_and_scoped_repositories",self.atomicity_and_scope),
            ("sql_free_real_application",self.sql_free_application),
            ("invalid_and_nested_operations",self.invalid_and_nested_operations),
            ("pool_checkout_overload_and_shutdown_three_cycles",self.pool_full_and_shutdown),
            ("database_restart_and_backend_loss_three_cycles",self.database_restart_and_backend_loss),
            ("deferred_real_commit_failure_three_cycles",self.deferred_commit_failures),
            ("serialization_at_real_commit_whole_uow_retry",self.serialization_commit_retry),
            ("unknown_commit_outcome_resolution",self.unknown_commit_outcome),
            ("pooled_transfer_shared_transaction",self.pooled_transfer_atomicity),
            ("cleanup_faults_quarantine_and_preserve_commit",self.cleanup_fault_quarantine),
        ):
            self.case(name,action)
        for database in self.pools:
            database.close()
            self.released(database)
        self.until(lambda:self.query("SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND application_name='arena-repository-boundary'")[0][0]==1,
            "closed pools retained database sessions")
        self.report.update(passed=True,idle_transactions=0,checked_out=0,
                           pool_count=len(self.pools),all_pools_closed=True)
        return self.report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root",type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument("--pg-bin",type=Path,required=True)
    parser.add_argument("--pg-share",type=Path)
    parser.add_argument("--pg-lib",type=Path)
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    args.repository_root=args.repository_root.resolve()
    args.pg_bin=args.pg_bin.resolve()
    if args.pg_bin.is_file():
        args.pg_bin=args.pg_bin.parent
    for name in tuple(os.environ):
        if name.startswith("PG"):
            os.environ.pop(name)
    import psycopg
    sys.path.insert(0,str(args.repository_root))
    from services.storage import application,database,repositories,transactions
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    cluster=RepositoryCluster(args)
    acceptance=Acceptance(args,cluster,psycopg,database,transactions,repositories,application)
    report=acceptance.report
    try:
        cluster.start()
        report=acceptance.execute()
    except (Exception,KeyboardInterrupt) as error:
        report.update(passed=False,error=f"{type(error).__name__}: {error}")
        if isinstance(error,transactions.StorageError):
            report["storage_error"]=dict(category=error.category,sqlstate=error.sqlstate,
                attempts=error.attempts,elapsed=error.elapsed,failures=error.failures,delays=error.delays)
        if cluster.log.exists():
            report["server_log_tail"]=cluster.log.read_text(errors="replace")[-6000:]
    finally:
        for pool in acceptance.pools:
            try:
                pool.close()
            except Exception as error:
                report.update(passed=False,pool_cleanup_error=str(error))
        report["final_pool_stats"]=[dict(pool.stats) for pool in acceptance.pools]
        try:
            cluster.close()
            report["cluster_cleaned"]=not cluster.root.exists()
        except Exception as error:
            report.update(passed=False,cleanup_error=str(error))
    output=json.dumps(report,indent=2,sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(output+"\n")
    print(output)
    return 0 if report["passed"] else 1


if __name__=="__main__":
    sys.exit(main())
