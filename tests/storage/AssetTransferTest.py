#!/usr/bin/env python3
"""Real PostgreSQL asset-transfer conservation, idempotency and retry acceptance."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import json
import math
import os
from pathlib import Path
import random
import signal
import sys
import threading
import time
from uuid import UUID
sys.dont_write_bytecode = True
from StorageMigrationTest import Cluster, Failure, require, run

class AssetCluster(Cluster):
    max_connections = 128

class ConnectionProxy:
    """Delegate to a real connection while observing one COMMIT boundary."""
    def __init__(self, connection, commit):
        object.__setattr__(self, "connection", connection)
        object.__setattr__(self, "commit_hook", commit)
    def __getattr__(self, name):
        return getattr(self.connection, name)
    def __setattr__(self, name, value):
        setattr(self.connection, name, value)
    def commit(self):
        return self.commit_hook(self.connection)

class FakeClock:
    """Only retry admission and jitter are virtual; SQL still executes in PostgreSQL."""
    def __init__(self):
        self.now = 0.0
        self.slept = []
        self.extra = 0.0
    def __call__(self):
        return self.now
    def sleep(self, delay):
        self.slept.append(delay)
        self.now += delay + self.extra

class Acceptance:
    def __init__(self, args, cluster, psycopg, assets):
        self.args, self.cluster, self.pg, self.api = args, cluster, psycopg, assets
        self.database = "arena_asset_transfer"
        self.next_id = 1
        self.stats = Counter()
        self.case_results = []
        self.report = dict(passed=False, cases=self.case_results,
                           psycopg_version=psycopg.__version__,
                           libpq_version=psycopg.pq.version(),
                           psycopg_pq_implementation=psycopg.pq.__impl__)
        self.budget = assets.RetryBudget(max_attempts=32, max_elapsed=60,
                                        base_delay=.01, max_delay=1,
                                        lock_timeout_ms=5000, statement_timeout_ms=10000)
    def uuid(self):
        value = UUID(int=self.next_id)
        self.next_id += 1
        return value
    def connect(self, *, autocommit=False, name="arena-assets"):
        return self.pg.connect(host=str(self.cluster.socket), port=5432,
                               dbname=self.database, user="arena_test",
                               connect_timeout=5, application_name=name, autocommit=autocommit)
    def query(self, sql, params=None):
        with self.connect(autocommit=True, name="arena-asset-observer") as connection:
            cursor = connection.execute(sql, params)
            return cursor.fetchall() if cursor.description else []
    def request(self, source=42, destination=43, item=200, quantity=1, identity=None):
        return self.api.TransferRequest(identity or self.uuid(), source, destination, item, quantity)
    def item(self, item, quantities):
        self.query("INSERT INTO arena.item(item_id,item_code,display_name) VALUES(%s,%s,'Asset test')",
                   (item, f"asset_{item}"))
        for player, quantity in quantities.items():
            self.query("INSERT INTO arena.inventory(player_id,item_id,quantity) VALUES(%s,%s,%s)",
                       (player, item, quantity))
    def balances(self, item):
        return {int(p): (int(q), int(r)) for p,q,r in self.query(
            "SELECT player_id,quantity,revision FROM arena.inventory WHERE item_id=%s ORDER BY player_id", (item,))}
    def receipt_count(self, identity):
        return self.query("SELECT count(*) FROM arena.asset_transfer WHERE transfer_id=%s", (identity,))[0][0]
    def record(self, result):
        require(result.attempts >= 1 and math.isfinite(result.elapsed), "invalid attempt/elapsed result")
        require(all(math.isfinite(x) and 0 <= x <= 5 for x in result.delays), "invalid jitter result")
        self.stats["attempts"] += result.attempts
        self.stats["max_attempts_observed"] = max(self.stats["max_attempts_observed"], result.attempts)
        self.stats["retry_delays"] += len(result.delays)
        self.stats.update(result.failures)
    def transfer(self, request, **options):
        result = self.api.transfer(self.connect, request, self.budget, **options)
        self.record(result)
        require(result.value.transfer_id == request.transfer_id, "receipt identity changed")
        return result
    def error(self, action, category, state=None, attempts=None):
        try:
            action()
        except self.api.TransferError as error:
            require(error.category == category, f"expected {category}, got {error.category}/{error.sqlstate}")
            if state:
                require(error.sqlstate == state, f"expected SQLSTATE {state}, got {error.sqlstate}")
            if attempts is not None:
                require(error.attempts == attempts, f"expected {attempts} attempts, got {error.attempts}")
            self.record(error) if error.attempts else None
            return error
        raise Failure("expected TransferError " + category)
    def case(self, name, action):
        self.report["current_case"] = name
        started = time.monotonic()
        detail = action() or {}
        idle = self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                          "AND state LIKE 'idle in transaction%'")[0][0]
        require(idle == 0, "failed operation leaked an idle transaction")
        self.case_results.append(dict(name=name, passed=True, elapsed=time.monotonic()-started, **detail))

    def setup(self):
        self.cluster.create(self.database)
        storage = self.args.repository_root/"services"/"storage"
        result = run([sys.executable, storage/"migrate.py", "up"], self.cluster.environment(self.database), timeout=95)
        require(result.returncode == 0, "asset migration failed: "+result.stderr)
        self.cluster.sql((storage/"fixtures"/"seed.sql").read_text(), self.database, single=True)
        require(self.query("SELECT to_regclass('arena.asset_transfer')")[0][0] is not None, "migration002 missing")
        self.report["server_version"] = self.query("SHOW server_version")[0][0]
        self.report["server_max_connections"] = int(self.query("SHOW max_connections")[0][0])
        self.query("CREATE TABLE public.asset_probe(id integer PRIMARY KEY,value integer NOT NULL CHECK(value>=0));"
                   "INSERT INTO public.asset_probe VALUES(1,0),(2,0)")

    def bulk(self):
        self.query("INSERT INTO arena.account(account_id,account_name,password_digest) "
                   "SELECT n,'asset_player_'||n,'$arena-pbkdf2-sha256$100000$'||repeat('00',16)||'$'||repeat('11',32) "
                   "FROM generate_series(10000,10099) n")
        self.query("INSERT INTO arena.player(player_id,account_id,display_name) "
                   "SELECT n,n,'Asset player '||n FROM generate_series(10000,10099) n")
        for item in (100,101,102):
            self.query("INSERT INTO arena.item(item_id,item_code,display_name) VALUES(%s,%s,'Bulk asset')", (item,f"bulk_{item}"))
        self.query("INSERT INTO arena.inventory(player_id,item_id,quantity) "
                   "SELECT p,i,100000 FROM generate_series(10000,10099)p CROSS JOIN generate_series(100,102)i")
        requests = []
        for worker in range(100):
            generator = random.Random(0xA55E7000 + worker)
            group = []
            for _ in range(10):
                source = 10000 + generator.randrange(100)
                destination = 10000 + generator.randrange(99)
                if destination >= source:
                    destination += 1
                group.append(self.request(source,destination,100+generator.randrange(3),generator.randint(1,19)))
            requests.append(group)
        barrier, lock, first_pids = threading.Barrier(100), threading.Lock(), set()
        def worker(group):
            first = True
            def factory():
                nonlocal first
                connection = self.connect()
                if first:
                    first = False
                    with lock:
                        first_pids.add(connection.info.backend_pid)
                    try:
                        barrier.wait(timeout=30)
                    except BaseException:
                        connection.close()
                        raise
                return connection
            results = []
            for request in group:
                result = self.api.transfer(factory,request,self.budget)
                results.append((request,result))
            return results
        completed = []
        with ThreadPoolExecutor(max_workers=100) as workers:
            futures = [workers.submit(worker, group) for group in requests]
            for future in as_completed(futures, timeout=120):
                completed.extend(future.result())
        require(len(first_pids)==100 and len(completed)==1000, "did not execute 100 concurrent workers/1000 transfers")
        expected = {(p,i):100000 for p in range(10000,10100) for i in (100,101,102)}
        for request,result in completed:
            self.record(result)
            require(not result.value.replayed and result.attempts <= 32, "unique funded transfer failed/replayed")
            expected[request.source_player_id,request.item_id] -= request.quantity
            expected[request.destination_player_id,request.item_id] += request.quantity
        actual = {(int(p),int(i)):int(q) for p,i,q in self.query(
            "SELECT player_id,item_id,quantity FROM arena.inventory WHERE item_id BETWEEN 100 AND 102")}
        require(actual==expected and min(actual.values())>=0, "bulk balances diverged from independent successful-command oracle")
        rows = self.query("SELECT source_player_id,destination_player_id,item_id,quantity FROM arena.asset_transfer WHERE item_id BETWEEN 100 AND 102")
        receipts = Counter(tuple(map(int,row)) for row in rows)
        oracle = Counter((r.source_player_id,r.destination_player_id,r.item_id,r.quantity) for group in requests for r in group)
        require(receipts==oracle, "bulk successful receipts disagree with requests")
        require(all(sum(q for (p,i),q in actual.items() if i==item)==10000000 for item in (100,101,102)),
                "per-item conservation failed")
        return dict(workers=100, simultaneous_initial_connections=100, transfers=1000, seed="0xA55E7000+worker")

    def concurrent_idempotency(self):
        self.item(210,{42:10000,43:0})
        request = self.request(item=210,quantity=7)
        gate = threading.Barrier(100)
        def call(_):
            gate.wait(timeout=20)
            return self.api.transfer(self.connect,request,self.budget)
        with ThreadPoolExecutor(max_workers=100) as workers:
            results = list(workers.map(call,range(100)))
        for result in results:
            self.record(result)
        require(sum(not r.value.replayed for r in results)==1, "same UUID mutated more than once")
        require(self.balances(210)=={42:(9993,1),43:(7,1)} and self.receipt_count(request.transfer_id)==1,
                "same UUID balance/receipt mismatch")
        for changed in (replace(request,quantity=8),replace(request,source_player_id=44),
                        replace(request,destination_player_id=44),replace(request,item_id=1)):
            self.error(lambda: self.transfer(changed),"idempotency_conflict",attempts=1)
        return dict(callers=100,mutations=1,replayed=99,payload_conflicts=4)

    @staticmethod
    def error_details(error):
        return dict(category=error.category,sqlstate=error.sqlstate,attempts=error.attempts,
                    elapsed=error.elapsed,failures=list(error.failures),delays=list(error.delays),
                    last_failure_category=error.last_failure_category)

    def hot_and_opposite(self):
        self.item(211,{42:10000,43:10000})
        self.item(212,{42:10000,43:10000})
        self.item(213,{42:10000})
        requests = [self.request(item=211) for _ in range(100)]
        requests += [self.request(source=42 if n%2 else 43,destination=43 if n%2 else 42,item=212) for n in range(100)]
        requests += [self.request(item=213) for _ in range(100)]
        successful, rejected, unexpected = [], [], []
        with ThreadPoolExecutor(max_workers=100) as workers:
            futures = {workers.submit(self.api.transfer,self.connect,r,self.budget):r for r in requests}
            for future in as_completed(futures,timeout=120):
                request = futures[future]
                try:
                    result = future.result()
                except self.api.TransferError as error:
                    rejected.append((request,error))
                    self.record(error)
                except Exception as error:
                    unexpected.append(error)
                else:
                    successful.append((request,result))
                    self.record(result)
        summary = dict(submitted=len(requests),success=len(successful),rejected=len(rejected),
                       failure_categories=dict(Counter(e.category for _,e in rejected)),
                       successful_requests=[dict(transfer_id=str(r.transfer_id),item_id=r.item_id,
                                                 source_player_id=r.source_player_id,
                                                 destination_player_id=r.destination_player_id,
                                                 quantity=r.quantity,attempts=result.attempts,
                                                 failures=list(result.failures),delays=list(result.delays))
                                            for r,result in successful],
                       failed_requests=[dict(transfer_id=str(r.transfer_id),item_id=r.item_id,
                                             **self.error_details(e)) for r,e in rejected])
        self.report["high_contention"] = summary
        require(not unexpected,"unexpected high-contention exception: "+repr(unexpected[:1]))
        require(len(successful)+len(rejected)==len(requests) and
                {r.transfer_id for r,_ in successful+rejected}=={r.transfer_id for r in requests},
                "high-contention report did not classify every submitted UUID")
        for request,error in rejected:
            require(error.category in ("retry_exhausted","budget_exhausted") and error.sqlstate=="40001",
                    "unexpected bounded rejection: "+str(self.error_details(error)))
            require(error.last_failure_category=="serialization" and error.failures and
                    all(state=="40001" for state in error.failures),"bounded rejection lost serialization history")
            require(1<=error.attempts<=self.budget.max_attempts and
                    len(error.failures)==error.attempts and len(error.delays)<=error.attempts,
                    "bounded rejection exceeded attempt accounting")
            if error.category=="retry_exhausted":
                require(error.attempts==self.budget.max_attempts and
                        len(error.delays)==error.attempts-1,"attempt cap was not enforced exactly")
            else:
                require(error.elapsed>=self.budget.max_elapsed,"budget rejected before its elapsed limit")
            require(all(delay<=min(self.budget.max_delay,self.budget.base_delay*(2**index))
                        for index,delay in enumerate(error.delays)),"bounded rejection exceeded jitter ceiling")
        expected = {211:{42:[10000,0],43:[10000,0]},
                    212:{42:[10000,0],43:[10000,0]},213:{42:[10000,0]}}
        successes = Counter()
        for request,result in successful:
            require(not result.value.replayed and result.value.transfer_id==request.transfer_id,
                    "unique high-contention command returned a replay")
            require(result.attempts<=self.budget.max_attempts and
                    len(result.delays)==result.attempts-1,"successful retry exceeded its attempt budget")
            require(not any(state!="40001" for state in result.failures),
                    "consistent inventory lock order raised an unexpected SQLSTATE")
            item = expected[request.item_id]
            for player,delta in ((request.source_player_id,-request.quantity),
                                 (request.destination_player_id,request.quantity)):
                row = item.setdefault(player,[0,0])
                row[0] += delta
                row[1] += 1
            successes[request.item_id] += 1
        actual_rows = self.query("SELECT transfer_id,source_player_id,destination_player_id,item_id,quantity "
                                 "FROM arena.asset_transfer WHERE item_id IN(211,212,213)")
        actual_receipts = Counter((identity,int(source),int(destination),int(item),int(quantity))
                                  for identity,source,destination,item,quantity in actual_rows)
        expected_receipts = Counter((r.transfer_id,r.source_player_id,r.destination_player_id,r.item_id,r.quantity)
                                    for r,_ in successful)
        require(actual_receipts==expected_receipts,"high-contention receipts diverged from successful commands")
        stored_ids = {row[0] for row in actual_rows}
        require(all(r.transfer_id not in stored_ids for r,_ in rejected),
                "bounded failed command retained a successful receipt")
        for item,rows in expected.items():
            require(successes[item]>0,"contention group made no successful progress")
            actual = self.balances(item)
            require(actual=={p:tuple(row) for p,row in rows.items()},
                    "contention balances/revisions diverged from committed-command oracle")
            require(all(quantity>=0 for quantity,_ in actual.values()),"contention created negative inventory")
            require(sum(quantity for quantity,_ in actual.values())==(10000 if item==213 else 20000),
                    "contention lost per-item assets")
        summary["groups"] = {str(item):dict(submitted=100,success=successes[item],
                             rejected=100-successes[item]) for item in (211,212,213)}
        summary["max_attempts"] = max([r.attempts for _,r in successful]+[e.attempts for _,e in rejected])
        return dict(submitted=300,success=len(successful),bounded_rejections=len(rejected),
                    failure_categories=summary["failure_categories"],groups=summary["groups"],
                    max_attempts=summary["max_attempts"],failed_receipts=0)

    def business_failures(self):
        self.item(220,{42:0})
        request = self.request(item=220,quantity=3)
        self.error(lambda:self.transfer(request),"insufficient_assets",attempts=1)
        require(self.balances(220)=={42:(0,0)} and self.receipt_count(request.transfer_id)==0,
                "insufficient transfer left a receipt/zero destination")
        self.query("UPDATE arena.inventory SET quantity=5 WHERE player_id=42 AND item_id=220")
        require(not self.transfer(request).value.replayed, "rejected command was incorrectly deduplicated")
        require(self.balances(220)=={42:(2,1),43:(3,1)}, "missing destination was not atomically created")
        self.item(221,{42:10,43:(1<<63)-1})
        overflow = self.request(item=221)
        before=self.balances(221)
        self.error(lambda:self.transfer(overflow),"quantity_overflow",attempts=1)
        require(self.balances(221)==before and not self.receipt_count(overflow.transfer_id), "overflow was not atomic")
        self.item(222,{42:10,43:0})
        self.query("UPDATE arena.inventory SET revision=9223372036854775807 WHERE item_id=222 AND player_id=43")
        revision = self.request(item=222)
        before=self.balances(222)
        self.error(lambda:self.transfer(revision),"revision_overflow",attempts=1)
        require(self.balances(222)==before and not self.receipt_count(revision.transfer_id), "revision overflow mutated balances")
        for request in (self.request(source=999999,item=220),self.request(destination=999999,item=220),self.request(item=999999)):
            self.error(lambda:self.transfer(request),"not_found","23503",1)
            require(not self.receipt_count(request.transfer_id), "missing FK retained a receipt")
        self.query("UPDATE arena.inventory SET quantity=2 WHERE player_id=18446744073709551615 AND item_id=18446744073709551615")
        maximum=self.request(source=(1<<64)-1,item=(1<<64)-1)
        self.transfer(maximum)
        require(self.transfer(maximum).value.replayed, "full uint64 payload did not replay exactly")
        return dict(insufficient_retry_succeeded=True,missing_destination_created=True,unsigned64_preserved=True)

    def invalid_requests(self):
        base=self.request()
        calls=0
        def forbidden():
            nonlocal calls
            calls+=1
            raise Failure("invalid request opened a database connection")
        for request in (None,replace(base,transfer_id=UUID(int=0)),replace(base,quantity=0),
                        replace(base,quantity=-1),replace(base,quantity=1.5),replace(base,quantity=True),
                        replace(base,quantity=1<<63),replace(base,source_player_id=0),
                        replace(base,source_player_id=1<<64),replace(base,source_player_id=True),
                        replace(base,destination_player_id=base.source_player_id)):
            self.error(lambda:self.api.transfer(forbidden,request),"invalid_request",attempts=0)
        require(calls==0,"invalid request reached database")
        for options in (dict(max_attempts=0),dict(max_attempts=65),dict(max_elapsed=float("nan")),
                        dict(max_elapsed=121),dict(base_delay=1,max_delay=.5),dict(lock_timeout_ms=0)):
            try:
                self.api.RetryBudget(**options)
            except ValueError:
                continue
            raise Failure("invalid retry budget accepted")
        return dict(invalid_requests=11,invalid_budgets=6)

    def receipt_constraints(self):
        cases = [
            ("primary_key","INSERT INTO arena.asset_transfer SELECT * FROM arena.asset_transfer LIMIT 1","23505","asset_transfer_pk"),
            ("update","UPDATE arena.asset_transfer SET quantity=quantity+1 WHERE transfer_id=(SELECT transfer_id FROM arena.asset_transfer LIMIT 1)","23514","asset_transfer_immutable"),
            ("delete","DELETE FROM arena.asset_transfer WHERE transfer_id=(SELECT transfer_id FROM arena.asset_transfer LIMIT 1)","23514","asset_transfer_immutable"),
        ]
        template = "INSERT INTO arena.asset_transfer(transfer_id,source_player_id,destination_player_id,item_id,quantity) VALUES (%s,%s,%s,%s,%s)"
        for name, fields, state, constraint in [
            ("zero_uuid",(UUID(int=0),42,43,220,1),"23514","asset_transfer_id_ck"),
            ("same_player",(self.uuid(),42,42,220,1),"23514","asset_transfer_players_ck"),
            ("zero_quantity",(self.uuid(),42,43,220,0),"23514","asset_transfer_quantity_ck"),
            ("missing_source",(self.uuid(),999999,43,220,1),"23503","asset_transfer_source_fk"),
            ("missing_destination",(self.uuid(),42,999999,220,1),"23503","asset_transfer_destination_fk"),
            ("missing_item",(self.uuid(),42,43,999999,1),"23503","asset_transfer_item_fk"),
            ("null_quantity",(self.uuid(),42,43,220,None),"23502",None),
        ]:
            cases.append((name,(template,fields),state,constraint))
        before = self.query("SELECT count(*) FROM arena.asset_transfer")[0][0]
        for name, statement, state, constraint in cases:
            connection = self.connect()
            try:
                try:
                    connection.execute(*statement) if isinstance(statement,tuple) else connection.execute(statement)
                except self.pg.Error as error:
                    require(error.sqlstate==state, f"receipt {name} wrong SQLSTATE {error.sqlstate}")
                    if constraint:
                        require(error.diag.constraint_name==constraint, f"receipt {name} wrong constraint {error.diag.constraint_name}")
                else:
                    raise Failure("invalid receipt SQL accepted: "+name)
            finally:
                connection.close()
            require(self.query("SELECT count(*) FROM arena.asset_transfer")[0][0]==before,"failed receipt SQL mutated data")
        return dict(negative_cases=len(cases))

    def debit_faults(self):
        for item, mode in ((230,"check"),(231,"foreign_key")):
            self.item(item,{42:10})
            self.query("CREATE TABLE public.asset_fault_parent(id integer PRIMARY KEY);"
                       "CREATE TABLE public.asset_fault_child(id integer CONSTRAINT unrelated_asset_fk REFERENCES public.asset_fault_parent(id));"
                       "CREATE TABLE public.asset_fault_check(value integer CONSTRAINT asset_fault_positive CHECK(value>0))")
            body = "INSERT INTO public.asset_fault_check VALUES(-1);" if mode=="check" else "INSERT INTO public.asset_fault_child VALUES(999);"
            self.query(f"""CREATE FUNCTION public.asset_fail_credit() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.item_id={item} AND NEW.player_id=43 THEN {body} END IF; RETURN NEW; END $$;
                CREATE TRIGGER asset_fail_credit BEFORE UPDATE ON arena.inventory
                FOR EACH ROW EXECUTE FUNCTION public.asset_fail_credit();""")
            request=self.request(item=item)
            try:
                self.error(lambda:self.transfer(request),"integrity_error","23514" if mode=="check" else "23503",1)
                require(self.balances(item)=={42:(10,0)} and not self.receipt_count(request.transfer_id),
                        "failure after debit left changed source/receipt/created destination")
            finally:
                self.query("DROP TRIGGER asset_fail_credit ON arena.inventory;"
                           "DROP FUNCTION public.asset_fail_credit();DROP TABLE public.asset_fault_child;"
                           "DROP TABLE public.asset_fault_parent;DROP TABLE public.asset_fault_check")
            require(not self.transfer(request).value.replayed,"rolled-back receipt was remembered")
            require(self.balances(item)=={42:(9,1),43:(1,1)},"same failed UUID could not succeed after fault removal")
        return dict(after_debit_faults=2,unrelated_foreign_key_is_integrity_error=True)

    def serialization_at_commit(self):
        self.query("UPDATE public.asset_probe SET value=0")
        snapshots, commits = threading.Barrier(2), threading.Barrier(2)
        commit_failures=[]
        lock=threading.Lock()
        def worker(index):
            attempt=0
            calls=0
            def factory():
                nonlocal attempt
                attempt+=1
                this_attempt=attempt
                def commit(connection):
                    if this_attempt==1:
                        commits.wait(timeout=15)
                    try:
                        connection.commit()
                    except self.pg.Error as error:
                        with lock:
                            commit_failures.append(error.sqlstate)
                        raise
                return ConnectionProxy(self.connect(),commit)
            def operation(connection):
                nonlocal calls
                calls+=1
                connection.execute("SELECT value FROM public.asset_probe ORDER BY id").fetchall()
                if calls==1:
                    snapshots.wait(timeout=15)
                connection.execute("UPDATE public.asset_probe SET value=value+1 WHERE id=%s",(index+1,))
                return index
            return self.api.run_transaction(factory,operation,self.budget)
        with ThreadPoolExecutor(max_workers=2) as workers:
            results=list(workers.map(worker,(0,1)))
        for result in results:
            self.record(result)
        require("40001" in commit_failures,"serialization conflict did not actually occur at COMMIT")
        require(sum(r.attempts for r in results)>=3,"COMMIT serialization abort did not retry whole operation")
        require(self.query("SELECT value FROM public.asset_probe ORDER BY id")==[(1,),(1,)],
                "whole-transaction serialization retry double-applied or lost writes")
        return dict(sqlstate="40001",stage="COMMIT",attempts=[r.attempts for r in results])

    def deadlock(self):
        self.query("UPDATE public.asset_probe SET value=0")
        barrier=threading.Barrier(2)
        budget=self.api.RetryBudget(max_attempts=8,max_elapsed=20,lock_timeout_ms=5000,statement_timeout_ms=8000)
        def worker(index):
            calls=0
            def operation(connection):
                nonlocal calls
                calls+=1
                connection.execute("UPDATE public.asset_probe SET value=value+1 WHERE id=%s",(index+1,))
                if calls==1:
                    barrier.wait(timeout=15)
                connection.execute("UPDATE public.asset_probe SET value=value+1 WHERE id=%s",(2-index,))
                return index
            return self.api.run_transaction(self.connect,operation,budget)
        with ThreadPoolExecutor(max_workers=2) as workers:
            results=list(workers.map(worker,(0,1)))
        for result in results:
            self.record(result)
        require(any("40P01" in r.failures for r in results),"real opposite row-lock cycle did not deadlock")
        require(self.query("SELECT value FROM public.asset_probe ORDER BY id")==[(2,),(2,)],"deadlock retry did not roll back first writes")
        return dict(sqlstate="40P01",attempts=[r.attempts for r in results])

    def nonretry_and_timeouts(self):
        for statement,state,category in [
            ("INSERT INTO public.asset_probe VALUES(1,0)","23505","integrity_error"),
            ("INSERT INTO public.asset_probe VALUES(3,-1)","23514","integrity_error"),
            ("SELECT 1/0","22012","database_error")]:
            self.error(lambda:self.api.run_transaction(self.connect,lambda c:c.execute(statement)),
                       category,state,1)
        before = self.query("SELECT value FROM public.asset_probe ORDER BY id")
        def swallowed_sql_error(connection):
            connection.execute("UPDATE public.asset_probe SET value=value+100 WHERE id=1")
            try:
                connection.execute("SELECT 1/0")
            except self.pg.Error as error:
                require(error.sqlstate=="22012","unexpected real callback SQL error")
            return "callback-returned-with-aborted-transaction"
        self.error(lambda:self.api.run_transaction(self.connect,swallowed_sql_error),
                   "database_error","25P02",1)
        require(self.query("SELECT value FROM public.asset_probe ORDER BY id")==before,
                "swallowed SQL failure was misreported as a committed mutation")
        timeout=self.api.RetryBudget(lock_timeout_ms=1000,statement_timeout_ms=40)
        self.error(lambda:self.api.run_transaction(self.connect,lambda c:c.execute("SELECT pg_sleep(.2)"),timeout),
                   "statement_timeout","57014",1)
        self.item(240,{42:10,43:0})
        hold=self.connect()
        try:
            hold.execute("SELECT * FROM arena.inventory WHERE player_id=42 AND item_id=240 FOR UPDATE")
            budget=self.api.RetryBudget(lock_timeout_ms=50,statement_timeout_ms=1000)
            request=self.request(item=240)
            self.error(lambda:self.api.transfer(self.connect,request,budget),"lock_timeout","55P03",1)
            require(not self.receipt_count(request.transfer_id),"lock timeout committed a receipt")
        finally:
            hold.close()
        require(self.balances(240)=={42:(10,0),43:(0,0)},"lock timeout changed balances")
        for mode in ("autocommit","nonidle"):
            created=[]
            def invalid():
                connection=self.connect(autocommit=mode=="autocommit")
                if mode=="nonidle":
                    connection.execute("SELECT 1")
                created.append(connection)
                return connection
            self.error(lambda:self.api.run_transaction(invalid,lambda c:None),"invalid_connection",attempts=1)
            require(created[0].closed,"invalid factory connection leaked")
        before = self.query("SELECT value FROM public.asset_probe ORDER BY id")
        def terminate(connection):
            connection.execute("UPDATE public.asset_probe SET value=value+100 WHERE id=1")
            connection.execute("SELECT pg_terminate_backend(pg_backend_pid())").fetchone()
        self.error(lambda:self.api.run_transaction(self.connect,terminate),"connection_error",attempts=1)
        require(self.query("SELECT value FROM public.asset_probe ORDER BY id")==before,
                "real backend termination retained an uncommitted write")
        return dict(nonretry_sqlstates=["23505","23514","22012","25P02","57014","55P03"],
                    invalid_connections=2,real_backend_disconnect_rolled_back=True)

    def budget_and_jitter(self):
        def abort(connection):
            connection.execute("DO $$BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END$$")
        details=[]
        for sample,expected in ((1.0,[.1,.15]),(.5,[.05,.075])):
            clock=FakeClock()
            backend_ids=[]
            def factory():
                connection=self.connect()
                backend_ids.append(connection.info.backend_pid)
                return connection
            budget=self.api.RetryBudget(max_attempts=3,max_elapsed=10,base_delay=.1,max_delay=.15)
            error=self.error(lambda:self.api.run_transaction(factory,abort,budget,clock=clock,sleep=clock.sleep,random_value=lambda:sample),
                             "retry_exhausted","40001",3)
            require(error.failures==("40001",)*3 and len(set(backend_ids))==3,"retry did not use fresh sessions")
            require(len(error.delays)==2 and all(math.isclose(a,b) for a,b in zip(error.delays,expected)),"exponential full jitter was not bounded/capped")
            require(error.last_failure_category=="serialization","retry failure lost its SQLSTATE classification")
            details.append(dict(sample=sample,delays=error.delays))
        clock=FakeClock()
        budget=self.api.RetryBudget(max_attempts=8,max_elapsed=.25,base_delay=.2,max_delay=.2)
        error=self.error(lambda:self.api.run_transaction(self.connect,abort,budget,clock=clock,sleep=clock.sleep,random_value=lambda:1.0),
                         "budget_exhausted","40001",2)
        require(math.isclose(sum(error.delays),.25),"jitter failed to clamp remaining budget")
        clock=FakeClock()
        clock.extra=1
        calls=0
        def factory():
            nonlocal calls
            calls+=1
            return self.connect()
        self.error(lambda:self.api.run_transaction(factory,abort,budget,clock=clock,sleep=clock.sleep,random_value=lambda:1.0),
                   "budget_exhausted","40001",1)
        require(calls==1,"overslept deadline admitted another connection")
        return dict(server_injected_abort_for_budget_only=True,jitter=details,oversleep_stopped_before_connect=True)

    def deadline_edges(self):
        clock=FakeClock()
        budget=self.api.RetryBudget(max_attempts=1,max_elapsed=.25,base_delay=.1,max_delay=.1)
        def abort(connection):
            connection.execute("DO $$BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END$$")
        def forbidden_sleep(_):
            raise Failure("one-attempt budget slept")
        error=self.error(lambda:self.api.run_transaction(self.connect,abort,budget,
                         clock=clock,sleep=forbidden_sleep),"retry_exhausted","40001",1)
        require(not error.delays,"one-attempt retry budget recorded a sleep")
        created=[]
        operations=0
        clock=FakeClock()
        def expired_factory():
            connection=self.connect()
            created.append(connection)
            clock.now=.5
            return connection
        def forbidden_operation(_):
            nonlocal operations
            operations+=1
            raise Failure("expired post-connect budget executed SQL callback")
        self.error(lambda:self.api.run_transaction(expired_factory,forbidden_operation,budget,
                         clock=clock),"budget_exhausted",attempts=1)
        require(operations==0 and created[0].closed,"expired connection was not closed before callback")
        clock=FakeClock()
        before=self.query("SELECT value FROM public.asset_probe WHERE id=1")[0][0]
        def acknowledged_factory():
            def commit(connection):
                connection.commit()
                clock.now=1
            return ConnectionProxy(self.connect(),commit)
        result=self.api.run_transaction(acknowledged_factory,
            lambda c:c.execute("UPDATE public.asset_probe SET value=value+1 WHERE id=1"),budget,clock=clock)
        require(result.attempts==1 and result.elapsed>budget.max_elapsed,
                "acknowledged commit was incorrectly rejected after the time window")
        require(self.query("SELECT value FROM public.asset_probe WHERE id=1")[0][0]==before+1,
                "acknowledged late commit was not applied once")
        self.record(result)
        return dict(one_attempt_never_sleeps=True,post_factory_expiry_skips_operation=True,
                    acknowledged_commit_after_window_is_success=True)

    def lost_commit_ack(self):
        self.item(250,{42:10,43:0})
        request=self.request(item=250,quantity=2)
        calls=0
        def factory():
            nonlocal calls
            calls+=1
            def commit(connection):
                connection.commit()
                raise self.pg.OperationalError("test transport acknowledgement lost after real commit")
            return ConnectionProxy(self.connect(),commit)
        error=self.error(lambda:self.api.transfer(factory,request,self.budget),"outcome_unknown",attempts=1)
        require(calls==1 and not error.delays,"unknown commit outcome retried automatically")
        require(self.receipt_count(request.transfer_id)==1 and self.balances(250)=={42:(8,1),43:(2,1)},
                "lost-ack injection did not actually commit")
        result=self.transfer(request)
        require(result.value.replayed and self.balances(250)=={42:(8,1),43:(2,1)},
                "same UUID did not safely resolve unknown outcome")
        return dict(real_commit=True,ack_failure_injected=True,automatic_retries=0,replayed_resolution=True)

    def execute(self):
        self.setup()
        for name,action in [
            ("100_workers_1000_funded_transfers",self.bulk),
            ("same_uuid_and_conflicting_payload",self.concurrent_idempotency),
            ("hot_pair_and_opposite_directions",self.hot_and_opposite),
            ("business_failures_and_unsigned64",self.business_failures),
            ("invalid_requests_and_budgets",self.invalid_requests),
            ("migration002_receipt_constraints",self.receipt_constraints),
            ("after_debit_atomic_failures",self.debit_faults),
            ("serialization_at_real_commit",self.serialization_at_commit),
            ("real_deadlock_whole_transaction_retry",self.deadlock),
            ("nonretry_sqlstates_and_timeouts",self.nonretry_and_timeouts),
            ("finite_retry_budget_and_full_jitter",self.budget_and_jitter),
            ("retry_deadline_commit_boundary_edges",self.deadline_edges),
            ("unknown_commit_outcome_resolution",self.lost_commit_ack),
        ]:
            self.case(name,action)
        self.report.update(passed=True,counters=dict(self.stats),
                           successful_receipts=int(self.query("SELECT count(*) FROM arena.asset_transfer")[0][0]),
                           idle_transactions=0)
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
    sys.path.insert(0,str(args.repository_root/"services"/"storage"))
    import assets
    signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    cluster=AssetCluster(args)
    acceptance=Acceptance(args,cluster,psycopg,assets)
    report=acceptance.report
    try:
        cluster.start()
        report=acceptance.execute()
    except (Exception,KeyboardInterrupt) as error:
        report.update(passed=False,error=f"{type(error).__name__}: {error}",counters=dict(acceptance.stats))
        if isinstance(error,assets.TransferError):
            report["transfer_error"] = acceptance.error_details(error)
        if cluster.log.exists():
            report["server_log_tail"]=cluster.log.read_text(errors="replace")[-6000:]
    finally:
        try:
            cluster.close()
            report["cluster_cleaned"]=not cluster.root.exists()
        except Exception as error:
            report.update(passed=False,cleanup_error=str(error))
    text=json.dumps(report,indent=2,sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(text+"\n")
    print(text)
    return 0 if report["passed"] else 1

if __name__=="__main__":
    sys.exit(main())
