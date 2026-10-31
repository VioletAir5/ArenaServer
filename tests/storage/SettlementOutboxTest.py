#!/usr/bin/env python3
"""Real PostgreSQL atomic settlement, crash recovery and outbox acceptance."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import UUID

sys.dont_write_bytecode = True
from RepositoryBoundaryTest import Acceptance as RepositoryAcceptance, RepositoryCluster
from StorageMigrationTest import Failure, require


class Acceptance(RepositoryAcceptance):
    def __init__(self, *args):
        super().__init__(*args)
        self.database = "arena_settlement_outbox"
        self.sequence = 70000

    def setup(self):
        super().setup()
        self.db = self.pool(max_size=8, max_waiting=128, acquire_timeout=3)
        from services.storage.outbox import Publisher, consume_event
        # Existing schema fixtures predate the reward ledger. Drain their one
        # pending event without fabricating rewards.
        Publisher(self.db).run_once(lambda event: consume_event(self.db, event))

    def terminal(self, *, aborted=False):
        identity = self.identity()
        created = self.db.run(lambda work: work.matches.create(
            identity, identity+(1 << 63), self.participants()), self.budget).value
        started = max(datetime.now(timezone.utc), created.created_at)
        if aborted:
            self.db.run(lambda work: work.matches.abort(
                identity, started, 0, 123, "ready_timeout"), self.budget)
        else:
            self.db.run(lambda work: work.matches.start(identity, started), self.budget)
            self.db.run(lambda work: work.matches.finish(
                identity, started+timedelta(microseconds=1), 100, (1 << 64)-1, 42), self.budget)
        return self.command(identity, rewards=not aborted)

    @staticmethod
    def command(identity, rewards=True):
        from services.storage.settlement import Reward, SettlementCommand
        return SettlementCommand(identity+1000000, identity, UUID(int=identity),
            UUID(int=identity+1000000), (Reward(42, 1, 3), Reward(43, 1, 2)) if rewards else ())

    def quantity(self):
        return tuple(self.query("SELECT player_id,quantity,revision FROM arena.inventory "
                                "WHERE item_id=1 AND player_id IN (42,43) ORDER BY player_id"))

    def receipt_counts(self, command):
        return tuple(self.query(
            "SELECT (SELECT count(*) FROM arena.settlement WHERE settlement_id=%s),"
            "(SELECT count(*) FROM arena.asset_ledger WHERE settlement_id=%s),"
            "(SELECT count(*) FROM arena.outbox WHERE event_id=%s),"
            "(SELECT count(*) FROM arena.consumer_receipt WHERE event_id=%s),"
            "(SELECT count(*) FROM arena.settlement_reward_projection WHERE event_id=%s)",
            (command.settlement_id, command.settlement_id, command.event_id,
             command.event_id, command.event_id))[0])

    def atomic_credit_and_replay(self):
        from services.storage.settlement import settle, Reward
        command = self.terminal()
        before = self.quantity()
        first = settle(self.db, command, budget=self.budget).value
        after = self.quantity()
        require(not first.replayed and after[0][1] == before[0][1]+3
                and after[1][1] == before[1][1]+2, "settlement lost or duplicated credits")
        require(after[0][2] == before[0][2]+1 and after[1][2] == before[1][2]+1,
                "inventory revision did not change exactly once")
        with ThreadPoolExecutor(max_workers=16) as workers:
            replay = list(workers.map(lambda _: settle(self.db, command, budget=self.budget).value,
                                      range(100)))
        require(all(r.replayed for r in replay) and self.quantity() == after,
                "concurrent producer replay minted rewards")
        require(self.receipt_counts(command)[:3] == (1,2,1), "atomic rows incomplete")
        changed = replace(command, rewards=(Reward(42,1,4), Reward(43,1,2)))
        self.error(lambda: settle(self.db, changed, budget=self.budget), "idempotency_conflict")
        self.error(lambda: settle(self.db, replace(command,event_id=UUID(int=1)),
                                  budget=self.budget), "idempotency_conflict")
        # A different settlement identity cannot settle the same match twice.
        self.error(lambda: settle(self.db, replace(command,settlement_id=self.identity()),
                                  budget=self.budget), "idempotency_conflict")
        return dict(concurrent_replays=100,credit_rows=2,exact_inventory_once=True)

    def rollback_all_writes(self):
        from services.storage.settlement import settle, Reward
        command = self.terminal()
        before = self.quantity()
        def fail(work):
            work.settlements.commit(command)
            raise self.tx.StorageError("business_rejected")
        self.error(lambda: self.db.run(fail,self.budget), "business_rejected")
        require(self.receipt_counts(command) == (0,0,0,0,0) and self.quantity() == before,
                "rollback leaked settlement, ledger, outbox or inventory")
        # Failure after an earlier credit in the canonical order must undo it too.
        invalid_item = replace(command, rewards=(Reward(42,1,3), Reward(43,987654321,1)))
        self.error(lambda: settle(self.db,invalid_item,budget=self.budget), "integrity_error")
        require(self.quantity() == before and self.receipt_counts(command) == (0,0,0,0,0),
                "late FK failure escaped atomic boundary")
        overflow = replace(command, rewards=(Reward(42,1,3), Reward(43,1,(1 << 63)-1)))
        self.error(lambda: settle(self.db,overflow,budget=self.budget), "quantity_overflow")
        require(self.quantity() == before and self.receipt_counts(command) == (0,0,0,0,0),
                "late quantity overflow escaped atomic boundary")
        settle(self.db,command,budget=self.budget)
        return dict(business_and_late_fk_rollback=True,late_overflow_rollback=True)

    def consumer_dedup_and_failure(self):
        from services.storage.settlement import settle
        from services.storage.outbox import consume_event
        command = self.terminal()
        settle(self.db,command,budget=self.budget)
        event = self.db.run(lambda work: work.outbox.get(command.event_id),self.budget).value
        before = self.quantity()
        def fail(work):
            work.outbox.consume(event)
            raise self.tx.StorageError("business_rejected")
        self.error(lambda: self.db.run(fail,self.budget), "business_rejected")
        require(self.receipt_counts(command)[3:] == (0,0), "consumer ack escaped rollback")
        with ThreadPoolExecutor(max_workers=16) as workers:
            replayed = list(workers.map(
                lambda _: consume_event(self.db,event,budget=self.budget).value, range(100)))
        require(replayed.count(False) == 1 and replayed.count(True) == 99,
                "consumer dedup was not event-atomic")
        require(self.quantity() == before and self.receipt_counts(command) == (1,2,1,1,2),
                "consumer minted again or projected twice")
        self.error(lambda: consume_event(self.db,replace(event,payload_json='{}'),
                                         budget=self.budget), "idempotency_conflict")
        require(not consume_event(self.db,event,consumer="independent_audit",
                                  budget=self.budget).value, "dedup scope is not consumer-specific")
        return dict(deliveries=100,new_receipts=1,replays=99,projection_rows=2,
                    consumer_rollback=True,consumer_scoped=True,no_second_mint=True)

    def leases_and_retry_deadline(self):
        from services.storage.settlement import settle
        from services.storage.outbox import Publisher, consume_event
        Publisher(self.db).run_once(lambda event: consume_event(self.db,event))
        command = self.terminal()
        settle(self.db,command,budget=self.budget)
        instant = datetime.now(timezone.utc)+timedelta(seconds=1)
        first = self.db.run(lambda w:w.outbox.claim(limit=1,lease_seconds=1,now=instant),
                            self.budget).value
        require(len(first)==1 and first[0].event.event_id==command.event_id, "first lease missing")
        require(not self.db.run(lambda w:w.outbox.claim(now=instant),self.budget).value,
                "live lease was claimed twice")
        later=instant+timedelta(seconds=2)
        second=self.db.run(lambda w:w.outbox.claim(limit=1,now=later),self.budget).value[0]
        require(second.lease_token!=first[0].lease_token and second.attempt==2,
                "lease reassignment did not fence old publisher")
        self.error(lambda:self.db.run(lambda w:w.outbox.acknowledge(first[0],now=later),self.budget),
                   "stale_delivery")
        dead=self.db.run(lambda w:w.outbox.fail(second,max_attempts=2,delay=.5,now=later),
                         self.budget).value
        require(dead and self.db.run(lambda w:w.outbox.stats(),self.budget).value["dead"]==1,
                "retry limit did not quarantine event")
        require(not self.db.run(lambda w:w.outbox.claim(now=later),self.budget).value,
                "dead event retried forever")
        self.db.run(lambda w:w.outbox.requeue_dead(command.event_id,now=later),self.budget)
        third=self.db.run(lambda w:w.outbox.claim(max_attempts=2,now=later),self.budget).value[0]
        require(third.attempt==1,"explicit requeue did not reset bounded allowance")
        next_time=later+timedelta(milliseconds=1)
        require(not self.db.run(lambda w:w.outbox.fail(third,delay=.5,max_attempts=2,
                                                      now=next_time),self.budget).value,
                "first requeued attempt was prematurely dead")
        require(not self.db.run(lambda w:w.outbox.claim(now=next_time),self.budget).value,
                "retry ignored available_at")
        final=self.db.run(lambda w:w.outbox.claim(now=next_time+timedelta(seconds=1)),
                          self.budget).value[0]
        self.db.run(lambda w:w.outbox.consume(final.event),self.budget)
        self.db.run(lambda w:w.outbox.acknowledge(final,now=next_time+timedelta(seconds=1)),
                    self.budget)
        require(self.db.run(lambda w:w.outbox.stats(),self.budget).value["pending"]==0,
                "retry backlog did not drain")
        return dict(stale_publisher_rejected=True,expiry_reassigned=True,
                    bounded_dead_letter=True,operator_requeue=True,retry_deadline=True)

    def publisher_failures_and_concurrency(self):
        from services.storage.settlement import settle
        from services.storage.outbox import Publisher, PermanentDeliveryError, consume_event
        commands=[self.terminal() for _ in range(12)]
        for command in commands:
            settle(self.db,command,budget=self.budget)
        def deliver(event):
            consume_event(self.db,event,budget=self.budget)
        with ThreadPoolExecutor(max_workers=2) as workers:
            results=list(workers.map(lambda _:Publisher(self.db).run_once(deliver,limit=6),range(2)))
        require(sum(r["published"] for r in results)==12,"dual publishers lost events")
        require(all(self.receipt_counts(c)==(1,2,1,1,2) for c in commands),
                "dual publishers duplicated or missed receipts")
        command=self.terminal()
        settle(self.db,command,budget=self.budget)
        samples=[]
        def poison(event):
            raise PermanentDeliveryError("test poison")
        publisher=Publisher(self.db,random_value=lambda:samples.append(True) or .5)
        outcome=publisher.run_once(poison)
        require(outcome["failed"]==1 and samples==[True]
                and self.db.run(lambda w:w.outbox.stats(),self.budget).value["dead"]==1,
                "poison event escaped DLQ")
        self.db.run(lambda w:w.outbox.requeue_dead(command.event_id),self.budget)
        require(publisher.run_once(deliver)["published"]==1, "operator could not replay poison")
        transient=self.terminal()
        settle(self.db,transient,budget=self.budget)
        def unavailable(event):
            raise OSError("controlled unavailable downstream")
        failed=publisher.run_once(unavailable)
        require(failed["failed"]==1,"transient delivery failure did not retain event")
        delay=self.query("SELECT extract(epoch FROM available_at-updated_at) "
                         "FROM arena.outbox WHERE event_id=%s",(transient.event_id,))[0][0]
        require(float(delay)==.005,"publisher retry did not apply bounded full jitter")
        instant=datetime.now(timezone.utc)+timedelta(seconds=1)
        delivery=self.db.run(lambda w:w.outbox.claim(now=instant),self.budget).value[0]
        consume_event(self.db,delivery.event,budget=self.budget)
        self.db.run(lambda w:w.outbox.acknowledge(delivery,now=instant),self.budget)
        return dict(dual_publishers=2,events=12,permanent_failure_dead_letter=True,
                    transient_retry_jitter_seconds=float(delay))

    def invalid_and_scope(self):
        from services.storage.settlement import settle, Reward
        command=self.terminal(aborted=True)
        self.error(lambda:settle(self.db,replace(command,rewards=(Reward(42,1,1),)),
                                budget=self.budget),"invalid_request")
        self.error(lambda:settle(self.db,replace(command,event_id=UUID(int=0)),
                                budget=self.budget),"invalid_request")
        self.error(lambda:settle(self.db,replace(command,result_version=2),
                                budget=self.budget),"invalid_request")
        saved=self.db.run(lambda w:(w.settlements,w.outbox),self.budget).value
        self.error(lambda:saved[0].get(command.settlement_id),"scope_closed")
        self.error(lambda:saved[1].get(command.event_id),"scope_closed")
        receipt=settle(self.db,command,budget=self.budget).value
        require(not receipt.replayed and self.receipt_counts(command)[:3]==(1,0,1),
                "aborted result cannot commit an empty settlement")
        for table,clause in (("asset_ledger","quantity=quantity+1"),
                             ("consumer_receipt","consumed_at=clock_timestamp()"),
                             ("settlement_reward_projection","quantity=quantity+1")):
            try:
                self.query("UPDATE arena."+table+" SET "+clause)
            except self.pg.errors.CheckViolation as error:
                require(error.diag.constraint_name=="reward_receipt_immutable_ck",
                        "wrong immutable receipt guard")
            else:
                raise Failure(table+" allowed receipt mutation")
        return dict(aborted_rewards_rejected=True,scope_revoked=True,zero_reward_abort=True,
                    ledger_receipt_projection_immutable=True)

    def six_crash_points(self):
        phases=("before_commit","after_commit","before_publish","after_publish","before_ack","after_ack")
        results=[]
        for phase in phases:
            command=self.terminal()
            before=self.quantity()
            args=[sys.executable,str(Path(__file__).resolve()),"--crash-phase",phase,
                  "--worker-socket",str(self.cluster.socket),"--worker-db",self.database,
                  "--match-id",str(command.match_id),"--repository-root",str(self.args.repository_root)]
            process=subprocess.run(args,capture_output=True,text=True,timeout=15,
                                   env=dict(os.environ,PYTHONDONTWRITEBYTECODE="1"))
            require(process.returncode==-signal.SIGKILL,
                    "crash child did not reach checkpoint "+phase+": "+process.stderr[-2000:])
            counts=self.receipt_counts(command)
            require(counts[:3]==((0,0,0) if phase=="before_commit" else (1,2,1)),
                    "crash violated transaction boundary at "+phase)
            from services.storage.settlement import settle
            self.db.run(lambda w:w.settlements.commit(command),self.budget)
            instant=datetime.now(timezone.utc)+timedelta(seconds=1)
            deliveries=self.db.run(lambda w:w.outbox.claim(now=instant),self.budget).value
            for delivery in deliveries:
                self.db.run(lambda w:w.outbox.consume(delivery.event),self.budget)
                self.db.run(lambda w:w.outbox.acknowledge(delivery,now=instant),self.budget)
            after=self.quantity()
            require(after[0][1]==before[0][1]+3 and after[1][1]==before[1][1]+2,
                    "crash recovery duplicated rewards at "+phase)
            require(self.receipt_counts(command)==(1,2,1,1,2),
                    "crash recovery left an event or projection incomplete at "+phase)
            results.append(dict(phase=phase,signal="SIGKILL",recovered=True,
                                inventory_exact_once=True,counts=list(self.receipt_counts(command))))
        require(self.db.run(lambda w:w.outbox.stats(),self.budget).value["pending"]==0,
                "crash backlog did not reach zero")
        return dict(matrix=results,all_six_recovered=True)

    def migration_reversible(self):
        database="arena_outbox_rollback"
        self.cluster.create(database)
        storage=self.args.repository_root/"services"/"storage"/"migrations"
        for name in ("001_initial","002_asset_transfer","003_settlement_outbox"):
            self.cluster.sql((storage/(name+".up.sql")).read_text(),database,single=True)
        self.cluster.sql((storage/"003_settlement_outbox.down.sql").read_text(),database,single=True)
        remain=self.cluster.sql("SELECT to_regclass('arena.account') IS NOT NULL "
                                "AND to_regclass('arena.asset_transfer') IS NOT NULL "
                                "AND to_regclass('arena.asset_ledger') IS NULL;",database).stdout.strip()
        require(remain=="t","003 down damaged the earlier schema")
        self.cluster.sql((storage/"003_settlement_outbox.up.sql").read_text(),database,single=True)
        for name in ("003_settlement_outbox","002_asset_transfer","001_initial"):
            self.cluster.sql((storage/(name+".down.sql")).read_text(),database,single=True)
        require(self.cluster.sql("SELECT to_regnamespace('arena') IS NULL;",database).stdout.strip()=="t",
                "migration chain did not fully roll back")
        return dict(additive_up_down_up=True,earlier_tables_preserved=True,full_chain_down=True)

    def execute(self):
        self.setup()
        for name,action in (
            ("atomic_inventory_ledger_outbox_and_100_producer_replays",self.atomic_credit_and_replay),
            ("rollback_of_all_rows_and_late_failure",self.rollback_all_writes),
            ("100_concurrent_consumer_deliveries_and_rollback",self.consumer_dedup_and_failure),
            ("fenced_leases_retry_deadline_dead_letter_and_requeue",self.leases_and_retry_deadline),
            ("dual_publishers_and_permanent_failure",self.publisher_failures_and_concurrency),
            ("invalid_rewards_and_scoped_repositories",self.invalid_and_scope),
            ("six_real_SIGKILL_crash_points",self.six_crash_points),
            ("additive_migration_and_full_rollback",self.migration_reversible),
        ):
            self.case(name,action)
        stats=self.db.run(lambda w:w.outbox.stats(),self.budget).value
        require(stats["pending"]==0 and stats["dead"]==0,"final outbox backlog not zero")
        for pool in self.pools:
            pool.close()
            self.released(pool)
        self.until(lambda:self.query("SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND application_name='arena-repository-boundary'")[0][0]==1,
            "closed pool retained database sessions")
        self.report.update(passed=True,outbox=stats,checked_out=0,idle_transactions=0)
        return self.report


def crash_worker(args):
    sys.path.insert(0,str(args.repository_root))
    from services.storage.database import Database
    from services.storage.outbox import consume_event
    command=Acceptance.command(args.match_id)
    with Database(kwargs=dict(host=args.worker_socket,port=5432,dbname=args.worker_db,
                              user="arena_test",connect_timeout=1),max_size=2,max_waiting=8) as db:
        def die():
            os.kill(os.getpid(),signal.SIGKILL)
        def apply(work):
            result=work.settlements.commit(command)
            if args.crash_phase=="before_commit":
                die()
            return result
        db.run(apply)
        if args.crash_phase=="after_commit":
            die()
        delivery=db.run(lambda w:w.outbox.claim(limit=1,lease_seconds=.05)).value[0]
        if args.crash_phase=="before_publish":
            die()
        consume_event(db,delivery.event)
        if args.crash_phase in ("after_publish","before_ack"):
            die()
        db.run(lambda w:w.outbox.acknowledge(delivery))
        die()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root",type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument("--pg-bin",type=Path)
    parser.add_argument("--pg-share",type=Path)
    parser.add_argument("--pg-lib",type=Path)
    parser.add_argument("--report",type=Path)
    parser.add_argument("--crash-phase")
    parser.add_argument("--worker-socket")
    parser.add_argument("--worker-db")
    parser.add_argument("--match-id",type=int)
    args=parser.parse_args()
    args.repository_root=args.repository_root.resolve()
    if args.crash_phase:
        crash_worker(args)
        return 1
    require(args.pg_bin is not None,"--pg-bin is required")
    args.pg_bin=args.pg_bin.resolve()
    if args.pg_share:
        args.pg_share=args.pg_share.resolve()
    if args.pg_lib:
        args.pg_lib=args.pg_lib.resolve()
    if args.pg_bin.is_file():
        args.pg_bin=args.pg_bin.parent
    for name in tuple(os.environ):
        if name.startswith("PG"):
            os.environ.pop(name)
    import psycopg
    sys.path.insert(0,str(args.repository_root))
    from services.storage import application,database,repositories,transactions
    cluster=RepositoryCluster(args)
    acceptance=Acceptance(args,cluster,psycopg,database,transactions,repositories,application)
    report=acceptance.report
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        cluster.start()
        report=acceptance.execute()
    except (Exception,KeyboardInterrupt) as error:
        report.update(passed=False,error=f"{type(error).__name__}: {error}")
        if isinstance(error,transactions.StorageError):
            report["storage_error"]=dict(category=error.category,sqlstate=error.sqlstate,
                                         attempts=error.attempts,failures=error.failures)
        if cluster.log.exists():
            report["server_log_tail"]=cluster.log.read_text(errors="replace")[-5000:]
    finally:
        for pool in acceptance.pools:
            try:
                pool.close()
            except Exception as error:
                report.update(passed=False,cleanup_error=str(error))
        report["pool_stats"]=[dict(pool.stats) for pool in acceptance.pools]
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
