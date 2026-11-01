#!/usr/bin/env python3
"""Real private PostgreSQL deadlines, blackholed I/O and exact loan recovery."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from uuid import UUID

sys.dont_write_bytecode = True
from StorageMigrationTest import Cluster, require, run
from PgFaultProxy import PgFaultProxy


class Acceptance:
    def __init__(self, args, cluster):
        from services.storage.database import Database
        from services.storage.worker import Worker
        from services.storage.transactions import StorageError
        from shared.runtime.rpc import Context, RpcError, DEADLINE_EXCEEDED
        import psycopg
        self.args, self.cluster, self.Database, self.Worker = args, cluster, Database, Worker
        self.StorageError, self.Context, self.RpcError = StorageError, Context, RpcError
        self.DEADLINE_EXCEEDED, self.pg = DEADLINE_EXCEEDED, psycopg
        self.database = 'arena_deadline_boundary'
        self.proxies = []
        self.pools, self.report = [], dict(passed=False, cases=[],
            psycopg_version=psycopg.__version__, libpq_version=psycopg.pq.version(),
            pq_implementation=psycopg.pq.__impl__, source_hashes={})
        self.report['test_source_hashes'] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (Path(__file__), Path(__file__).with_name('PgFaultProxy.py'))}
        selected = args.source_root or args.repository_root
        for name in ('database.py', 'worker.py', 'transactions.py', 'deadlines.py'):
            path = selected/'services/storage'/name
            if path.exists():
                self.report['source_hashes'][name] = hashlib.sha256(path.read_bytes()).hexdigest()
        rpc_source = selected/'shared/runtime/rpc.py'
        self.report['source_hashes']['shared/runtime/rpc.py'] = hashlib.sha256(rpc_source.read_bytes()).hexdigest()

    def admin(self, sql, params=None):
        with self.pg.connect(host=str(self.cluster.socket), dbname=self.database,
                user='arena_test', port=5432, connect_timeout=1, autocommit=True) as connection:
            cursor = connection.execute(sql, params)
            return cursor.fetchall() if cursor.description else []

    def setup(self):
        self.cluster.create(self.database)
        result = run([sys.executable, self.args.repository_root/'services/storage/migrate.py', 'up'],
                     self.cluster.environment(self.database), timeout=95)
        require(result.returncode == 0, 'private migrations failed: '+result.stderr)
        self.cluster.sql((self.args.repository_root/'services/storage/fixtures/seed.sql').read_text(),
                         self.database, single=True)

    async def until(self, predicate, message, timeout=1):
        end = time.monotonic()+timeout
        while not predicate():
            if time.monotonic() >= end:
                self.report['failure_socket_fds'] = {str(p): os.readlink(p) for p in Path('/proc/self/fd').iterdir() if p.exists() and os.readlink(p).startswith('socket:[')}
                inodes = {value.partition('[')[2].rstrip(']') for value in self.report['failure_socket_fds'].values()}
                self.report['failure_own_socket_rows'] = [line for name in ('tcp', 'unix') for line in Path('/proc/self/net/'+name).read_text().splitlines() if any(inode in line.split() for inode in inodes)]
                self.report['failure_proxies'] = [dict(port=p.port, tasks=len(p.tasks), writers=len(p.writers), closing=p.closing) for p in self.proxies]
                require(False, message)
            await asyncio.sleep(.001)

    async def quiet(self, worker, database, origin):
        await self.until(lambda: worker.stats['active'] == database.stats['checked_out'] == 0,
                         'actual worker/loan exceeded 100ms cleanup bound',
                         max(.0001, origin+.1-time.monotonic()))
        elapsed = time.monotonic()-origin
        require(elapsed <= .1, f'worker/loan cleanup took {elapsed:.6f}s')
        require(database.stats.get('requests_waiting', 0) == 0, 'retired SDK waiter remained queued')
        return elapsed

    async def make(self, *, workers=1, connection_class=None):
        proxy = await PgFaultProxy(self.cluster.socket/'.s.PGSQL.5432').start()
        self.proxies.append(proxy)
        observed = []
        base = self.Database
        class RecordingDatabase(base):
            def run(inner, *values, **options):
                try:
                    return super().run(*values, **options)
                except self.StorageError as error:
                    observed.append(error)
                    raise
        values = dict(kwargs=dict(host='127.0.0.1', port=proxy.port,
            dbname=self.database, user='arena_test', connect_timeout=1,
            application_name='arena-deadline-proxy', sslmode='disable', gssencmode='disable'),
            max_size=1, max_waiting=2, acquire_timeout=.25)
        if connection_class is not None:
            values['connection_class'] = connection_class
        database = RecordingDatabase(**values)
        worker = self.Worker(database, max_workers=workers)
        self.pools.append(database)
        try:
            require(await worker.run(lambda work: work.accounts.get_player(42),
                self.Context(1, 'warm-real-pool', time.monotonic()+2, '')) is not None,
                'private fixture player missing')
        except BaseException:
            await proxy.close()
            await worker.close()
            database.close()
            raise
        return proxy, database, worker, observed

    async def close(self, proxy, database, worker):
        # Stop the connection producer before its proxy dependency. Leaving
        # refill connects alive while closing a Python 3.10 asyncio listener can
        # race raw accept/transport construction and leak a test-owned socket.
        # A red assertion may first need to release blocked peers, but keeps the
        # listener available until the actual worker/pool have joined.
        if worker.stats['active']:
            await proxy.disconnect()
        await asyncio.wait_for(worker.close(), 3)
        await asyncio.to_thread(database.close)
        await proxy.close()
        require(worker.stats['active'] == database.stats['checked_out'] == 0,
                'closing private resources retained a worker/loan')
        require(database.stats['closed'], 'private pool remained open')

    async def timeout(self, worker, operation, *, duration=.1, correlation='pg-deadline'):
        context = self.Context(2, correlation, time.monotonic()+duration, '')
        try:
            await worker.run(operation, context)
        except self.RpcError as error:
            require(error.status == self.DEADLINE_EXCEEDED, 'unexpected RPC timeout classification')
        except self.StorageError as error:
            require(error.category in ('budget_exhausted', 'request_cancelled',
                'statement_timeout', 'pool_timeout', 'outcome_unknown'),
                'unexpected storage timeout classification '+error.category)
        else:
            raise AssertionError('blackholed query unexpectedly succeeded')
        excess = time.monotonic()-context.deadline
        require(abs(excess) <= .02, f'caller deadline excess {excess:.6f}s exceeds20ms')
        return context.deadline, excess

    async def blackhole(self, *, health=False, cancel=False):
        proxy, database, worker, observed = await self.make()
        entered = threading.Event()
        try:
            sql = '' if health else 'SELECT pg_sleep(5)'
            proxy.arm(sql, cancel_blackhole=cancel)
            def operation(work):
                entered.set()
                return work._session.connection().execute('SELECT pg_sleep(5)').fetchone()
            origin, excess = await self.timeout(worker, operation)
            require(proxy.hit.is_set(), 'fault did not reach the intended real query')
            if health:
                require(not entered.is_set(), 'health-check fault reached business callback')
            elapsed = await self.quiet(worker, database, origin)
            if not health:
                await asyncio.wait_for(proxy.cancel_hit.wait(), .2)
            await self.until(lambda: proxy.active_cancels == 0,
                'independent cancellation socket/task retained', max(.0001, origin+.1-time.monotonic()))
            remote_elapsed = None
            if cancel:
                require(proxy.cancel_connections == 1, 'cancel channel was not exercised exactly once')
            else:
                await self.until(lambda: self.admin("SELECT count(*) FROM pg_stat_activity "
                    "WHERE application_name='arena-deadline-proxy' AND state='active' "
                    "AND query='SELECT pg_sleep(5)'")[0][0] == 0,
                    'reachable PostgreSQL query did not stop', max(.0001, origin+.1-time.monotonic()))
                remote_elapsed = (time.monotonic()-origin)*1000
                require(remote_elapsed <= 100, 'reachable backend cleanup exceeded100ms')
            result = await worker.run(lambda work: work.accounts.get_player(42),
                self.Context(3, 'next-borrower', time.monotonic()+1, ''))
            require(result.player_id == 42, 'expired request cancelled next borrower')
            return dict(deadline_excess_ms=excess*1000, local_cleanup_ms=elapsed*1000,
                health_check_before_business=health, cancel_channel_blackholed=cancel,
                cancel_connections=proxy.cancel_connections, next_borrower_succeeded=True,
                independent_cancel_resources_remaining=proxy.active_cancels,
                reachable_backend_stopped_ms=remote_elapsed,
                remote_100ms_cancellation_claimed=not cancel and not health)
        finally:
            await self.close(proxy, database, worker)

    async def active_cancel(self):
        proxy, database, worker, observed = await self.make()
        try:
            def operation(work):
                return work._session.connection().execute('SELECT pg_sleep(5)').fetchone()
            task = asyncio.create_task(worker.run(operation,
                self.Context(4, 'active-cancel', time.monotonic()+2, '')))
            await self.until(lambda: self.admin("SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name='arena-deadline-proxy' AND state='active' "
                "AND query='SELECT pg_sleep(5)'")[0][0] == 1, 'real sleep never became active')
            started = time.monotonic()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            elapsed = await self.quiet(worker, database, started)
            await asyncio.wait_for(proxy.cancel_hit.wait(), .1)
            await self.until(lambda: self.admin("SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name='arena-deadline-proxy' AND state='active' "
                "AND query='SELECT pg_sleep(5)'")[0][0] == 0, 'real pg_sleep survived cancellation',
                max(.0001, started+.1-time.monotonic()))
            backend_elapsed = time.monotonic()-started
            require(backend_elapsed <= .1, 'reachable backend cancellation exceeded100ms')
            require((await worker.run(lambda work: work.accounts.get_player(42),
                self.Context(5, 'reuse-after-cancel', time.monotonic()+1, ''))).player_id == 42,
                'late cancellation reached the next borrower')
            return dict(actual_local_cleanup_ms=elapsed*1000,
                reachable_backend_cancelled_within_ms=backend_elapsed*1000,
                independent_cancel_connections=proxy.cancel_connections)
        finally:
            await self.close(proxy, database, worker)

    async def queued_cancel(self):
        proxy, database, worker, observed = await self.make(workers=2)
        held = None
        try:
            held = asyncio.create_task(worker.run(lambda work:
                work._session.connection().execute('SELECT pg_sleep(5)').fetchone(),
                self.Context(6, 'held-pool', time.monotonic()+2, '')))
            await self.until(lambda: database.stats['checked_out'] == 1, 'first loan was not held')
            queued = asyncio.create_task(worker.run(lambda _: 'must-not-run',
                self.Context(7, 'queued-cancel', time.monotonic()+2, '')))
            await self.until(lambda: database.stats.get('requests_waiting', 0) == 1,
                             'second request never queued for a loan')
            started = time.monotonic()
            queued.cancel()
            await asyncio.gather(queued, return_exceptions=True)
            await self.until(lambda: worker.stats['active'] == 1
                and database.stats.get('requests_waiting', 0) == 0,
                'cancelled pool waiter retained capacity', .1)
            elapsed = time.monotonic()-started
            require(elapsed <= .1, 'queued cancellation exceeded100ms')
            require(database.stats['checked_out'] == 1, 'queued cancellation stole live loan')
            held_cancelled = time.monotonic()
            held.cancel()
            await asyncio.gather(held, return_exceptions=True)
            await self.quiet(worker, database, held_cancelled)
            held = None
            return dict(queued_waiter_cleanup_ms=elapsed*1000, retired_waiters=0)
        finally:
            if held is not None:
                held.cancel()
                await asyncio.gather(held, return_exceptions=True)
            await self.close(proxy, database, worker)

    async def commit_loss(self):
        from services.storage.assets import TransferRequest
        proxy, database, worker, observed = await self.make()
        command = TransferRequest(UUID('426db131-18ab-42a8-a489-cd860ef90031'), 42, 43, 1, 1)
        before = self.admin('SELECT player_id,quantity,revision FROM arena.inventory '
                            'WHERE item_id=1 AND player_id IN (42,43) ORDER BY player_id')
        invocations = 0
        try:
            proxy.arm('COMMIT')
            def transfer(work):
                nonlocal invocations
                invocations += 1
                return work.transfer(command)
            origin, excess = await self.timeout(worker, transfer, duration=.15,
                                                  correlation='commit-ack-loss')
            require(proxy.hit.is_set(), 'COMMIT fault did not execute')
            elapsed = await self.quiet(worker, database, origin)
            require(observed and observed[-1].category == 'outcome_unknown',
                    'interrupted COMMIT was disguised as rollback or retryable error')
            require(observed[-1].attempts == invocations == 1, 'unknown commit was retried automatically')
            receipt = self.admin('SELECT source_player_id,destination_player_id,item_id,quantity '
                                'FROM arena.asset_transfer WHERE transfer_id=%s', (command.transfer_id,))
            require(receipt == [(42,43,1,1)], 'actual committed transfer receipt missing')
            replay = await worker.run(lambda work: work.transfer(command),
                self.Context(8, 'same-command-resolution', time.monotonic()+1, ''))
            require(replay.replayed and replay.transfer_id == command.transfer_id,
                    'same immutable command did not resolve as replay')
            after = self.admin('SELECT player_id,quantity,revision FROM arena.inventory '
                              'WHERE item_id=1 AND player_id IN (42,43) ORDER BY player_id')
            expected = [(player, quantity+(-1 if player==42 else 1), revision+1)
                        for player,quantity,revision in before]
            require(after == expected, 'commit loss/replay duplicated asset mutation')
            return dict(deadline_excess_ms=excess*1000, local_cleanup_ms=elapsed*1000,
                classification=observed[-1].category, original_invocations=invocations,
                durable_receipts=1, same_id_replayed=True, asset_mutations=1)
        finally:
            await self.close(proxy, database, worker)

    async def rpc_to_healthcheck_blackhole(self):
        from shared.runtime.protocol import Protocol
        from shared.runtime.rpc import Client, Server
        from services.storage.server import StorageService
        require(self.args.descriptor is not None, "end-to-end PG RPC case requires the actual descriptor")
        protocol = Protocol(self.args.descriptor)
        token = 'private-day31-rpc-token-'+'x'*40
        proxy = await PgFaultProxy(self.cluster.socket/'.s.PGSQL.5432').start()
        self.proxies.append(proxy)
        database = self.Database(kwargs=dict(host='127.0.0.1',port=proxy.port,
            dbname=self.database,user='arena_test',connect_timeout=1,
            application_name='arena-deadline-proxy',sslmode='disable',gssencmode='disable'),
            max_size=1,max_waiting=1,acquire_timeout=.25)
        self.pools.append(database)
        service = StorageService(protocol,database,max_workers=1)
        server = Server(protocol,service.handlers,service_token=token)
        try:
            # Prime the real pool, not a replacement worker or handler.
            await asyncio.to_thread(database.run,lambda work:work.accounts.get_player(42))
            port = await server.start()
            service.start()
            client = Client(protocol,'127.0.0.1',port,service_token=token)
            proxy.arm('')
            payload = protocol.message('arena.storage.FindAccountRequest')(account_name='alice').SerializeToString()
            deadline = time.monotonic()+.1
            try:
                await client.call('account.find',payload,timeout=.1,
                                  correlation_id='actual-rpc-pg-health-blackhole')
            except self.RpcError as error:
                require(error.status == self.DEADLINE_EXCEEDED,
                        'end-to-end health-check fault returned another error')
            else:
                raise AssertionError('blackholed authenticated account RPC unexpectedly succeeded')
            excess = time.monotonic()-deadline
            require(abs(excess) <= .02, 'end-to-end client deadline error exceeded20ms')
            require(proxy.hit.is_set(), 'actual RPC did not reach the intended PG health check')
            await self.until(lambda: service.worker.stats['active'] == 0
                and database.stats['checked_out'] == 0
                and database.stats.get('requests_waiting',0) == 0
                and proxy.active_cancels == 0 and server.inflight == client.inflight == 0,
                'actual RPC/Storage/SQL chain retained resources beyond100ms',
                max(.0001,deadline+.1-time.monotonic()))
            cleanup_elapsed = time.monotonic()-deadline
            require(cleanup_elapsed <= .1,'actual end-to-end resource cleanup exceeded100ms')
            # New calls traverse the same live production service after the
            # expired request has relinquished its connection and cancellation.
            raw = await client.call('account.find',payload,timeout=1,
                                    correlation_id='actual-rpc-pg-next-borrower')
            reply = protocol.message('arena.storage.FindAccountReply').FromString(raw)
            require(reply.player.player_id == 42 and reply.account.account_id == 1,
                    'old cancellation poisoned a later actual RPC borrower')
            await self.until(lambda: server.inflight == client.inflight == 0
                and service.worker.stats['active'] == database.stats['checked_out'] == 0,
                'the subsequent healthy RPC retained work after completion',.1)
            return dict(authenticated_real_rpc=True,production_storage_handler=True,
                deadline_excess_ms=excess*1000,end_to_end_cleanup_ms=cleanup_elapsed*1000,
                rpc_inflight=server.inflight,client_inflight=client.inflight,
                sql_workers=service.worker.stats['active'],checked_out=database.stats['checked_out'],
                cancel_resources=proxy.active_cancels,next_real_rpc_succeeded=True)
        finally:
            await server.close(grace=.1)
            # The service owns the pool; allow an already-started maintenance
            # refill to finish with its dependency loop running before its sync
            # pool close. The request cleanup bound above does not count refill
            # infrastructure as a request-owned SQL loan.
            if not database.stats['closed']:
                await self.until(lambda: database.stats.get('pool_available',0) == 1,
                                 'owned pool refill failed before service shutdown',2)
            await service.close()
            await proxy.close()
            require(service.worker.stats['active'] == database.stats['checked_out'] == 0
                    and database.stats['closed'], 'actual service shutdown retained SQL resources')

    async def expired(self):
        proxy, database, worker, observed = await self.make()
        calls = 0
        before = worker.stats['completed']
        try:
            def forbidden(_):
                nonlocal calls
                calls += 1
            for index in range(1000):
                try:
                    await worker.run(forbidden,
                        self.Context(index+10, 'already-expired', time.monotonic()-1, ''))
                except self.RpcError as error:
                    require(error.status == self.DEADLINE_EXCEEDED, 'wrong expired request error')
                else:
                    raise AssertionError('expired request was accepted')
            require(calls == 0 and worker.stats['completed'] == before,
                    'expired request entered the SQL executor')
            require(worker.stats['active'] == database.stats['checked_out'] == 0,
                    'expired request retained a task or loan')
            return dict(expired_requests=1000, business_calls=0, admitted_sql_jobs=0)
        finally:
            await self.close(proxy, database, worker)

    async def acknowledged(self):
        from services.storage.deadlines import RequestGuard, guard_scope
        from psycopg.waiting import Wait
        cancelled = threading.Event()
        pg = self.pg
        class AfterAcknowledgment(pg.Connection):
            def commit(inner):
                super().commit()
                cancelled.set()
        proxy, database, worker, observed = await self.make(connection_class=AfterAcknowledgment)
        cancelled.clear()
        try:
            def parsed_completion():
                # Use the actual borrowed PostgreSQL descriptor for readiness.
                # The sentinel represents a driver result already parsed when
                # cancellation flips; it is not a remote COMMIT acknowledgment.
                with database._lease() as connection:
                    sentinel = object()
                    def generator():
                        yield Wait.W
                        cancelled.set()
                        return sentinel
                    with guard_scope(RequestGuard(time.monotonic()+1, cancelled)):
                        value = connection.wait(generator())
                    require(value is sentinel and cancelled.is_set(),
                            'parsed generator completion lost to post-result cancellation')
                    require(not connection.closed
                            and connection.info.transaction_status == pg.pq.TransactionStatus.IDLE,
                            'parsed completion cancelled or closed the actual idle connection')
            await asyncio.to_thread(parsed_completion)
            require(proxy.cancel_connections == 0,
                    'parsed generator completion created an unnecessary cancellation socket')
            cancelled.clear()
            # An explicit post-ack fault hook surrounds a real PostgreSQL COMMIT;
            # it is separate from the real network acknowledgement-loss case.
            result = await asyncio.to_thread(database.run,
                lambda work: work.accounts.get_player(42),
                _request_guard=RequestGuard(time.monotonic()+1, cancelled))
            require(cancelled.is_set() and result.value.player_id == 42,
                    'a cancellation after actual COMMIT acknowledgment replaced success')
            require(database.stats['checked_out'] == 0, 'post-ack cancellation leaked a loan')
            return dict(real_commit_acknowledged=True, injected_post_ack_cancel=True,
                parsed_generator_completion_seam=True, generator_used_real_pg_socket=True,
                parsed_completion_kept_connection_open=True, result='success')
        finally:
            await self.close(proxy, database, worker)

    def socket_fds(self):
        result = 0
        for descriptor in Path('/proc/self/fd').iterdir():
            try:
                result += os.readlink(descriptor).startswith('socket:[')
            except FileNotFoundError:
                pass
        return result

    async def run(self):
        socket_baseline = self.socket_fds()
        self.report['socket_fd_baseline'] = socket_baseline
        actions = [
            ('checkout_healthcheck_reply_blackhole', lambda: self.blackhole(health=True)),
            ('query_reply_blackhole', self.blackhole),
            ('actual_rpc_storage_healthcheck_blackhole', self.rpc_to_healthcheck_blackhole),
            ('cancel_channel_blackhole', lambda: self.blackhole(cancel=True)),
            ('reachable_backend_cancel_and_next_borrower', self.active_cancel),
            ('queued_checkout_cancellation', self.queued_cancel),
            ('real_commit_ack_loss_same_identity_resolution', self.commit_loss),
            ('1000_expired_requests_no_sql', self.expired),
            ('post_ack_cancellation_preserves_success', self.acknowledged)]
        for name, action in actions:
            if self.args.only and self.args.only != name:
                continue
            self.report['current_case'] = name
            started = time.monotonic()
            details = await action()
            require(self.admin("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                "AND state LIKE 'idle in transaction%'")[0][0] == 0, 'idle transaction remained after case')
            await self.until(lambda: self.socket_fds() <= socket_baseline,
                'closed pools/proxies leaked a physical socket descriptor', 1)
            self.report['cases'].append(dict(name=name, passed=True, socket_fds_after=self.socket_fds(),
                elapsed_seconds=time.monotonic()-started, **details))
        require(bool(self.report['cases']), 'requested case does not exist')
        self.report.update(passed=True, all_pools_closed=all(p.stats['closed'] for p in self.pools),
            final_checked_out=sum(p.stats['checked_out'] for p in self.pools),
            socket_fds_after=self.socket_fds(),
            note='100ms bound measures request workers/loans/retired waiters/cancel copies; pool refill is separately owned infrastructure, joined at shutdown.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, required=True)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--descriptor',type=Path)
    parser.add_argument('--pg-bin', type=Path, required=True)
    parser.add_argument('--pg-share', type=Path)
    parser.add_argument('--pg-lib', type=Path)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--only')
    args = parser.parse_args()
    args.repository_root = args.repository_root.resolve()
    require(args.descriptor is not None or args.only not in (None,'actual_rpc_storage_healthcheck_blackhole'),
            'full acceptance requires --descriptor; the end-to-end case must not be skipped')
    sys.path.insert(0, str(args.source_root or args.repository_root))
    cluster = Cluster(args)
    test = Acceptance(args, cluster)
    try:
        cluster.start()
        test.setup()
        asyncio.run(test.run())
    except BaseException as error:
        test.report.update(error=str(error), error_type=type(error).__name__)
        raise
    finally:
        cluster.close()
        test.report['cluster_cleaned'] = not cluster.root.exists()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(test.report, indent=2)+'\n')
    print('PostgresDeadlineCancellation: '+str(len(test.report['cases']))+' cases passed')


if __name__ == '__main__':
    main()