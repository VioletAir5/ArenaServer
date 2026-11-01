#!/usr/bin/env python3
"""Actual TCP/WS bots, production Gateway, Storage process and private PostgreSQL."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, ROOT/'tests/storage', ROOT/'tests/room', ROOT/'tests/auth',
               ROOT/'tests/match', ROOT/'tests/gateway/net'):
    sys.path.insert(0, str(folder))
from StorageMigrationTest import Cluster, require, run
from StorageRpcTest import Acceptance as StorageFixture
from FullJourneyIntegrationTest import Bot
from RoomIntegrationTest import unsigned_field
from OnlinePresenceIntegrationTest import scalar
from TcpServerIntegrationTest import reserve_port, wait_until


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.cluster = Cluster(args)
        self.storage = StorageFixture(args, self.cluster)
        self.storage.publish_outbox = True
        self.gateway = None
        self.peers = []
        self.logs = []
        self.counter = 0
        self.report = dict(passed=False, cases=[])
        self.jwt_key = secrets.token_hex(32)

    def query(self, sql, values=()):
        return self.storage.query(sql, values)

    def setup(self):
        self.cluster.start()
        self.storage.setup()
        salt = secrets.token_bytes(16)
        key = hashlib.pbkdf2_hmac('sha256', b'test-password', salt, 100000, 32)
        digest = '$arena-pbkdf2-sha256$100000$' + salt.hex() + '$' + key.hex()
        self.query("UPDATE arena.account SET password_digest=%s WHERE account_name IN ('alice','bob')", (digest,))
        self.query("UPDATE arena.player SET mmr=CASE player_id WHEN 42 THEN 1440 ELSE 1480 END WHERE player_id IN (42,43)")
        self.initial = self.query("SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id")

    def content(self):
        return self.log_path.read_text(errors='replace')

    def start_gateway(self, *, ready_ticks=100):
        self.counter += 1
        self.tcp, self.ws = reserve_port(), reserve_port()
        while self.ws == self.tcp:
            self.ws = reserve_port()
        environment = self.cluster.environment(self.storage.database)
        environment.pop('ARENA_ACCOUNTS_FILE', None)
        environment.update(ARENA_JWT_KEY=self.jwt_key, ARENA_INTERNAL_TOKEN=self.storage.token,
            ARENA_STORAGE_PORT=str(self.storage.port), ARENA_SETTLEMENT_SPOOL_DIR=str(self.cluster.root/'spool'),
            ARENA_TCP_PORT=str(self.tcp), ARENA_WS_PORT=str(self.ws), ARENA_BATTLE_TICKS='40',
            ARENA_READY_TIMEOUT_TICKS=str(ready_ticks), ARENA_DISCONNECT_GRACE_MS='3000',
            ARENA_REPLAY_DIR=str(self.cluster.root/'replays'), PYTHONDONTWRITEBYTECODE='1')
        environment.setdefault('UBSAN_OPTIONS', 'halt_on_error=1:print_stacktrace=1')
        self.log_path = self.cluster.root/f'gateway-{self.counter}.log'
        self.log_stream = self.log_path.open('w')
        self.gateway = subprocess.Popen([str(self.args.gateway), 'asio', 'handshake'],
            cwd=self.args.repository_root, stdin=subprocess.DEVNULL,
            stdout=self.log_stream, stderr=self.log_stream, env=environment)
        wait_until(self.gateway, lambda: 'GATEWAY_READY' in self.content(), 'persistent Gateway startup')

    def connect(self, ws=False):
        peer = Bot((self.ws if ws else self.tcp, ws))
        self.peers.append(peer)
        return peer

    def stop_gateway(self, *, crash=False):
        process, self.gateway = self.gateway, None
        if process is None:
            return
        process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        process.wait(timeout=10)
        self.log_stream.close()
        text = self.content()
        self.logs.append(text)
        if crash:
            require(process.returncode == -signal.SIGKILL, 'Gateway crash injection failed')
        else:
            require(process.returncode == 0, 'Gateway failed to drain: '+text[-2500:])
            require('STORAGE_COUNTS auth_active=0 work_pending=0' in text,
                    'Gateway did not reclaim authentication/worker slots: '+text[-2000:])
            require('actors=0' in text, 'Gateway retained Room actors')
        require(not any(marker in text for marker in ('runtime error:', 'SanitizerTool:',
                'ERROR: AddressSanitizer', 'LeakSanitizer', 'WARNING: ThreadSanitizer')),
                'sanitizer diagnostic in Gateway')
        for peer in self.peers:
            try:
                peer.close()
            except OSError:
                pass
        self.peers.clear()

    def open_match(self, *, ready=True):
        alice, bob = self.connect(), self.connect(True)
        grant = alice.login()
        bob.login(b'bob')
        first, second = alice.match_join(101+self.counter), bob.match_join(201+self.counter)
        ticket = scalar(first, 2)
        assigned = {}
        def matched():
            assigned.update(alice.match_status(ticket))
            return scalar(assigned, 4) == 3 and scalar(assigned, 13) != 0
        wait_until(self.gateway, matched, 'durable-ID room allocation')
        match = scalar(assigned, 9)
        require(match >= 1007 and scalar(assigned, 13) == (1 << 63) | match, 'invalid durable allocation')
        state = alice.status()
        if ready:
            alice.ready(state, 42)
            bob.ready(state, 43)
            playing = {}
            def active():
                playing.update(alice.status())
                return playing['phase'] == 3
            wait_until(self.gateway, active, 'persistent battle starts')
            assert alice.control(playing['snapshot'], 42, 1, 1, -1)[1] == 1
            assert bob.control(playing['snapshot'], 43, 1, -1, 1)[1] == 1
        return alice, bob, grant, match

    def finished(self, peer):
        latest = {}
        def done():
            latest.update(peer.status())
            return latest['phase'] == 5
        wait_until(self.gateway, done, 'PostgreSQL acknowledged settlement')
        return latest['settlement']

    def verify(self, match, result, *, aborted=False):
        rows = self.query('SELECT room_id,state,final_tick,final_checksum,started_at,finished_at '
                          'FROM arena.game_match WHERE match_id=%s', (match,))
        require(len(rows) == 1 and int(rows[0][0]) == result['room'], 'match row absent')
        row = rows[0]
        require(row[1] == ('aborted' if aborted else 'completed') and int(row[2]) == result['tick']
                and int(row[3]) == result['checksum'], 'persisted result differs from wire')
        require(row[5] is not None and (row[4] is None) == aborted, 'incorrect start/finish facts')
        participants = self.query('SELECT player_id,initial_mmr,last_input_seq,final_x,final_y,last_dx,last_dy '
                                  'FROM arena.match_player WHERE match_id=%s ORDER BY slot', (match,))
        require([(int(p[0]),p[1]) for p in participants] == [(42,1440),(43,1480)], 'rating snapshot was not canonical')
        if not aborted:
            require([tuple(map(int,p[2:])) for p in participants] == [(1,1,-1,1,-1),(1,-1,1,-1,1)],
                    'final sequence/position/control facts lost')
        counts = self.query('SELECT (SELECT count(*) FROM arena.settlement WHERE match_id=%s),'
            '(SELECT count(*) FROM arena.asset_ledger WHERE settlement_id=%s),'
            '(SELECT count(*) FROM arena.outbox WHERE settlement_id=%s)', (match,match,match))[0]
        require(counts == (1, 0 if aborted else 2, 1), 'settlement/ledger/outbox not atomic')
        def published():
            rows = self.query("SELECT o.state, (SELECT count(*) FROM arena.consumer_receipt r "
                "WHERE r.event_id=o.event_id AND r.consumer='reward_audit'), "
                "(SELECT count(*) FROM arena.settlement_reward_projection p "
                "WHERE p.event_id=o.event_id AND p.consumer='reward_audit') "
                "FROM arena.outbox o WHERE o.settlement_id=%s", (match,))
            return rows == [('published', 1, 0 if aborted else 2)]
        wait_until(self.gateway, published, 'runtime outbox consumer and acknowledgement')
        return dict(match_id=match, final_tick=result['tick'], checksum=result['checksum'])
    def complete_and_reconnect(self):
        self.start_gateway()
        alice, bob, grant, match = self.open_match()
        alice.close(reset=True)
        alice = self.connect(True)
        resumed = alice.login(token=grant['token'])
        require(resumed['resumed'] and resumed['session'] == grant['session'], 'grace reconnect changed logical session')
        result = self.finished(alice)
        require(alice.result(match) == result and bob.result(match) == result, 'participants see different result')
        details = self.verify(match, result)
        self.stop_gateway()
        self.start_gateway()
        recovered = self.connect()
        recovered.login()
        require(recovered.result(match) == result, 'restart lookup changed historical result')
        self.stop_gateway()
        return dict(**details, rst_resumed=True, historical_query_after_restart=True)

    def pg_outage(self):
        self.start_gateway()
        alice, bob, _, match = self.open_match()
        stopped = run([self.cluster.tool('pg_ctl'), '-D', self.cluster.data, '-m', 'fast',
            '-w', '-t', '15', 'stop'], self.cluster.base, timeout=20)
        require(stopped.returncode == 0, 'could not stop private PG')
        try:
            latest = {}
            def settling():
                latest.update(alice.status())
                return latest['phase'] == 4
            wait_until(self.gateway, settling, 'unacknowledged result stays Settling')
            require('settlement' not in latest, 'exposed result before commit acknowledgment')
            require(alice.query()['room'] != 0 and bob.query()['room'] != 0,
                    'released allocation before durable commit')
            wait_until(self.gateway, lambda: bool(list((self.cluster.root/'spool').glob('*.pending'))),
                       'fsynced pending settlement')
        finally:
            options = (f"-c listen_addresses='' -c unix_socket_directories='{self.cluster.socket}' "
                '-c unix_socket_permissions=0700 -c port=5432 -c max_connections=16 '
                '-c log_min_messages=warning -c timezone=UTC')
            restarted = run([self.cluster.tool('pg_ctl'), '-D', self.cluster.data, '-l', self.cluster.log,
                '-w', '-t', '15', '-o', options, 'start'], self.cluster.base, timeout=20)
            require(restarted.returncode == 0, 'private PG did not restart')
        result = self.finished(alice)
        details = self.verify(match, result)
        require(alice.query()['room'] == bob.query()['room'] == 0, 'committed match kept live bindings')
        self.stop_gateway()
        return dict(**details, no_early_finish=True, same_gateway_recovered=True)

    def crash_recovery(self):
        self.start_gateway()
        alice, _, _, match = self.open_match()
        self.storage.stop()
        frozen = {}
        def settling():
            frozen.update(alice.status())
            return frozen['phase'] == 4
        wait_until(self.gateway, settling, 'Storage-down result freezes')
        wait_until(self.gateway, lambda: bool(list((self.cluster.root/'spool').glob('*.pending'))),
                   'durable result spool before SIGKILL')
        expected = frozen['snapshot']
        require(not self.query('SELECT match_id FROM arena.game_match WHERE match_id=%s', (match,)),
                'match unexpectedly committed while Storage stopped')
        self.stop_gateway(crash=True)
        self.storage.start()
        self.start_gateway()
        wait_until(self.gateway, lambda: bool(self.query('SELECT 1 FROM arena.settlement WHERE match_id=%s', (match,))),
                   'pending result replay after Gateway crash')
        alice = self.connect()
        alice.login()
        result = alice.result(match)
        require(result['checksum'] == expected['checksum'] and result['tick'] == expected['tick'],
                'spool replay changed frozen state')
        details = self.verify(match, result)
        self.stop_gateway()
        # Another restart exercises an empty or acknowledged spool and the same query.
        self.start_gateway()
        alice = self.connect(True)
        alice.login()
        require(alice.result(match) == result, 'replayed receipt differs after second restart')
        self.verify(match, result)
        self.stop_gateway()
        return dict(**details, actual_sigkill=True, restart_replays=2)

    def ready_abort(self):
        self.start_gateway(ready_ticks=8)
        alice, _, _, match = self.open_match(ready=False)
        result = self.finished(alice)
        details = self.verify(match, result, aborted=True)
        require(result['reason'] == 3, 'ready timeout reason changed')
        self.stop_gateway()
        return dict(**details, never_started=True, awarded=0)

    def run(self):
        self.setup()
        for name, action in (('tcp_ws_match_reconnect_and_restart_query', self.complete_and_reconnect),
                             ('pg_down_freezes_until_ack_and_recovers', self.pg_outage),
                             ('durable_spool_sigkill_and_idempotent_replay', self.crash_recovery),
                             ('never_started_abort_without_rewards', self.ready_abort)):
            self.report['current_case'] = name
            started = time.monotonic()
            details = action()
            self.report['cases'].append(dict(name=name, passed=True,
                elapsed=time.monotonic()-started, **details))
        # Inspect quiescent pool state, not a healthy publisher between its SQL statements.
        self.storage.stop()
        self.report['publisher_shutdowns'] = [entry['publisher'] for entry in self.storage.log]
        final = self.query('SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id')
        require([(int(p),int(q)) for p,q in final] == [(int(p),int(q)+30) for p,q in self.initial],
                'restarts duplicated or lost settlement awards')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'")[0][0] == 0,
                'Gateway journey left idle transaction')
        match_ids = [case['match_id'] for case in self.report['cases']]
        ledger = self.query('SELECT player_id,item_id,sum(quantity) FROM arena.asset_ledger '
            'WHERE settlement_id=ANY(%s) GROUP BY player_id,item_id ORDER BY player_id,item_id', (match_ids,))
        projection = self.query('SELECT player_id,item_id,sum(quantity) FROM arena.settlement_reward_projection '
            "WHERE consumer='reward_audit' AND event_id IN "
            '(SELECT event_id FROM arena.outbox WHERE settlement_id=ANY(%s)) '
            'GROUP BY player_id,item_id ORDER BY player_id,item_id', (match_ids,))
        require(projection == ledger, 'runtime consumer projection duplicated or lost committed ledger facts')
        self.report.update(outbox_published=4, consumer_projection_matches_ledger=True)
        self.report.update(passed=True, completed_matches=3, aborted_matches=1,
                           repeated_awards=0, redis_available=False, final_inventory=[list(map(int,r)) for r in final])
        return self.report

    def close(self):
        self.stop_gateway()
        self.storage.stop()
        self.cluster.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=ROOT)
    parser.add_argument('--pg-bin', type=Path, required=True)
    parser.add_argument('--pg-share', type=Path)
    parser.add_argument('--pg-lib', type=Path)
    parser.add_argument('--descriptor', type=Path, required=True)
    parser.add_argument('--gateway', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    for name in ('repository_root','pg_bin','pg_share','pg_lib','descriptor','gateway','report'):
        value = getattr(args,name)
        if value is not None:
            setattr(args,name,value.resolve())
    acceptance = Acceptance(args)
    report = acceptance.report
    try:
        report = acceptance.run()
    except Exception as error:
        report.update(passed=False,error=f'{type(error).__name__}: {error}')
        if acceptance.gateway is not None:
            report['gateway_log_tail'] = acceptance.content()[-5000:]
    finally:
        try:
            acceptance.close()
            report['cluster_cleaned'] = not acceptance.cluster.root.exists()
        except Exception as error:
            report.update(passed=False,cleanup_error=str(error))
        report['gateway_runs'] = len(acceptance.logs)
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
