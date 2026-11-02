#!/usr/bin/env python3
"""Real four-process TCP/WS, durable Lobby recovery and PostgreSQL settlement acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import zlib

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, ROOT/'tests/storage', ROOT/'tests/room', ROOT/'tests/auth',
               ROOT/'tests/lobby', ROOT/'tests/match', ROOT/'tests/gateway/net'):
    sys.path.insert(0, str(folder))
from StorageMigrationTest import require
from ServiceStackFixture import ServiceStack
from AuthProcessIntegrationTest import health_http
from OnlinePresenceIntegrationTest import scalar


class StorageAckProxy:
    """Forward real role-authenticated RPC; hold only settlement replies after execution."""
    def __init__(self, test, target):
        self.test, self.target = test, target
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.closing = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.hit = threading.Event()
        self.connections, self.workers = set(), set()
        self.held_methods = []
        self.acceptor = threading.Thread(target=self._accept, name='settlement-ack-proxy')
        self.acceptor.start()

    @staticmethod
    def frame(peer):
        def exact(size):
            data = bytearray()
            while len(data) < size:
                part = peer.recv(size-len(data))
                if not part:
                    raise EOFError()
                data.extend(part)
            return bytes(data)
        header = exact(4)
        size, = struct.unpack('!I', header)
        require(0 < size <= 256*1024, 'unbounded private RPC frame')
        return header+exact(size)

    def _accept(self):
        while not self.closing.is_set():
            try:
                peer, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.lock:
                if len(self.workers) >= 16:
                    peer.close()
                    continue
                self.connections.add(peer)
                worker = threading.Thread(target=self._serve, args=(peer,), name='settlement-ack-peer')
                self.workers.add(worker)
            worker.start()

    def _serve(self, peer):
        upstream = None
        try:
            peer.settimeout(2)
            request = self.frame(peer)
            method = self.test.protocol.Request.FromString(request[4:]).method
            upstream = socket.create_connection(('127.0.0.1', self.target), timeout=2)
            with self.lock:
                self.connections.add(upstream)
            upstream.sendall(request)
            response = self.frame(upstream)
            if not self.release.is_set() and method in (
                    'match.save_result', 'match.get', 'settlement.get'):
                with self.lock:
                    self.held_methods.append(method)
                self.hit.set()
                while not self.release.wait(.02):
                    if self.closing.is_set():
                        return
            peer.sendall(response)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.test.report.setdefault('proxy_errors', []).append(type(error).__name__)
        finally:
            for connection in (peer, upstream):
                if connection is not None:
                    with self.lock:
                        self.connections.discard(connection)
                    connection.close()
            with self.lock:
                self.workers.discard(threading.current_thread())

    def arm(self):
        self.hit.clear()
        self.release.clear()

    def close(self):
        self.closing.set()
        self.release.set()
        self.listener.close()
        self.acceptor.join(2)
        with self.lock:
            connections, workers = tuple(self.connections), tuple(self.workers)
        for peer in connections:
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for worker in workers:
            worker.join(3)
        require(not self.acceptor.is_alive() and not self.workers and not self.connections,
                'fault proxy retained a thread or socket')


class Acceptance(ServiceStack):
    def __init__(self, args):
        super().__init__(args, battle_ticks=80, ready_ticks=20, grace_ms=30000)
        self.counter, self.logs = 0, []
        self.proxy = None
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
        descriptors = descriptor_pb2.FileDescriptorSet.FromString(args.lobby_descriptor.read_bytes())
        pool = descriptor_pool.DescriptorPool()
        for descriptor in descriptors.file:
            pool.AddSerializedFile(descriptor.SerializeToString())
        self.Metadata = message_factory.GetMessageClass(pool.FindMessageTypeByName('arena.lobby_meta.Metadata'))
        self.matches, self.identities = set(), set()
        self.report['scope'] = 'real Gateway/Auth/Lobby/Storage, durable authority and PostgreSQL ACK boundaries'

    @property
    def gateway(self):
        return self.children['gateway'].process

    @property
    def tcp(self):
        return self.ports['gateway']

    @property
    def ws(self):
        return self.ports['gateway-ws']

    @property
    def log_path(self):
        paths = self.children['gateway'].log_paths
        return paths[-1] if paths else self.cluster.root/'gateway-not-started.log'

    def content(self):
        return self.log_path.read_text(errors='replace') if self.log_path.exists() else ''

    def db_connection(self):
        import psycopg
        return psycopg.connect(host=str(self.cluster.socket), port=5432, dbname=self.database,
                               user='arena_test', connect_timeout=1, autocommit=True)

    def command(self, role):
        command, environment = super().command(role)
        if role == 'storage':
            command.append('--publish')
        return command, environment

    def manifest(self, version=1, desired=True):
        value = super().manifest(version, desired)
        if self.proxy is not None:
            for node in value['nodes']:
                if node['service'] == 'storage':
                    node['port'] = self.proxy.port
        return value

    def setup(self):
        self.proxy = StorageAckProxy(self, self.ports['storage'])
        super().setup()
        self.query('UPDATE arena.player SET mmr=CASE player_id WHEN 42 THEN 1440 ELSE 1480 END WHERE player_id IN(42,43)')
        self.initial = self.query('SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id')
        self.sample('real-role-listeners')

    def start_gateway(self, *, ready_ticks=None):
        if self.gateway is None:
            self.children['gateway'].start()

    def close_peers(self):
        for peer in self.peers:
            peer.close()
        self.peers.clear()

    def stop_gateway(self, *, crash=False):
        if self.gateway is None:
            return
        self.children['gateway'].stop(crash=crash)
        text = self.content()
        self.logs.append(text)
        require(not any(marker in text for marker in ('runtime error:', 'SanitizerTool:',
                'ERROR: AddressSanitizer', 'LeakSanitizer', 'WARNING: ThreadSanitizer')),
                'sanitizer diagnostic in Gateway')
        self.close_peers()

    def stop_lobby(self, *, crash=False):
        child=self.children['lobby']
        if child.process is None:
            return None
        child.stop(crash=crash)
        if crash:
            return None
        rows=[]
        for line in child.log_paths[-1].read_text(errors='replace').splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        require(rows and rows[-1].get('state') == 'stopped','Lobby physical stop JSON absent')
        stopped=rows[-1]
        for key in ('committed_revision','queued','matched_pairs','room_actors','room_ticks',
                    'settlements_committed','settlements_blocked'):
            require(type(stopped.get(key)) is int and stopped[key] >= 0,'actual Lobby metric missing: '+key)
        self.report.setdefault('lobby_shutdowns',[]).append(stopped)
        return stopped

    def metadata(self):
        # Inspect complete checksummed records only. Recovery, not this observer,
        # proves that a record survives SIGKILL; visibility alone is not an fsync ACK.
        path = self.cluster.root/'lobby-state'/'authority.wal'
        data = path.read_bytes()
        require(data[:8] == b'ARLOBBY1' and len(data) >= 16, 'authority WAL header absent')
        require(zlib.crc32(data[:12]) == struct.unpack('!I', data[12:16])[0], 'authority header CRC')
        records, offset, revision = {}, 16, 0
        while offset+24 <= len(data):
            header = data[offset:offset+24]
            require(header[:4] == b'KVTX', 'authority frame magic')
            size, sequence, crc, header_crc = struct.unpack('!IQII', header[4:])
            require(zlib.crc32(header[:20]) == header_crc, 'authority frame header CRC')
            if offset+24+size > len(data):
                break
            payload = data[offset+24:offset+24+size]
            require(zlib.crc32(payload) == crc and sequence == revision+1, 'authority frame CRC/revision')
            count, = struct.unpack('!I', payload[:4])
            cursor, seen = 4, set()
            for _ in range(count):
                tag, key_size, value_size = struct.unpack('!BII', payload[cursor:cursor+9])
                cursor += 9
                key = payload[cursor:cursor+key_size].decode()
                cursor += key_size
                value = payload[cursor:cursor+value_size]
                cursor += value_size
                require(tag in (0, 1) and key not in seen and cursor <= len(payload), 'authority mutation bounds')
                seen.add(key)
                if tag:
                    records[key] = value
                else:
                    records.pop(key, None)
            require(cursor == len(payload), 'authority frame trailing data')
            revision = sequence
            offset += 24+size
        require('authority/meta' in records, 'committed metadata missing')
        meta = self.Metadata.FromString(records['authority/meta'])
        return meta, revision

    def find_intent(self, match):
        meta, _ = self.metadata()
        matches = [value for value in meta.settlements if value.match_id == match]
        require(len(matches) <= 1, 'durable settlement intent duplicated')
        return matches[0] if matches else None

    def intent(self, match):
        value = self.find_intent(match)
        require(value is not None, 'durable settlement intent missing')
        return value

    @staticmethod
    def budget(intent):
        return (intent.stable_idempotency_key, intent.max_attempts,
                intent.first_attempt_nanos, intent.deadline_nanos,
                intent.retry_base_ms, intent.retry_max_ms, bytes(intent.request))

    def open_match(self, *, ready=True):
        self.counter += 1
        alice, bob = self.connect(), self.connect(True)
        grant = alice.login()
        self.bob_grant = bob.login(b'bob')
        first = alice.match_join(10000+self.counter*2)
        bob.match_join(10001+self.counter*2)
        ticket, assigned = scalar(first, 2), {}
        def paired():
            assigned.update(alice.match_status(ticket))
            return scalar(assigned, 4) == 3 and scalar(assigned, 13) != 0
        self.until(paired, 'durable-ID room allocation', 8)
        match, room = scalar(assigned, 9), scalar(assigned, 13)
        require(match >= 1007 and room == ((1 << 63) | match), 'invalid durable allocation')
        state = alice.status()
        identity = (room, state['snapshot']['incarnation'])
        require(match not in self.matches and identity not in self.identities, 'durable match/room incarnation reused')
        self.matches.add(match)
        self.identities.add(identity)
        if ready:
            alice.ready(state, 42)
            bob.ready(state, 43)
            playing = {}
            def active():
                playing.update(alice.status())
                return playing['phase'] == 3
            self.until(active, 'persistent battle starts')
            require(alice.control(playing['snapshot'], 42, 1, 1, -1)[1] == 1, 'Alice input rejected')
            require(bob.control(playing['snapshot'], 43, 1, -1, 1)[1] == 1, 'Bob input rejected')
        return alice, bob, grant, match

    def finished(self, peer):
        latest = {}
        def done():
            latest.update(peer.status())
            return latest['phase'] == 5
        self.until(done, 'PostgreSQL acknowledged settlement', 30)
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
        self.until(published, 'runtime outbox consumer and acknowledgement', 10)
        return dict(match_id=match, final_tick=result['tick'], checksum=result['checksum'])

    def complete_and_reconnect(self):
        alice, bob, grant, match = self.open_match()
        alice.close(reset=True)
        alice = self.connect(True)
        resumed = alice.login(token=grant['token'])
        require(resumed['resumed'] and resumed['session'] == grant['session']
                and resumed['room'] == ((1 << 63) | match), 'grace reconnect changed session/room')
        result = self.finished(alice)
        require(alice.result(match) == result and bob.result(match) == result, 'participants see different result')
        details = self.verify(match, result)
        self.stop_gateway()
        self.start_gateway()
        recovered = self.connect()
        restored = recovered.login(token=resumed['token'])
        require(restored['resumed'] and restored['session'] == grant['session'], 'Gateway restart invented fresh session')
        require(recovered.result(match) == result, 'restart lookup changed historical result')
        self.close_peers()
        return dict(**details, rst_resumed=True, historical_query_after_restart=True)

    def pg_outage(self):
        alice, bob, _, match = self.open_match()
        pids = self.sample('before-postgres-outage')
        self.cluster.stop_server()
        try:
            latest = {}
            def settling():
                latest.update(alice.status())
                return latest['phase'] == 4
            self.until(settling, 'unacknowledged result stays Settling', 8)
            require('settlement' not in latest, 'exposed result before commit acknowledgement')
            require(alice.query()['room'] == bob.query()['room'] == ((1 << 63) | match),
                    'released allocation before durable commit')
            self.until(lambda: self.find_intent(match) is not None and self.intent(match).phase == 0,
                       'WAL frozen pending intent before Storage recovery')
            before = self.intent(match)
            require(before.attempts == 0, 'offline Storage consumed an unsent attempt')
        finally:
            self.cluster.restart_server()
        self.until(lambda: health_http(self.ports['gateway-health'], '/health/ready')[0] == 200,
                   'same roles recover PostgreSQL', 8)
        result = self.finished(alice)
        require(self.budget(self.intent(match)) == self.budget(before), 'PG recovery reset durable retry budget')
        details = self.verify(match, result)
        require(alice.query()['room'] == bob.query()['room'] == 0, 'committed match kept live bindings')
        require(self.sample('after-postgres-recovery') == pids, 'PG outage restarted a service')
        self.close_peers()
        return dict(**details, no_early_finish=True, same_four_processes_recovered=True)

    def crash_recovery(self):
        alice, _, grant, match = self.open_match()
        self.proxy.arm()
        frozen = {}
        def held_commit():
            frozen.update(alice.status())
            return frozen['phase'] == 4 and self.proxy.hit.is_set()
        self.until(held_commit, 'real Storage commit with its ACK withheld', 10)
        require('settlement' not in frozen and alice.query()['room'] == ((1 << 63) | match),
                'unknown outcome acknowledged or detached a room early')
        require(self.query('SELECT count(*) FROM arena.settlement WHERE match_id=%s', (match,))[0][0] == 1,
                'fault did not occur after the real PostgreSQL commit')
        original = self.intent(match)
        require(0 < original.attempts < original.max_attempts <= 64, 'invalid durable attempt claim')
        original_budget = self.budget(original)
        attempts = [original.attempts]
        expected = frozen['snapshot']
        for restart in range(2):
            previous = self.intent(match)
            self.stop_lobby(crash=True)
            self.children['lobby'].start()
            alice.close(reset=True)
            alice = self.connect(restart == 0)
            resumed = alice.login(token=grant['token'])
            require(resumed['resumed'] and resumed['session'] == grant['session']
                    and resumed['room'] == expected['room'], 'Lobby recovery lost logical session/room')
            grant = resumed
            state = alice.status()
            require(state['phase'] == 4 and state['snapshot']['incarnation'] == expected['incarnation']
                    and state['snapshot']['checksum'] == expected['checksum']
                    and state['snapshot']['tick'] == expected['tick'],
                    'recovery replaced or advanced a frozen unacknowledged room')
            self.until(lambda: self.intent(match).attempts > previous.attempts,
                       'durable recovery reserves a bounded reconciliation attempt', 8)
            current = self.intent(match)
            require(self.budget(current) == original_budget and
                    previous.attempts < current.attempts <= current.max_attempts,
                    'process restart reset elapsed/attempt/idempotency budget')
            attempts.append(current.attempts)
        self.proxy.release.set()
        result = self.finished(alice)
        require(result['checksum'] == expected['checksum'] and result['tick'] == expected['tick'],
                'unknown-outcome reconciliation changed the committed frozen state')
        details = self.verify(match, result)
        acknowledged = self.intent(match)
        require(acknowledged.phase == 4 and self.budget(acknowledged) == original_budget
                and acknowledged.attempts <= acknowledged.max_attempts, 'ACK escaped its original retry budget')
        require('match.save_result' in self.proxy.held_methods and 'match.get' in self.proxy.held_methods,
                'test did not exercise actual save and restart reconciliation response boundaries')
        self.close_peers()
        return dict(**details, actual_lobby_sigkills=2, session_and_room_preserved=True,
                    uncertain_ack_reconciled=True, durable_attempts=attempts,
                    max_attempts=acknowledged.max_attempts, budget_never_reset=True)

    def ready_abort(self):
        alice, _, _, match = self.open_match(ready=False)
        result = self.finished(alice)
        details = self.verify(match, result, aborted=True)
        require(result['reason'] == 3, 'ready timeout reason changed')
        self.close_peers()
        return dict(**details, never_started=True, awarded=0)

    def export_replays(self):
        self.close_peers()
        self.stop_gateway()
        stopped=self.stop_lobby()
        require(stopped['matched_pairs'] == 4 and stopped['settlements_committed'] == 4
                and stopped['settlements_blocked'] == 0 and stopped['queued'] == 0,
                'committed Lobby metrics disagree with four acknowledged matches')
        directory = self.cluster.root/'exported-replays'
        exported = subprocess.run([str(self.args.replay_export), '--state-dir',
            str(self.cluster.root/'lobby-state'), '--output-dir', str(directory)],
            cwd=self.args.repository_root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, timeout=10)
        require(exported.returncode == 0, 'offline durable replay export failed: '+exported.stdout[-1000:])
        archives = json.loads(exported.stdout)['files']
        expected = {case['match_id']: case for case in self.report['cases']}
        require(len(archives) == len(expected), 'missing/extra complete replay history')
        verified = []
        for index, item in enumerate(archives):
            match = int(item['room_id']) & ((1 << 63)-1)
            require(match in expected, 'archive has an unknown logical match')
            output = directory/('verify-'+str(index)+'.json')
            command = subprocess.run([str(self.args.replay), str(directory/item['file']),
                '--report', str(output)], cwd=self.args.repository_root, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
            require(command.returncode == 0, 'original replay tool rejected a durable archive')
            replay = json.loads(output.read_text())
            require(int(replay['final_tick']) == expected[match]['final_tick'] and
                    int(replay['final_checksum']) == expected[match]['checksum'],
                    'durable restart segments changed real-time checksum')
            verified.append(match)
        self.report['offline_replay_verified_matches'] = verified

    def run(self):
        self.setup()
        for name, action in (
                ('tcp_ws_match_reconnect_and_restart_query', self.complete_and_reconnect),
                ('pg_down_freezes_until_ack_and_recovers', self.pg_outage),
                ('durable_lobby_unknown_ack_sigkill_and_budget_reconciliation', self.crash_recovery),
                ('never_started_abort_without_rewards', self.ready_abort)):
            self.report['current_case'] = name
            started = time.monotonic()
            details = action()
            self.report['cases'].append(dict(name=name, passed=True,
                elapsed=time.monotonic()-started, **details))
        self.export_replays()
        self.children['auth'].stop()
        self.children['storage'].stop()
        final = self.query('SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id')
        require([(int(p),int(q)) for p,q in final] == [(int(p),int(q)+30) for p,q in self.initial],
                'restarts duplicated or lost settlement awards')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'")[0][0] == 0,
                'journey left idle transaction')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='arena-storage'")[0][0] == 0,
                'closed Storage retained physical sessions')
        match_ids = [case['match_id'] for case in self.report['cases']]
        require(len(set(match_ids)) == 4, 'match IDs reused across role restarts')
        ledger = self.query('SELECT player_id,item_id,sum(quantity) FROM arena.asset_ledger '
            'WHERE settlement_id=ANY(%s) GROUP BY player_id,item_id ORDER BY player_id,item_id', (match_ids,))
        projection = self.query('SELECT player_id,item_id,sum(quantity) FROM arena.settlement_reward_projection '
            "WHERE consumer='reward_audit' AND event_id IN "
            '(SELECT event_id FROM arena.outbox WHERE settlement_id=ANY(%s)) '
            'GROUP BY player_id,item_id ORDER BY player_id,item_id', (match_ids,))
        require(projection == ledger, 'runtime consumer projection duplicated or lost committed ledger facts')
        require(not self.report.get('proxy_errors'), 'fault proxy encountered an internal failure')
        self.report.update(outbox_published=4, consumer_projection_matches_ledger=True,
            passed=True, completed_matches=3, aborted_matches=1, repeated_awards=0,
            redis_available=False, final_inventory=[list(map(int,r)) for r in final])
        return self.report

    def close(self):
        # Retire the observer's sockets before the shared fixture measures parent FDs.
        try:
            self.stop_gateway()
            self.stop_lobby()
        finally:
            if self.proxy is not None:
                self.proxy.close()
                self.proxy = None
            super().close()


def add_arguments(parser):
    parser.add_argument('--repository-root', type=Path, default=ROOT)
    for name in ('pg-bin', 'descriptor', 'lobby-descriptor', 'auth', 'lobby', 'gateway',
                 'replay-export', 'replay', 'report'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--pg-share', type=Path)
    parser.add_argument('--pg-lib', type=Path)


def source_hashes(root):
    names = ('tests/storage/PersistentGatewayTest.py', 'tests/lobby/ServiceStackFixture.py',
        'tests/auth/AuthProcessIntegrationTest.py', 'tests/room/JourneyProtocol.py',
        'services/gateway/app/main.cpp', 'services/gateway/src/net/RemoteLobbyRoutes.cpp', 'services/gateway/src/net/LobbyClient.cpp',
        'services/auth/app/main.cpp', 'services/auth/src/AuthService.cpp',
        'services/lobby/app/main.cpp', 'services/lobby/src/LobbyAuthority.cpp',
        'services/lobby/src/LobbyService.cpp', 'services/lobby/src/DurableStateStore.cpp',
        'services/lobby/proto/authority_meta.proto', 'services/storage/server.py',
        'services/storage/database.py', 'shared/runtime/rpc.py')
    return {name: hashlib.sha256((root/name).read_bytes()).hexdigest()
            for name in names if (root/name).exists()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    test = Acceptance(args)
    try:
        test.run()
    except (Exception, KeyboardInterrupt) as error:
        test.report.update(passed=False, error=type(error).__name__+': '+str(error))
        test.report['gateway_log_tail'] = test.content()[-5000:]
    finally:
        try:
            test.close()
        except Exception as error:
            test.report.update(passed=False, cleanup_error=type(error).__name__+': '+str(error))
    test.report['source_hashes'] = source_hashes(args.repository_root)
    output = json.dumps(test.report, indent=2)
    test.assert_no_secrets(output)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(output+'\n')
    print(json.dumps(dict(passed=test.report['passed'], cases=len(test.report['cases']),
        error=test.report.get('error'), report=str(args.report))))
    return 0 if test.report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
