#!/usr/bin/env python3
"""Real Gateway/Auth/Lobby/two-Storage configuration, discovery and physical drain acceptance."""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for directory in (ROOT, ROOT/'tests/storage', ROOT/'tests/room', ROOT/'tests/auth', ROOT/'tests/gateway/net'):
    sys.path.insert(0, str(directory))
from PersistentGatewayTest import Acceptance as Journey, add_arguments, source_hashes
from StorageMigrationTest import require, run
from TcpServerIntegrationTest import reserve_port
from RoomIntegrationTest import unsigned_field
from OnlinePresenceIntegrationTest import scalar


def health_http(port, path='/health/config', timeout=.7):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=timeout)
    try:
        connection.request('GET', path, headers={'Connection': 'close'})
        response = connection.getresponse()
        body = response.read(16385)
        require(len(body) <= 16384, 'unbounded health response')
        return response.status, json.loads(body)
    finally:
        connection.close()


def write_config(path, value):
    temporary = path.with_suffix('.next')
    temporary.write_text(json.dumps(value, separators=(',', ':'))+'\n')
    temporary.replace(path)


def count(body, method, kind='methods'):
    require(isinstance(body.get(kind), dict), 'actual Storage handler counters missing: '+kind)
    return int(body[kind].get(method, 0))


def pending_reload(body):
    metrics = body['reload']
    require('active' in metrics and ('peak' in metrics or 'peak_active' in metrics), 'bounded reload counters missing')
    require(int(metrics.get('peak', metrics.get('peak_active'))) <= 1, 'parallel reload jobs accumulated')
    return int(metrics['active'])


class StorageNode:
    def __init__(self, test, name, port, health_port):
        self.test, self.name, self.port, self.health_port = test, name, port, health_port
        self.config_path = test.cluster.root/(name+'.json')
        self.log_path = test.cluster.root/(name+'.log')
        self.process = self.stream = self.stopped = None

    def start(self):
        write_config(self.config_path, self.test.manifest(1))
        environment = self.test.environment()
        environment.update(ARENA_STORAGE_CALLER_TOKENS=json.dumps(dict(auth=self.test.storage_token,lobby=self.test.other_token)),
            ARENA_RUNTIME_CONFIG=str(self.config_path), ARENA_HEALTH_PORT=str(self.health_port),
            ARENA_NODE_ID=self.name, PYTHONDONTWRITEBYTECODE='1')
        self.stream = self.log_path.open('w')
        self.process = subprocess.Popen([sys.executable, '-m', 'services.storage.server',
            '--descriptor', str(self.test.args.descriptor), '--port', str(self.port), '--workers', '2', '--publish'],
            cwd=self.test.args.repository_root, env=environment, stdin=subprocess.DEVNULL,
            stdout=self.stream, stderr=self.stream)
        self.test.until(lambda: self.health('/health/ready')[0] == 200, self.name+' actual PG readiness', 8)
        body = self.health()[1]
        require(body['node_id'] == self.name and int(body['config_version']) == 1, 'Storage identity missing')
        count(body, 'account.find')

    def health(self, path='/health/config'):
        return health_http(self.health_port, path)

    def stop(self, *, already_signalled=False):
        if self.process is None:
            if self.stream is not None and not self.stream.closed:
                self.stream.close()
            return self.stopped
        process, self.process = self.process, None
        if process.poll() is None and not already_signalled:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=3)
            raise AssertionError(self.name+' exceeded finite shutdown')
        finally:
            self.stream.close()
        text = self.log_path.read_text(errors='replace')
        require(process.returncode == 0, self.name+' abnormal shutdown: '+text[-4000:])
        rows = []
        for line in text.splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        require(rows and rows[-1].get('state') == 'stopped', self.name+' stop counters missing')
        self.stopped = rows[-1]
        require(self.stopped['worker']['active'] == self.stopped['pool']['checked_out'] == 0,
                self.name+' retained SQL workers/loans')
        require(self.stopped['pool']['closed'] and self.stopped['publisher']['closed']
                and not self.stopped['publisher']['active'], self.name+' retained pool/publisher')
        return self.stopped


class Acceptance(Journey):

    def __init__(self, args):
        super().__init__(args)
        self.used_ports = set(self.ports.values())
        self.nodes = [StorageNode(self,'node-a',self.ports['storage'],self.ports['storage-health']),
                      StorageNode(self,'node-b',self.port(),self.port())]
        self.health_port = self.ports['gateway-health']
        self.gateway_config = self.configs['gateway']
        self.gateway_version, self.current = 1, None
        self.role_versions = dict(auth=1,lobby=1)
        self.ready_ticks = 100
        self.report.update(clock_fixture=dict(common_origin=bool(os.environ.get('ARENA_TEST_CLOCK_REALTIME_NS'))),
                           parent_fd_baseline=len(os.listdir('/proc/self/fd')))

    def port(self):
        while True:
            value = reserve_port()
            if value not in self.used_ports:
                self.used_ports.add(value)
                return value


    def manifest(self, version=1, selected=None, *, desired=True, grace=5000):
        return dict(version=str(version), desiredReady=desired, drainGraceMs=grace,
            nodes=[dict(service='storage',nodeId=node.name,host='127.0.0.1',port=node.port,
                        ready=selected is None or index == selected) for index,node in enumerate(self.nodes)]
                + [dict(service=role,nodeId=role+'-smoke',host='127.0.0.1',
                        port=self.ports[role],ready=True) for role in ('auth','lobby','gateway')])

    def until(self, action, description, timeout=5, *, processes=True):
        deadline, last = time.monotonic()+timeout, None
        while time.monotonic() < deadline:
            if processes:
                children = [node.process for node in self.nodes if node.process is not None]
                children.extend(child.process for child in self.children.values() if child.process is not None)
                require(all(child.poll() is None for child in children), 'child exited during '+description)
            try:
                result = action()
                if result:
                    return result
            except (OSError,http.client.HTTPException) as error:
                last = type(error).__name__
            time.sleep(.005)
        raise AssertionError('timed out: '+description+(' ('+last+')' if last else ''))


    def setup(self):
        self.cluster.start()
        self.cluster.create(self.database)
        migrated = run([sys.executable,self.args.repository_root/'services/storage/migrate.py','up'],
                       self.cluster.environment(self.database),timeout=95)
        require(migrated.returncode == 0, 'private migration failed: '+migrated.stderr)
        self.cluster.sql((self.args.repository_root/'services/storage/fixtures/seed.sql').read_text(),
                         self.database,single=True)
        salt = secrets.token_bytes(16)
        key = hashlib.pbkdf2_hmac('sha256', b'test-password', salt,100000,32)
        digest = '$arena-pbkdf2-sha256$100000$'+salt.hex()+'$'+key.hex()
        self.query("UPDATE arena.account SET password_digest=%s WHERE account_name IN ('alice','bob')",(digest,))
        self.query('UPDATE arena.player SET mmr=CASE player_id WHEN 42 THEN 1440 ELSE 1480 END WHERE player_id IN(42,43)')
        self.initial = self.query('SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id')
        self.current = self.manifest(1,0)
        for path in self.configs.values():
            write_config(path,self.current)
        for node in self.nodes:
            node.start()
        self.children['auth'].start()
        self.children['lobby'].start()
        self.start_gateway()
        pids=[self.gateway.pid,self.children['auth'].process.pid,self.children['lobby'].process.pid,
              *(node.process.pid for node in self.nodes)]
        require(len(set(pids)) == 5,'discovery fixture did not use five independent service processes')
        self.report['service_pids']=pids


    def start_gateway(self, *, ready_ticks=100):
        super().start_gateway()
        self.until(lambda:self.health('/health/ready')[0] == 200,'Gateway real Lobby dependency readiness',8)

    def health(self,path='/health/config'):
        return health_http(self.health_port,path)


    def publish(self,value,timeout=3,*,wait_ready=True):
        started = time.monotonic()
        def storage_routes(config):
            return [(node['nodeId'],node['port'],node['ready']) for node in config['nodes']
                    if node['service'] == 'storage']
        # Auth owns account discovery; Lobby owns durable IDs/results. Gateway
        # publishes only its own immutable snapshot and discovers Lobby.
        if self.current is not None and storage_routes(value) != storage_routes(self.current):
            for role in ('auth','lobby'):
                version=self.role_versions[role]+1
                selected=copy.deepcopy(value)
                selected.update(version=str(version),desiredReady=True)
                for node in selected['nodes']:
                    if node['service'] in ('auth','lobby','gateway'):
                        node['ready']=True
                write_config(self.configs[role],selected)
                self.children[role].process.send_signal(signal.SIGHUP)
                self.until(lambda role=role,version=version:
                    int(health_http(self.ports[role+'-health'])[1]['config_version']) == version,
                    role+' selected-Storage publication',timeout)
                self.role_versions[role]=version
        write_config(self.gateway_config,value)
        self.gateway.send_signal(signal.SIGHUP)
        self.until(lambda:int(self.health()[1]['config_version']) == int(value['version']),
                   'Gateway publication',timeout)
        self.current,self.gateway_version = copy.deepcopy(value),int(value['version'])
        available=all(any(node['service'] == role and node['ready'] for node in value['nodes'])
                      for role in ('lobby','storage'))
        if wait_ready and value['desiredReady'] and available:
            self.until(lambda:self.health('/health/ready')[0] == 200,'published dependency readiness',timeout)
        return time.monotonic()-started

    def next_manifest(self,selected=0,*,desired=True):
        return self.manifest(self.gateway_version+1,selected,desired=desired)

    def close_peers(self):
        for peer in self.peers:
            try:
                peer.close()
            except OSError:
                pass
        self.peers.clear()

    def play(self,node):
        self.counter += 1
        before = [other.health()[1] for other in self.nodes]
        alice,bob,_,match = self.open_match()
        result = self.finished(alice)
        self.verify(match,result)
        require(alice.query()['room'] == bob.query()['room'] == 0,'finished players remained bound')
        index = self.nodes.index(node)
        self.until(lambda:count(node.health()[1],'match.save_result','completions') >
                   count(before[index],'match.save_result','completions'),'actual result commit on selected Storage')
        after = [other.health()[1] for other in self.nodes]
        require(count(after[index],'account.find') >= count(before[index],'account.find')+2,
                'selected node did not perform actual Auth-to-Storage authentication')
        require(count(after[1-index],'account.find') == count(before[1-index],'account.find')
                and count(after[1-index],'match.save_result') == count(before[1-index],'match.save_result'),
                'Auth/Lobby used an unselected Storage for new business')
        self.close_peers()
        return dict(match_id=match,actual_selected_node=node.name,final_checksum=result['checksum'],
                    methods_before=[entry['methods'] for entry in before],methods_after=[entry['methods'] for entry in after])

    def startup_and_route_a(self):
        require(int(self.health()[1]['config_version']) == 1,'Gateway initial version missing')
        result = self.play(self.nodes[0])
        require(count(self.nodes[0].health()[1],'match.allocate_ids') > 0,'prime/prefetch did not use real node A')
        return result

    def lock_accounts(self):
        connection = self.db_connection()
        connection.autocommit = False
        connection.execute("SET LOCAL lock_timeout='500ms'")
        connection.execute('LOCK TABLE arena.account IN ACCESS EXCLUSIVE MODE')
        return connection

    def wait_account_barrier(self,node,before):
        self.until(lambda:count(node.health()[1],'account.find') > before and
            self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                       "AND state='active' AND wait_event_type='Lock' "
                       "AND query LIKE '%FROM arena.account%account_name%'")[0][0] > 0,
            'actual account lookup inside PostgreSQL',1)

    def route_switch_and_old_inflight(self):
        a,b = self.nodes
        before = [node.health()[1] for node in self.nodes]
        peer = self.connect()
        lock = self.lock_accounts()
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                pending = workers.submit(peer.login)
                try:
                    self.wait_account_barrier(a,count(before[0],'account.find'))
                    write_config(a.config_path,self.manifest(2,desired=False))
                    a.process.send_signal(signal.SIGHUP)
                    self.until(lambda:a.health('/health/ready')[0] == 503,'old node not-ready before route change',1)
                    require(a.health('/health/alive')[0] == 200,'quiescing old node lost liveness')
                    switch = self.publish(self.next_manifest(1),wait_ready=False)
                    lock.rollback()
                    require(pending.result(timeout=3)['player'] == 42,'old admitted route lost its result')
                finally:
                    lock.rollback()
            after = [node.health()[1] for node in self.nodes]
            require(count(after[0],'account.find','completions') == count(before[0],'account.find','completions')+1,
                    'old admitted call did not complete on original Storage')
            require(count(after[1],'account.find') == count(before[1],'account.find'),'old call replayed on replacement')
        finally:
            lock.close(); self.close_peers()
        write_config(a.config_path,self.manifest(3))
        a.process.send_signal(signal.SIGHUP)
        self.until(lambda:a.health('/health/ready')[0] == 200,'old node restored for later explicit rollback')
        self.until(lambda:self.health('/health/ready')[0] == 200,'replacement route ready')
        fresh = self.connect()
        require(fresh.login(b'bob')['player'] == 43,'new business did not reach replacement route')
        self.close_peers()
        result = self.play(b)
        burned=self.exhaust_reserved_ids(b,count(before[1],'match.allocate_ids'))
        result['cancelled_tickets_to_next_real_block']=burned
        result.update(old_admitted_node='node-a',old_call_replayed=False,switch_ms=switch*1000)
        return result

    def exhaust_reserved_ids(self,node,before):
        peer=self.connect()
        peer.login()
        issued=set()
        initial,_=self.metadata()
        wait_started=None
        def replenished():
            current,_=self.metadata()
            return (count(node.health()[1],'match.allocate_ids') > before
                and current.ticket_last > initial.ticket_last
                and initial.ticket_last < current.ticket_next <= current.ticket_last
                and health_http(self.ports['lobby-health'],'/health/ready')[0] == 200
                and self.health('/health/ready')[0] == 200)
        for index in range(257):
            current,_=self.metadata()
            if current.ticket_next > current.ticket_last:
                # The final cancelled ticket consumed the committed range.
                # The production owner replenishes it asynchronously; do not
                # request another logical join before the new WAL ACK.
                wait_started=time.monotonic()
                self.until(replenished,'actual B allocation, durable new range and dependency readiness',8)
                break
            if count(node.health()[1],'match.allocate_ids') > before:
                wait_started=time.monotonic()
                self.until(replenished,'new ID block is durably published after Storage execution',8)
                break
            ticket=scalar(peer.match_join(900000+index),2)
            require(ticket >= initial.ticket_next and ticket not in issued,'durable ticket ID reused')
            issued.add(ticket)
            peer.rpc(42,unsigned_field(1,ticket),43)
        self.until(replenished,'next real Lobby ID block reserved on selected node B',8)
        current,_=self.metadata()
        require(current.ticket_last > initial.ticket_last and current.ticket_next > initial.ticket_last,
                'allocator reservation reused the old durable range')
        self.report['id_replenishment']=dict(cancelled_tickets=len(issued),
            previous_first=initial.ticket_next,previous_last=initial.ticket_last,
            next_first=current.ticket_next,next_last=current.ticket_last,
            selected_storage=node.name,actual_allocate_observed=True,
            wait_ms=0 if wait_started is None else (time.monotonic()-wait_started)*1000)
        self.close_peers()
        return len(issued)

    def assert_new_connection_rejected(self):
        started = time.monotonic()
        with socket.create_connection(('127.0.0.1',self.tcp),timeout=.7) as peer:
            peer.settimeout(.7)
            try:
                data = peer.recv(1)
            except ConnectionResetError:
                data = b''
            require(data == b'','not-ready Gateway accepted a new connection')
        elapsed = time.monotonic()-started
        require(elapsed <= 1,'admission cutoff exceeded1s')
        return elapsed

    def invalid_reload_and_rollback(self):
        old = self.gateway_version
        valid = self.next_manifest(1)
        values = []
        missing = copy.deepcopy(valid); missing.pop('desiredReady')
        values.append(('missing',json.dumps(missing)))
        port = copy.deepcopy(valid); port['nodes'][0]['port'] = 65536
        values.append(('port_range',json.dumps(port)))
        grace = copy.deepcopy(valid); grace['drainGraceMs'] = 10001
        values.append(('drain_range',json.dumps(grace)))
        unknown = copy.deepcopy(valid); unknown['unexpected'] = 1
        values.append(('unknown',json.dumps(unknown)))
        text = json.dumps(valid,separators=(',',':'))
        values.extend([('partial',text[:len(text)//2]),('duplicate_version','{"version":"999",'+text[1:]),
            ('duplicate_alias',text.replace('"nodeId":"node-a"','"node_id":"other","nodeId":"node-a"')),
            ('deep','['*32+'0'+']'*32),('oversize',' '*65537)])
        for name,raw in values:
            before = int(self.health()[1]['reload']['rejected'])
            self.gateway_config.write_text(raw)
            self.gateway.send_signal(signal.SIGHUP)
            self.until(lambda:int(self.health()[1]['reload']['rejected']) > before,'invalid '+name+' rejection')
            body = self.health()[1]
            require(int(body['config_version']) == old,'invalid config replaced old snapshot: '+name)
            require(self.health('/health/ready')[0] == 200,'invalid config broke old ready route: '+name)
            pending_reload(body)
        write_config(self.gateway_config,self.current)
        peer = self.connect(); require(peer.login()['player'] == 42,'old backend stopped serving')
        self.close_peers()
        before_a = count(self.nodes[0].health()[1],'account.find')
        rollback = self.publish(self.next_manifest(0))
        require(rollback <= 30,'content rollback exceeded30s')
        peer = self.connect(); peer.login(); self.close_peers()
        require(count(self.nodes[0].health()[1],'account.find') == before_a+1,'rollback did not restore actual node A')
        stale = copy.deepcopy(self.current); stale['version'] = str(old)
        before = int(self.health()[1]['reload']['rejected'])
        write_config(self.gateway_config,stale); self.gateway.send_signal(signal.SIGHUP)
        self.until(lambda:int(self.health()[1]['reload']['rejected']) > before,'stale rejection')
        require(int(self.health()[1]['config_version']) == self.gateway_version,'lower version published')
        write_config(self.gateway_config,self.current)
        return dict(invalid_cases=[name for name,_ in values],invalid_replaced_old=0,
                    rollback_ms=rollback*1000,actual_rollback_node='node-a',stale_rejected=True)

    def no_ready_route_and_reversible_quiesce(self):
        before = [node.health()[1] for node in self.nodes]
        unavailable = self.next_manifest(0)
        for node in unavailable['nodes']:
            node['ready'] = False
        started = time.monotonic()
        self.publish(unavailable,wait_ready=False)
        self.until(lambda:self.health('/health/ready')[0] == 503,'all nodes offline readiness',1)
        probe_count = 8
        for _ in range(probe_count):
            self.assert_new_connection_rejected()
        cutoff = time.monotonic()-started
        require(cutoff <= 1,'publication-to-admission cutoff exceeded1s')
        require(self.health('/health/alive')[0] == 200,'missing route killed Gateway liveness')
        from shared.runtime.rpc import UNAVAILABLE
        self.expect(self.request(900000),{UNAVAILABLE})
        after = [node.health()[1] for node in self.nodes]
        require(all(count(a,'account.find') == count(b,'account.find') for a,b in zip(after,before)),
                'empty discovery fell back to legacy endpoint')
        self.publish(self.next_manifest(0))
        peer = self.connect(); grant = peer.login()
        no_lobby=self.next_manifest(0)
        for node in no_lobby['nodes']:
            if node['service'] == 'lobby':
                node['ready']=False
        self.publish(no_lobby,wait_ready=False)
        self.until(lambda:self.health('/health/ready')[0] == 503,'Gateway has no selected Lobby',1)
        peer.rpc(20,b'',21,error=2002)
        self.assert_new_connection_rejected()
        self.publish(self.next_manifest(0))
        require(peer.query()['session'] == grant['session'],'missing Lobby route destroyed retained session')
        self.publish(self.next_manifest(0,desired=False),wait_ready=False)
        self.until(lambda:self.health('/health/ready')[0] == 503,'manual not-ready state')
        peer.rpc(20,b'',21,error=2002)
        self.assert_new_connection_rejected()
        require(self.health('/health/alive')[0] == 200,'quiesce killed control liveness')
        self.publish(self.next_manifest(0,desired=True))
        require(peer.query()['session'] == grant['session'],'quiesce destroyed retained session')
        own = self.next_manifest(0)
        for node in own['nodes']:
            if node['service'] == 'gateway':
                node['ready'] = False
        self.publish(own,wait_ready=False)
        self.until(lambda:self.health('/health/ready')[0] == 503,'own Gateway node not-ready')
        require(self.health()[1]['desired_ready'],'own-node rejection incorrectly changed operator intent')
        peer.rpc(20,b'',21,error=2002)
        self.assert_new_connection_rejected()
        self.publish(self.next_manifest(0))
        require(peer.query()['session'] == grant['session'],'own-node quiesce destroyed retained session')
        self.close_peers()
        node = self.nodes[1]
        local = self.manifest(2,desired=False)
        write_config(node.config_path,local);node.process.send_signal(signal.SIGHUP)
        self.until(lambda:node.health('/health/ready')[0] == 503,'Storage manual quiesce')
        require(node.health('/health/alive')[0] == 200,'Storage quiesce killed liveness')
        from shared.runtime.rpc import RpcError,UNAVAILABLE
        try:
            self.direct_find(node)
        except RpcError as error:
            require(error.status == UNAVAILABLE,'Storage quiesce returned wrong public error')
        else:
            raise AssertionError('quiesced Storage admitted new business')
        restored = self.manifest(3)
        write_config(node.config_path,restored);node.process.send_signal(signal.SIGHUP)
        self.until(lambda:node.health('/health/ready')[0] == 200,'Storage unquiesce')
        require(self.direct_find(node).player.player_id == 42,'restored Storage stopped serving')
        return dict(no_implicit_endpoint_fallback=True,gateway_lobby_fallback_rejected=True,auth_storage_fallback_rejected=True,admission_cutoff_ms=cutoff*1000,
                    publication_to_cutoff_ms=cutoff*1000,probe_count=probe_count,
                    quiesce_reversible=True,session_preserved=True,gateway_own_node_quiesce=True)

    def startup_port_collisions(self):
        cases = []
        for kind,business in (('health','tcp'),('health','ws'),('external','tcp'),('external','ws')):
            tcp,ws = self.port(),self.port()
            held = None
            try:
                if kind == 'external':
                    held = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    occupied = self.port()
                    held.bind(('127.0.0.1',occupied)); held.listen(1)
                    if business == 'tcp':
                        tcp = occupied
                    else:
                        ws = occupied
                    health = self.port()
                else:
                    health = tcp if business == 'tcp' else ws
                label = kind+'-'+business
                environment = self.environment()
                environment.update(ARENA_TCP_PORT=str(tcp),ARENA_WS_PORT=str(ws),
                    ARENA_HEALTH_PORT=str(health),
                    ARENA_RUNTIME_CONFIG=str(self.gateway_config),ARENA_NODE_ID='gateway-collision',
                    ARENA_LOBBY_PORT=str(self.ports['lobby']),ARENA_LOBBY_TOKEN=self.lobby_token)
                result = subprocess.run([str(self.args.gateway),'asio','handshake'],
                    cwd=self.args.repository_root,env=environment,stdin=subprocess.DEVNULL,
                    text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=8)
                require(result.returncode == 2,label+' collision did not reject cleanly: '
                        +str(result.returncode)+' '+result.stdout[-1500:])
                cases.append(dict(collision=kind,business=business,exit_code=result.returncode,
                                  external_listener_held=held is not None))
            finally:
                if held is not None:
                    held.close()
        require(self.health('/health/ready')[0] == 200,'collision child affected running Gateway')
        return dict(collisions=cases,no_sigabrt=True)

    def thousand_reloads_with_concurrent_reads(self):
        base = self.gateway_version+1
        self.publish(self.manifest(base,0,desired=bool(base%2)),wait_ready=False)
        errors = []
        limit = time.monotonic()+90
        def reads():
            for _ in range(1250):
                require(time.monotonic()<limit,'concurrent config reads exceeded normal test guard')
                code,value = self.health()
                if code != 200 or value['desired_ready'] != bool(int(value['config_version'])%2):
                    errors.append('mixed version/readiness')
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as workers:
            futures = [workers.submit(reads) for _ in range(8)]
            for version in range(base+1,base+1001):
                require(time.monotonic()<limit,'reload sequence exceeded normal test guard')
                self.publish(self.manifest(version,0,desired=bool(version%2)),wait_ready=False)
            for future in futures:
                future.result(timeout=10)
        elapsed = time.monotonic()-started
        require(not errors,'health reads observed partial configuration fields')
        self.until(lambda:not pending_reload(self.health()[1]),'reload completion')
        metrics = self.health()[1]['reload']
        self.publish(self.next_manifest(0,desired=True))
        return dict(actual_sighup_publications=1000,concurrent_health_reads=10000,
                    inconsistent_snapshots=0,elapsed_seconds=elapsed,reload=metrics)

    def postgres_outage_health_and_recovery(self):
        self.counter += 1
        alice,bob,_,match = self.open_match()
        initial_room = alice.status()
        pids = [node.process.pid for node in self.nodes]
        result = run([self.cluster.tool('pg_ctl'),'-D',self.cluster.data,'-m','fast','-w','-t','15','stop'],
                     self.cluster.base,timeout=20)
        require(result.returncode == 0,'private PostgreSQL stop failed')
        try:
            for node in self.nodes:
                self.until(lambda node=node:node.health('/health/ready')[0] == 503,node.name+' PG-down readiness')
                require(node.health('/health/alive')[0] == 200,'PG outage killed Storage liveness')
            self.until(lambda:self.health('/health/ready')[0] == 503,'Gateway PG-down readiness')
            require(self.health('/health/alive')[0] == 200,'PG outage killed Gateway liveness')
            self.assert_new_connection_rejected()
            live_room = alice.status()
            require(live_room['phase'] == 3 and live_room['snapshot']['room'] == initial_room['snapshot']['room']
                    and live_room['snapshot']['tick'] > initial_room['snapshot']['tick'],
                    'PG outage stopped or detached an already playing Room')
            _,input_status = alice.control(live_room['snapshot'],42,1,1,-1)
            require(input_status == 2,'PG outage blocked local Room input processing')
            require(alice.query()['room'] == live_room['snapshot']['room'],'PG outage lost Room binding')
        finally:
            options = (f"-c listen_addresses='' -c unix_socket_directories='{self.cluster.socket}' "
                '-c unix_socket_permissions=0700 -c port=5432 -c max_connections=16 '
                '-c log_min_messages=warning -c timezone=UTC')
            result = run([self.cluster.tool('pg_ctl'),'-D',self.cluster.data,'-l',self.cluster.log,
                          '-w','-t','15','-o',options,'start'],self.cluster.base,timeout=20)
            require(result.returncode == 0,'private PostgreSQL restart failed')
        for node in self.nodes:
            self.until(lambda node=node:node.health('/health/ready')[0] == 200,node.name+' PG recovery',8)
        self.until(lambda:self.health('/health/ready')[0] == 200,'Gateway same-process recovery',8)
        require([node.process.pid for node in self.nodes] == pids,'outage recovery restarted Storage')
        result = self.finished(alice)
        self.verify(match,result)
        self.close_peers()
        peer = self.connect(); peer.login(); self.close_peers()
        return dict(storage_pids=pids,same_processes_recovered=True,alive_independent_of_pg=True,
                    live_room_preserved=True,local_input_processed_while_pg_down=True,match_id=match,
                    ticks_during_outage=live_room['snapshot']['tick']-initial_room['snapshot']['tick'])

    def bounded_health_peers(self):
        peers = []
        try:
            for _ in range(16):
                peer = socket.create_connection(('127.0.0.1',self.health_port),timeout=.5)
                peer.settimeout(.7); peer.sendall(b'GET /health/alive HTTP/1.1\r\nHost:')
                peers.append(peer)
            started = time.monotonic()
            for peer in peers:
                try:
                    while peer.recv(4096):
                        pass
                except ConnectionResetError:
                    pass
            require(time.monotonic()-started < 1,'slow headers retained control peers')
        finally:
            for peer in peers:
                peer.close()
        self.until(lambda:self.health('/health/alive')[0] == 200,'health slot recovery')
        return dict(slow_headers=16,reclaimed_within_seconds=1)

    def direct_find(self,node):
        async def lookup():
            from shared.runtime.rpc import Client
            client = Client(self.protocol,'127.0.0.1',node.port,service_token=self.storage_token,caller_service='auth',target_service='storage')
            payload = self.protocol.message('arena.storage.FindAccountRequest')(account_name='alice').SerializeToString()
            response = await client.call('account.find',payload,timeout=2,correlation_id='drain-owned-query')
            require(client.inflight == 0,'direct RPC retained admission')
            return self.protocol.message('arena.storage.FindAccountReply').FromString(response)
        return asyncio.run(lookup())

    def terminal_storage_drain(self):
        node = self.nodes[1]
        before = count(node.health()[1],'account.find')
        lock = self.lock_accounts()
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                pending = workers.submit(self.direct_find,node)
                try:
                    self.wait_account_barrier(node,before)
                    started = time.monotonic()
                    node.process.send_signal(signal.SIGTERM)
                    self.until(lambda:node.health('/health/ready')[0] == 503 and node.health()[1]['draining'],
                               'Storage not-ready before drain',1)
                    require(node.health('/health/alive')[0] == 200,'Storage control died before SQL drained')
                    lock.rollback()
                    require(pending.result(timeout=3).player.player_id == 42,'accepted Storage request abandoned')
                    stopped = node.stop(already_signalled=True)
                    elapsed = time.monotonic()-started
                finally:
                    lock.rollback()
        finally:
            lock.close()
        require(elapsed <= 10,'Storage drain exceeded10s')
        return dict(not_ready_before_close=True,alive_during_drain=True,elapsed_seconds=elapsed,
                    checked_out=stopped['pool']['checked_out'],workers=stopped['worker']['active'])

    def terminal_gateway_drain(self):
        self.until(lambda:self.health('/health/ready')[0] == 200,'Gateway ready before drain setup')
        node = self.nodes[0]
        before = count(node.health()[1],'account.find')
        peer = self.connect()
        lock = self.lock_accounts()
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                pending = workers.submit(peer.login)
                try:
                    self.wait_account_barrier(node,before)
                    started = time.monotonic()
                    self.gateway.send_signal(signal.SIGTERM)
                    self.until(lambda:self.health('/health/ready')[0] == 503 and self.health()[1]['draining'],
                               'Gateway not-ready before drain',1)
                    require(self.health('/health/alive')[0] == 200,'Gateway control died before business drain')
                    lock.rollback()
                    require(pending.result(timeout=3)['player'] == 42,'admitted Gateway/Lobby/Auth authentication abandoned')
                    self.stop_gateway()
                    elapsed = time.monotonic()-started
                finally:
                    lock.rollback()
        finally:
            lock.close()
        require(elapsed <= 10,'Gateway drain exceeded10s')
        return dict(not_ready_before_close=True,alive_during_drain=True,elapsed_seconds=elapsed)

    def run(self):
        self.setup()
        for name,action in (
            ('real_gateway_account_ids_match_result_on_node_a',self.startup_and_route_a),
            ('switch_to_b_preserves_old_inflight_and_routes_new_business',self.route_switch_and_old_inflight),
            ('invalid_files_keep_old_route_and_higher_version_rolls_back',self.invalid_reload_and_rollback),
            ('no_ready_nodes_no_fallback_and_reversible_quiesce',self.no_ready_route_and_reversible_quiesce),
            ('startup_health_business_port_collisions_fail_cleanly',self.startup_port_collisions),
            ('1000_real_reloads_and_10000_concurrent_health_reads',self.thousand_reloads_with_concurrent_reads),
            ('postgres_outage_alive_ready_and_same_process_recovery',self.postgres_outage_health_and_recovery),
            ('finite_slow_health_headers_reclaim_slots',self.bounded_health_peers),
            ('storage_not_ready_precedes_real_inflight_drain',self.terminal_storage_drain),
            ('gateway_not_ready_precedes_real_inflight_drain',self.terminal_gateway_drain)):
            self.report['current_case'] = name
            started = time.monotonic()
            details = action()
            self.report['cases'].append(dict(name=name,passed=True,case_elapsed_seconds=time.monotonic()-started,**details))
            if self.args.through == name:
                self.report.update(passed=True,partial_debug_scope=True)
                return self.report
        for node in self.nodes:
            node.stop()
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND state LIKE 'idle in transaction%'")[0][0] == 0,'idle transaction leaked')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND application_name='arena-storage'")[0][0] == 0,'closed Storage retained sessions')
        final = self.query('SELECT player_id,quantity FROM arena.inventory WHERE item_id=1 AND player_id IN(42,43) ORDER BY player_id')
        require([(int(p),int(q)) for p,q in final] == [(int(p),int(q)+30) for p,q in self.initial],
                'node switching duplicated/lost settlement awards')
        self.report.update(passed=True,storage_shutdowns=[node.stopped for node in self.nodes],
                           matched_rounds=3,repeated_awards=0,idle_transactions=0)
        return self.report

    def close(self):
        errors = []
        for role in ('gateway','lobby','auth'):
            try:
                if role == 'gateway':
                    self.stop_gateway()
                elif role == 'lobby':
                    self.stop_lobby()
                else:
                    self.children[role].stop()
            except Exception as error:
                errors.append(role+': '+str(error))
        self.close_peers()
        for node in self.nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append(node.name+': '+str(error))
        try:
            destination=self.args.report.parent/(self.args.report.stem+'-logs')
            destination.mkdir(parents=True,exist_ok=True)
            artifacts=[]
            for node in self.nodes:
                if node.log_path.exists():
                    output=destination/(node.name+'.log')
                    output.write_text(node.log_path.read_text(errors='replace'))
                    artifacts.append(str(output))
            if self.cluster.log.exists():
                output=destination/'postgres.log'
                output.write_text(self.cluster.log.read_text(errors='replace'))
                artifacts.append(str(output))
            self.report['artifacts']=artifacts
        except Exception as error:
            errors.append('artifact capture: '+str(error))
        try:
            super().close()
        except Exception as error:
            errors.append('shared process cleanup: '+str(error))
        require(not errors,'; '.join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    parser.add_argument('--through',help='Debug only: stop after a named case and mark partial scope')
    args = parser.parse_args()
    for name,value in vars(args).items():
        if isinstance(value,Path):
            setattr(args,name,value.resolve())
    test = Acceptance(args)
    report = test.report
    try:
        report = test.run()
    except BaseException as error:
        report.update(passed=False,error=f'{type(error).__name__}: {error}')
        if test.gateway is not None:
            report['gateway_log_tail'] = test.content()[-7000:]
        report['storage_log_tails'] = {node.name:node.log_path.read_text(errors='replace')[-5000:]
                                     for node in test.nodes if node.log_path.exists()}
        if test.cluster.log.exists():
            report['pg_log_tail'] = test.cluster.log.read_text(errors='replace')[-7000:]
    finally:
        try:
            test.close()
            report['cluster_cleaned'] = not test.cluster.root.exists()
        except Exception as error:
            report.update(passed=False,cleanup_error=str(error))
        report['parent_fd_final'] = len(os.listdir('/proc/self/fd'))
        if report['parent_fd_final'] > report['parent_fd_baseline']:
            report.update(passed=False,parent_fd_leak=True)
        names = ['shared/proto/runtime_config.proto','shared/runtime/config.py','shared/runtime/health.py',
            'shared/src/runtime/RuntimeControl.cpp','shared/src/runtime/RuntimeConfig.cpp','shared/src/rpc/Client.cpp',
            'tests/runtime/ConfigDiscoveryIntegrationTest.py']
        report['source_hashes'] = source_hashes(args.repository_root)
        report['source_hashes'].update({name:hashlib.sha256((args.repository_root/name).read_bytes()).hexdigest()
                                       for name in names if (args.repository_root/name).exists()})
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ('passed','cases','error') if key in report},indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
