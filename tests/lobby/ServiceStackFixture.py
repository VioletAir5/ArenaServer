#!/usr/bin/env python3
"""Shared private Gateway/Auth/Lobby/Storage and two Room process lifecycle fixture."""
import hashlib
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
for folder in (ROOT, ROOT/'tests/auth', ROOT/'tests/storage', ROOT/'tests/room',
               ROOT/'tests/match', ROOT/'tests/gateway/net'):
    sys.path.insert(0, str(folder))
from AuthProcessIntegrationTest import Acceptance as AuthFixture, Child, health_http
from StorageMigrationTest import require
from JourneyProtocol import Bot


class ServiceChild(Child):
    def start(self):
        require(self.process is None, 'restart without joining previous process')
        self.serial += 1
        path = self.test.cluster.root/f'{self.role}-{self.serial}.log'
        self.log_paths.append(path)
        self.stream = path.open('w')
        command, environment = self.test.command(self.role)
        self.process = subprocess.Popen(command, cwd=self.test.args.repository_root,
            env=environment, stdin=subprocess.DEVNULL, stdout=self.stream, stderr=self.stream)
        self.history.append(self.process.pid)
        marker = 'GATEWAY_READY' if self.role == 'gateway' else '"state":"listening"'
        self.test.until(lambda:marker in path.read_text(errors='replace'), self.role+' listening', 10)
        def ready():
            status, health = health_http(self.test.ports[self.role+'-health'], '/health/ready')
            service = 'room' if self.role in self.test.room_roles else self.role
            node_id = self.role if service == 'room' else self.role+'-smoke'
            return status == 200 and health.get('service') == service and health.get('node_id') == node_id
        self.test.until(ready, self.role+' real dependency readiness', 10)

    def stop(self, crash=False):
        if self.process is None:
            if self.stream is not None and not self.stream.closed:
                self.stream.close()
            return
        process, self.process = self.process, None
        unexpected = process.poll() is not None
        if not unexpected:
            process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=3)
            self.test.report['forced_kills'] += 1
            raise AssertionError(self.role+' did not finish finite shutdown')
        finally:
            self.stream.close()
        require(not unexpected and process.returncode == (-signal.SIGKILL if crash else 0),
                self.role+' abnormal exit; see retained log')
        text = self.log_paths[-1].read_text(errors='replace')
        if not crash and self.role == 'gateway':
            require('LOBBY_COUNTS tracked=0 business_inflight=0 heartbeat_inflight=0 probe_inflight=0 owner_posts=0 stopped=1' in text,
                    'Gateway remote physical work did not drain')
            require('connection_admission active=0 ' in text, 'Gateway connections did not drain')
        elif not crash:
            rows=[]
            for line in text.splitlines():
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
            require(rows and rows[-1].get('state') == 'stopped', self.role+' stop report missing')
            if self.role in self.test.room_roles:
                require(rows[-1].get('service') == 'room' and all(rows[-1].get(key) == 0
                        for key in ('active','journal_pending','rpc_active','rpc_peers')) and
                        rows[-1].get('journal_closed') is True,
                        'Room retained physical jobs, RPC peers or journal resources')
                # Retained route/stage counts are durable values, not live RPC loans.
                self.test.report.setdefault('room_shutdowns', []).append(dict(role=self.role, **rows[-1]))
            else:
                require(all(rows[-1].get(key) == 0 for key in ('active','auth_inflight','storage_inflight',
                        'room_inflight','journal_pending','owner_pending','coordinator_pending')) and
                        rows[-1].get('journal_closed') is True and rows[-1].get('local_room_actors') == 0,
                        'Lobby retained actual operations or journal resources at shutdown')
        self.test.report['shutdowns'].append(dict(role=self.role,pid=process.pid,crash_injected=crash,
                                                  exit_code=process.returncode))



class ServiceStack(AuthFixture):
    def __init__(self, args, *, battle_ticks=400, ready_ticks=600, grace_ms=10000):
        selected = getattr(args, 'room', None)
        if selected is None:
            selected = Path(args.lobby).resolve().parents[2]/'services/room/worker/ArenaRoom'
        args.room = Path(selected).resolve()
        require(args.room.is_file() and os.access(args.room, os.X_OK), 'real ArenaRoom executable is required')
        with args.room.open('rb') as executable:
            require(executable.read(4) == b'\x7fELF', 'Room worker must be a real ELF executable')
        super().__init__(args)
        self.battle_ticks,self.ready_ticks,self.grace_ms=battle_ticks,ready_ticks,grace_ms
        self.database='arena_lobby_process'
        self.password='test-password'
        self.lobby_token=secrets.token_hex(32)
        self.room_token=secrets.token_hex(32)
        self.room_roles=('room-a','room-b')
        self.secrets=(*self.secrets, self.password, self.lobby_token, self.room_token)
        for role in self.room_roles:
            (self.cluster.root/role).mkdir(mode=0o700)
        used=set(self.ports.values())
        for role in ('lobby','lobby-health','gateway','gateway-ws','gateway-health',
                     *(name for room in self.room_roles for name in (room, room+'-health'))):
            while True:
                with socket.socket() as peer:
                    peer.bind(('127.0.0.1',0)); port=peer.getsockname()[1]
                if port not in used:
                    used.add(port); self.ports[role]=port; break
        self.configs.update({role:self.cluster.root/(role+'.json') for role in (*self.room_roles,'lobby','gateway')})
        self.children.update({role:ServiceChild(self,role) for role in (*self.room_roles,'lobby','gateway')})
        self.peers=[]
        self.report.update(scope='private real six-process test stack with two Room owners',
                           physical_process_samples=[],restart_timeline=[])


    def manifest(self, version=1, desired=True):
        result=super().manifest(version,desired)
        result['nodes'].extend(dict(service=role,nodeId=role+'-smoke',host='127.0.0.1',
            port=self.ports[role],ready=True) for role in ('lobby','gateway'))
        result['nodes'].extend(dict(service='room',nodeId=role,host='127.0.0.1',
            port=self.ports[role],ready=True) for role in self.room_roles)
        return result


    def command(self, role):
        if role in ('auth','storage'):
            command, environment = super().command(role)
            if role == 'auth':
                environment = {key:value for key,value in environment.items() if not key.startswith('PG')}
            return command, environment
        if role in self.room_roles:
            # Keep the same-rate civil clock shared by the existing test wrapper.
            # RPC deadlines must use one origin across every real service role.
            clock_keys=('LD_PRELOAD','ARENA_TEST_CLOCK_REALTIME_NS','ARENA_TEST_CLOCK_MONOTONIC_NS')
            allowed=('PATH','LANG','LC_ALL','TZ','ASAN_OPTIONS','LSAN_OPTIONS','UBSAN_OPTIONS',*clock_keys)
            environment={name:value for name,value in os.environ.items() if name in allowed}
            environment.update(ARENA_NODE_ID=role, ARENA_HEALTH_PORT=str(self.ports[role+'-health']),
                ARENA_RUNTIME_CONFIG=str(self.configs[role]), ARENA_ROOM_PORT=str(self.ports[role]),
                ARENA_ROOM_STATE_DIR=str(self.cluster.root/role), ARENA_ROOM_TOKEN=self.room_token)
            require(not any(name.startswith('PG') for name in environment) and
                    {name for name in environment if name.startswith('ARENA_') and name not in clock_keys} ==
                    {'ARENA_NODE_ID','ARENA_HEALTH_PORT','ARENA_RUNTIME_CONFIG','ARENA_ROOM_PORT',
                     'ARENA_ROOM_STATE_DIR','ARENA_ROOM_TOKEN'}, 'Room inherited a foreign credential or endpoint')
            return [str(self.args.room)], environment
        environment={key:value for key,value in self.environment().items() if not key.startswith('PG')}
        environment.update(ARENA_NODE_ID=role+'-smoke',ARENA_HEALTH_PORT=str(self.ports[role+'-health']),
            ARENA_RUNTIME_CONFIG=str(self.configs[role]),ARENA_LOBBY_PORT=str(self.ports['lobby']),
            ARENA_LOBBY_TOKEN=self.lobby_token)
        if role == 'lobby':
            environment.update(ARENA_AUTH_PORT=str(self.ports['auth']),ARENA_STORAGE_PORT=str(self.ports['storage']),
                ARENA_AUTH_TOKEN=self.auth_token,ARENA_STORAGE_LOBBY_TOKEN=self.other_token,
                ARENA_LOBBY_STATE_DIR=str(self.cluster.root/'lobby-state'),
                ARENA_ROOM_TOKEN=self.room_token,
                ARENA_ROOM_NODES=json.dumps([dict(nodeId=node,host='127.0.0.1',port=self.ports[node])
                                            for node in self.room_roles], separators=(',',':')),
                ARENA_BATTLE_TICKS=str(self.battle_ticks),ARENA_READY_TIMEOUT_TICKS=str(self.ready_ticks),
                ARENA_DISCONNECT_GRACE_MS=str(self.grace_ms),ARENA_TRANSPORT_LEASE_MS='1000')
            command=[str(self.args.lobby)]
        else:
            environment.update(ARENA_TCP_PORT=str(self.ports['gateway']),
                               ARENA_WS_PORT=str(self.ports['gateway-ws']))
            command=[str(self.args.gateway),'asio','handshake']
        require('ARENA_JWT_KEY' not in environment and 'ARENA_INTERNAL_TOKEN' not in environment and
                'ARENA_ACCOUNTS_FILE' not in environment, 'business process inherited signer/legacy credentials')
        return command,environment


    def setup(self):
        super().setup()
        salt=secrets.token_bytes(16)
        digest='$arena-pbkdf2-sha256$100000$'+salt.hex()+'$'+hashlib.pbkdf2_hmac(
            'sha256', self.password.encode(),salt,100000).hex()
        self.query("UPDATE arena.account SET password_digest=%s WHERE account_name='bob'",(digest,))
        for role in self.room_roles:
            self.children[role].start()
        self.children['lobby'].start(); self.children['gateway'].start()
        self.sample('six-process-stack-ready')


    def connect(self, websocket=False):
        peer=Bot((self.ports['gateway-ws' if websocket else 'gateway'],websocket))
        self.peers.append(peer)
        return peer


    def sample(self, label):
        values={}
        for role, child in self.children.items():
            require(child.process is not None and child.process.poll() is None, role+' not alive at sample')
            pid=child.process.pid
            descriptors=list(Path(f'/proc/{pid}/fd').iterdir())
            sockets=0
            for path in descriptors:
                try:
                    sockets += os.readlink(path).startswith('socket:')
                except FileNotFoundError:
                    pass  # Concurrent RPC completion retired a sampled descriptor.
            rss=int(Path(f'/proc/{pid}/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
            birth=int(Path(f'/proc/{pid}/stat').read_text().rpartition(')')[2].split()[19])
            values[role]=dict(pid=pid,start_ticks=birth,fd=len(descriptors),sockets=sockets,rss_bytes=rss,
                             service='room' if role in self.room_roles else role,
                             node_id=role if role in self.room_roles else role+'-smoke',
                             executable=os.readlink(f'/proc/{pid}/exe'))
        self.report['physical_process_samples'].append(dict(label=label,at=time.monotonic(),roles=values))
        return {role:value['pid'] for role,value in values.items()}


    def close(self):
        for peer in self.peers:
            try:
                peer.close()
            except OSError:
                pass
        errors=[]
        for role in ('gateway','lobby',*reversed(self.room_roles)):
            try:
                self.children[role].stop()
            except Exception as error:
                errors.append(role+': '+type(error).__name__+': '+str(error))
        super().close()
        if errors:
            self.report.update(passed=False,service_cleanup_errors=errors)
