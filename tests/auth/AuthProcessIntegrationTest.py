#!/usr/bin/env python3
"""Real Auth process contracts against strict-role Storage and private PostgreSQL."""
import argparse
import asyncio
import base64
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'storage'))
from StorageMigrationTest import Cluster, require, run


class PrivateCluster(Cluster):
    def stop_server(self):
        result = run([self.tool('pg_ctl'), '-D', self.data, '-m', 'fast', '-w', '-t', '15', 'stop'], self.base, timeout=20)
        require(result.returncode == 0, 'private PG stop failed')

    def restart_server(self):
        options = (f"-c listen_addresses='' -c unix_socket_directories='{self.socket}' "
            f'-c unix_socket_permissions=0700 -c port=5432 -c max_connections={self.max_connections} '
            '-c log_min_messages=warning -c timezone=UTC')
        result = run([self.tool('pg_ctl'), '-D', self.data, '-l', self.log,
            '-w', '-t', '15', '-o', options, 'start'], self.base, timeout=20)
        require(result.returncode == 0, 'private PG restart failed')


def health_http(port, path='/health/config'):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=.2)
    try:
        connection.request('GET', path, headers={'Connection':'close'})
        response = connection.getresponse()
        body = response.read(16385)
        require(len(body) <= 16384, 'health body exceeded its bound')
        return response.status, json.loads(body)
    finally:
        connection.close()


def atomic_config(path, document):
    candidate = path.with_suffix('.next')
    candidate.write_text(json.dumps(document, separators=(',', ':'))+'\n')
    candidate.replace(path)


class Child:
    def __init__(self, test, role):
        self.test, self.role = test, role
        self.process = self.stream = None
        self.serial = 0
        self.history = []
        self.log_paths = []

    def start(self):
        require(self.process is None, 'child restart without a completed previous stop')
        self.serial += 1
        path = self.test.cluster.root/f'{self.role}-{self.serial}.log'
        self.log_paths.append(path)
        self.stream = path.open('w')
        command, environment = self.test.command(self.role)
        self.process = subprocess.Popen(command, cwd=self.test.args.repository_root, env=environment,
            stdin=subprocess.DEVNULL, stdout=self.stream, stderr=self.stream)
        self.history.append(self.process.pid)
        def listening():
            for line in path.read_text(errors='replace').splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get('service') == self.role and row.get('state') == 'listening':
                    self.test.started[self.role] = row
                    return True
            return False
        self.test.until(listening, self.role+' listening line', timeout=8)
        started = self.test.started[self.role]
        require(started['port'] == self.test.ports[self.role] and
                started['health_port'] == self.test.ports[self.role+'-health'], 'child bound another endpoint')
        self.test.until(lambda:health_http(started['health_port'], '/health/ready')[0] == 200,
                        self.role+' actual dependency ready', timeout=8)

    def stop(self):
        if self.process is None:
            if self.stream is not None and not self.stream.closed:
                self.stream.close()
            return
        process, self.process = self.process, None
        unexpected = process.poll() is not None
        if not unexpected:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=3)
            self.test.report['forced_kills'] += 1
            raise AssertionError(self.role+' failed finite shutdown')
        finally:
            self.stream.close()
        require(not unexpected and process.returncode == 0, self.role+' abnormal process exit')
        rows = []
        for line in self.log_paths[-1].read_text(errors='replace').splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        require(rows and rows[-1].get('state') == 'stopped', self.role+' stop counters missing')
        stopped = rows[-1]
        if self.role == 'auth':
            require(all(stopped[key] == 0 for key in ('active','queued','busy','workers')),
                    'Auth retained real jobs or workers')
        else:
            require(stopped['worker']['active'] == stopped['pool']['checked_out'] == 0 and stopped['pool']['closed'],
                    'Storage retained real workers or database loans')
        self.test.report['shutdowns'].append(dict(role=self.role, pid=process.pid, counters=stopped))


class Acceptance:
    def __init__(self, args):
        self.args, self.cluster = args, PrivateCluster(args)
        self.database = 'arena_auth_process'
        from shared.runtime.protocol import Protocol
        self.protocol = Protocol(args.descriptor)
        self.password = 'Auth process fixture password'
        self.signer_key = secrets.token_hex(32)
        self.auth_token, self.storage_token, self.other_token = (secrets.token_hex(32) for _ in range(3))
        self.secrets = (self.password,self.signer_key,self.auth_token,self.storage_token,self.other_token)
        self.ports, allocated = {}, set()
        for name in ('storage','storage-health','auth','auth-health'):
            while True:
                with socket.socket() as peer:
                    peer.bind(('127.0.0.1',0)); port = peer.getsockname()[1]
                if port not in allocated:
                    allocated.add(port);self.ports[name] = port;break
        self.started = {}
        self.configs = {role:self.cluster.root/(role+'.json') for role in ('storage','auth')}
        self.children = {role:Child(self,role) for role in ('storage','auth')}
        self.report = dict(passed=False,cases=[],shutdowns=[],forced_kills=0,
            scope='Auth subprocess contracts only; Gateway/Lobby migration and 90-restart acceptance are not covered',
            parent_fd_baseline=len(os.listdir('/proc/self/fd')),
            clock_fixture=bool(os.environ.get('ARENA_TEST_CLOCK_REALTIME_NS')))
        self.original = None

    def manifest(self, version=1, desired=True):
        return dict(version=str(version),desiredReady=desired,drainGraceMs=5000,nodes=[
            dict(service=role,nodeId=role+'-smoke',host='127.0.0.1',port=self.ports[role],ready=True)
            for role in ('storage','auth')])

    def environment(self):
        values = self.cluster.environment(self.database)
        for name in tuple(values):
            if name.startswith('ARENA_') and not name.startswith('ARENA_TEST_CLOCK_'):
                values.pop(name)
        values['PYTHONDONTWRITEBYTECODE'] = '1'
        return values

    def command(self, role):
        environment = self.environment()
        environment.update(ARENA_NODE_ID=role+'-smoke', ARENA_HEALTH_PORT=str(self.ports[role+'-health']),
            ARENA_RUNTIME_CONFIG=str(self.configs[role]))
        if role == 'storage':
            environment['ARENA_STORAGE_CALLER_TOKENS'] = json.dumps(dict(auth=self.storage_token,lobby=self.other_token))
            require('ARENA_INTERNAL_TOKEN' not in environment and 'ARENA_JWT_KEY' not in environment,
                    'Storage fixture inherited legacy or signer credentials')
            command = [sys.executable,'-m','services.storage.server','--descriptor',str(self.args.descriptor),
                       '--port',str(self.ports['storage']),'--workers','2']
        else:
            environment.update(ARENA_AUTH_PORT=str(self.ports['auth']),ARENA_STORAGE_PORT=str(self.ports['storage']),
                ARENA_AUTH_TOKEN=self.auth_token,ARENA_STORAGE_AUTH_TOKEN=self.storage_token,ARENA_JWT_KEY=self.signer_key)
            command = [str(self.args.auth)]
        return command,environment

    def until(self, predicate, message, timeout=4):
        deadline, last = time.monotonic()+timeout, None
        while time.monotonic() < deadline:
            for child in self.children.values():
                if child.process is not None:
                    require(child.process.poll() is None, child.role+' exited before '+message)
            try:
                if predicate():
                    return
            except (OSError,ConnectionError,TimeoutError,http.client.HTTPException,ValueError) as error:
                last = type(error).__name__
            time.sleep(.01)
        raise AssertionError('timeout waiting for '+message+((': '+last) if last else ''))

    def query(self, statement, values=()):
        import psycopg
        with psycopg.connect(host=str(self.cluster.socket),port=5432,dbname=self.database,user='arena_test',
                             connect_timeout=1,autocommit=True) as connection:
            cursor = connection.execute(statement, values or None)
            return cursor.fetchall() if cursor.description else []

    def message(self, name, **fields):
        return self.protocol.message('arena.auth.'+name)(**fields)

    def request(self, epoch, *, account='alice', credential=None, jwt=False):
        return self.message('AuthenticateRequest',proposed_epoch=epoch,credential_kind=1 if jwt else 0,
                            account='' if jwt else account,credential=credential or self.password)

    def call(self, request, *, token=None, caller='lobby', timeout=.8):
        async def invoke():
            from shared.runtime.rpc import Client
            client = Client(self.protocol,'127.0.0.1',self.ports['auth'],
                service_token=self.auth_token if token is None else token,caller_service=caller,target_service='auth')
            try:
                payload = await client.call('auth.authenticate',request.SerializeToString(),timeout=timeout)
                return self.protocol.message('arena.auth.AuthenticateReply').FromString(payload)
            finally:
                require(client.inflight == 0, 'client retained an RPC loan')
        return asyncio.run(invoke())

    def expect(self, request, statuses, **options):
        from shared.runtime.rpc import RpcError
        started = time.monotonic()
        try:
            self.call(request,**options)
        except RpcError as error:
            require(error.status in statuses, 'unexpected public Auth status '+str(error.status))
            elapsed = time.monotonic()-started
            require(elapsed <= 1, 'dependency/public failure exceeded one second')
            return dict(status=error.status,elapsed_ms=elapsed*1000)
        raise AssertionError('invalid Auth call succeeded')

    def verify_token(self, grant, epoch):
        def decode(value):
            return base64.urlsafe_b64decode(value+'='*((4-len(value)%4)%4))
        parts = grant.session_token.split('.')
        require(len(parts) == 3, 'token encoding changed')
        signature = hmac.new(self.signer_key.encode(),('.'.join(parts[:2])).encode(),hashlib.sha256).digest()
        require(hmac.compare_digest(decode(parts[2]),signature), 'Auth token signature invalid')
        header,payload = (json.loads(decode(value)) for value in parts[:2])
        require(header == dict(alg='HS256',typ='JWT') and payload['sub'] == '42' and payload['epoch'] == str(epoch),
                'token profile/canonical identity changed')
        require(grant.player_id == 42 and grant.epoch == epoch and grant.expires_at_unix == payload['exp']
                and payload['exp'] > time.time(), 'token expiry/identity changed')

    def setup(self):
        self.cluster.start();self.cluster.create(self.database)
        result = run([sys.executable,self.args.repository_root/'services/storage/migrate.py','up'],
                     self.cluster.environment(self.database),timeout=95)
        require(result.returncode == 0, 'Auth fixture migrations failed')
        self.cluster.sql((self.args.repository_root/'services/storage/fixtures/seed.sql').read_text(),self.database,single=True)
        salt = bytes(16)
        derived = hashlib.pbkdf2_hmac('sha256',self.password.encode(),salt,100000).hex()
        digest = '$arena-pbkdf2-sha256$100000$'+salt.hex()+'$'+derived
        self.query('UPDATE arena.account SET password_digest=%s WHERE account_name=%s',(digest,'alice'))
        for role,path in self.configs.items():
            atomic_config(path,self.manifest())
        self.children['storage'].start();self.children['auth'].start()

    def password_and_jwt(self):
        self.original = self.call(self.request(10));self.verify_token(self.original,10)
        failures = [self.expect(self.request(11,credential='wrong fixture credential'),{10}),
                    self.expect(self.request(12,account='missing-account'),{10})]
        self.query("UPDATE arena.account SET state='disabled' WHERE account_name='alice'")
        try:
            failures.append(self.expect(self.request(13),{10}))
        finally:
            self.query("UPDATE arena.account SET state='active' WHERE account_name='alice'")
        resumed = self.call(self.request(14,jwt=True,credential=self.original.session_token))
        self.verify_token(resumed,14)
        require(resumed.resumed and resumed.previous_epoch == 10 and
                resumed.previous_expires_at_unix == self.original.expires_at_unix, 'JWT prior claims not preserved')
        return dict(player_id=42,real_password_kdf=True,independent_hs256_verification=True,
                    candidate_only_no_lobby_install=True,failures=failures)

    def rejected_role_and_version(self):
        before = health_http(self.ports['storage-health'])[1]['methods']['account.find']
        spoof = self.expect(self.request(20),{10},caller='gateway')
        request = self.protocol.Request(request_id=1,correlation_id='auth-old-version',method='auth.authenticate',
            deadline_unix_ms=int(time.time()*1000)+800,payload=self.request(21).SerializeToString(),
            service_token=self.auth_token,protocol_version=0,caller_service='lobby',target_service='auth',service_path=['lobby'])
        def exact(peer, size):
            data = bytearray()
            while len(data) < size:
                block = peer.recv(size-len(data));require(block,'raw version response ended early');data.extend(block)
            return bytes(data)
        with socket.create_connection(('127.0.0.1',self.ports['auth']),timeout=.8) as peer:
            data = request.SerializeToString();peer.sendall(struct.pack('!I',len(data))+data)
            length, = struct.unpack('!I',exact(peer,4));require(0 < length <= 256*1024,'raw response unbounded')
            response = self.protocol.Response.FromString(exact(peer,length))
        require(response.status == 11 and response.protocol_version == 1,'old wire version did not fail closed')
        require(health_http(self.ports['storage-health'])[1]['methods']['account.find'] == before,
                'role/version rejection executed authentication business')
        return dict(spoof=spoof,old_version_status=11,business_calls_after_rejection=0)

    def auth_restarts(self):
        storage_pid = self.children['storage'].process.pid
        pids = [self.children['auth'].process.pid]
        for index in range(2):
            self.children['auth'].stop();self.children['auth'].start()
            resumed = self.call(self.request(30+index,jwt=True,credential=self.original.session_token))
            self.verify_token(resumed,30+index)
            require(resumed.previous_epoch == 10,'restart fabricated another prior authority epoch')
            pids.append(self.children['auth'].process.pid)
        require(len(set(pids)) == 3 and self.children['storage'].process.pid == storage_pid,'Auth restart affected Storage')
        return dict(restarts=2,auth_pids=pids,storage_pid=storage_pid,same_key_prior_jwt_verified=True,candidate_only=True)

    def postgres_outage(self):
        pids = {role:child.process.pid for role,child in self.children.items()}
        self.cluster.stop_server()
        try:
            self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 503,'Auth not-ready during PG outage')
            require(health_http(self.ports['auth-health'],'/health/alive')[0] == 200,'Auth liveness depended on PG')
            failed = self.expect(self.request(40),{3,4,5})
        finally:
            self.cluster.restart_server()
        self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 200,'Auth dependency recovery',8)
        self.verify_token(self.call(self.request(41)),41)
        require(pids == {role:child.process.pid for role,child in self.children.items()},'outage recovery replaced a service')
        return dict(same_processes=True,alive_independent=True,failed_call=failed)

    def storage_restart(self):
        auth_pid = self.children['auth'].process.pid
        self.children['storage'].stop()
        self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 503,'Auth not-ready after Storage stop')
        failed = self.expect(self.request(50),{3,4,5,9})
        self.children['storage'].start()
        self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 200,'same Auth Storage recovery',8)
        self.verify_token(self.call(self.request(51)),51)
        require(self.children['auth'].process.pid == auth_pid,'Storage restart crashed Auth')
        return dict(auth_pid_preserved=True,storage_restarts=1,failed_call=failed)

    def quiesce(self):
        pid = self.children['auth'].process.pid
        before = health_http(self.ports['storage-health'])[1]['methods']['account.find']
        atomic_config(self.configs['auth'],self.manifest(2,False))
        self.children['auth'].process.send_signal(signal.SIGHUP)
        self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 503,'Auth operator quiesce')
        require(health_http(self.ports['auth-health'],'/health/alive')[0] == 200,'quiescence closed Auth health')
        failure = self.expect(self.request(60),{3})
        require(health_http(self.ports['storage-health'])[1]['methods']['account.find'] == before,'quiesced Auth ran business')
        atomic_config(self.configs['auth'],self.manifest(3))
        self.children['auth'].process.send_signal(signal.SIGHUP)
        self.until(lambda:health_http(self.ports['auth-health'],'/health/ready')[0] == 200,'Auth operator restore')
        self.verify_token(self.call(self.request(61)),61)
        require(pid == self.children['auth'].process.pid,'quiesce required an Auth restart')
        return dict(reversible=True,pid_preserved=True,rejected=failure)

    def startup_collisions(self):
        cases = []
        for mode in ('same-port','held-business','held-health'):
            held = socket.socket()
            try:
                held.bind(('127.0.0.1',0));held.listen(1)
                occupied = held.getsockname()[1]
                environment = self.command('auth')[1]
                if mode == 'same-port':
                    environment.update(ARENA_AUTH_PORT=str(occupied),ARENA_HEALTH_PORT=str(occupied))
                elif mode == 'held-business':
                    environment.update(ARENA_AUTH_PORT=str(occupied),ARENA_HEALTH_PORT='0')
                else:
                    environment.update(ARENA_AUTH_PORT='0',ARENA_HEALTH_PORT=str(occupied))
                result = subprocess.run([str(self.args.auth)],cwd=self.args.repository_root,env=environment,
                    stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=8)
                self.assert_no_secrets(result.stdout)
                require(result.returncode == 2,'Auth listener collision did not exit cleanly')
                cases.append(dict(mode=mode,exit_code=result.returncode))
            finally:
                held.close()
        return dict(collisions=cases,no_sigabrt=True)

    def assert_no_secrets(self, text):
        require(not any(secret in text for secret in self.secrets),'fixture secret exposed in process output/report')

    def run(self):
        self.setup()
        for name, action in (
            ('real_password_kdf_jwt_verify_and_resign',self.password_and_jwt),
            ('spoofed_role_and_old_protocol_rejected_before_business',self.rejected_role_and_version),
            ('two_auth_restarts_preserve_signing_profile',self.auth_restarts),
            ('postgres_outage_fast_failure_and_same_process_recovery',self.postgres_outage),
            ('storage_restart_does_not_restart_auth',self.storage_restart),
            ('versioned_auth_quiesce_is_reversible',self.quiesce),
            ('auth_listener_collisions_fail_cleanly',self.startup_collisions)):
            self.report['current_case'] = name
            started = time.monotonic();details = action()
            self.until(lambda:self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                "AND state LIKE 'idle in transaction%'")[0][0] == 0,'no lingering idle transaction')
            self.report['cases'].append(dict(name=name,passed=True,elapsed_seconds=time.monotonic()-started,**details))
        for role in ('auth','storage'):
            self.children[role].stop()
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                           "AND application_name='arena-storage'")[0][0] == 0,'Storage physical sessions retained')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND state LIKE 'idle in transaction%'")[0][0] == 0,'final idle transaction leaked')
        self.report.update(passed=True,idle_transactions=0,checked_out=0,active_workers=0)

    def close(self):
        errors = []
        for role in ('auth','storage'):
            try:
                self.children[role].stop()
            except Exception as error:
                errors.append(role+': '+type(error).__name__+': '+str(error))
        logs = self.args.report.parent/(self.args.report.stem+'-logs')
        logs.mkdir(parents=True,exist_ok=True)
        for child in self.children.values():
            for source in child.log_paths:
                try:
                    self.assert_no_secrets(source.read_text(errors='replace'))
                    shutil.copyfile(source,logs/source.name)
                except Exception as error:
                    errors.append(type(error).__name__+': '+str(error))
        self.report['logs'] = str(logs)
        self.report['owned_child_pids'] = {role:child.history for role,child in self.children.items()}
        try:
            self.cluster.close();self.report['cluster_cleaned'] = not self.cluster.root.exists()
        except Exception as error:
            errors.append('cluster: '+type(error).__name__+': '+str(error))
        self.report['parent_fd_final'] = len(os.listdir('/proc/self/fd'))
        if self.report['parent_fd_final'] != self.report['parent_fd_baseline']:
            errors.append('parent physical descriptor count did not recover')
        if errors:
            self.report.update(passed=False,cleanup_errors=errors)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--descriptor',type=Path,required=True)
    parser.add_argument('--auth',type=Path,required=True)
    parser.add_argument('--pg-bin',type=Path,required=True)
    parser.add_argument('--pg-share',type=Path)
    parser.add_argument('--pg-lib',type=Path)
    parser.add_argument('--report',type=Path,required=True)
    args = parser.parse_args();args.repository_root=args.repository_root.resolve()
    args.descriptor=args.descriptor.resolve();args.auth=args.auth.resolve();args.pg_bin=args.pg_bin.resolve()
    sys.path.insert(0,str(args.repository_root))
    acceptance = Acceptance(args)
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        acceptance.run()
    except (Exception,KeyboardInterrupt) as error:
        acceptance.report.update(passed=False,error=type(error).__name__+': '+str(error))
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        acceptance.close()
    acceptance.report['source_hashes'] = {name:hashlib.sha256((args.repository_root/name).read_bytes()).hexdigest()
        for name in ('services/auth/app/main.cpp','services/auth/src/AuthService.cpp','shared/src/rpc/Server.cpp',
                     'shared/runtime/rpc.py','services/storage/server.py','tests/auth/AuthProcessIntegrationTest.py')}
    output = json.dumps(acceptance.report,indent=2)
    acceptance.assert_no_secrets(output)
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(output+'\n')
    print(json.dumps(dict(passed=acceptance.report['passed'],cases=len(acceptance.report['cases']),
                          report=str(args.report),error=acceptance.report.get('error'))))
    return 0 if acceptance.report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
