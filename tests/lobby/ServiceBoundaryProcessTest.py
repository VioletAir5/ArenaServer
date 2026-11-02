#!/usr/bin/env python3
"""Real service listener contracts and kernel-observed connect-delay failure."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import struct
import sys
import time
import traceback

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ServiceStackFixture import ServiceStack
from JourneyProtocol import Bot
from AuthProcessIntegrationTest import atomic_config, health_http
from StorageMigrationTest import require
from shared.runtime.protocol import Protocol

MAX_FRAME = 256 * 1024
BUSINESS_METHODS = ('account.find', 'player.get', 'match.allocate_ids',
                    'match.save_result', 'match.get', 'settlement.get')


def failure_trace(error):
    """Never serialize exception args: protobuf assertions may own bearer tokens."""
    return dict(type=type(error).__name__, locations=[
        dict(file=str(Path(frame.filename).resolve()), line=frame.lineno,
             function=frame.name)
        for frame in traceback.extract_tb(error.__traceback__)])


def secret_field_indices(value, secrets, path=()):
    """Identify a rejected report field using ordinals, without printing its data."""
    if isinstance(value, str):
        return [list(path)] if any(secret in value for secret in secrets) else []
    found = []
    if isinstance(value, dict):
        for index, (key, item) in enumerate(value.items()):
            found.extend(secret_field_indices(key, secrets, (*path, index, 'key')))
            found.extend(secret_field_indices(item, secrets, (*path, index, 'value')))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found.extend(secret_field_indices(item, secrets, (*path, index)))
    return found

def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def process_identity(pid):
    try:
        # comm can contain spaces; starttime is field 22 after its closing ')'.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return dict(pid=pid, start_ticks=int(fields[19]))
    except FileNotFoundError:
        return None


def socket_inodes(pid):
    result = {}
    for path in Path(f'/proc/{pid}/fd').iterdir():
        try:
            link = os.readlink(path)
        except FileNotFoundError:
            continue
        if link.startswith('socket:['):
            result[link[8:-1]] = int(path.name)
    return result


def owned_tcp(pid, port):
    """Join a process's actual descriptors to its network namespace TCP rows."""
    inodes = socket_inodes(pid)
    values = []
    for table in ('tcp', 'tcp6'):
        for line in Path(f'/proc/{pid}/net/{table}').read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) < 10 or fields[9] not in inodes:
                continue
            remote_port = int(fields[2].rsplit(':', 1)[1], 16)
            if remote_port == port:
                values.append(dict(fd=inodes[fields[9]], inode=fields[9],
                    local=fields[1], remote=fields[2], state=fields[3], table=table))
    return values


class SaturatedListener:
    """Own an unaccepted loopback queue; never simulate delayed response bytes."""
    def __init__(self, evidence):
        self.evidence = evidence
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fillers = []
        self.proof = []
        try:
            self.listener.bind(('127.0.0.1', 0))
            self.port = self.listener.getsockname()[1]
            self.listener.listen(0)
            evidence.update(host="127.0.0.1", port=self.port, listen_backlog=0,
                            accepted_connections=0, response_byte_delay=False, witness_samples=[])
            # Linux's zero-backlog accept queue admits one completed handshake.
            first = socket.create_connection(('127.0.0.1', self.port), timeout=.2)
            self.fillers.append(first)
            for _ in range(8):
                witness = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.fillers.append(witness)
                witness.setblocking(False)
                outcome = witness.connect_ex(('127.0.0.1', self.port))
                require(outcome in (0, errno.EINPROGRESS, errno.EWOULDBLOCK,
                                    errno.EALREADY), 'queue witness connect failed unexpectedly')
                end = time.monotonic() + .15
                while time.monotonic() < end:
                    rows = owned_tcp(os.getpid(), self.port)
                    evidence["witness_samples"].append(dict(at=time.monotonic(), rows=rows))
                    self.proof = [row for row in rows
                                  if row['fd'] == witness.fileno() and row['state'] == '02']
                    if self.proof:
                        evidence.update(held_fillers=len(self.fillers) - 1,
                                        parent_kernel_witness=self.proof)
                        witness.close()
                        self.fillers.pop()
                        return
                    time.sleep(.002)
            raise AssertionError('kernel did not form a genuine SYN_SENT connection')
        except BaseException:
            self.close()
            raise

    def close(self):
        for peer in self.fillers:
            peer.close()
        self.fillers.clear()
        self.listener.close()


class Acceptance(ServiceStack):
    def __init__(self, args):
        super().__init__(args, battle_ticks=400, ready_ticks=600, grace_ms=10000)
        self.protocol = Protocol(args.descriptor)
        self.status_enum = self.protocol.pool.FindEnumTypeByName('arena.rpc.Status')
        self.sequence = 1000
        self.identities = {}
        self.version = 1
        self.report.update(scope='actual four-process listener and TCP connect-delay boundaries',
            exclusions=['DNS is not involved: runtime routes accept loopback IP literals only',
                        'No reverse RPC handlers or three-service circular execution are invented',
                        'Persistent lost-ACK coverage belongs to tests/storage/PersistentGatewayTest.py, not this file',
                        'This file does not execute the separate 90-restart acceptance'],
            raw_attempts=[], connect_samples=[], runtime_changes=[], resource_samples=[],
            declared_groups=['public_tcp_ws_login', 'strict_listener_metadata',
                             'auth_storage_real_connect_delay_and_same_pid_recovery'])

    def status(self, name):
        return self.status_enum.values_by_name[name].number

    def named_status(self, number):
        entry = self.status_enum.values_by_number.get(number)
        require(entry is not None, 'response used an unknown status')
        return dict(name=entry.name, numeric=number)

    def live_pids(self):
        result = self.sample('boundary-pid-check')
        for role, pid in result.items():
            identity = process_identity(pid)
            require(identity is not None, role + ' disappeared')
            if role in self.identities:
                require(identity == self.identities[role], role + ' unexpectedly restarted')
            else:
                self.identities[role] = identity
        return result

    def resources(self, label):
        values = {}
        for role, child in self.children.items():
            require(child.process is not None and child.process.poll() is None,
                    role + ' unexpectedly exited')
            pid = child.process.pid
            values[role] = dict(pid=pid, fd=len(list(Path(f'/proc/{pid}/fd').iterdir())),
                                sockets=len(socket_inodes(pid)))
        self.report['resource_samples'].append(dict(label=label, at=time.monotonic(), roles=values))
        return values

    def business_counts(self):
        methods = health_http(self.ports['storage-health'])[1]['methods']
        return {name: int(methods.get(name, 0)) for name in BUSINESS_METHODS}

    def idle(self):
        self.until(lambda: self.query("SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND state LIKE 'idle in transaction%'")[0][0] == 0,
            'no idle PostgreSQL transaction')
        self.until(lambda: health_http(self.ports['storage-health'])[1]['pool']['checked_out'] == 0,
                   'no physical PostgreSQL loan')

    def payload(self, role):
        if role == 'auth':
            return self.protocol.message('arena.auth.AuthenticateRequest')(
                proposed_epoch=100, credential_kind=0, account='alice', credential=self.password)
        request = self.protocol.message('arena.lobby.ExecuteRequest')(
            gateway_instance='0123456789abcdef0123456789abcdef', connection_id=9000)
        request.message.request_id = 1
        request.message.msg_id = 10
        request.message.login_request.account = 'alice'
        request.message.login_request.credential = self.password.encode("utf-8")
        return request

    def envelope(self, role, **changes):
        self.sequence += 1
        values = dict(request_id=self.sequence, correlation_id=f'boundary_{self.sequence}',
            deadline_unix_ms=int(time.time() * 1000) + 800,
            method='auth.authenticate' if role == 'auth' else 'lobby.execute',
            payload=self.payload(role).SerializeToString(),
            service_token=self.auth_token if role == 'auth' else self.lobby_token,
            protocol_version=1, caller_service='lobby' if role == 'auth' else 'gateway',
            target_service=role, service_path=['lobby'] if role == 'auth' else ['gateway'])
        values.update(changes)
        return self.protocol.Request(**values)

    def raw(self, role, request, label):
        """Send real descriptor-encoded metadata without a client-side cycle guard."""
        started = time.monotonic()
        deadline = started + 1.1
        row = dict(label=label, role=role, request_id=request.request_id,
            correlation_id=request.correlation_id, request_version=request.protocol_version,
            caller=request.caller_service, target=request.target_service,
            service_path=list(request.service_path), deadline_unix_ms=request.deadline_unix_ms,
            status=None, completed=False)
        self.report['raw_attempts'].append(row)
        def exact(peer, amount):
            data = bytearray()
            while len(data) < amount:
                remaining = deadline - time.monotonic()
                require(remaining > 0, 'raw listener reply exceeded client bound')
                peer.settimeout(remaining)
                block = peer.recv(amount - len(data))
                require(block, 'raw listener closed before its typed reply')
                data.extend(block)
            return bytes(data)
        try:
            with socket.create_connection(('127.0.0.1', self.ports[role]), timeout=.2) as peer:
                wire = request.SerializeToString()
                peer.sendall(struct.pack('!I', len(wire)) + wire)
                amount, = struct.unpack('!I', exact(peer, 4))
                require(0 < amount <= MAX_FRAME, 'raw listener response exceeded frame bound')
                response = self.protocol.Response.FromString(exact(peer, amount))
            require(response.request_id == request.request_id and
                    response.correlation_id == request.correlation_id and
                    response.protocol_version == 1, 'listener lost request/correlation/version metadata')
            row.update(elapsed_ms=(time.monotonic() - started) * 1000,
                       status=self.named_status(response.status), completed=True)
            return response, row
        except BaseException as error:
            row.update(elapsed_ms=(time.monotonic() - started) * 1000,
                       client_error=type(error).__name__)
            raise

    def public_login(self):
        pids = self.live_pids()
        require(len(set(pids.values())) == 4, 'roles do not own distinct processes')
        self.alice = self.connect()
        self.bob = self.connect(True)
        require(isinstance(self.alice, Bot) and isinstance(self.bob, Bot), 'unexpected public bot adapter')
        self.alice_grant = self.alice.login()
        self.bob_grant = self.bob.login(b'bob')
        self.secrets += (self.alice_grant['token'].decode(), self.bob_grant['token'].decode())
        require(self.alice_grant['player'] == 42 and self.bob_grant['player'] == 43,
                'real public login returned another account')
        require(self.alice.query()['session'] == self.alice_grant['session'] and
                self.bob.query()['session'] == self.bob_grant['session'],
                'public login session projection did not bind')
        self.baseline = self.resources('after-public-login')
        return dict(distinct_processes=4, real_pbkdf_and_signer=True, transports=['tcp', 'websocket'])

    def metadata_boundaries(self):
        before = self.business_counts()
        checks = []
        for role in ('lobby', 'auth'):
            for version in (0, 2):
                checks.append((role, f'{role}_version_{version}', 'INCOMPATIBLE_VERSION',
                               dict(protocol_version=version)))
            checks.append((role, role + '_wrong_immediate_role', 'UNAUTHENTICATED',
                dict(caller_service='gateway' if role == 'auth' else 'auth',
                     service_path=['gateway'] if role == 'auth' else ['auth'])))
            checks.append((role, role + '_expired_parent_deadline', 'DEADLINE_EXCEEDED',
                           dict(deadline_unix_ms=int(time.time() * 1000) - 1000)))
            path = ['gateway', 'auth', 'lobby', 'gateway'] if role == 'lobby' else ['gateway', 'auth', 'lobby']
            checks.append((role, role + '_cyclic_incoming_metadata', 'CALL_CYCLE', dict(service_path=path)))
        rows = []
        for role, label, expected, changes in checks:
            response, row = self.raw(role, self.envelope(role, **changes), label)
            require(response.status == self.status(expected), label + ' returned another typed status')
            require(not response.payload, 'rejected metadata exposed a business payload')
            require(row['elapsed_ms'] <= 1000, 'metadata rejection exceeded one second')
            rows.append(row)
        self.idle()
        after = self.business_counts()
        require(after == before, 'rejected listener metadata executed PostgreSQL business methods')
        self.live_pids()
        self.resources('after-listener-rejections')
        return dict(requests=len(rows), business_before=before, business_after=after,
            status_values={name: self.status(name) for name in
                ('INCOMPATIBLE_VERSION', 'UNAUTHENTICATED', 'DEADLINE_EXCEEDED', 'CALL_CYCLE')},
            cyclic_metadata_only=True, actual_reverse_rpc_handlers=False)

    def reload_auth(self, storage_port, label):
        self.version += 1
        document = self.manifest(self.version)
        for node in document['nodes']:
            if node['service'] == 'storage':
                node['port'] = storage_port
        atomic_config(self.configs['auth'], document)
        self.children['auth'].process.send_signal(signal.SIGHUP)
        self.until(lambda: int(health_http(self.ports['auth-health'])[1]['config_version']) == self.version,
                   'Auth route reload publication', 3)
        self.report['runtime_changes'].append(dict(label=label, role='auth', version=self.version,
            storage_host='127.0.0.1', storage_port=storage_port, same_pid=self.children['auth'].process.pid))

    def connect_delay(self):
        before_pids = self.live_pids()
        before_business = self.business_counts()
        self.report['blackhole'] = {}
        blackhole = SaturatedListener(self.report['blackhole'])
        restored = False
        try:
            self.reload_auth(blackhole.port, 'saturate-storage-connect-route')
            auth_pid = before_pids['auth']
            prior_inodes = {row['inode'] for row in owned_tcp(auth_pid, blackhole.port)}
            request = self.envelope('auth', deadline_unix_ms=int(time.time() * 1000) + 700)
            samples = []
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix='boundary-rpc') as pool:
                future = pool.submit(self.raw, 'auth', request, 'auth_real_storage_connect_delay')
                while not future.done():
                    observed = time.monotonic()
                    rows = owned_tcp(auth_pid, blackhole.port)
                    if rows:
                        samples.append(dict(at=observed, pid=auth_pid, rows=rows,
                            active=int(health_http(self.ports['auth-health'])[1]['inflight'])))
                    time.sleep(.002)
                response, row = future.result(timeout=.1)
            self.report['connect_samples'].extend(samples)
            proof = [sample for sample in samples if sample['active'] > 0 and any(
                value['state'] == '02' and value['inode'] not in prior_inodes for value in sample['rows'])]
            require(proof, 'actual Auth PID did not expose a new SYN_SENT socket during the real business call')
            require(response.status == self.status('DEADLINE_EXCEEDED'),
                    'connect delay did not preserve the typed deadline failure')
            require(row['elapsed_ms'] <= 1000, 'actual dependency connect delay exceeded one second')
            require(self.business_counts() == before_business,
                    'blackholed route fell back to the configured Storage endpoint')
            self.live_pids()
            alive = health_http(self.ports['auth-health'], '/health/alive')[0]
            ready = health_http(self.ports['auth-health'], '/health/ready')[0]
            self.report['dependency_health_during_delay'] = dict(auth_alive=alive, auth_ready=ready)
            require(alive == 200 and ready == 503, 'connect outage confused readiness and liveness')
            self.resources('during-kernel-syn-sent-failure')
            self.reload_auth(self.ports['storage'], 'restore-storage-connect-route')
            restored = True
        finally:
            try:
                if not restored:
                    self.reload_auth(self.ports['storage'], 'restore-route-after-failure')
            finally:
                blackhole.close()
        self.until(lambda: health_http(self.ports['auth-health'], '/health/ready')[0] == 200 and
                           health_http(self.ports['lobby-health'], '/health/ready')[0] == 200 and
                           health_http(self.ports['gateway-health'], '/health/ready')[0] == 200,
                   'same four PIDs dependency recovery', 5)
        recovered = self.connect()
        grant = recovered.login(token=self.alice_grant['token'])
        self.secrets += (grant['token'].decode(),)
        require(grant['resumed'] and grant['session'] == self.alice_grant['session'] and
                grant['room'] == 0 and grant['epoch'] > self.alice_grant['epoch'],
                'same-PID recovery lost the authenticated session')
        require(recovered.query()['session'] == grant['session'], 'recovered session is not usable')
        require(self.live_pids() == before_pids, 'connect failure restarted a participant or spectator')
        self.idle()
        self.until(lambda: not owned_tcp(before_pids['auth'], self.report['blackhole']['port']),
                   'actual failed-connect descriptors retired', 2)
        settled = self.resources('after-same-pid-recovery')
        for role, value in settled.items():
            require(value['fd'] <= self.baseline[role]['fd'] + 4 and
                    value['sockets'] <= self.baseline[role]['sockets'] + 2,
                    role + ' retained an unbounded descriptor/socket delta')
        return dict(owner_role='auth', dependency='storage', kernel_state='SYN_SENT', kernel_state_hex='02',
            business_reply=row['status'], elapsed_ms=row['elapsed_ms'], new_owned_syn_samples=len(proof),
            pids_before=before_pids, pids_after=before_pids, recovered_session_preserved=True,
            dns_participated=False, no_static_endpoint_fallback=True)

    def run(self):
        self.setup()
        for name, action in (('public_tcp_ws_login', self.public_login),
                             ('strict_listener_metadata', self.metadata_boundaries),
                             ('auth_storage_real_connect_delay_and_same_pid_recovery', self.connect_delay)):
            self.report['current_case'] = name
            started = time.monotonic()
            details = action()
            self.report['cases'].append(dict(name=name, passed=True,
                elapsed_seconds=time.monotonic() - started, **details))
        self.idle()
        for peer in self.peers:
            peer.close()
        for role in ('gateway', 'lobby', 'auth', 'storage'):
            self.children[role].stop()
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND application_name='arena-storage'")[0][0] == 0, 'Storage physical PostgreSQL sessions leaked')
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND state LIKE 'idle in transaction%'")[0][0] == 0, 'idle transaction leaked after service shutdown')
        self.report.update(passed=True, idle_transactions=0, checked_out=0, unexpected_crashes=0,
                           all_services_gracefully_closed=True)

    def close(self):
        super().close()
        leftovers = [identity for identity in self.identities.values()
                     if process_identity(identity['pid']) == identity]
        self.report['leftover_processes'] = leftovers
        if leftovers:
            self.report['passed'] = False
        logs = Path(self.report['logs'])
        for path in logs.glob('*.log'):
            value = path.read_text(errors='replace')
            self.assert_no_secrets(value)
            require(not re.search(r'(AddressSanitizer|LeakSanitizer|runtime error:)', value),
                    'sanitizer diagnostic present in child log')
            require(not re.search(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|\$arena-pbkdf2-', value),
                    'encoded credential present in child log')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=ROOT)
    for name in ('descriptor', 'auth', 'lobby', 'gateway', 'pg-bin', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--pg-share', type=Path)
    parser.add_argument('--pg-lib', type=Path)
    args = parser.parse_args()
    for name in ('repository_root', 'descriptor', 'auth', 'lobby', 'gateway', 'pg_bin', 'report'):
        setattr(args, name, getattr(args, name).resolve())
    test = Acceptance(args)
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        test.run()
    except (Exception, KeyboardInterrupt) as error:
        details = failure_trace(error)
        test.report.update(passed=False, error=details['type'],
                           failure_locations=details['locations'])
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            test.close()
        except Exception as error:
            test.report.update(passed=False, final_cleanup_error=failure_trace(error))
        # The shared fixture already marks cleanup failures red. Its historical
        # text fields may contain an exception's untrusted message; retain the
        # failed condition and source instead of serializing that message.
        for name, source in (('cleanup_errors', 'tests/auth/AuthProcessIntegrationTest.py'),
                             ('service_cleanup_errors', 'tests/lobby/ServiceStackFixture.py')):
            failures = test.report.pop(name, None)
            if failures:
                test.report['passed'] = False
                test.report.setdefault('fixture_cleanup_failures', []).append(dict(
                    type='FixtureCleanupFailure', count=len(failures),
                    file=str((args.repository_root / source).resolve()),
                    method='Acceptance.close' if name == 'cleanup_errors' else 'ServiceStack.close'))
    sources = ('services/auth/src/AuthService.cpp', 'services/auth/app/main.cpp',
        'services/lobby/src/LobbyService.cpp', 'services/lobby/app/main.cpp',
        'services/gateway/src/net/RemoteLobbyRoutes.cpp', 'services/gateway/app/main.cpp',
        'services/storage/server.py', 'shared/runtime/rpc.py', 'shared/runtime/protocol.py',
        'shared/src/rpc/Server.cpp', 'shared/src/rpc/Client.cpp', 'shared/src/runtime/RuntimeConfig.cpp',
        'shared/proto/rpc.proto', 'tests/lobby/ServiceStackFixture.py',
        'tests/room/JourneyProtocol.py', 'tests/lobby/ServiceBoundaryProcessTest.py')
    test.report['source_hashes'] = {name: digest(args.repository_root / name) for name in sources}
    test.report['binary_hashes'] = {name: digest(getattr(args, name)) for name in ('auth', 'lobby', 'gateway')}
    test.report['descriptor_sha256'] = digest(args.descriptor)
    test.report['package_hashes'] = {'storage_python_sources': {
        str(path.relative_to(args.repository_root)): digest(path)
        for path in sorted((args.repository_root / 'services/storage').rglob('*.py'))}}
    output = json.dumps(test.report, indent=2)
    matched_fields = secret_field_indices(test.report, test.secrets)
    if matched_fields:
        # Keep the gate closed while making an otherwise opaque rejection
        # diagnosable. No matched key, value, secret or exception args escape.
        print(json.dumps(dict(passed=False, secret_gate_rejected=True,
            matching_field_indices=matched_fields,
            error=test.report.get('error'),
            failure_locations=test.report.get('failure_locations', []))))
    test.assert_no_secrets(output)
    require(not re.search(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|\$arena-pbkdf2-', output),
            'report contained an encoded credential')
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(output + '\n')
    print(json.dumps(dict(passed=test.report['passed'], cases=len(test.report['cases']),
        current_case=test.report.get('current_case'), error=test.report.get('error'), report=str(args.report))))
    return 0 if test.report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())