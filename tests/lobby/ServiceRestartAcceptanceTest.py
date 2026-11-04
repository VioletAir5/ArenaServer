#!/usr/bin/env python3
"""Fixed-denominator Day33 acceptance: 90 real role restarts in 75 fault windows."""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import socket
import struct
import sys
import threading
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ServiceStackFixture import ServiceStack, health_http
from JourneyProtocol import Bot
from OnlinePresenceIntegrationTest import parse, text, scalar
from RoomIntegrationTest import unsigned_field
from WebSocketIntegrationTest import packet, send_frame, ORIGIN
from MessageDispatchIntegrationTest import envelope

ROLES = ('auth', 'lobby', 'gateway')
TRANSACTIONS = ('status', 'input', 'status', 'input', 'ticket',
                'input', 'state', 'input', 'status', 'input')
BASELINE = tuple(f'baseline/{action}/{name}' for action in
                 ('login', 'join', 'assigned', 'ready', 'input') for name in ('alice', 'bob'))
FINAL = tuple(f'final/{action}/{name}' for action in ('status', 'result', 'state')
              for name in ('alice', 'bob'))
RETRYABLE_PUBLIC = {1004, 1005, 1006, 2002}
MAX_FRAME = 256 * 1024


def schedule():
    """The complete schedule is declared before any process is started."""
    result = []
    for index in range(20):
        for role in ROLES:
            result.append(dict(index=len(result) + 1, kind='solo', roles=[role],
                               signal='SIGTERM' if index % 2 == 0 else 'SIGKILL'))
    for index in range(5):
        for pair in (('auth', 'lobby'), ('auth', 'gateway'), ('lobby', 'gateway')):
            result.append(dict(index=len(result) + 1, kind='pair', roles=list(pair),
                               signal='SIGKILL' if index % 2 == 0 else 'SIGTERM'))
    assert len(result) == 75 and sum(len(w['roles']) for w in result) == 90
    assert all(sum(role in w['roles'] for w in result) == 30 for role in ROLES)
    return result


class InvariantFailure(Exception):
    """Messages are fixed public diagnostics, never request bodies or credentials."""


class PublicFailure(InvariantFailure):
    """Preserve a received public code; individual contracts decide its meaning."""
    def __init__(self, code):
        super().__init__('unexpected public error code ' + str(code))
        self.code = code


class TransientFailure(Exception):
    def __init__(self, status):
        super().__init__(status)
        self.status = status


def require(value, diagnostic):
    if not value:
        raise InvariantFailure(diagnostic)


class ObservedBot(Bot):
    """The same public Bot API with bounded I/O and explicit public error decoding."""
    def __init__(self, stack, websocket, deadline):
        self.stack, self.ws, self.closed, self.request_id = stack, websocket, False, 0
        self.deadline = deadline
        port = stack.ports['gateway-ws' if websocket else 'gateway']
        self.socket = socket.create_connection(('127.0.0.1', port), self.remaining())
        try:
            if websocket:
                key = base64.b64encode(os.urandom(16)).decode('ascii')
                request = ('GET /ws HTTP/1.1\r\n' + f'Host: 127.0.0.1:{port}\r\n' +
                    'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
                    f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n' +
                    f'Origin: {ORIGIN}\r\n\r\n')
                self.socket.settimeout(self.remaining())
                self.socket.sendall(request.encode('ascii'))
                reply = bytearray()
                while not reply.endswith(b'\r\n\r\n'):
                    reply.extend(self.exact(1))
                    require(len(reply) <= 8192, 'WebSocket response exceeds bound')
                expected = base64.b64encode(hashlib.sha1((key +
                    '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
                require(bytes(reply).startswith(b'HTTP/1.1 101') and expected in reply,
                        'WebSocket upgrade does not identify the expected handshake')
        except BaseException:
            self.socket.close()
            self.closed = True
            raise

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError('bounded public transport deadline')
        return min(1.0, left)

    def exact(self, size):
        data = bytearray()
        while len(data) < size:
            self.socket.settimeout(self.remaining())
            block = self.socket.recv(size - len(data))
            if not block:
                raise EOFError('public transport ended before complete response')
            data.extend(block)
        return bytes(data)

    def response(self):
        if not self.ws:
            header = self.exact(8)
            magic, size = struct.unpack('!II', header)
            require(magic == 0x4152454E and 7 <= size <= MAX_FRAME, 'invalid bounded TCP frame')
            return header + self.exact(size)
        chunks, total, initial = [], 0, True
        while True:
            first, second = self.exact(2)
            require(not second & 0x80, 'masked server WebSocket frame')
            size = second & 127
            if size == 126:
                size = struct.unpack('!H', self.exact(2))[0]
            elif size == 127:
                size = struct.unpack('!Q', self.exact(8))[0]
            require(size <= MAX_FRAME and total + size <= MAX_FRAME,
                    'WebSocket reply exceeds bounded public frame')
            opcode = first & 15
            if opcode == 8:
                raise EOFError('server closed public WebSocket')
            require(opcode == (2 if initial else 0), 'unexpected WebSocket data opcode')
            chunks.append(self.exact(size)); total += size; initial = False
            if first & 0x80:
                return b''.join(chunks)

    def rpc(self, msg, payload, response_msg=None, error=None):
        require(error is None, 'formal bot exposes typed errors for explicit contract validation')
        self.request_id += 1
        frame = packet(envelope(msg, self.request_id, msg, payload))
        self.socket.settimeout(self.remaining())
        if self.ws:
            send_frame(self.socket, 2, frame)
        else:
            self.socket.sendall(frame)
        frame = self.response()
        require(len(frame) >= 15, 'public response header is incomplete')
        magic, length, kind, wire_id, version = struct.unpack('!IIHIB', frame[:15])
        require((magic, kind, wire_id, version) == (0x4152454E, 2, 7, 1) and
                length == len(frame) - 8, 'public response framing changed')
        reply = parse(frame[15:])
        require(scalar(reply, 1) == self.request_id, 'public request ID was not preserved')
        if scalar(reply, 4) == 14:
            code = scalar(parse(scalar(reply, 14)), 1)
            if code in RETRYABLE_PUBLIC:
                raise TransientFailure('PUBLIC_' + str(code))
            raise PublicFailure(code)
        require(scalar(reply, 4) == response_msg, 'unexpected public response message ID')
        return scalar(reply, response_msg)

    def login_operation(self, operation):
        payload = text(2, operation.credential) + text(5, operation.nonce)
        if operation.jwt:
            payload += unsigned_field(4, 1)
        else:
            payload += text(1, operation.account)
        reply = parse(self.rpc(10, payload, 11))
        require(scalar(reply, 1) == 1, 'login response was not accepted')
        token = scalar(reply, 2, b'')
        require(isinstance(token, bytes) and token, 'login response lacks a bearer token')
        return dict(token=token, player=scalar(reply, 4), epoch=scalar(reply, 5),
                    session=scalar(reply, 6), room=scalar(reply, 7), resumed=scalar(reply, 8))

    def close(self, reset=False):
        if self.closed:
            return
        self.closed = True
        try:
            if reset:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        finally:
            self.socket.close()


@dataclass
class LoginOperation:
    nonce: bytes
    credential: bytes
    account: bytes
    jwt: bool
    before_epoch: int


@dataclass
class Player:
    name: str
    identity: int
    websocket: bool
    peer: ObservedBot | None = None
    grant: dict | None = None
    operation: LoginOperation | None = None
    session: int = 0
    ticket: int = 0
    membership: int = 0
    acknowledged: int = 0
    issued: int = 0
    history_missing_ns: int | None = None
    credential_expiry: int = 0
    login_complete: bool = False
    renewal: dict | None = None


class Acceptance(ServiceStack):
    def __init__(self, args):
        super().__init__(args, battle_ticks=args.battle_ticks, ready_ticks=100000,
                         grace_ms=args.grace_ms)
        self.plan = schedule()
        self.stop_requested = threading.Event()
        self.lock = threading.Lock()
        self.global_deadline = time.monotonic() + args.total_timeout
        self.current_window = 0
        self.players = {name: Player(name, identity, websocket) for name, identity, websocket in
                        (('alice', 42, False), ('bob', 43, True))}
        self.match = self.room = self.incarnation = 0
        self.phase = 'baseline'
        self.frozen_terminal = None
        self.ticket_release_lower_ns = None
        self.ticket_release_upper_ns = None
        self.ticket_retention_ns = 60_000_000_000
        self.history_not_found = self.protocol.pool.FindEnumTypeByName(
            'arena.protocol.ErrorCode').values_by_name['ERR_BUSINESS_FAILURE'].number
        # This listener profile has no queue-policy environment override. Fail
        # closed if its source default changes instead of silently changing the
        # predeclared acceptance policy to fit an observed failure.
        policy_path = args.repository_root / 'services/match/include/match/MatchQueue.h'
        policy_source = policy_path.read_text()
        queue_struct = re.search(r'struct QueueConfig\s*\{(.*?)\n\};', policy_source, re.S)
        require(queue_struct is not None, 'cannot identify the production queue policy')
        retention = re.search(r'std::chrono::milliseconds\s+retention\s*\{\s*(\d+)\s*\}', queue_struct.group(1))
        require(retention is not None and int(retention.group(1)) == 60000,
                'production queue retention differs from the declared 60-second contract')
        self.expected_stopping = set()
        self.pid_birth = {}
        logical_ids = list(BASELINE)
        for window in self.plan:
            number = window['index']
            logical_ids += [f'window/{number}/reconnect/{name}' for name in self.players]
            logical_ids += [f'window/{number}/transaction/{name}/{i}'
                            for name in self.players for i in range(10)]
        logical_ids += list(FINAL)
        require(len(logical_ids) == 1666 and len(set(logical_ids)) == 1666,
                'fixed logical denominator changed')
        self.logical = {identity: dict(id=identity, status='not_started', attempts=0) for identity in logical_ids}
        for identity, operation in self.logical.items():
            if identity.startswith('final/status/'):
                operation['credential_contract'] = ('When at most 60 seconds remain on the signed 300-second credential, '
                    'complete a real TOKEN handoff before expiry with the same session/room/membership and acknowledged inputs; '
                    'every failed transport, login or verification attempt remains a raw failure.')
            if '/transaction/' in identity and identity.endswith('/4'):
                operation['contract'] = 'bounded matched-ticket retention plus current room/session/ACK continuity'
        self.report.update(scope='Day33 fixed 90-restart acceptance with six real processes and two Room bystanders',
            plan_version='day33-fixed-1666-validated-results-strict-failure-deadline-v5',
            schedule=self.plan, logical_denominator=1666, logical_operations=list(self.logical.values()),
            raw_attempts=[], failed_attempt_deadline_violations=[], fault_windows=[], role_restarts=[], resource_curve=[], wait_graph_samples=[],
            token_renewals=[], token_renewal_policy=dict(lifetime_seconds=300, before_expiry_seconds=60,
                max_attempts=40, raw_attempt_timeout_ms=1000, fixed_logical_denominator=1666,
                operation='Credential maintenance is a required sub-contract of its original business operation; '
                          'it does not add successes to the availability denominator.'),
            final_phase_observations=[], ticket_history_observations=[], ticket_history_policy=dict(
                retention_ms=60000, clock='same-host CLOCK_MONOTONIC nanoseconds',
                source='services/match/include/match/MatchQueue.h::QueueConfig::retention',
                policy_sha256=hashlib.sha256(policy_path.read_bytes()).hexdigest(),
                release_source='LobbyAuthority::advance -> MatchQueue::release(reclaimFrom=now)',
                status_source='MatchQueue::status maintains requested history before returning',
                public_missing_code=self.history_not_found, public_missing_name='ERR_BUSINESS_FAILURE',
                domain_missing_name='QueueError::NotFound',
                operation='same matched ticket before TTL; bounded history NotFound after TTL; room and ACK state always unchanged',
                lower_bound='immediately before issuing the second player join',
                upper_bound='immediately after the first Matched-with-room status reply',
                ambiguous_interval='Either outcome is permitted only when its owner cutoff can lie inside the proven expiry interval',
                no_early_missing=True, no_retention_renewal=True, no_ticket_resurrection=True),
            unexpected_exits=[], sanitizer_diagnostics=[], coverage=dict(
                independently_tested_not_counted_in_90=['normal RPC contract', 'explicit timeout and cancellation',
                    'public errors', 'incompatible version', 'call cycle', 'connect delay'],
                fault_restarts=90, fault_windows=75, simultaneous_pair_windows=15,
                transactions_per_window=20, reconnects_per_window=2,
                production_retry_policy_overridden=False),
            limitations=['SIGKILL cannot run a process heap-leak checker; graceful exits, sanitizer output, '
                         'replacement process descriptor curves and final parent FD recovery are separate evidence.',
                         'Owned-socket port observations identify service edges, not individual RPC methods.',
                         'The logical denominator is declared before startup; failed/unexecuted operations remain in it.'])

    def check_time(self, deadline):
        if self.stop_requested.is_set() or time.monotonic() >= min(deadline, self.global_deadline):
            raise InvariantFailure('acceptance or logical operation deadline exhausted')

    def check_processes(self):
        for role, child in self.children.items():
            if role in self.expected_stopping:
                continue
            if child.process is None or child.process.poll() is not None:
                row = dict(role=role, pid=child.process.pid if child.process else None,
                           window=self.current_window)
                self.report['unexpected_exits'].append(row)
                raise InvariantFailure('a spectator or unselected service exited unexpectedly')

    def raw(self, logical, kind, deadline, action):
        self.check_time(deadline); self.check_processes()
        started = time.monotonic()
        row = dict(logical=logical, window=self.current_window, phase=self.phase, kind=kind,
                   started=started, status='started')
        try:
            result = action(min(deadline, started + 1.0))
            row['status'] = 'OK'
            return result
        except TransientFailure as error:
            row['status'] = error.status
            raise
        except PublicFailure as error:
            row['status'] = 'PUBLIC_' + str(error.code)
            raise
        except (OSError, EOFError, TimeoutError) as error:
            row['status'] = type(error).__name__
            raise TransientFailure(row['status']) from None
        except Exception:
            row['status'] = 'INVARIANT_FAILURE'
            raise
        finally:
            row['elapsed_ms'] = (time.monotonic() - started) * 1000
            with self.lock:
                self.report['raw_attempts'].append(row)
                if row['status'] != 'OK' and row['elapsed_ms'] > 1000:
                    self.report['failed_attempt_deadline_violations'].append(dict(
                        logical=logical, window=self.current_window, kind=kind,
                        status=row['status'], elapsed_ms=row['elapsed_ms']))
            if row['status'] != 'OK' and row['elapsed_ms'] > 1000:
                # Preserve the actual status and attempt above; exceeding the
                # hard observed bound cannot become an ordinary retry.
                raise InvariantFailure('failed raw attempt exceeded the strict one-second gate')

    def retry(self, logical, deadline, action):
        row = self.logical[logical]
        if row['status'] == 'not_started':
            row.update(status='running', started=time.monotonic())
        rng = random.Random(int(hashlib.sha256(logical.encode()).hexdigest()[:16], 16))
        for index in range(40):
            self.check_time(deadline)
            row['attempts'] += 1
            try:
                result = action(deadline)
                row.update(status='succeeded', finished=time.monotonic())
                return result
            except TransientFailure:
                left = min(deadline, self.global_deadline) - time.monotonic()
                if left <= 0:
                    break
                delay = rng.uniform(0, min(.4, .02 * 2 ** min(index, 6), left))
                time.sleep(delay)
            except Exception:
                row.update(status='failed', finished=time.monotonic())
                raise
        row.update(status='failed', finished=time.monotonic())
        raise InvariantFailure('finite logical retry budget exhausted')

    def open_peer(self, player, logical, deadline):
        if player.peer is not None and not player.peer.closed:
            return player.peer
        def connect(end):
            peer = ObservedBot(self, player.websocket, end)
            self.peers.append(peer)
            player.peer = peer
            return peer
        return self.raw(logical, 'connect_ws' if player.websocket else 'connect_tcp', deadline, connect)

    def close_peer(self, player):
        if player.peer is not None:
            try:
                player.peer.close(reset=True)
            except OSError:
                pass
            player.peer = None

    def ack_floor(self):
        with self.lock:
            return {player.identity: player.acknowledged for player in self.players.values()}

    @staticmethod
    def control_for(player, sequence):
        dx, dy = ((1, 0), (0, 1), (-1, 0), (0, -1))[(sequence - 1) % 4]
        return (dx, dy) if player == 42 else (-dx, -dy)

    @classmethod
    def position_for(cls, player, sequence):
        dx = dy = 0
        for i in range(1, sequence % 4 + 1):
            x, y = cls.control_for(player, i)
            dx += x; dy += y
        return dx, dy

    def verify_world(self, state, floor, *, final=False, settling=False):
        snapshot = state['snapshot']
        require(state['match'] == self.match and snapshot['room'] == self.room and
                snapshot['incarnation'] == self.incarnation, 'room, match or actor incarnation changed')
        require(not (final and settling), 'terminal room validation requested conflicting phases')
        if final or settling:
            require(self.phase == 'settlement', 'a restart-stage room stopped Playing')
        require(state['phase'] == (5 if final else 4 if settling else 3),
                'the long-running match ended before its planned boundary')
        if final or settling:
            require(state['played'] == state['duration'] == self.args.battle_ticks,
                    'natural settlement did not consume the exact declared battle duration')
            if settling:
                require('settlement' not in state, 'Settling exposed a result before the external durable ACK')
            else:
                require('settlement' in state and state['settlement']['reason'] == 1 and
                        state['settlement']['match'] == self.match and
                        state['settlement']['room'] == self.room and
                        state['settlement']['tick'] == snapshot['tick'] and
                        state['settlement']['checksum'] == snapshot['checksum'],
                        'terminal room has an early abort or inconsistent frozen result')
            frozen = dict(room=snapshot['room'], incarnation=snapshot['incarnation'],
                          tick=snapshot['tick'], checksum=snapshot['checksum'],
                          played=state['played'], duration=state['duration'])
            # Both bots must retain the same cutoff while PostgreSQL commits.
            # A direct Finished observation still requires its own durable ACK.
            with self.lock:
                if self.frozen_terminal is not None:
                    require(frozen == self.frozen_terminal, 'terminal snapshot changed after Settling began')
                elif settling:
                    self.frozen_terminal = frozen
                    self.report['frozen_terminal_observation'] = dict(frozen, observed_at=time.monotonic())
        require(set(snapshot['players']) == {42, 43}, 'fixed match roster changed')
        with self.lock:
            issued = {p.identity: p.issued for p in self.players.values()}
        for player in self.players.values():
            member = snapshot['players'][player.identity]
            require(member['session'] == player.session and member['epoch'] == player.membership,
                    'session or room membership generation changed')
            require(floor[player.identity] <= member['seq'] <= issued[player.identity],
                    'an acknowledged sequence disappeared or an unissued sequence appeared')
            require((member['x'], member['y']) == self.position_for(player.identity, member['seq']),
                    'input state diverged from the deterministic issued log')
            if member['seq']:
                require((member['dx'], member['dy']) == self.control_for(player.identity, member['seq']),
                        'last input controls diverged from the ordered log')

    def credential_expiry(self, grant):
        """Validate issuer output without retaining credentials in evidence."""
        try:
            token = grant['token'].decode('ascii')
            parts = token.split('.')
            require(len(parts) == 3 and len(token) <= 4096, 'invalid credential encoding')
            def decode(value):
                return base64.urlsafe_b64decode(value + '=' * ((4 - len(value) % 4) % 4))
            signature = hmac.new(self.signer_key.encode(), '.'.join(parts[:2]).encode(), hashlib.sha256).digest()
            require(hmac.compare_digest(decode(parts[2]), signature), 'credential signature mismatch')
            header, payload = (json.loads(decode(value)) for value in parts[:2])
            require(header == dict(alg='HS256', typ='JWT') and
                    payload.get('iss') == 'arena-auth' and payload.get('aud') == 'arena-gateway' and
                    payload.get('sub') == str(grant['player']) and payload.get('epoch') == str(grant['epoch']),
                    'credential identity or profile changed')
            require(all(type(payload.get(key)) is int for key in ('iat', 'nbf', 'exp')) and
                    payload['iat'] == payload['nbf'] <= time.time() < payload['exp'] and
                    payload['exp'] - payload['iat'] == 300, 'production credential lifetime changed')
            return payload['exp']
        except InvariantFailure:
            raise
        except (KeyError, TypeError, ValueError, UnicodeError):
            raise InvariantFailure('invalid credential issuer output') from None

    @staticmethod
    def begin_token_login(player):
        player.operation = LoginOperation(os.urandom(16).hex().encode(), player.grant['token'],
                                          b'', True, player.grant['epoch'])
        player.login_complete = False

    def renew_due(self, player, logical, deadline, *, forced=False):
        now = time.time()
        if player.renewal is None:
            if not forced and player.credential_expiry - now > 60:
                return
            require(player.credential_expiry > now, 'credential renewal started after expiry')
            player.renewal = dict(logical=logical, player=player.identity, websocket=player.websocket,
                started=time.monotonic(), previous_expiry=player.credential_expiry,
                previous_epoch=player.grant['epoch'], attempts=0, status='running', forced=forced)
            with self.lock:
                self.report['token_renewals'].append(player.renewal)
            self.begin_token_login(player)
            self.close_peer(player)
        row = player.renewal
        require(row['attempts'] < 40, 'credential renewal exhausted its finite attempt budget')
        row['attempts'] += 1
        # Keep this proof and nonce until login AND its room verification complete.
        # An unknown ACK must not turn into a second logical login operation.
        self.login_once(player, logical, deadline)
        require(player.credential_expiry >= row['previous_expiry'] and
                (row['forced'] or player.credential_expiry > row['previous_expiry']),
                'credential renewal did not refresh expiry')
        row.update(status='succeeded', finished=time.monotonic(), renewed_expiry=player.credential_expiry,
            renewed_epoch=player.grant['epoch'], logical_session_preserved=True,
            room_membership_and_acknowledged_inputs_verified=True)
        player.renewal = None

    def login_once(self, player, logical, deadline, *, baseline=False):
        if player.login_complete and not baseline:
            self.begin_token_login(player)
        try:
            peer = self.open_peer(player, logical, deadline)
            peer.deadline = min(deadline, time.monotonic() + 1)
            grant = self.raw(logical, 'login', deadline, lambda end:
                             self.with_deadline(peer, end, lambda: peer.login_operation(player.operation)))
            require(grant['player'] == player.identity, 'Auth resolved the wrong account identity')
            if baseline:
                require(grant['room'] == 0 and grant['session'] > 0 and
                        (not player.session or player.session == grant['session']),
                        'the initial password operation changed its logical session')
                require(all(other is player or not other.session or other.session != grant['session']
                            for other in self.players.values()),
                        'two accounts share a logical session')
                player.session = grant['session']
            else:
                allowed_rooms = (self.room, 0) if self.phase == 'settlement' else (self.room,)
                require(grant['resumed'] and grant['session'] == player.session and grant['room'] in allowed_rooms and
                        grant['epoch'] > player.operation.before_epoch,
                        'reconnect fabricated a fresh session or lost its room binding')
            player.credential_expiry = self.credential_expiry(grant)
            player.grant = grant
            with self.lock:
                self.secrets = (*self.secrets, grant['token'].decode('ascii'))
            if not baseline:
                floor = self.ack_floor()
                state = self.raw(logical, 'room.status', deadline, lambda end:
                    self.with_deadline(peer, end, peer.status))
                self.verify_world(state, floor,
                    final=self.phase == 'settlement' and state['phase'] == 5,
                    settling=self.phase == 'settlement' and state['phase'] == 4)
            player.login_complete = True
            return grant
        except TransientFailure:
            self.close_peer(player)
            raise

    @staticmethod
    def with_deadline(peer, end, action):
        peer.deadline = end
        return action()

    def business(self, player, logical, kind, deadline, action):
        try:
            self.renew_due(player, logical, deadline)
            if player.peer is None or player.peer.closed:
                self.login_once(player, logical, deadline)
            return self.raw(logical, kind, deadline, lambda end:
                self.with_deadline(player.peer, end, lambda: action(player.peer)))
        except TransientFailure:
            self.close_peer(player)
            raise

    def status(self, player, logical, deadline, *, final=False):
        floor = self.ack_floor()
        state = self.business(player, logical, 'room.status', deadline, lambda peer: peer.status())
        self.verify_world(state, floor, final=final)
        return state

    def input(self, player, logical, deadline):
        with self.lock:
            sequence = player.acknowledged + 1
            player.issued = max(player.issued, sequence)
        dx, dy = self.control_for(player.identity, sequence)
        state = self.status(player, logical, deadline)
        floor = self.ack_floor()
        snapshot, result = self.business(player, logical, 'room.input', deadline,
            lambda peer: peer.control(state['snapshot'], player.identity, sequence, dx, dy))
        require(result in (1, 2), 'stable ordered input was neither applied nor its exact duplicate')
        state['snapshot'] = snapshot
        self.verify_world(state, floor)
        require(snapshot['players'][player.identity]['seq'] == sequence,
                'input acknowledgement did not include its exact committed sequence')
        with self.lock:
            player.acknowledged = sequence
        return snapshot
    def ticket_history(self, player, logical, deadline):
        """Assert finite historical-ticket semantics without weakening room continuity."""
        require(self.ticket_release_lower_ns is not None and self.ticket_release_upper_ns is not None,
                'ticket history has no measured match-time bounds')
        earliest = self.ticket_release_lower_ns + self.ticket_retention_ns
        latest = self.ticket_release_upper_ns + self.ticket_retention_ns
        observation = dict(logical=logical, player=player.identity, ticket=player.ticket,
            window=self.current_window, earliest_expiry_ns=earliest, latest_expiry_ns=latest,
            public_code=None, validated=False)
        def lookup(peer):
            observation['started_ns'] = time.monotonic_ns()
            try:
                return peer.match_status(player.ticket)
            finally:
                observation['finished_ns'] = time.monotonic_ns()
        try:
            try:
                ticket = self.business(player, logical, 'match.status', deadline, lookup)
            except PublicFailure as error:
                observation.update(public_code=error.code, outcome='public_error')
                require(error.code == self.history_not_found,
                        'ticket history returned an unrelated public error')
                require(observation['finished_ns'] >= earliest,
                        'ticket history disappeared before its earliest legal expiry')
                observation['outcome'] = 'expired_history_not_found'
                if player.history_missing_ns is None:
                    player.history_missing_ns = observation['finished_ns']
            else:
                observation.update(public_code=0, outcome='matched_history')
                require(player.history_missing_ns is None, 'reclaimed ticket history resurrected after restart')
                require(observation['started_ns'] < latest,
                        'ticket history remained queryable beyond its latest legal expiry')
                require(scalar(ticket, 2) == player.ticket and scalar(ticket, 4) == 3 and
                        scalar(ticket, 9) == self.match and scalar(ticket, 13) == self.room and
                        scalar(ticket, 1) == player.identity and not scalar(ticket, 12) and
                        scalar(ticket, 3) == (101 if player.identity == 42 else 201) and
                        scalar(ticket, 10) == (43 if player.identity == 42 else 42) and
                        scalar(ticket, 11) == self.players['bob' if player.name == 'alice' else 'alice'].ticket,
                        'retained ticket changed its immutable assignment')
            # The owner cutoff lies between the actual request start and reply.
            # Never infer a lifetime from an assumed restart or network delay.
            observation['time_region'] = ('definitely_before_expiry'
                if observation['finished_ns'] < earliest else 'definitely_after_expiry'
                if observation['started_ns'] >= latest else 'bounded_ambiguous_interval')
            self.status(player, logical, deadline)
            current = self.business(player, logical, 'session.state', deadline, lambda peer: peer.query())
            require(current['session'] == player.session and current['room'] == self.room and
                    set(current['members']) == {42, 43},
                    'historical ticket reclamation changed the current session or room')
            observation['validated'] = True
            return observation
        finally:
            with self.lock:
                self.report['ticket_history_observations'].append(observation)

    def baseline(self):
        deadline = min(self.global_deadline, time.monotonic() + 30)
        for player in self.players.values():
            player.operation = LoginOperation(os.urandom(16).hex().encode(), self.password.encode(),
                                              player.name.encode(), False, 0)
            self.retry(f'baseline/login/{player.name}', deadline,
                       lambda end, p=player: self.login_once(p, f'baseline/login/{p.name}', end, baseline=True))
        require(self.players['alice'].session != self.players['bob'].session, 'two accounts share a logical session')
        canonical_mmr = {int(p):int(mmr) for p,mmr in self.query('SELECT player_id,mmr FROM arena.player WHERE player_id IN (42,43)')}
        self.initial_inventory = {int(p): int(quantity) for p, quantity in self.query(
            'SELECT player_id,quantity FROM arena.inventory WHERE player_id IN (42,43) AND item_id=1')}
        for player in self.players.values():
            logical = f'baseline/join/{player.name}'
            if player.name == 'bob':
                self.ticket_release_lower_ns = time.monotonic_ns()
            def join(end, p=player, key=logical):
                join_key = 101 if p.identity == 42 else 201
                ticket = self.business(p, key, 'match.join', end, lambda peer: peer.match_join(join_key))
                require(scalar(ticket, 1) == p.identity and scalar(ticket, 3) == join_key and
                        scalar(ticket, 4) in (1, 3), 'match join returned another identity, key or lifecycle')
                require(scalar(ticket, 5) == canonical_mmr[p.identity],
                        'ticket rating did not come from actual canonical PostgreSQL player data')
                require(scalar(ticket, 2) > 0 and (not p.ticket or p.ticket == scalar(ticket, 2)),
                        'match join has no stable server-owned ticket')
                p.ticket = scalar(ticket, 2)
                return ticket
            self.retry(logical, deadline, join)
        for player in self.players.values():
            logical = f'baseline/assigned/{player.name}'
            def assigned(end, p=player, key=logical):
                ticket = self.business(p, key, 'match.status', end, lambda peer: peer.match_status(p.ticket))
                if scalar(ticket, 4) != 3 or not scalar(ticket, 13):
                    raise TransientFailure('WAIT_ASSIGNED')
                if self.ticket_release_upper_ns is None:
                    self.ticket_release_upper_ns = time.monotonic_ns()
                    require(self.ticket_release_lower_ns is not None and
                            self.ticket_release_lower_ns <= self.ticket_release_upper_ns,
                            'matched ticket has no ordered release-time bounds')
                    self.report['ticket_history_policy'].update(
                        release_lower_ns=self.ticket_release_lower_ns,
                        release_upper_ns=self.ticket_release_upper_ns,
                        earliest_expiry_ns=self.ticket_release_lower_ns + self.ticket_retention_ns,
                        latest_expiry_ns=self.ticket_release_upper_ns + self.ticket_retention_ns)
                match, room = scalar(ticket, 9), scalar(ticket, 13)
                if self.match:
                    require((match, room) == (self.match, self.room), 'two tickets have different assignments')
                self.match, self.room = match, room
                state = self.business(p, key, 'room.status', end, lambda peer: peer.status())
                require(state['match'] == self.match and set(state['snapshot']['players']) == {42, 43},
                        'initial room has an inconsistent fixed cohort')
                self.incarnation = state['snapshot']['incarnation']
                for participant in self.players.values():
                    member = state['snapshot']['players'][participant.identity]
                    require(member['session'] == participant.session and member['seq'] == 0,
                            'initial room did not use its authenticated logical sessions')
                    participant.membership = member['epoch']
                return state
            self.retry(logical, deadline, assigned)
        for player in self.players.values():
            logical = f'baseline/ready/{player.name}'
            def ready(end, p=player, key=logical):
                state = self.business(p, key, 'room.status', end, lambda peer: peer.status())
                accepted = self.business(p, key, 'room.ready', end, lambda peer: peer.ready(state, p.identity))
                require(accepted['match'] == self.match and accepted['snapshot']['room'] == self.room and
                        accepted['snapshot']['incarnation'] == self.incarnation and accepted['phase'] in (1, 2, 3) and
                        set(accepted['members']) == {42, 43} and accepted['members'][p.identity]['ready'],
                        'readiness acknowledgement did not apply to the original room cohort')
                require(all(accepted['members'][other.identity]['session'] == other.session and
                            accepted['members'][other.identity]['epoch'] == other.membership
                            for other in self.players.values()),
                        'readiness acknowledgement changed a member generation')
                return accepted
            self.retry(logical, deadline, ready)
        for player in self.players.values():
            logical = f'baseline/input/{player.name}'
            def first_input(end, p=player, key=logical):
                state = self.business(p, key, 'room.status', end, lambda peer: peer.status())
                if state['phase'] != 3:
                    raise TransientFailure('WAIT_PLAYING')
                return self.input(p, key, end)
            self.retry(logical, deadline, first_input)
        self.report['baseline'] = dict(match_id=self.match, room_id=str(self.room),
            incarnation=self.incarnation, sessions={p.name: p.session for p in self.players.values()},
            acknowledged={p.name: p.acknowledged for p in self.players.values()})

    def socket_table(self):
        result = {}
        for name in ('tcp', 'tcp6'):
            for line in Path('/proc/net/' + name).read_text().splitlines()[1:]:
                fields = line.split()
                if len(fields) >= 10:
                    result[fields[9]] = dict(family=name, local_port=int(fields[1].split(':')[1], 16),
                                             remote_port=int(fields[2].split(':')[1], 16), state=fields[3])
        return result

    @staticmethod
    def numeric_metrics(value, prefix=''):
        result = {}
        if isinstance(value,dict):
            for key,child in value.items():
                result.update(Acceptance.numeric_metrics(child,prefix + ('/' if prefix else '') + str(key)))
        elif isinstance(value,list):
            for index,child in enumerate(value):
                result.update(Acceptance.numeric_metrics(child,prefix + '/' + str(index)))
        elif isinstance(value,(bool,int,float)) and (not isinstance(value,float) or math.isfinite(value)):
            result[prefix] = value
        return result

    def resources(self, label, *, allow_missing=False):
        table = self.socket_table()
        sampled, edges, waits = {}, [], []
        for role, child in self.children.items():
            process = child.process
            if process is None or process.poll() is not None:
                require(allow_missing and role in self.expected_stopping, 'live resource sample has a missing role')
                sampled[role] = dict(pid=process.pid if process else None, exited=True)
                continue
            pid = process.pid
            descriptors = list(Path(f'/proc/{pid}/fd').iterdir())
            owned = []
            for descriptor in descriptors:
                try:
                    link = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                if link.startswith('socket:['):
                    owned.append(link[8:-1])
            rss = int(Path(f'/proc/{pid}/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
            birth = int(Path(f'/proc/{pid}/stat').read_text().rpartition(')')[2].split()[19])
            self.pid_birth[pid] = birth
            sampled[role] = dict(pid=pid, start_ticks=birth, fd=len(descriptors), sockets=len(owned), rss_bytes=rss)
            try:
                health_status,health = health_http(self.ports[role+'-health'])
                self.assert_no_secrets(json.dumps(health))
                sampled[role].update(health_http_status=health_status,numeric_health=self.numeric_metrics(health))
            except (OSError,TimeoutError,ValueError) as error:
                sampled[role]['health_sample_error'] = type(error).__name__
            for inode in owned:
                entry = table.get(inode)
                if entry:
                    target = next((r for r in self.children if self.ports[r] == entry['remote_port']), None)
                    if target:
                        edges.append(dict(source=role, target=target, state=entry['state'],
                            evidence='owned socket inode and remote configured service port'))
            for task in Path(f'/proc/{pid}/task').iterdir():
                try:
                    waits.append(dict(role=role,pid=pid,tid=int(task.name),
                        comm=(task/'comm').read_text().strip(),wchan=(task/'wchan').read_text().strip()))
                except FileNotFoundError:
                    pass
        at = time.monotonic()
        self.report['resource_curve'].append(dict(label=label,window=self.current_window,at=at,roles=sampled))
        self.report['wait_graph_samples'].append(dict(label=label,at=at,
            service_edges=edges,thread_wait_points=waits,
            distinction='OS socket/wchan observations; no RPC-method or deadlock inference'))
        return sampled

    def stop_selected(self, window, row, deadline):
        roles = window['roles']
        sig = getattr(signal, window['signal'])
        targets = [(role,self.children[role],self.children[role].process) for role in roles]
        for role,child,process in targets:
            if process is None or process.poll() is not None:
                self.report['unexpected_exits'].append(dict(role=role,pid=process.pid if process else None,
                                                            window=self.current_window,phase='before_signal'))
                raise InvariantFailure('target exited before its injected fault')
        self.expected_stopping = set(roles)
        signaled = []
        for role,child,process in targets:
            # All liveness preflight is complete before the first signal.
            # These unreaped owned PIDs cannot be reused between kill calls.
            os.kill(process.pid,sig)
            event = dict(role=role,pid=process.pid,signal=window['signal'],at=time.monotonic())
            signaled.append((child,process,event)); row['signals'].append(event)
        require(len(signaled) == len(roles), 'not all pair members received their signal')
        # Both signals above happen before any wait/poll barrier or replacement.
        row['all_signals_sent_at'] = time.monotonic()
        exit_deadline = min(deadline, self.global_deadline, time.monotonic() + 10)
        while any(process.poll() is None for _,process,_ in signaled):
            self.check_time(exit_deadline); self.check_processes(); time.sleep(.005)
        self.resources('all-targets-down', allow_missing=True)
        failed = 0
        for player in self.players.values():
            logical = f'window/{window["index"]}/reconnect/{player.name}'
            self.logical[logical].update(status='running',started=row['started'])
            self.logical[logical]['attempts'] += 1
            self.close_peer(player)
            try:
                self.login_once(player, logical, deadline)
            except TransientFailure:
                failed += 1
            else:
                raise InvariantFailure('a new login succeeded while its required real service was down')
        require(failed == 2, 'fault window lacks both real failed reconnect attempts')
        row['raw_failed_reconnects_while_down'] = failed
        for child, process, event in signaled:
            joined = time.monotonic()
            require(joined >= row['all_signals_sent_at'], 'pair join preceded the second injected signal')
            process.wait(timeout=max(.01, min(10, deadline - joined)))
            expected = -signal.SIGKILL if sig == signal.SIGKILL else 0
            if process.returncode != expected:
                self.report['unexpected_exits'].append(dict(role=child.role,pid=process.pid,
                    window=self.current_window,exit_code=process.returncode,expected_exit_code=expected))
                raise InvariantFailure('selected role exited with an uninjected failure code')
            child.process = None
            if child.stream is not None:
                child.stream.close()
            contents = child.log_paths[-1].read_text(errors='replace')
            self.assert_no_secrets(contents)
            counters = self.stop_counters(child.role, contents) if sig == signal.SIGTERM else None
            self.report['shutdowns'].append(dict(role=child.role,pid=process.pid,
                crash_injected=sig == signal.SIGKILL,exit_code=process.returncode,counters=counters))
            restart = dict(window=window['index'],role=child.role,old_pid=process.pid,
                signal=window['signal'],signal_at=event['at'],joined_at=time.monotonic(),
                exit_code=process.returncode,graceful_counters=counters,completed=False)
            self.report['role_restarts'].append(restart)
        self.expected_stopping.clear()

    @staticmethod
    def stop_counters(role, contents):
        if role == 'gateway':
            require('LOBBY_COUNTS tracked=0 business_inflight=0 heartbeat_inflight=0 probe_inflight=0 owner_posts=0 stopped=1' in contents,
                    'Gateway did not release actual remote jobs')
            require('connection_admission active=0 ' in contents, 'Gateway retained physical connections')
            return dict(tracked=0,business_inflight=0,heartbeat_inflight=0,probe_inflight=0,owner_posts=0,stopped=True)
        rows = []
        for line in contents.splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
        require(rows and rows[-1].get('state') == 'stopped', 'normal selected stop has no final physical counters')
        last = rows[-1]
        fields = ('active','queued','busy','workers') if role == 'auth' else (
            'active','auth_inflight','storage_inflight','room_inflight','journal_pending','owner_pending','coordinator_pending')
        require(all(last.get(key) == 0 for key in fields), 'normal selected stop retained actual jobs')
        if role == 'lobby':
            require(last.get('journal_closed') is True and last.get('local_room_actors') == 0,
                    'Lobby retained a physical journal worker or local room kernel')
        return {key:last[key] for key in fields}

    def window(self, window):
        self.current_window, self.phase = window['index'], 'fault_window'
        before = self.sample(f'window-{self.current_window}-before')
        self.resources('before-fault')
        deadline = min(self.global_deadline, time.monotonic() + self.args.window_timeout)
        row = dict(index=self.current_window,roles=window['roles'],signal=window['signal'],
                   started=time.monotonic(),signals=[],passed=False,logical_count=22)
        self.report['fault_windows'].append(row)
        for player in self.players.values():
            # The original proof and nonce stay unchanged through unknown-ACK
            # retries until that login and its room verification complete.
            self.begin_token_login(player)
        self.stop_selected(window,row,deadline)
        for role in ROLES:
            if role in window['roles']:
                self.check_time(deadline)
                started = time.monotonic(); self.children[role].start()
                restart = next(item for item in reversed(self.report['role_restarts'])
                               if item['window'] == self.current_window and item['role'] == role)
                restart.update(new_pid=self.children[role].process.pid,start_at=started,ready_at=time.monotonic(),completed=True)
                require(restart['new_pid'] != restart['old_pid'], 'restart did not replace the actual PID')
        for player in self.players.values():
            logical = f'window/{self.current_window}/reconnect/{player.name}'
            self.retry(logical,deadline,lambda end,p=player,key=logical: self.login_once(p,key,end))
        row['reconnected_at'] = time.monotonic()
        self.phase = 'post_fault_transactions'
        def transactions(player):
            for index, kind in enumerate(TRANSACTIONS):
                logical = f'window/{self.current_window}/transaction/{player.name}/{index}'
                def action(end, p=player, mode=kind, key=logical):
                    if mode == 'input':
                        return self.input(p,key,end)
                    if mode == 'status':
                        return self.status(p,key,end)
                    if mode == 'ticket':
                        return self.ticket_history(p, key, end)
                    state = self.business(p,key,'session.state',end,lambda peer:peer.query())
                    require(state['session'] == p.session and state['room'] == self.room and
                            set(state['members']) == {42,43}, 'current session/room projection changed')
                    return state
                self.retry(logical,deadline,action)
        with ThreadPoolExecutor(max_workers=2,thread_name_prefix='restart-bot') as workers:
            futures = [workers.submit(transactions,p) for p in self.players.values()]
            for future in futures:
                future.result(timeout=max(.01,deadline-time.monotonic()))
        after = self.sample(f'window-{self.current_window}-after')
        require(all(after[role] == before[role] for role in self.children if role not in window['roles']),
                'a selected fault restarted a spectator or Storage')
        self.check_processes(); self.resources('after-recovery')
        row.update(passed=True,finished=time.monotonic(),before_pids=before,after_pids=after,
            acknowledged={p.name:p.acknowledged for p in self.players.values()})

    def final_status(self, player, logical, deadline):
        row = self.logical[logical]; row.update(status='running',started=time.monotonic())
        max_polls = math.ceil(self.args.final_timeout / .2) + 2
        last_phase = 3
        for _ in range(max_polls):
            self.check_time(deadline); row['attempts'] += 1
            try:
                floor = self.ack_floor()
                state = self.business(player,logical,'room.status',deadline,lambda peer:peer.status())
                phase = state['phase']
                observation = dict(logical=logical, player=player.identity, at=time.monotonic(),
                    phase=phase, phase_name={3:'Playing',4:'Settling',5:'Finished'}.get(phase,'Unexpected'),
                    tick=state['snapshot']['tick'], played=state['played'], duration=state['duration'],
                    reason=state.get('settlement',{}).get('reason'))
                row.update(last_phase=phase, last_tick=observation['tick'], last_played=state['played'])
                if phase != last_phase:
                    with self.lock:
                        self.report['final_phase_observations'].append(observation)
                require(phase in (3,4,5) and phase >= last_phase,
                        'natural completion regressed or entered an undeclared room phase')
                self.verify_world(state,floor,final=phase == 5,settling=phase == 4)
                last_phase = phase
                if phase == 5:
                    # Finished is published after the external durable ACK. Verify
                    # its actual PostgreSQL facts before crediting this operation.
                    facts = self.query('SELECT m.state,m.final_tick,m.final_checksum,s.settlement_id,s.state '
                        'FROM arena.game_match m LEFT JOIN arena.settlement s ON s.match_id=m.match_id '
                        'WHERE m.match_id=%s',(self.match,))
                    require(len(facts) == 1 and facts[0][0] == 'completed' and
                            int(facts[0][1]) == observation['tick'] and
                            int(facts[0][2]) == state['snapshot']['checksum'] and
                            int(facts[0][3]) == self.match and facts[0][4] == 'committed',
                            'Finished has no matching actual PostgreSQL settlement ACK')
                    row.update(status='succeeded',finished=time.monotonic(),postgres_ack_verified=True)
                    return state
            except TransientFailure:
                pass
            time.sleep(min(.2,max(0,deadline-time.monotonic())))
        row.update(status='failed',finished=time.monotonic())
        raise InvariantFailure('natural bounded match completion did not publish a durable result')

    def settlement(self):
        self.phase = 'settlement'; deadline = min(self.global_deadline,time.monotonic()+self.args.final_timeout)
        with ThreadPoolExecutor(max_workers=2,thread_name_prefix='final-bot') as workers:
            futures = {p.name:workers.submit(self.final_status,p,f'final/status/{p.name}',deadline)
                       for p in self.players.values()}
            states = {name:future.result(timeout=max(.01,deadline-time.monotonic())) for name,future in futures.items()}
        results = {}
        for player in self.players.values():
            logical = f'final/result/{player.name}'
            def verified_result(end, p=player, key=logical):
                result = self.business(p,key,'settlement.get',end,lambda peer:peer.result(self.match))
                require(result == states[p.name]['settlement'], 'terminal public projections disagree')
                require(result['match'] == self.match and result['reason'] == 1,
                        'formal match did not complete normally through all 90 restarts')
                require(not results or result == next(iter(results.values())),
                        'players received different final results')
                return result
            results[player.name] = self.retry(logical,deadline,verified_result)
            logical = f'final/state/{player.name}'
            def verified_state(end, p=player, key=logical):
                state = self.business(p,key,'session.state',end,lambda peer:peer.query())
                require(state['session'] == p.session and state['player'] == p.identity and state['room'] == 0,
                        'real settlement left a dangling room membership')
                return state
            self.retry(logical,deadline,verified_state)
        require(results['alice'] == results['bob'], 'players received different final results')
        result = results['alice']
        require(result['match'] == self.match and result['reason'] == 1,
                'formal match did not complete normally through all 90 restarts')
        rows = self.query('SELECT state,final_tick,final_checksum FROM arena.game_match WHERE match_id=%s',(self.match,))
        require(len(rows) == 1 and rows[0][0] == 'completed' and int(rows[0][1]) == result['tick'] and
                int(rows[0][2]) == result['checksum'], 'public completion is not backed by real PostgreSQL match facts')
        receipts = self.query("SELECT settlement_id,state FROM arena.settlement WHERE match_id=%s",(self.match,))
        require(len(receipts) == 1 and int(receipts[0][0]) == self.match and receipts[0][1] == 'committed',
                'real committed unique settlement receipt missing')
        require(self.query('SELECT count(*) FROM arena.outbox WHERE settlement_id=%s',(self.match,))[0][0] == 1,
                'settlement outbox was missing or duplicated')
        participants = self.query('SELECT player_id,last_input_seq,final_x,final_y FROM arena.match_player WHERE match_id=%s ORDER BY player_id',(self.match,))
        require([int(row[0]) for row in participants] == [42,43], 'PostgreSQL final participant roster changed')
        for identity, sequence, x, y in participants:
            player = next(p for p in self.players.values() if p.identity == int(identity))
            require(int(sequence) == player.acknowledged == player.issued and
                    (int(x),int(y)) == self.position_for(player.identity,int(sequence)),
                    'PostgreSQL did not retain the full acknowledged input sequence')
        balances = {int(p):int(q) for p,q in self.query(
            'SELECT player_id,quantity FROM arena.inventory WHERE player_id IN (42,43) AND item_id=1')}
        require(all(balances[p] == self.initial_inventory.get(p,0)+10 for p in (42,43)),
                'real settlement did not apply its exactly-once participant rewards')
        self.report['settlement'] = dict(match_id=self.match,room_id=str(self.room),final_tick=result['tick'],
            checksum=str(result['checksum']),postgres_state='completed',unique_committed_receipt=True,
            unique_outbox=True,rooms_released=True,acknowledged_sequences={p.name:p.acknowledged for p in self.players.values()})

    def final_resources(self):
        for player in self.players.values():
            self.close_peer(player)
        deadline = min(self.global_deadline,time.monotonic()+5)
        while True:
            final_sample = self.resources('final-connections-draining')
            if all(final_sample[role]['fd'] <= self.initial_resources[role]['fd'] + 4 and
                   final_sample[role]['sockets'] <= self.initial_resources[role]['sockets'] + 2 for role in self.children):
                break
            self.check_time(deadline); time.sleep(.05)
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'")[0][0] == 0,
                'actual PostgreSQL idle transaction survived the journey')
        for sample in self.report['resource_curve']:
            for role,values in sample['roles'].items():
                if not values.get('exited'):
                    require(values['fd'] <= self.initial_resources[role]['fd'] + 32,
                            'physical descriptor curve exceeded its declared bounded envelope')
        self.report['resource_limits'] = dict(fd_peak_allowance=32,fd_final_allowance=4,socket_final_allowance=2,
            baseline=self.initial_resources,final=final_sample,idle_transactions=0,
            caveat='Probe/health connections may be sampled in flight; final child shutdown independently requires exact zero physical jobs.')

    def run(self):
        self.setup()
        initial_pids = self.sample('initial-ready-before-clients')
        require(len(self.children) == 6 and len(set(initial_pids.values())) == 6, 'six service instances are not independent actual processes')
        self.initial_resources = self.resources('initial-ready-before-clients')
        self.baseline()
        for window in self.plan:
            self.report['current_case'] = f'fault_window_{window["index"]}'
            self.window(window)
        require(len(self.report['role_restarts']) == 90 and len(self.report['fault_windows']) == 75,
                'formal restart schedule did not complete exactly')
        require(all(sum(row['role'] == role for row in self.report['role_restarts']) == 30 for role in ROLES),
                'role restart denominator changed')
        require(all(row['passed'] for row in self.report['fault_windows']), 'a failed fault window was omitted')
        observations = self.report['ticket_history_observations']
        require(len([row for row in observations if row['validated']]) == 150,
                'a predeclared historical-ticket transaction was omitted')
        require(all(player.history_missing_ns is not None for player in self.players.values()),
                'the full restart plan never verified reclamation of both original tickets')
        require(all(any(row['player'] == player.identity and row.get('outcome') == 'matched_history' and
                        row.get('time_region') == 'definitely_before_expiry' for row in observations)
                    for player in self.players.values()),
                'the full restart plan never verified retained history before expiry')
        self.report['current_case'] = 'real_postgres_final_settlement'
        self.settlement(); self.final_resources()
        self.report['cases'].append(dict(name='90_actual_role_restarts_75_fault_windows',passed=True,
            fixed_logical_denominator=1666,restarts=90,windows=75))
        self.report['passed'] = True

    def finish_report(self):
        for row in self.logical.values():
            if row['status'] in ('running','not_started'):
                row['status'] = 'not_completed'
        success = sum(row['status'] == 'succeeded' for row in self.logical.values())
        percentage = success / 1666
        self.report['availability'] = dict(successful_logical_operations=success,planned_logical_operations=1666,
            failed_or_unexecuted_logical_operations=1666-success,ratio=percentage,required_ratio=.999,
            raw_attempts=len(self.report['raw_attempts']),raw_failures=sum(row['status'] != 'OK' for row in self.report['raw_attempts']),
            raw_business_attempts=sum(not row['kind'].startswith('connect_') for row in self.report['raw_attempts']),
            raw_business_failures=sum(not row['kind'].startswith('connect_') and row['status'] != 'OK' for row in self.report['raw_attempts']),
            definition='Success only after the predeclared business or bounded-history contract is validated; every received public error remains a raw failure, including an expected expired-history result.')
        self.report['passed'] = bool(self.report['passed'] and percentage >= .999 and
            not self.report['unexpected_exits'] and not self.report.get('cleanup_errors') and
            not self.report.get('service_cleanup_errors') and not self.report['forced_kills'] and
            not self.report['failed_attempt_deadline_violations'] and
            all(row['status'] == 'succeeded' for row in self.report['token_renewals']))
        latencies = sorted(row['elapsed_ms'] for row in self.report['raw_attempts'])
        self.report['restart_totals'] = dict(planned=90,
            completed=sum(row.get('completed',False) for row in self.report['role_restarts']),
            by_role={role:sum(row['role'] == role and row.get('completed',False) for row in self.report['role_restarts']) for role in ROLES},
            unexpected_exits=len(self.report['unexpected_exits']),
            injected_sigkill=sum(row['signal'] == 'SIGKILL' for row in self.report['role_restarts']))
        self.report['resource_peaks'] = {role:{field:max((sample['roles'][role][field]
            for sample in self.report['resource_curve'] if field in sample['roles'][role]),default=None) for field in ('fd','sockets','rss_bytes')}
            for role in self.children}
        self.report['raw_latency_ms'] = {name:(latencies[max(0,math.ceil(len(latencies)*fraction)-1)] if latencies else None)
            for name,fraction in (('p50',.5),('p99',.99),('max',1))}
        self.report['wait_graph'] = dict(source_declared_edges=[
            ['gateway','lobby','execute/detach/heartbeat'],['lobby','auth','authenticate/health.ready'],
            ['auth','storage','account.find/health.ready'],['lobby','storage','player.get/ID allocation/settlement/reconciliation'],
            ['lobby','authority WAL owner','durable-before-publication'],['storage','private PostgreSQL','transaction-scoped repositories']],
            observed_samples='wait_graph_samples: owned socket edges plus per-thread /proc wchan; methods above come from source, not kernel sampling')
        logs = []
        for child in self.children.values():
            logs += child.log_paths
        for path in logs:
            # close() already retained and secret-checked these logs before removing
            # the private cluster. Inspect retained copies, not nonexistent temp paths.
            retained = Path(self.report.get('logs','.')) / path.name
            if retained.is_file():
                contents = retained.read_text(errors='replace')
                if re.search(r'AddressSanitizer|LeakSanitizer|runtime error:|ThreadSanitizer: data race|SanitizerTool: CHECK failed',contents):
                    self.report['sanitizer_diagnostics'].append(retained.name)
                require('$arena-pbkdf2' not in contents and not re.search(r'eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{20,}',contents),
                        'credential material appeared in a retained service log')
        if self.report['sanitizer_diagnostics']:
            self.report['passed'] = False
        live = []
        for role,child in self.children.items():
            for pid in child.history:
                path = Path(f'/proc/{pid}/stat')
                try:
                    birth = int(path.read_text().rpartition(')')[2].split()[19])
                except FileNotFoundError:
                    continue
                if pid not in self.pid_birth or self.pid_birth[pid] == birth:
                    live.append(dict(role=role,pid=pid,start_ticks=birth))
        self.report['remaining_owned_processes'] = live
        if live:
            self.report['passed'] = False
        self.report['source_hashes'] = {name:hashlib.sha256((self.args.repository_root/name).read_bytes()).hexdigest()
            for name in ('services/gateway/app/main.cpp','services/lobby/app/main.cpp',
                'services/lobby/src/LobbyService.cpp','services/lobby/src/LobbyAuthority.cpp',
                'services/auth/src/AuthService.cpp','services/storage/server.py',
                'services/room/worker/app/main.cpp','services/room/worker/src/RoomWorker.cpp',
                'services/room/src/RoomRuntime.cpp',
                'tests/lobby/ServiceRestartAcceptanceTest.py','tests/lobby/ServiceStackFixture.py',
                'services/match/include/match/MatchQueue.h','services/match/src/MatchQueue.cpp')}
        binaries = {}
        for role in (*ROLES, 'room'):
            path = Path(getattr(self.args,role)).resolve()
            digest = hashlib.sha256()
            with path.open('rb') as binary:
                while block := binary.read(1024*1024):
                    digest.update(block)
            binaries[role] = dict(path=str(path),sha256=digest.hexdigest())
        self.report['binary_hashes'] = binaries
        self.report['parent_fd_after_report_validation'] = len(os.listdir('/proc/self/fd'))
        if self.report['parent_fd_after_report_validation'] != self.report['parent_fd_baseline']:
            self.report['passed'] = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('descriptor','auth','lobby','gateway','pg-bin','report'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--room', type=Path)
    parser.add_argument('--repository-root',type=Path,default=ROOT)
    parser.add_argument('--pg-share',type=Path); parser.add_argument('--pg-lib',type=Path)
    parser.add_argument('--battle-ticks',type=int,default=6000)
    parser.add_argument('--grace-ms',type=int,default=120000)
    parser.add_argument('--window-timeout',type=float,default=60)
    parser.add_argument('--final-timeout',type=float,default=450)
    parser.add_argument('--total-timeout',type=float,default=540)
    args = parser.parse_args()
    require(3000 <= args.battle_ticks <= 12000 and 60000 <= args.grace_ms <= 3600000,
            'formal continuity configuration is outside declared bounds')
    require(20 <= args.window_timeout <= 60 and 60 <= args.final_timeout <= 480 and
            300 <= args.total_timeout <= 540,'formal finite deadline bounds changed')
    args.repository_root = args.repository_root.resolve()
    test = Acceptance(args)
    def interrupt(*_):
        test.stop_requested.set()
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM,interrupt)
    try:
        test.run()
    except (Exception,KeyboardInterrupt) as error:
        test.stop_requested.set()
        test.report.update(passed=False,error_type=type(error).__name__,
            error=str(error) if isinstance(error,InvariantFailure) else 'process/transport acceptance failed; inspect recorded phase and raw status')
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        test.close()
    try:
        test.finish_report()
    except Exception as error:
        test.report.update(passed=False,report_validation_error=type(error).__name__)
    output = json.dumps(test.report,indent=2)
    test.assert_no_secrets(output)
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(output+'\n')
    print(json.dumps(dict(passed=test.report['passed'],restarts=sum(row.get('completed',False) for row in test.report['role_restarts']),
        windows=len(test.report['fault_windows']),availability=test.report.get('availability'),
        error=test.report.get('error'),report=str(args.report))))
    return 0 if test.report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())