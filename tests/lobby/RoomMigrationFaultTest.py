#!/usr/bin/env python3
"""Real phase-crash and delayed-old-reply Room ownership acceptance."""
import asyncio
import json
import os
from pathlib import Path
import signal
import socket
import struct
import sys
import threading
import time
import traceback
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from RoomMigrationProcessTest import Acceptance, arguments, check, digest, checker_self_test
from PersistentGatewayTest import StorageAckProxy, read_journal_records


def wire_fields(data):
    """Bounded private-proto inspection for phase selection, never ACK evidence."""
    offset = 0
    def varint():
        nonlocal offset
        value = 0
        for shift in range(0, 70, 7):
            check(offset < len(data), 'truncated phase-selection varint')
            byte = data[offset]; offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError('oversized phase-selection varint')
    fields = {}
    while offset < len(data):
        tag = varint(); number, kind = tag >> 3, tag & 7
        check(number > 0, 'invalid phase-selection field')
        if kind == 0:
            value = varint()
        elif kind == 2:
            length = varint()
            check(length <= len(data)-offset, 'truncated phase-selection bytes')
            value = data[offset:offset+length]; offset += length
        elif kind in (1, 5):
            length = 8 if kind == 1 else 4
            check(length <= len(data)-offset, 'truncated phase-selection fixed field')
            value = data[offset:offset+length]; offset += length
        else:
            raise ValueError('unsupported phase-selection wire type')
        fields[number] = value
    return fields


PHASE_LABELS={0:'无迁移',1:'准备',2:'冻结中',3:'已冻结',4:'已转移',5:'已提交',6:'已中止'}
VICTIM_LABELS={'source':'源 Room','target':'目标 Room','coordinator':'Lobby'}


def write_phase_timeline(report,path):
    """Render actual observer samples; unsampled transitions are never interpolated."""
    cases=report.get('phase_crash',{}).get('cases',[])
    indexed={(case['phase'],case['victim']):case for case in cases}
    rows=[]
    svg=['<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="910" viewBox="0 0 1400 910">',
         '<rect width="1400" height="910" fill="#101827"/>',
         '<g font-family="Microsoft YaHei,Noto Sans CJK SC,sans-serif" fill="#e5edf7">',
         '<text x="24" y="36" font-size="23">Room 迁移故障阶段时序（真实观测）</text>',
         '<text x="24" y="64" font-size="13">时间戳为 RPC 解码完成；阶段点不代表内部 WAL 提交瞬间，未采样中间阶段不补画。</text>']
    left,width=390,960
    for second in range(11):
        x=left+width*second/10
        svg.append(f'<line x1="{x}" y1="94" x2="{x}" y2="848" stroke="#334155"/>')
        svg.append(f'<text x="{x}" y="88" font-size="12" text-anchor="middle">{second}s</text>')
    for index,(phase,victim) in enumerate((p,v) for p in ('PREPARING','FROZEN','TRANSFERRED','COMMITTED')
                                        for v in ('source','target','coordinator')):
        case=indexed.get((phase,victim))
        y=120+index*61
        label=f'{phase} / {VICTIM_LABELS[victim]}'
        svg.append(f'<text x="24" y="{y}" font-size="15">{escape(label)}</text>')
        row=dict(requested_phase=phase,victim=victim,executed=case is not None,observations=[])
        if case is None:
            svg.append(f'<text x="24" y="{y+21}" font-size="12" fill="#94a3b8">未执行</text>')
        else:
            killed=case.get('kill_started_at_ns')
            recovered=case.get('recovered_at_ns')
            observations=case.get('observations',[])
            # Keep all samples in JSON, and one point per actual phase transition in SVG.
            row.update(kill_started_at_ns=killed,kill_joined_at_ns=case.get('kill_joined_at_ns'),
                recovered_at_ns=recovered,recovery_ms=case.get('recovery_ms'),
                recovery_phase=case.get('recovered',{}).get('phase'),passed=case.get('passed',False),
                observations=observations)
            pre=[];last=None
            for observation in observations:
                observed=observation['observed_at_ns'];actual=observation['phase']
                if actual == last: continue
                last=actual
                if killed is None or observed < killed:
                    pre.append(PHASE_LABELS[actual]);continue
                delta=(observed-killed)/1e6
                if delta <= 10000:
                    x=left+width*delta/10000
                    svg.append(f'<circle cx="{x:.2f}" cy="{y}" r="4" fill="#60a5fa"><title>{escape(PHASE_LABELS[actual])} {delta:.2f} ms（观测）</title></circle>')
            before=' → '.join(pre) or '无崩溃前阶段样本'
            svg.append(f'<text x="24" y="{y+21}" font-size="11" fill="#94a3b8">{escape(before)}</text>')
            if killed is not None:
                svg.append(f'<path d="M{left-4},{y-6}l8,12m0,-12l-8,12" stroke="#fb7185" stroke-width="2"/>')
            if recovered is not None and killed is not None:
                measured=(recovered-killed)/1e6
                x=left+width*min(measured,10000)/10000
                phase_label=PHASE_LABELS.get(row['recovery_phase'],'未知')
                svg.append(f'<line x1="{left}" y1="{y}" x2="{x:.2f}" y2="{y}" stroke="#34d399" stroke-width="2"/>')
                svg.append(f'<circle cx="{x:.2f}" cy="{y}" r="5" fill="#34d399"/>')
                svg.append(f'<text x="{left+8}" y="{y+22}" font-size="12">恢复 {measured:.2f} ms；阶段 {escape(phase_label)}</text>')
        rows.append(row)
    svg.extend(['<text x="24" y="884" font-size="13">× SIGKILL　蓝点：采样到的阶段变化　绿点：首次超过崩溃前锚点的已提交 tick；恢复硬上限 10 秒。</text>','</g></svg>'])
    svg_path=path.with_suffix('.phases.svg');json_path=path.with_suffix('.phases.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    svg_path.write_text(''.join(svg),encoding='utf-8')
    json_path.write_text(json.dumps(dict(timestamp_semantics='observer RPC decode completion; not internal commit instant',
        time_axis='milliseconds since SIGKILL started',recovery_limit_ms=10000,rows=rows),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return dict(svg=str(svg_path),json=str(json_path),planned_rows=12,executed_rows=len(cases),
        timestamp_semantics='observer RPC decode completion; not internal commit instant')


def timeline_self_test():
    import tempfile
    cases=[]
    for index,(phase,victim) in enumerate((p,v) for p in ('PREPARING','FROZEN','TRANSFERRED','COMMITTED')
                                        for v in ('source','target','coordinator')):
        killed=1_000_000_000+index*20_000_000_000
        cases.append(dict(phase=phase,victim=victim,passed=True,kill_started_at_ns=killed,
            kill_joined_at_ns=killed+1_000_000,recovered_at_ns=killed+125_000_000,recovery_ms=125,
            recovered=dict(phase=5),observations=[dict(phase=1,observed_at_ns=killed-1_000_000),
                dict(phase=5,observed_at_ns=killed+120_000_000)]))
    with tempfile.TemporaryDirectory(prefix='phase-timeline-unit-') as folder:
        path=Path(folder)/'synthetic-unit-report.json'
        artifacts=write_phase_timeline(dict(phase_crash=dict(cases=cases)),path)
        tree=ET.parse(artifacts['svg']);data=json.loads(Path(artifacts['json']).read_text())
        text=''.join(tree.getroot().itertext())
        check(len(data['rows']) == 12 and text.count('恢复 125.00 ms') == 12 and '10s' in text,
            'synthetic phase timeline row/numeric labels')
        check(all([p['phase'] for p in row['observations']] == [1,5] for row in data['rows']),
            'phase timeline invented unsampled intermediate phases')
    return dict(timeline_unit_rows=12,timeline_fixture='synthetic unit only',timeline_business_processes_started=0)


class SyncGate:
    """One bounded listener, one disk writer; only the real sync returns its ACK."""
    def __init__(self, root):
        self.path = root/'fsync-gate.sock'
        self.wal = root/'lobby-state'/'authority.wal'
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.path)); self.listener.listen(1); self.listener.settimeout(.1)
        self.closing = threading.Event(); self.release = threading.Event(); self.release.set()
        self.hit = threading.Event(); self.lock = threading.Lock()
        self.selected = None; self.errors = []; self.samples = []
        self.thread = threading.Thread(target=self.serve, name='room-phase-sync-gate')
        self.thread.start()

    def arm(self, room, candidate_phase):
        with self.lock:
            self.selected = (room, candidate_phase)
            self.hit.clear(); self.release.clear()

    def serve(self):
        while not self.closing.is_set():
            try:
                peer, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with peer:
                try:
                    peer.settimeout(2)
                    request = bytearray()
                    while len(request) < 8:
                        part = peer.recv(8-len(request))
                        check(bool(part), 'sync gate disconnected')
                        request.extend(part)
                    size, = struct.unpack('!Q', request)
                    with self.lock:
                        selected = self.selected
                    if selected is not None:
                        data = self.wal.read_bytes()
                        check(len(data) == size, 'disk writer changed WAL during sync gate')
                        records, revision = read_journal_records(data)
                        for key, value in records.items():
                            if not key.startswith('room-directory/room/'):
                                continue
                            room = wire_fields(value)
                            handoff = wire_fields(room.get(13, b''))
                            if room.get(2) == selected[0] and handoff.get(9, 0) == selected[1]:
                                self.samples.append(dict(room=selected[0],candidate_phase=selected[1],
                                    candidate_revision=revision,visibility_is_ack=False))
                                self.hit.set()
                                check(self.release.wait(12), 'sync gate watchdog expired')
                                break
                    peer.sendall(b'R')
                except (OSError, Exception) as error:
                    with self.lock:
                        armed = self.selected is not None
                    if not self.closing.is_set() and armed:
                        self.errors.append(type(error).__name__)

    def disarm(self):
        with self.lock:
            self.selected = None
        self.release.set()

    def close(self):
        self.disarm(); self.closing.set(); self.listener.close(); self.thread.join(3)
        check(not self.thread.is_alive(), 'sync gate thread retained')
        self.path.unlink(missing_ok=True)


class RoomProxy(StorageAckProxy):
    """Unmodified native frames; optionally pause before stage or after execute."""
    def __init__(self, test, target):
        self.mode = None
        self.hints_release = threading.Event(); self.hints_release.set()
        self.hints_hit = threading.Event()
        self.held_at = None; self.held_payload = None
        super().__init__(test, target)

    def arm_method(self, mode):
        check(mode in ('stage-request', 'execute-reply'), 'unknown proxy gate')
        self.mode = mode; self.held_at = None
        self.arm()

    def hold(self):
        self.held_at = time.monotonic(); self.hit.set()
        while not self.release.wait(.005):
            if self.closing.is_set():
                return False
        return True

    def _serve(self, peer):
        upstream = None
        try:
            peer.settimeout(2)
            request = self.frame(peer)
            method = self.test.protocol.Request.FromString(request[4:]).method
            if method in ('room.freeze','room.retire') and not self.hints_release.is_set():
                self.hints_hit.set()
                while not self.hints_release.wait(.005):
                    if self.closing.is_set(): return
            if self.mode == 'stage-request' and method == 'room.stage' and not self.release.is_set():
                if not self.hold(): return
            upstream = socket.create_connection(('127.0.0.1', self.target), timeout=2)
            with self.lock: self.connections.add(upstream)
            upstream.sendall(request)
            response = self.frame(upstream)
            if self.mode == 'execute-reply' and method == 'room.execute' and not self.release.is_set():
                reply = self.test.protocol.Response.FromString(response[4:])
                check(reply.status == 0, 'held execute was not a genuine worker success')
                if self.held_payload is None:
                    self.held_payload = bytes(self.test.protocol.Request.FromString(request[4:]).payload)
                if not self.hold(): return
            peer.sendall(response)
        except (OSError, EOFError):
            pass
        except Exception as error:
            self.test.report.setdefault('proxy_errors', []).append(type(error).__name__)
        finally:
            for connection in (peer, upstream):
                if connection is not None:
                    with self.lock: self.connections.discard(connection)
                    connection.close()
            with self.lock: self.workers.discard(threading.current_thread())


class FaultAcceptance(Acceptance):
    def __init__(self, args):
        self.proxies = {}
        self.sync_gate = None
        super().__init__(args)
        self.sync_gate = SyncGate(self.cluster.root)
        self.proxies = {node: RoomProxy(self, self.ports[node]) for node in self.room_roles}
        self.report.update(scope='real Room four-phase x three-role crash matrix and delayed reply',
            phase_crash=dict(implemented=True,executed=0,cases=[]),late_reply={},
            full_count_gate=False,day34_hard_acceptance_complete=False)

    def manifest(self, version=1, desired=True):
        result = super().manifest(version, desired)
        for node in result['nodes']:
            if node.get('service') == 'room' and node.get('nodeId') in self.proxies:
                node['port'] = self.proxies[node['nodeId']].port
        return result

    def command(self, role):
        command, env = super().command(role)
        if role == 'lobby':
            env['ARENA_ROOM_NODES'] = json.dumps([dict(nodeId=node,host='127.0.0.1',
                port=self.proxies[node].port) for node in self.room_roles], separators=(',', ':'))
            env['LD_PRELOAD'] = str(self.args.fsync_shim) + (':'+os.environ['LD_PRELOAD'] if os.environ.get('LD_PRELOAD') else '')
            env['ARENA_TEST_FSYNC_WAL'] = str(self.sync_gate.wal)
            env['ARENA_TEST_FSYNC_SOCKET'] = str(self.sync_gate.path)
        return command, env

    def initialize(self):
        self.setup(); self.initial_pids = self.sample('fault-initial-ready')
        check(len(set(self.initial_pids.values())) == 6, 'six independent service processes required')
        self.physical_workers('fault-initial-ready')
        self.alice, self.bob = self.connect(), self.connect(True)
        self.alice.socket.settimeout(1); self.bob.socket.settimeout(1)
        a,b = self.grant(self.alice,b'alice'),self.grant(self.bob,b'bob')
        self.sessions = {42:a['session'],43:b['session']}

    def room(self, index):
        room = 950000+index
        row = dict(index=index,room=room,incarnation='',attempts=[],observations=[],phase_observations=[],passed=False)
        self.rooms.append(row)
        check(self.alice.join(room)['room'] == self.bob.join(room)['room'] == room, 'fault room joins')
        state = self.alice.room_snapshot()
        row['incarnation'] = str(state['incarnation'])
        self.bindings = {p:(m['session'],m['epoch']) for p,m in state['players'].items()}
        check({p:b[0] for p,b in self.bindings.items()} == self.sessions, 'fault changed logical sessions')
        row['observations'].append(dict(tick=state['tick'],checksum=state['checksum']))
        state = self.control(row,self.alice,state,42,1,1,0,1)
        state = self.control(row,self.bob,state,43,1,0,1,1)
        before = self.status(room,state['incarnation'])
        source = before.owner.split(':',1)[0]
        target = next(n for n in self.room_roles if n != source)
        request = self.message('arena.lobby.MigrationRequest',version=1,room_id=room,
            incarnation=state['incarnation'],owner_epoch=before.owner_epoch,migration_id=1,target_node=target)
        return row,state,before,source,target,request

    def wait_status(self,row,predicate,timeout=2):
        end = min(self.deadline,time.monotonic()+timeout)
        while True:
            value = self.status(row['room'],int(row['incarnation']))
            row['phase_observations'].append(self.view(value))
            if predicate(value): return value
            check(time.monotonic() < end, 'published migration phase watchdog')
            time.sleep(.002)

    def wait_snapshot_after(self,row,tick,deadline):
        # Query the committed image until the real periodic writer advances it.
        # A read itself must never manufacture a tick or consume an input slot.
        end=min(self.deadline,deadline)
        while True:
            check(time.monotonic() < end, 'committed Room tick did not advance before deadline')
            value=self.snapshot(row)
            check(time.monotonic() <= end, 'committed Room snapshot exceeded deadline')
            if value['tick'] > tick:
                return value
            time.sleep(.005)

    def finish_room(self,row,state):
        state = self.control(row,self.alice,state,42,1,1,0,2)
        state = self.control(row,self.alice,state,42,2,-1,0,1)
        state = self.control(row,self.bob,state,43,2,0,-1,1)
        state = self.control(row,self.bob,state,43,2,0,-1,2)
        check(all(p['seq'] == 2 and p['x'] == p['y'] == 0 for p in state['players'].values()),
            'fault lost or double-applied an acknowledged control')
        self.snapshot(row)
        check(self.alice.leave()['room'] == self.bob.leave()['room'] == 0, 'fault room release')
        row['passed'] = True

    def phase_case(self,index,phase,victim):
        row,state,before,source,target,request = self.room(index)
        phase_number = {'PREPARING':1,'FROZEN':3,'TRANSFERRED':4,'COMMITTED':5}[phase]
        case = dict(phase=phase,victim=victim,passed=False,room=row['room'],
            observations=row['phase_observations'])
        self.report['phase_crash']['cases'].append(case)
        if phase in ('PREPARING','TRANSFERRED'):
            self.sync_gate.arm(row['room'],2 if phase == 'PREPARING' else 4)
        if phase == 'FROZEN': self.proxies[target].arm_method('stage-request')
        prepared=self.call('room.migration.start',request,'arena.lobby.MigrationReply')
        row['phase_observations'].append(self.view(prepared))
        if phase in ('PREPARING','TRANSFERRED'):
            check(self.sync_gate.hit.wait(2), 'selected pre-sync phase not reached')
        if phase == 'FROZEN': check(self.proxies[target].hit.wait(2), 'real stage request not held')
        published = self.wait_status(row,lambda v:v.phase == phase_number)
        case['published_before_kill'] = self.view(published)
        recovery_anchor=published.physical_tick
        if phase == 'COMMITTED':
            # Obtain a real target-owner public ACK before crashing that owner.
            target_ack = self.wait_snapshot_after(row,published.physical_tick,time.monotonic()+2)
            recovery_anchor=target_ack['tick']
            case['target_ack_before_kill_tick']=recovery_anchor
        role = source if victim == 'source' else target if victim == 'target' else 'lobby'
        old_pid = self.children[role].process.pid
        killed = time.monotonic()
        case['kill_started_at_ns']=time.monotonic_ns()
        self.children[role].stop(crash=True)
        case['kill_joined_at_ns']=time.monotonic_ns()
        self.sync_gate.disarm()
        for proxy in self.proxies.values():
            proxy.release.set(); proxy.hints_release.set()
        self.children[role].start()
        check(self.children[role].process.pid != old_pid, 'fault role did not restart')
        # Every retry below is read-only until the single recorded duplicate control.
        until = min(self.deadline,killed+10)
        while True:
            try:
                current = self.status(row['room'],int(row['incarnation']))
                row['phase_observations'].append(self.view(current))
                if current.phase in (5,6):
                    state = self.snapshot(row)
                    if state['tick'] > recovery_anchor: break
            except Exception:
                pass
            check(time.monotonic() < until, 'phase crash recovery exceeded ten seconds')
            time.sleep(.01)
        case['recovered_at_ns']=time.monotonic_ns()
        case.update(recovery_ms=(time.monotonic()-killed)*1000,recovered=self.view(current),
            recovery_anchor_tick=recovery_anchor,first_recovery_tick=state['tick'],
            killed_pid=old_pid,restarted_pid=self.children[role].process.pid)
        check(case['recovery_ms'] <= 10000, 'phase crash recovery SLO')
        self.finish_room(row,state)
        case['passed'] = True
        self.report['phase_crash']['executed'] += 1
        self.worker_ids.clear(); self.physical_workers('after-'+phase+'-'+victim)
        self.sample('after-'+phase+'-'+victim)

    def late_case(self,index):
        row,state,before,source,target,request = self.room(index)
        proxy = self.proxies[source]
        proxy.hints_hit.clear(); proxy.hints_release.clear()
        proxy.arm_method('execute-reply')
        check(proxy.hit.wait(1), 'no genuine old execute reply held')
        started = proxy.held_at
        from shared.runtime.rpc import RpcError
        old_request = self.protocol.message('arena.room.ExecuteRequest').FromString(proxy.held_payload)
        try:
            self.call('room.migration.start',request,'arena.lobby.MigrationReply')
            current = self.wait_status(row,lambda v:v.phase == 5, .8)
            state = self.wait_snapshot_after(row,current.physical_tick,started+.95)
            check(proxy.hints_hit.wait(.1), 'old cache invalidation was not physically partitioned')
            # A real isolated success proves the partitioned old worker still thinks it owns the cache.
            isolated = self.call('room.execute',old_request,'arena.room.ExecuteReply',caller='lobby',
                token=self.room_token,target='room',port=self.ports[source])
            check(isolated.worker_id == before.owner and isolated.owner_epoch == before.owner_epoch,
                'split-brain probe did not execute on the old owner epoch')
            elapsed = time.monotonic()-started
            check(elapsed < .95, 'control progression relied on old data RPC timeout')
            proxy.release.set()
            after = self.status(row['room'],int(row['incarnation']))
            check(after.owner == current.owner and after.owner_epoch == current.owner_epoch and
                after.physical_tick >= state['tick'], 'old reply rolled back canonical ownership or image')
        finally:
            proxy.release.set(); proxy.hints_release.set()
        until = time.monotonic()+1
        stale_attempts = 0
        while True:
            stale_attempts += 1
            try:
                self.call('room.execute',old_request,'arena.room.ExecuteReply',caller='lobby',
                    token=self.room_token,target='room',port=self.ports[source])
            except RpcError as error:
                check(error.status == 7, 'old owner execute failed with a non-fence error')
                break
            check(time.monotonic() < until, 'old physical owner was not fenced after partition healed')
            time.sleep(.005)
        repeat = self.call('room.migration.start',request,'arena.lobby.MigrationReply')
        check(repeat.phase == 5 and repeat.owner == current.owner and repeat.owner_epoch == current.owner_epoch,
            'repeated committed migration changed canonical ownership')
        self.finish_room(row,state)
        self.report['late_reply'] = dict(passed=True,held_ms=elapsed*1000,
            old_owner=before.owner,new_owner=current.owner,old_epoch=before.owner_epoch,
            new_epoch=current.owner_epoch,old_execute_rejected=1,old_execute_attempted=stale_attempts,
            partitioned_old_owner_real_execute_succeeded=True,source_hints_partitioned=True,
            repeated_committed_start=True,canonical_history_verified_after_cleanup=False)

    def run(self):
        self.initialize()
        index = 0
        for phase in ('PREPARING','FROZEN','TRANSFERRED','COMMITTED'):
            for victim in ('source','target','coordinator'):
                self.phase_case(index,phase,victim); index += 1
        self.late_case(index)
        check(self.report['phase_crash']['executed'] == 12, 'phase matrix denominator')
        check(not self.sync_gate.errors and not self.report.get('proxy_errors'), 'fault instrumentation errors')
        self.report['sync_gate_samples'] = list(self.sync_gate.samples)
        self.export_and_check()
        self.report['late_reply']['canonical_history_verified_after_cleanup'] = True
        self.report.update(passed=True,phase_fault_gate=True)

    def close(self):
        if self.sync_gate: self.sync_gate.disarm()
        for proxy in self.proxies.values():
            proxy.release.set(); proxy.hints_release.set()
        errors = []
        for proxy in self.proxies.values():
            try: proxy.close()
            except Exception as error: errors.append(type(error).__name__)
        if self.sync_gate:
            try: self.sync_gate.close()
            except Exception as error: errors.append(type(error).__name__)
        super().close()
        if errors: self.report.update(passed=False,instrumentation_cleanup_errors=errors)


def main():
    # Reuse the exact real-process arguments and bound checks from the count gate.
    extra = sys.argv.index('--fsync-shim') if '--fsync-shim' in sys.argv else None
    shim = None
    if extra is not None:
        shim = Path(sys.argv[extra+1]).resolve(); del sys.argv[extra:extra+2]
    args = arguments()
    if args.self_test:
        check(wire_fields(b'\x10\x07\x6a\x02\x48\x03')[2] == 7, 'phase selector identity')
        check(wire_fields(wire_fields(b'\x10\x07\x6a\x02\x48\x03')[13])[9] == 3, 'phase selector phase')
        print(json.dumps(dict(**checker_self_test(),**timeline_self_test(),phase_selector_checks=2))); return 0
    check(shim is not None and shim.is_file(), 'real pre-fdatasync shim required')
    args.fsync_shim = shim; args.count = 13; args.diagnostic = False
    test = None; report = dict(passed=False)
    sources = {str(p.relative_to(args.repository_root)):digest(p) for folder in ('services','shared','tests/lobby')
        for p in (args.repository_root/folder).rglob('*') if p.is_file() and p.suffix in ('.py','.cpp','.h','.c','.proto')}
    binaries = {name:digest(getattr(args,name)) for name in ('auth','lobby','gateway','room','replay','replay_export','fsync_shim')}
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        test = FaultAcceptance(args); test.run()
    except (Exception,KeyboardInterrupt) as error:
        report.update(error=type(error).__name__,failure_locations=[dict(file=Path(f.filename).name,line=f.lineno,
            function=f.name) for f in traceback.extract_tb(error.__traceback__)])
        if type(error).__name__ == 'CheckFailure': report['invariant'] = str(error)
        if test: test.report.update(report)
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        if test:
            try: test.close()
            except Exception as error: test.report.update(passed=False,cleanup_error=type(error).__name__)
            report = test.report
        report.update(source_manifest=sources,binary_manifest=binaries,
            sources_unchanged=all(digest(args.repository_root/p) == sha for p,sha in sources.items()),
            binaries_unchanged=all(digest(getattr(args,name)) == sha for name,sha in binaries.items()))
        if not report['sources_unchanged'] or not report['binaries_unchanged']: report['passed'] = False
        if test:
            report['remaining_owned_pids'] = [dict(role=role,pid=pid) for role,child in test.children.items()
                for pid in child.history if Path('/proc',str(pid)).exists()]
            if report['remaining_owned_pids']: report['passed'] = False
            test.assert_no_secrets(json.dumps(report))
        report['phase_timeline_artifacts']=write_phase_timeline(report,args.report)
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(passed=bool(report.get('passed')),phase_fault_gate=bool(report.get('phase_fault_gate')))))
    return 0 if report.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())