#!/usr/bin/env python3
"""Real Gateway/Auth/Lobby/Storage journey and durable active-room restart contracts."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, ROOT/'tests/auth', ROOT/'tests/storage', ROOT/'tests/room',
               ROOT/'tests/match', ROOT/'tests/gateway/net'):
    sys.path.insert(0, str(folder))
from AuthProcessIntegrationTest import health_http
from StorageMigrationTest import require
from OnlinePresenceIntegrationTest import scalar


from ServiceStackFixture import ServiceStack


class Acceptance(ServiceStack):
    def unchanged_journey(self):
        before=self.sample('independent-listeners-ready')
        require(len(before) == 6 and len(set(before.values())) == 6,'six roles are not distinct processes')
        self.alice,self.bob=self.connect(),self.connect(True)
        self.alice_grant=self.alice.login(); self.bob_grant=self.bob.login(b'bob')
        require(self.alice_grant['player'] == 42 and self.bob_grant['player'] == 43,'canonical identity changed')
        self.alice_ticket=scalar(self.alice.match_join(101),2)
        self.bob_ticket=scalar(self.bob.match_join(201),2)
        assigned={}
        def paired():
            assigned.update(self.alice.match_status(self.alice_ticket))
            return scalar(assigned,4) == 3 and scalar(assigned,13) != 0
        self.until(paired,'two unchanged client tickets matched',8)
        self.match=scalar(assigned,9); self.room=scalar(assigned,13)
        state=self.alice.status()
        require(state['match'] == self.match and set(state['snapshot']['players']) == {42,43}, 'room roster changed')
        self.alice.ready(state,42); self.bob.ready(state,43)
        active={}
        def playing():
            active.update(self.alice.status()); return active['phase'] == 3
        self.until(playing,'both ready starts active room',5)
        self.incarnation=active['snapshot']['incarnation']
        require(self.alice.control(active['snapshot'],42,1,1,-1)[1] == 1,'first ordered input rejected')
        self.last_player=self.alice.status()['snapshot']['players'][42]
        return dict(pids=before,match=self.match,room=str(self.room),incarnation=self.incarnation,
                    tcp_and_websocket=True,canonical_mmr=True)

    def lobby_crash_recovers_room(self):
        before=self.sample('before-lobby-kill')
        started=time.monotonic()
        self.children['lobby'].stop(crash=True)
        self.until(lambda:health_http(self.ports['gateway-health'],'/health/ready')[0] == 503,
                   'Gateway not-ready after Lobby loss',1)
        observed_ms=(time.monotonic()-started)*1000
        self.children['lobby'].start()
        self.until(lambda:health_http(self.ports['gateway-health'],'/health/ready')[0] == 200,
                   'same Gateway sees recovered Lobby',8)
        self.alice.close(reset=True)
        self.alice=self.connect(True)
        grant=self.alice.login(token=self.alice_grant['token'])
        require(grant['resumed'] and grant['session'] == self.alice_grant['session'] and
                grant['room'] == self.room and grant['epoch'] > self.alice_grant['epoch'],
                'Lobby restart lost session/room or fabricated fresh login')
        state=self.alice.status()
        require(state['snapshot']['incarnation'] == self.incarnation and
                state['snapshot']['players'][42]['seq'] == self.last_player['seq'] and
                state['snapshot']['players'][42]['x'] == self.last_player['x'] and
                state['snapshot']['players'][42]['y'] == self.last_player['y'],
                'WAL restart lost acknowledged room input')
        require(scalar(self.alice.match_status(self.alice_ticket),9) == self.match,
                'Lobby restart lost ticket assignment')
        self.alice_grant=grant
        after=self.sample('after-lobby-recovery')
        require(all(after[role] == before[role] for role in ('gateway','auth','storage')),
                'Lobby restart restarted another role')
        return dict(not_ready_ms=observed_ms,session_room_input_preserved=True,
                    ticket_preserved=True,other_pids_preserved=True)

    def auth_restart_keeps_room(self):
        before=self.sample('before-auth-stop')
        failed=self.connect(True)
        self.children['auth'].stop()
        started=time.monotonic()
        self.until(lambda:health_http(self.ports['gateway-health'],'/health/ready')[0] == 503,
                   'Gateway Auth dependency failure',1)
        require(self.alice.status()['match'] == self.match,'Auth outage blocked existing room status')
        call=time.monotonic(); failed.login(error=2002)
        elapsed=(time.monotonic()-call)*1000
        require(elapsed <= 1000,'Auth failure did not return within one second')
        failed.close()
        self.children['auth'].start()
        self.until(lambda:health_http(self.ports['gateway-health'],'/health/ready')[0] == 200,
                   'same Lobby sees restarted Auth',8)
        after=self.sample('after-auth-recovery')
        require(all(after[role] == before[role] for role in self.children if role != 'auth'),
                'Auth restart affected another process')
        return dict(failed_login_ms=elapsed,other_pids_preserved=True,
                    outage_elapsed_ms=(time.monotonic()-started)*1000)

    def gateway_restart_keeps_room(self):
        before=self.sample('before-gateway-kill')
        self.children['gateway'].stop(crash=True)
        self.children['gateway'].start()
        self.alice=self.connect(); self.bob=self.connect(True)
        for label in ('alice','bob'):
            peer=getattr(self,label); old=getattr(self,label+'_grant')
            grant=peer.login(token=old['token'])
            require(grant['resumed'] and grant['session'] == old['session'] and grant['room'] == self.room,
                    'Gateway restart lost current logical membership')
            setattr(self,label+'_grant',grant)
        current=self.alice.status()
        require(current['snapshot']['incarnation'] == self.incarnation and current['match'] == self.match,
                'Gateway restart replaced active room actor')
        after=self.sample('after-gateway-recovery')
        require(all(after[role] == before[role] for role in ('auth','lobby','storage')),
                'Gateway restart affected another process')
        return dict(other_pids_preserved=True,logical_membership_preserved=True)

    def actual_settlement(self):
        latest={}
        def finished():
            latest.update(self.alice.status()); return latest['phase'] == 5
        self.until(finished,'actual Storage-acknowledged settlement',30)
        result=self.alice.result(self.match)
        require(result == self.bob.result(self.match) == latest['settlement'], 'terminal result disagrees')
        rows=self.query('SELECT final_tick,final_checksum,state FROM arena.game_match WHERE match_id=%s',(self.match,))
        require(len(rows) == 1 and int(rows[0][0]) == result['tick'] and int(rows[0][1]) == result['checksum']
                and rows[0][2] == 'completed', 'published terminal result absent from PostgreSQL')
        require(self.alice.query()['room'] == self.bob.query()['room'] == 0,'settlement left dangling membership')
        self.terminal_result=result
        return dict(match=self.match,final_tick=result['tick'],checksum=str(result['checksum']),
                    real_postgres_ack=True,room_membership_released=True)

    def offline_replay_export(self):
        directory=self.cluster.root/'exported-replays'
        command=[str(self.args.replay_export),'--state-dir',str(self.cluster.root/'lobby-state'),
                 '--output-dir',str(directory)]
        locked=subprocess.run(command,cwd=self.args.repository_root,stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=8)
        require(locked.returncode == 2 and json.loads(locked.stdout)['status'] == 'unavailable',
                'replay exporter did not respect the live Lobby journal lock')
        for peer in self.peers:
            peer.close()
        self.children['gateway'].stop(); self.children['lobby'].stop()
        exported=subprocess.run(command,cwd=self.args.repository_root,stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=8)
        require(exported.returncode == 0, 'offline exporter failed: '+exported.stdout)
        files=json.loads(exported.stdout)['files']
        require(len(files) == 1 and int(files[0]['room_id']) == self.room and
                int(files[0]['incarnation']) == self.incarnation, 'exported another logical room')
        archive=directory/files[0]['file']; report=directory/'verified.json'
        verified=subprocess.run([str(self.args.replay),str(archive),'--report',str(report)],
            cwd=self.args.repository_root,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,text=True,timeout=8)
        require(verified.returncode == 0,'original replay tool rejected exported ordered inputs')
        replay=json.loads(report.read_text())
        require(int(replay['final_tick']) == self.terminal_result['tick'] and
                int(replay['final_checksum']) == self.terminal_result['checksum'],
                'real-time and replay final checksums differ')
        before=hashlib.sha256(archive.read_bytes()).hexdigest()
        repeated=subprocess.run(command,cwd=self.args.repository_root,stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=8)
        require(repeated.returncode == 2 and hashlib.sha256(archive.read_bytes()).hexdigest() == before,
                'repeated export overwrote an existing archive')
        return dict(live_lock_rejected=True,replay_checksums_verified=len(replay['checksums']),
                    final_tick=self.terminal_result['tick'],checksum=str(self.terminal_result['checksum']),
                    existing_archive_preserved=True)

    def run(self):
        self.report['scope']='real six-process smoke and active-room continuity; not the 90-restart gate'
        self.setup()
        for name,action in (('unchanged_tcp_ws_login_match_room_inputs',self.unchanged_journey),
            ('lobby_sigkill_recovers_acknowledged_active_room',self.lobby_crash_recovers_room),
            ('auth_restart_preserves_live_room',self.auth_restart_keeps_room),
            ('gateway_sigkill_preserves_logical_membership',self.gateway_restart_keeps_room),
            ('real_postgres_settlement_releases_room',self.actual_settlement),
            ('offline_export_and_original_replay_tool_verify_durable_segments',self.offline_replay_export)):
            self.report['current_case']=name
            start=time.monotonic(); details=action()
            self.report['cases'].append(dict(name=name,passed=True,elapsed_seconds=time.monotonic()-start,**details))
        self.report['restart_timeline']=list(self.report['shutdowns'])
        self.report['passed']=True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('descriptor','auth','lobby','gateway','replay','replay-export','pg-bin','report'):
        parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--room', type=Path)
    parser.add_argument('--repository-root',type=Path,default=ROOT)
    parser.add_argument('--pg-share',type=Path); parser.add_argument('--pg-lib',type=Path)
    args=parser.parse_args()
    args.repository_root=args.repository_root.resolve()
    test=Acceptance(args)
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        test.run()
    except (Exception,KeyboardInterrupt) as error:
        test.report.update(passed=False,error=type(error).__name__+': '+str(error))
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        test.close()
    test.report['source_hashes']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [ROOT/'services/gateway/app/main.cpp',ROOT/'services/lobby/app/main.cpp',
                  ROOT/'services/lobby/src/LobbyAuthority.cpp',ROOT/'services/lobby/src/LobbyService.cpp',
                  ROOT/'tests/lobby/ServiceStackFixture.py', ROOT/'services/room/worker/app/main.cpp',
                  ROOT/'services/room/worker/src/RoomWorker.cpp', ROOT/'services/room/src/RoomRuntime.cpp', Path(__file__)]}
    output=json.dumps(test.report,indent=2)
    test.assert_no_secrets(output)
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(output+'\n')
    print(json.dumps(dict(passed=test.report['passed'],cases=len(test.report['cases']),
                         current_case=test.report.get('current_case'),error=test.report.get('error'),report=str(args.report))))
    return 0 if test.report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
