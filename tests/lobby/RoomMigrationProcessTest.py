#!/usr/bin/env python3
"""Real 1000-distinct-Room migration gate; phase crashes are a separate gate."""
import argparse
import asyncio
import base64
from collections import Counter
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import signal
import struct
import subprocess
import sys
import time
import traceback

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, *(ROOT/'tests'/name for name in ('lobby','auth','storage','room','gateway/net','match'))):
    sys.path.insert(0,str(folder))
from ServiceStackFixture import ServiceStack
from RoomIntegrationTest import canonical_checksum


class CheckFailure(Exception):
    """Safe constant invariant text; never an untrusted response."""


def check(condition, message):
    if not condition:
        raise CheckFailure(message)


def token_expiry(grant, signing_key, now):
    """Verify the real issuer output; return only the expiry used for scheduling."""
    token=grant['token'].decode('ascii')
    parts=token.split('.')
    check(len(parts) == 3 and len(token) <= 4096,'invalid renewal credential encoding')
    def decode(value):
        return base64.urlsafe_b64decode(value+'='*((4-len(value)%4)%4))
    signature=hmac.new(signing_key.encode(),'.'.join(parts[:2]).encode(),hashlib.sha256).digest()
    check(hmac.compare_digest(decode(parts[2]),signature),'renewal credential signature mismatch')
    header,payload=(json.loads(decode(value)) for value in parts[:2])
    check(header == dict(alg='HS256',typ='JWT') and
          payload.get('iss') == 'arena-auth' and payload.get('aud') == 'arena-gateway' and
          payload.get('sub') == str(grant['player']) and payload.get('epoch') == str(grant['epoch']),
          'renewal credential profile or identity mismatch')
    check(all(type(payload.get(k)) is int for k in ('iat','nbf','exp')) and
          payload['iat'] == payload['nbf'] <= now < payload['exp'] and
          payload['exp']-payload['iat'] == 300,'production credential lifetime changed')
    return payload['exp']


def renewal_due(expires, now):
    return expires-now <= 60


def renewal_self_test():
    key='test-only-signing-key-never-used-by-business'
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).rstrip(b'=')
    header=encode(dict(alg='HS256',typ='JWT'))
    def credential(payload):
        signed=header+b'.'+encode(payload)
        return signed+b'.'+base64.urlsafe_b64encode(hmac.new(key.encode(),signed,hashlib.sha256).digest()).rstrip(b'=')
    payload=dict(iss='arena-auth',aud='arena-gateway',sub='42',epoch='7',iat=100,nbf=100,exp=400)
    grant=dict(token=credential(payload),player=42,epoch=7)
    check(token_expiry(grant,key,200) == 400,'renewal expiry positive fixture')
    check(not renewal_due(400,339) and renewal_due(400,340) and renewal_due(400,400),
          'renewal scheduling boundary')
    negatives=[(grant,'different-signing-key',200),(grant,key,400),
        (dict(grant,player=43),key,200),
        (dict(grant,token=credential(dict(payload,exp=401))),key,200)]
    for candidate,secret,now in negatives:
        try: token_expiry(candidate,secret,now)
        except CheckFailure: pass
        else: raise CheckFailure('renewal verifier accepted negative fixture')
    return dict(renewal_helper_checks=6,renewal_business_processes_started=0)


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            value.update(block)
    return value.hexdigest()


def percentile(values, fraction):
    check(bool(values),'empty latency population')
    return sorted(values)[max(0,math.ceil(len(values)*fraction)-1)]


class ObservedMigrationReply:
    """Test-only metadata: RPC decode completion, never an internal commit instant."""
    def __init__(self,value,observed_at_ns):
        self.value,self.observed_at_ns=value,observed_at_ns

    def __getattr__(self,name):
        return getattr(self.value,name)


class ReplayReader:
    """Reads an actual completed version-one archive, never a live WAL."""
    def __init__(self,data):
        check(104 <= len(data) <= 64*1024*1024,'replay byte bound')
        self.data,self.offset=data,0

    def take(self,size):
        check(size <= len(self.data)-self.offset,'truncated replay value')
        value=self.data[self.offset:self.offset+size]
        self.offset+=size
        return value

    def integer(self,kind):
        return struct.unpack('<'+kind,self.take(struct.calcsize('<'+kind)))[0]

    def member(self):
        value=tuple(self.integer('Q') for _ in range(3))
        check(all(value),'zero replay member identity')
        return value

    def player(self):
        identity,session,epoch=self.member()
        return dict(player=identity,session=session,epoch=epoch,
            x=self.integer('i'),y=self.integer('i'),seq=self.integer('Q'),
            dx=self.integer('i'),dy=self.integer('i'))


def read_archive(data):
    reader=ReplayReader(data)
    check(reader.take(8) == b'AREPLAY'+bytes([0]),'replay magic')
    check(reader.integer('I') == reader.integer('I') == 1,'replay format/checksum version')
    members,inputs,history=(reader.integer('Q') for _ in range(3))
    limit=reader.integer('i')
    check(0 < members <= 4096 and 0 < inputs <= 65536 and
          0 < history <= 1000000 and limit > 0,'replay policy bound')
    room,incarnation,tick,checksum=(reader.integer('Q') for _ in range(4))
    check(room and incarnation and tick == 0,'replay initial identity/anchor')
    count=reader.integer('I')
    check(count <= members,'replay initial roster bound')
    players={}
    for _ in range(count):
        player=reader.player()
        check(player['player'] not in players,'duplicate initial replay player')
        players[player['player']]=player
    check(list(players) == sorted(players),'noncanonical initial replay roster')
    state=dict(room=room,incarnation=incarnation,tick=tick,checksum=checksum,
               limit=limit,version=1,players=players)
    check(canonical_checksum(state) == checksum,'initial replay checksum')
    records=[]
    while True:
        tag,length=reader.integer('I'),reader.integer('I')
        if tag == 0x21444e45:
            check(length == 16 and reader.integer('Q') == len(records) and
                  reader.integer('Q') == tick and reader.offset == len(data),'replay footer')
            return state,records
        check(tag == 0x4b434954 and len(records) < 1000000 and length >= 24,'replay tick frame')
        start=reader.offset
        index,expected=reader.integer('Q'),reader.integer('Q')
        roster_count,attempt_count=reader.integer('I'),reader.integer('I')
        check(index == tick+1 and roster_count <= members and attempt_count <= inputs and
              length == 24+24*roster_count+40*attempt_count,'replay tick/count continuity')
        roster=[reader.member() for _ in range(roster_count)]
        check([m[0] for m in roster] == sorted(set(m[0] for m in roster)),'replay roster ordering')
        attempts=[(*reader.member(),reader.integer('Q'),reader.integer('i'),reader.integer('i'))
                  for _ in range(attempt_count)]
        check(attempts == sorted(attempts) and reader.offset-start == length,'replay attempt ordering/length')
        records.append(dict(tick=index,checksum=expected,members=roster,inputs=attempts))
        tick=index


def check_history(data, expected_attempts, observations):
    """Rebuild integer state and match all ACKs, including retained duplicates."""
    state,records=read_archive(data)
    events,checksums=[],{state['tick']:state['checksum']}
    for record in records:
        previous=state['players']
        state['players']={
            member[0]:dict(previous[member[0]])
            if member[0] in previous and
               (previous[member[0]]['session'],previous[member[0]]['epoch']) == member[1:]
            else dict(player=member[0],session=member[1],epoch=member[2],x=0,y=0,seq=0,dx=0,dy=0)
            for member in record['members']}
        for attempt in record['inputs']:
            identity,session,epoch,sequence,dx,dy=attempt
            player=state['players'].get(identity)
            status=7 if sequence == 0 or not -1 <= dx <= 1 or not -1 <= dy <= 1 else None
            if status is None and (player is None or (player['session'],player['epoch']) != (session,epoch)):
                status=6
            if status is None:
                if sequence < player['seq']:
                    status=4
                elif sequence == player['seq']:
                    status=2 if (dx,dy) == (player['dx'],player['dy']) else 5
                elif sequence != player['seq']+1:
                    status=3
                else:
                    status=1
                    player.update(seq=sequence,dx=dx,dy=dy,
                        x=max(-state['limit'],min(state['limit'],player['x']+dx)),
                        y=max(-state['limit'],min(state['limit'],player['y']+dy)))
            events.append((*attempt,status))
        state['tick']=record['tick']
        state['checksum']=canonical_checksum(state)
        check(state['checksum'] == record['checksum'],'ordered input checksum divergence')
        checksums[state['tick']]=state['checksum']
    check(Counter(events) == Counter(tuple(value) for value in expected_attempts),
          'lost, duplicated or unacknowledged input attempt')
    for observed in observations:
        check(observed['tick'] in checksums and checksums[observed['tick']] == observed['checksum'],
              'acknowledged tick/checksum absent from complete history')
    return dict(ticks=len(records),final_tick=state['tick'],checksum=str(state['checksum']),
        applied=sum(e[-1] == 1 for e in events),duplicates=sum(e[-1] == 2 for e in events),
        checksums=checksums)


def checker_self_test():
    def member(value):
        return struct.pack('<QQQ',*value)
    alice,bob=(42,11,1),(43,12,2)
    initial=dict(room=7,incarnation=9,tick=0,limit=100,version=1,players={})
    initial['checksum']=canonical_checksum(initial)
    header=b'AREPLAY'+bytes([0])+struct.pack('<IIQQQiQQQQI',1,1,64,256,200,100,7,9,0,initial['checksum'],0)
    first=dict(initial,tick=1,players={
        42:dict(player=42,session=11,epoch=1,x=1,y=0,seq=1,dx=1,dy=0),
        43:dict(player=43,session=12,epoch=2,x=0,y=0,seq=0,dx=0,dy=0)})
    first['checksum']=canonical_checksum(first)
    last=dict(first,tick=2,players={42:dict(first['players'][42]),
        43:dict(player=43,session=12,epoch=2,x=0,y=1,seq=1,dx=0,dy=1)})
    last['checksum']=canonical_checksum(last)
    def frame(index,checksum,attempts):
        payload=struct.pack('<QQII',index,checksum,2,len(attempts))+member(alice)+member(bob)
        payload+=b''.join(member(a[:3])+struct.pack('<Qii',*a[3:]) for a in attempts)
        return struct.pack('<II',0x4b434954,len(payload))+payload
    a,b=(*alice,1,1,0),(*bob,1,0,1)
    f1,f2=frame(1,first['checksum'],[a]),frame(2,last['checksum'],[a,b])
    footer=struct.pack('<IIQQ',0x21444e45,16,2,2)
    data=header+f1+f2+footer
    attempts=[(*a,1),(*a,2),(*b,1)]
    observed=[dict(tick=1,checksum=first['checksum']),dict(tick=2,checksum=last['checksum'])]
    result=check_history(data,attempts,observed)
    check(result['applied'] == 2 and result['duplicates'] == 1,'checker positive fixture')
    negatives=[(data,attempts[:-1],observed),(data,attempts+[(*a,1)],observed),
        (header+f2+f1+footer,attempts,observed),(data[:-1],attempts,observed),
        (data,attempts,[dict(tick=2,checksum=last['checksum']^1)]),
        (header+f1+frame(2,last['checksum']^1,[a,b])+footer,attempts,observed)]
    for content,expected,acks in negatives:
        try:
            check_history(content,expected,acks)
        except CheckFailure:
            pass
        else:
            raise CheckFailure('checker accepted a negative fixture')
    return dict(checker_unit_passed=1+len(negatives),business_processes_started=0)


class Acceptance(ServiceStack):
    def __init__(self,args):
        super().__init__(args,grace_ms=30000)
        self.operations_token=secrets.token_hex(32)
        while self.operations_token in self.secrets:
            self.operations_token=secrets.token_hex(32)
        self.secrets+=(self.operations_token,)
        self.deadline=time.monotonic()+args.total_timeout
        self.call_serial=0
        self.rooms=[]
        self.initial_pids=None
        self.worker_ids={}
        self.grants={}
        self.grant_expiries={}
        self.report.update(scope='real cross-process distinct-Room migration count gate',
            diagnostic=bool(args.diagnostic),requested_rooms=args.count,unique_room_count=0,migration_count=0,
            migration_cases=self.rooms,admin_rpc=[],worker_samples=[],
            token_renewals=[],token_renewal_count=0,token_lifetime_seconds=300,in_room_renewal_verified=False,
            stale_epoch=dict(admin_attempts=0,admin_rejected=0,private_apply_available=False,
                private_apply_gap='No external central private-Apply mutation API exists.'),
            phase_crash=dict(implemented=False,executed=0,planned=[dict(phase=phase,role=role)
                for phase in ('PREPARING','FROZEN','TRANSFERRED','COMMITTED')
                for role in ('source','target','coordinator')]),
            full_count_gate=False,day34_hard_acceptance_complete=False,
            coverage_gaps=['Phase-crash matrix is outside this incremental count harness.',
                          'Old private worker proposals cannot be injected into central Apply over a network API.'])

    def command(self,role):
        command,environment=super().command(role)
        if role == 'lobby':
            environment['ARENA_OPERATIONS_TOKEN']=self.operations_token
        else:
            check(self.operations_token not in environment.values(),'operations credential escaped Lobby')
        return command,environment

    def remaining(self,maximum=1.0):
        remaining=self.deadline-time.monotonic()
        check(remaining > 0,'whole migration gate deadline')
        return min(maximum,remaining)

    def message(self,name,**values):
        return self.protocol.message(name)(**values)

    def call(self,method,request,reply,*,caller='operations',token=None,target='lobby',port=None):
        async def exchange():
            from shared.runtime.rpc import Client
            client=Client(self.protocol,'127.0.0.1',port or self.ports['lobby'],max_inflight=1,
                caller_service=caller,target_service=target,
                service_token=self.operations_token if token is None else token)
            try:
                payload=await client.call(method,request.SerializeToString() if request is not None else b'',
                    timeout=self.remaining(),correlation_id='day34-'+str(self.call_serial))
                value=self.protocol.message(reply).FromString(payload)
                return ObservedMigrationReply(value,time.monotonic_ns()) if reply == 'arena.lobby.MigrationReply' else value
            finally:
                check(client.inflight == 0,'admin RPC retained an inflight loan')
        self.call_serial+=1
        started=time.monotonic()
        row=dict(method=method,caller=caller,target=target,started=started,status='started')
        try:
            value=asyncio.run(exchange());row['status']=0;return value
        except Exception as error:
            row['status']=getattr(error,'status',type(error).__name__)
            raise
        finally:
            row['elapsed_ms']=(time.monotonic()-started)*1000
            self.report['admin_rpc'].append(row)

    def rejected(self,method,request,status,**options):
        from shared.runtime.rpc import RpcError
        try:
            self.call(method,request,'arena.lobby.MigrationReply',**options)
        except RpcError as error:
            check(error.status == status,'wrong typed admin rejection')
            return
        raise CheckFailure('invalid role, epoch or conflicting migration was accepted')

    def status(self,room,incarnation,migration=0):
        value=self.call('room.migration.status',self.message('arena.lobby.MigrationStatusRequest',
            version=1,room_id=room,incarnation=incarnation,migration_id=migration),'arena.lobby.MigrationReply')
        check(value.version == 1 and value.room_id == room and value.incarnation == incarnation and
              value.owner_epoch > 0 and value.committed_revision > 0,'invalid canonical migration status')
        return value

    @staticmethod
    def view(value):
        return dict(observed_at_ns=value.observed_at_ns,
            timestamp_semantics='observer RPC decode completion; not internal WAL commit time',
            room=value.room_id,incarnation=str(value.incarnation),migration=value.migration_id,
            owner=value.owner,owner_epoch=value.owner_epoch,phase=value.phase,source=value.source,
            target=value.target,source_epoch=value.source_epoch,target_epoch=value.target_epoch,
            cutoff=value.cutoff,deadline_nanos=value.deadline_nanos,physical_tick=value.physical_tick,
            logical_tick=value.logical_tick,checksum=str(value.checksum),cursor=value.cursor,
            committed_revision=value.committed_revision)

    def physical_workers(self,label):
        rows={}
        for node in self.room_roles:
            reply=self.call('health.ready',None,'arena.room.HealthReply',caller='lobby',
                           token=self.room_token,target='room',port=self.ports[node])
            check(reply.version == 1 and reply.ready and reply.node_id == node and
                  reply.worker_id.startswith(node+':') and len(reply.worker_id) == len(node)+33,
                  'physical Room identity probe mismatch')
            if node in self.worker_ids:
                check(self.worker_ids[node] == reply.worker_id,'Room worker unexpectedly restarted')
            self.worker_ids[node]=reply.worker_id
            rows[node]=dict(worker_id=reply.worker_id,pid=self.children[node].process.pid,
                active_routes=reply.stats.active_routes,inactive_stages=reply.stats.inactive_stages,
                cache_bytes=reply.stats.cache_bytes,journal_pending=reply.stats.journal_pending,
                journal_file_bytes=reply.stats.journal_file_bytes,rpc_active=reply.stats.rpc_active)
        self.report['worker_samples'].append(dict(label=label,roles=rows))

    def role_boundaries(self):
        request=self.message('arena.lobby.MigrationStatusRequest',version=1,room_id=1,incarnation=1)
        self.rejected('room.migration.status',request,10,caller='gateway',token=self.lobby_token)
        request=self.message('arena.lobby.MigrationRequest',version=1,room_id=1,incarnation=1,
                            owner_epoch=1,migration_id=1,target_node='room-b')
        self.rejected('room.migration.start',request,10,caller='gateway',token=self.lobby_token)
        self.rejected('lobby.execute',self.message('arena.lobby.ExecuteRequest'),10)
        self.report['role_boundaries']=dict(gateway_admin_rejected=True,operations_execute_rejected=True)

    def remember_grant(self,result):
        self.secrets+=(result['token'].decode(),)
        expires=token_expiry(result,self.signer_key,time.time())
        self.grants[result['player']]=result
        self.grant_expiries[result['player']]=expires
        return result

    def grant(self,peer,account):
        return self.remember_grant(peer.login(account))

    def renew(self,player,room,*,forced=False):
        name='alice' if player == 42 else 'bob'
        old=getattr(self,name)
        prior=self.grants[player]
        previous_expiry=self.grant_expiries[player]
        check(previous_expiry > time.time(),'renewal started after credential expiry')
        replacement=self.connect(old.ws)
        replacement.socket.settimeout(old.socket.gettimeout())
        fresh=self.remember_grant(replacement.login(token=prior['token']))
        check(fresh['resumed'] and fresh['player'] == player and
              fresh['session'] == self.sessions[player] == prior['session'] and
              fresh['room'] == room and fresh['epoch'] > prior['epoch'],
              'TOKEN renewal replaced logical session or room binding')
        expires=self.grant_expiries[player]
        check(expires >= previous_expiry and (forced or expires > previous_expiry),
              'TOKEN renewal did not refresh expiry')
        setattr(self,name,replacement)
        old.close()
        self.report['token_renewals'].append(dict(previous_expiry=previous_expiry,
            renewed_expiry=expires,forced_in_room=forced,logical_session_preserved=True))
        self.report['token_renewal_count']=len(self.report['token_renewals'])

    def renew_due(self):
        # Run between Room lifetimes; no migration latency sample includes login.
        for player in (42,43):
            if renewal_due(self.grant_expiries[player],time.time()):
                self.renew(player,0)

    def renew_in_room(self,row,state):
        for player in (42,43):
            self.renew(player,row['room'],forced=True)
        after=self.snapshot(row)
        check(after['incarnation'] == state['incarnation'] and after['players'] == state['players'] and
              after['tick'] >= state['tick'],'TOKEN handoff changed membership, input sequence or positions')
        check(self.alice.query()['session'] == self.sessions[42] and
              self.bob.query()['session'] == self.sessions[43],'TOKEN handoff replaced original sessions')
        self.report['in_room_renewal_verified']=True
        return after

    def snapshot(self,row):
        value=self.alice.room_snapshot()
        check(value['room'] == row['room'] and value['incarnation'] == int(row['incarnation']) and
              set(value['players']) == {42,43},'room identity or member set changed during migration')
        check({p:(m['session'],m['epoch']) for p,m in value['players'].items()} == self.bindings,
              'session or membership epoch changed during migration')
        row['observations'].append(dict(tick=value['tick'],checksum=value['checksum']))
        return value

    def control(self,row,peer,state,player,sequence,dx,dy,expected_status):
        self.remaining()
        reply,status=peer.control(state,player,sequence,dx,dy)
        check(status == expected_status,'input result changed across worker ownership')
        check(reply['room'] == row['room'] and reply['incarnation'] == int(row['incarnation']) and
              {p:(m['session'],m['epoch']) for p,m in reply['players'].items()} == self.bindings,
              'input ACK changed room/session/membership')
        member=reply['players'][player]
        row['attempts'].append((player,member['session'],member['epoch'],sequence,dx,dy,status))
        row['observations'].append(dict(tick=reply['tick'],checksum=reply['checksum']))
        return reply

    def migrate_room(self,number):
        self.renew_due()
        room=900000+number
        row=dict(index=number,room=room,incarnation='',passed=False,phase_observations=[],
                 attempts=[],observations=[])
        self.rooms.append(row)
        check(self.alice.join(room)['room'] == self.bob.join(room)['room'] == room,'manual room join failed')
        state=self.alice.room_snapshot()
        check(state['room'] == room and set(state['players']) == {42,43} and
              all(m['seq'] == 0 for m in state['players'].values()),'new room roster/sequence')
        self.bindings={p:(m['session'],m['epoch']) for p,m in state['players'].items()}
        check({p:b[0] for p,b in self.bindings.items()} == self.sessions,
              'serial room creation replaced original logical sessions')
        row['incarnation']=str(state['incarnation'])
        row['observations'].append(dict(tick=state['tick'],checksum=state['checksum']))
        state=self.control(row,self.alice,state,42,1,1,0,1)
        state=self.control(row,self.bob,state,43,1,0,1,1)
        if number == 0:
            state=self.renew_in_room(row,state)
        before=self.status(room,state['incarnation'])
        source=before.owner.split(':',1)[0]
        check(source in self.room_roles and self.worker_ids[source] == before.owner,
              'route is not a real worker process')
        target=next(node for node in self.room_roles if node != source)
        row.update(source=source,target=target,before=self.view(before))
        request=self.message('arena.lobby.MigrationRequest',version=1,room_id=room,
            incarnation=state['incarnation'],owner_epoch=before.owner_epoch,migration_id=1,target_node=target)
        started=time.monotonic()
        prepared=self.call('room.migration.start',request,'arena.lobby.MigrationReply')
        check(prepared.migration_id == 1 and prepared.phase in (1,2,3,4,5) and
              prepared.source == before.owner and prepared.target == self.worker_ids[target] and
              prepared.source_epoch == before.owner_epoch and prepared.target_epoch == before.owner_epoch+1 and
              prepared.committed_revision > before.committed_revision,'durable Prepare identity/fence mismatch')
        row['phase_observations'].append(self.view(prepared))
        state=self.control(row,self.bob,state,43,2,0,-1,1)
        until=min(self.deadline,started+10)
        while True:
            current=self.status(room,state['incarnation'],1)
            row['phase_observations'].append(self.view(current))
            check(current.phase != 6,'normal migration aborted')
            if current.phase == 5:
                break
            check(time.monotonic() < until,'normal migration did not commit within recovery budget')
            time.sleep(.005)
        check(current.owner == self.worker_ids[target] and current.owner_epoch == before.owner_epoch+1 and
              current.physical_tick >= before.physical_tick and current.cursor >= current.cutoff,
              'committed route does not name the target worker')
        state=self.control(row,self.alice,state,42,1,1,0,2)
        first_target_ack=time.monotonic()
        check(state['tick'] > current.physical_tick,'first target ACK did not advance a durable physical tick')
        after=self.status(room,state['incarnation'],1)
        check(after.owner == current.owner and after.owner_epoch == current.owner_epoch and
              after.physical_tick >= state['tick'],'target tick lost its committed owner fence')
        row['migration_latency_ms']=(first_target_ack-started)*1000
        state=self.control(row,self.alice,state,42,2,-1,0,1)
        state=self.control(row,self.bob,state,43,2,0,-1,2)
        check(all(m['seq'] == 2 and (m['x'],m['y']) == (0,0) for m in state['players'].values()),
              'acknowledged controls were lost or applied twice')
        repeat=self.call('room.migration.start',request,'arena.lobby.MigrationReply')
        check(repeat.phase == 5 and repeat.owner == current.owner and repeat.source == current.source and
              repeat.target == current.target and repeat.source_epoch == current.source_epoch and
              repeat.target_epoch == current.target_epoch and repeat.cutoff == current.cutoff and
              repeat.deadline_nanos == current.deadline_nanos,'same migration ID/body changed the handoff')
        conflict=self.message('arena.lobby.MigrationRequest')
        conflict.CopyFrom(request);conflict.target_node=source
        self.rejected('room.migration.start',conflict,7)
        stale=self.message('arena.lobby.MigrationRequest',version=1,room_id=room,
            incarnation=state['incarnation'],owner_epoch=before.owner_epoch,migration_id=2,target_node=source)
        self.report['stale_epoch']['admin_attempts']+=1
        self.rejected('room.migration.start',stale,7)
        self.report['stale_epoch']['admin_rejected']+=1
        self.rejected('room.migration.status',self.message('arena.lobby.MigrationStatusRequest',
            version=1,room_id=room,incarnation=state['incarnation'],migration_id=2),7)
        self.snapshot(row)
        check(self.alice.query()['session'] == self.sessions[42] and
              self.bob.query()['session'] == self.sessions[43],'migration replaced live sessions')
        check(self.alice.leave()['room'] == self.bob.leave()['room'] == 0,'room bindings did not release')
        check(self.sample('migration-'+str(number)) == self.initial_pids,
              'normal migration restarted a physical service')
        row.update(source=source,target=target,before=self.view(before),committed=self.view(current),passed=True)
        self.report['unique_room_count']=len({r['room'] for r in self.rooms if r['passed']})
        self.report['migration_count']=sum(r['passed'] for r in self.rooms)
        if number % 25 == 0:
            self.physical_workers('migration-'+str(number))
            values=[r['migration_latency_ms'] for r in self.rooms if r['passed']]
            progress=dict(completed_rooms=len(values),requested_rooms=self.args.count,
                p99_ms=percentile(values,.99),max_ms=max(values),final_acceptance=False)
            path=self.args.report.with_suffix('.progress.json')
            temporary=path.with_suffix('.tmp')
            temporary.write_text(json.dumps(progress)+'\n')
            temporary.replace(path)

    def baseline_smoke(self):
        self.setup()
        self.initial_pids=self.sample('migration-initial-ready')
        check(len(self.initial_pids) == len(set(self.initial_pids.values())) == 6,
              'six real independent processes required')
        self.physical_workers('before-migrations')
        self.role_boundaries()
        self.alice,self.bob=self.connect(),self.connect(True)
        self.alice.socket.settimeout(2);self.bob.socket.settimeout(2)
        first,second=self.grant(self.alice,b'alice'),self.grant(self.bob,b'bob')
        check(first['player'] == 42 and second['player'] == 43,'canonical bot identities')
        self.sessions={42:first['session'],43:second['session']}
        self.migrate_room(0)

    def migrate1000(self):
        self.baseline_smoke()
        for index in range(1,self.args.count):
            self.migrate_room(index)
        check(len(self.rooms) == self.args.count and all(r['passed'] for r in self.rooms),
              'migration denominator incomplete')
        check(len({r['incarnation'] for r in self.rooms}) == self.args.count,'actor incarnation reused')
        self.physical_workers('after-migrations')
        values=[r['migration_latency_ms'] for r in self.rooms]
        p99=percentile(values,.99)
        self.report['resource_sampled_max']={role:{metric:max(
            sample['roles'][role][metric] for sample in self.report['physical_process_samples'])
            for metric in ('fd','sockets','rss_bytes')} for role in self.children}
        self.report['rpc_rejection_counts']={str(status):sum(row['status'] == status
            for row in self.report['admin_rpc']) for status in (7,10)}
        check(self.report['in_room_renewal_verified'] and
              all(item['logical_session_preserved'] for item in self.report['token_renewals']),
              'renewal/session preservation proof missing')
        self.report['token_renewal_count']=len(self.report['token_renewals'])
        self.report['migration_directions']=dict(Counter(row['source']+'->'+row['target'] for row in self.rooms))
        self.report['latency']=dict(definition='first start RPC -> first public target-owner durable tick ACK',
            population=len(values),p50_ms=percentile(values,.5),p99_ms=p99,max_ms=max(values),limit_ms=2000)
        check(p99 <= 2000,'migration P99 exceeded two seconds')
        check(self.report['stale_epoch']['admin_attempts'] == self.report['stale_epoch']['admin_rejected'] == self.args.count,
              'stale admin-epoch rejection denominator changed')
        self.export_and_check()
        self.report.update(passed=True,full_count_gate=self.args.count == 1000 and not self.args.diagnostic)

    def export_and_check(self):
        for peer in self.peers:
            peer.close()
        self.children['gateway'].stop();self.children['lobby'].stop()
        for role in reversed(self.room_roles):
            self.children[role].stop()
        output=self.args.report.parent/(self.args.report.stem+'-replays-'+str(os.getpid())+'-'+str(time.time_ns()))
        output.mkdir(mode=0o700,parents=True)
        self.report['replay_directory']=str(output)
        exported=subprocess.run([str(self.args.replay_export),'--state-dir',str(self.cluster.root/'lobby-state'),
            '--output-dir',str(output)],cwd=self.args.repository_root,stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=self.remaining(60))
        self.assert_no_secrets(exported.stdout+exported.stderr)
        check(exported.returncode == 0,'offline migration replay export failed')
        manifest=json.loads(exported.stdout)
        check(manifest.get('status') == 'ok' and len(manifest['files']) == self.args.count,'offline archive denominator')
        expected={(r['room'],int(r['incarnation'])):r for r in self.rooms}
        verified=[]
        for entry in manifest['files']:
            identity=(int(entry['room_id']),int(entry['incarnation']))
            check(identity in expected,'offline exporter introduced a different actor')
            name=entry['file'];check(Path(name).name == name,'unsafe archive filename')
            row=expected.pop(identity);path=output/name
            result=check_history(path.read_bytes(),row['attempts'],row['observations'])
            verification=output/(name+'.verified.json')
            native=subprocess.run([str(self.args.replay),str(path),'--report',str(verification)],cwd=self.args.repository_root,
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                timeout=self.remaining(8))
            self.assert_no_secrets(native.stdout+native.stderr)
            check(native.returncode == 0,'original arena_replay rejected migrated history')
            report=json.loads(native.stdout)
            check(report['status'] == 'ok' and int(report['final_tick']) == result['final_tick'] and
                  str(report['final_checksum']) == result['checksum'] and
                  {int(v['tick']):int(v['checksum']) for v in report['checksums']} ==
                  {tick:checksum for tick,checksum in result['checksums'].items() if tick > 0},
                  'native replay and independent every-tick checker disagree')
            check(result['applied'] == 4 and result['duplicates'] == 2,'per-room input denominator')
            verified.append(dict(room=identity[0],incarnation=str(identity[1]),file=name,sha256=digest(path),
                verification=verification.name,verification_sha256=digest(verification),
                ticks=result['ticks'],checksum=result['checksum'],applied=result['applied'],duplicates=result['duplicates']))
        check(not expected,'migrated actor has no complete offline replay')
        self.report['replay_verified']=verified
        self.report['applied_controls']=sum(r['applied'] for r in verified)
        self.report['duplicate_attempts_preserved']=sum(r['duplicates'] for r in verified)
        self.report['lost_controls']=self.report['duplicated_applications']=0


def arguments(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--repository-root',type=Path,default=ROOT)
    for name in ('pg-bin','pg-share','pg-lib','descriptor','auth','lobby','gateway','room','replay-export','replay','report'):
        parser.add_argument('--'+name,type=Path)
    parser.add_argument('--count',type=int,default=1000)
    parser.add_argument('--diagnostic',action='store_true')
    parser.add_argument('--total-timeout',type=float,default=1200)
    args=parser.parse_args(argv)
    if not args.self_test:
        for name in ('pg_bin','descriptor','auth','lobby','gateway','room','replay_export','replay','report'):
            if getattr(args,name) is None:
                parser.error('real migration gate requires --'+name.replace('_','-'))
        if not 1 <= args.count <= 1000 or args.count != 1000 and not args.diagnostic:
            parser.error('reduced counts require --diagnostic; the complete count is 1000')
        if not math.isfinite(args.total_timeout) or not 30 <= args.total_timeout <= 1800:
            parser.error('total timeout must be finite and within 30..1800 seconds')
    for name,value in vars(args).items():
        if isinstance(value,Path):
            setattr(args,name,value.resolve())
    return args


def main():
    args=arguments()
    if args.self_test:
        print(json.dumps(dict(**checker_self_test(),**renewal_self_test())));return 0
    names=['tests/lobby/RoomMigrationProcessTest.py','tests/lobby/ServiceStackFixture.py',
        'tests/room/RoomIntegrationTest.py','tests/room/JourneyProtocol.py','shared/runtime/rpc.py',
        'shared/src/persistence/AtomicJournal.cpp','shared/proto/lobby.proto','shared/proto/room.proto',
        'services/lobby/app/main.cpp','services/lobby/src/LobbyService.cpp','services/lobby/src/LobbyAuthority.cpp',
        'services/lobby/src/RoomCoordinator.cpp','services/lobby/src/RoomClient.cpp',
        'services/lobby/src/RoomMigration.cpp','services/lobby/src/ReplayArchive.cpp',
        'services/room/worker/app/main.cpp','services/room/worker/src/RoomWorker.cpp',
        'services/room/worker/src/RoomWire.cpp','services/room/src/RoomRuntime.cpp','services/room/src/RoomActor.cpp',
        'services/room/src/ReplayFile.cpp','services/room/app/ReplayMain.cpp']
    sources={name:digest(args.repository_root/name) for name in names}
    binaries={name:dict(path=str(getattr(args,name)),sha256=digest(getattr(args,name)))
              for name in ('auth','lobby','gateway','room','replay_export','replay')}
    test=None
    failure=dict(passed=False)
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        test=Acceptance(args);test.migrate1000()
    except (Exception,KeyboardInterrupt) as error:
        failure.update(error=type(error).__name__,rpc_status=getattr(error,'status',None),failure_locations=[dict(
            file=Path(f.filename).name,line=f.lineno,function=f.name)
            for f in traceback.extract_tb(error.__traceback__)])
        if isinstance(error,CheckFailure):
            failure['invariant']=str(error)
        if test is not None:
            test.report.update(failure)
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        if test is not None:
            try:
                test.close()
            except Exception as error:
                test.report.update(passed=False,cleanup_error=type(error).__name__)
    report=test.report if test is not None else failure
    report.update(source_hashes=sources,binary_hashes=binaries)
    if sources != {name:digest(args.repository_root/name) for name in names}:
        report.update(passed=False,source_changed_during_gate=True)
    if test is not None:
        report['not_run_room_count']=args.count-len(test.rooms)
        report['remaining_owned_pids']=[dict(role=role,pid=pid) for role,child in test.children.items()
            for pid in child.history if Path('/proc',str(pid)).exists()]
        if report['remaining_owned_pids']:
            report['passed']=False
    report['full_count_gate']=bool(report.get('passed') and args.count == 1000 and not args.diagnostic)
    output=json.dumps(report,indent=2)
    if test is not None:
        test.assert_no_secrets(output)
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(output+'\n')
    print(json.dumps(dict(passed=report['passed'],full_count_gate=report.get('full_count_gate',False),
        unique_room_count=report.get('unique_room_count',0),report=str(args.report),error=report.get('error'))))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
