#!/usr/bin/env python3
"""Normal configuration, reload ownership and read-only health acceptance."""
import argparse
import asyncio
from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch


def require(value, message):
    if not value:
        raise AssertionError(message)


def document(version=1, **changes):
    value=dict(version=str(version),desiredReady=True,drainGraceMs=5000,
        nodes=[dict(service='storage',nodeId='node-a',host='127.0.0.1',port=10000+version%1000,ready=True)])
    value.update(changes)
    return json.dumps(value)


async def until(predicate, timeout=5):
    deadline=time.monotonic()+timeout
    while not predicate():
        require(time.monotonic()<deadline,'condition did not become true')
        await asyncio.sleep(.001)


async def storage_lifecycle(args):
    from services.storage import server as storage
    cases, failures = [], []
    options = SimpleNamespace(port=0, runtime_config=None, health_port=None,
        descriptor=args.descriptor, workers=1, redis_socket=None, publish=False)
    environment = dict(ARENA_INTERNAL_TOKEN='x'*32)

    async def explicit_empty_config():
        with patch.dict(os.environ, dict(environment, ARENA_RUNTIME_CONFIG=''), clear=True), \
                patch.object(storage, 'Database', side_effect=AssertionError('invalid config reached pool construction')) as database:
            try:
                await storage.run_server(options)
            except ValueError:
                pass
            else:
                raise AssertionError('explicit empty config was accepted')
            database.assert_not_called()
        return dict(name='explicit_empty_config_fails_before_pool', passed=True)

    async def cancelled_stop():
        probe_entered, probe_cleanup, release_probe = asyncio.Event(), asyncio.Event(), asyncio.Event()
        pool_entered, release_pool = threading.Event(), threading.Event()
        services, handlers = [], {}
        class Database:
            closed = False
            @property
            def stats(self):
                return dict(checked_out=0, closed=self.closed)
            def close(self):
                pool_entered.set()
                require(release_pool.wait(5), 'pool shutdown was not released')
                self.closed = True
        database = Database()
        async def probe(_):
            probe_entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                probe_cleanup.set()
                await release_probe.wait()
        original_service = storage.StorageService
        def service(*values, **keywords):
            result = original_service(*values, **keywords)
            services.append(result)
            return result
        loop = asyncio.get_running_loop()
        runner = None
        with patch.dict(os.environ, dict(environment, ARENA_HEALTH_PORT='0'), clear=True), \
                patch.object(storage, 'Database', return_value=database), \
                patch.object(storage, 'StorageService', side_effect=service), \
                patch.object(storage._RuntimeState, '_probe', probe), \
                patch.object(loop, 'add_signal_handler', side_effect=lambda sig, callback: handlers.__setitem__(sig, callback)), \
                redirect_stdout(io.StringIO()):
            try:
                runner = asyncio.create_task(storage.run_server(options))
                await until(probe_entered.is_set)
                handlers[signal.SIGTERM]()
                await until(probe_cleanup.is_set)
                runner.cancel(); await asyncio.sleep(.005)
                runner.cancel(); await asyncio.sleep(.005)
                require(not runner.done(), 'cancelled runtime close skipped worker/pool shutdown')
                release_probe.set()
                await until(pool_entered.is_set)
                runner.cancel(); await asyncio.sleep(.005)
                require(not runner.done() and services[0].worker.stats['closed'] and not database.closed,
                    'service shutdown abandoned the actual pool close job')
                release_pool.set()
                await runner
                require(database.closed and services[0].worker.stats['active']==0,
                    'stop returned before worker/pool cleanup completed')
            finally:
                release_probe.set(); release_pool.set()
                if runner is not None:
                    if not runner.done(): runner.cancel()
                    await asyncio.gather(runner, return_exceptions=True)
                for instance in services:
                    await instance.close()
        return dict(name='repeated_cancel_stop_joins_probe_worker_pool', passed=True)

    for test in (explicit_empty_config, cancelled_stop):
        try:
            cases.append(await test())
        except Exception as error:
            failures.append(test.__name__+': '+str(error))
    require(not failures, '; '.join(failures))
    return cases


async def run(args):
    from shared.runtime.config import ConfigSnapshot,ConfigStore,ConfigReloader,ConfigError,MAX_BYTES
    from shared.runtime.health import HealthServer
    from shared.runtime.protocol import Protocol
    from shared.runtime.rpc import Server,Client,RpcError,UNAVAILABLE
    protocol=Protocol(args.descriptor)
    cases=[]
    initial=ConfigSnapshot.parse(protocol,document())
    invalid=['','{','{}','[]','[{}]','null','true',document(0),document(nodes=[]),
        '{"version":"3",'+document(2)[1:],'{"desired_ready":false,'+document(2)[1:],
        '{"unknown":true,'+document(2)[1:],document(2)[:-1], ' '* (MAX_BYTES+1),
        document(2).replace('"nodeId":','"node_id":"node-b","nodeId":'),
        document(2).replace('"version":','"ver\\u0073ion":"3","version":'),
        document(drainGraceMs=10001),document(desiredReady=None),
        document().replace('"port": 10001','"port": 65536'),
        document().replace('"ready": true','"ready": "true"'),
        '{"unknown":'+ '['*20+'0'+']'*20+'}']
    for text in invalid:
        try:
            ConfigSnapshot.parse(protocol,text)
        except ConfigError as error:
            require(error.category=='invalid','unstable invalid classification')
        else:
            raise AssertionError('accepted invalid config: '+text[:100])
    require(initial.resolve('storage').port==10001,'discovery mismatch')
    try:initial.version=7
    except FrozenInstanceError:pass
    else:raise AssertionError('mutable snapshot')
    store=ConfigStore(initial)
    old=store.snapshot()
    calls=[]
    def notify(candidate):
        calls.append(candidate.version)
        require(store.snapshot() is candidate,'notification ran before publication')
        try:store.publish(ConfigSnapshot.parse(protocol,document(3)))
        except ConfigError as error:require(error.category=='busy','unbounded notification reentry')
        else:raise AssertionError('recursive publication accepted')
    store.publish(ConfigSnapshot.parse(protocol,document(2)),notify)
    require(calls==[2] and old.version==1,'retained snapshot changed')
    try:store.publish(old)
    except ConfigError as error:require(error.category=='stale','old version accepted')
    else:raise AssertionError('stale version accepted')
    store.publish(ConfigSnapshot.parse(protocol,document(3)),lambda _:(_ for _ in ()).throw(RuntimeError('observer')))
    require(store.snapshot().version==3 and store.notification_failures==1,'observer undid publication')
    cases.append(dict(name='strict_document_and_retained_versions',invalid=len(invalid),passed=True))

    store=ConfigStore(initial)
    round=threading.Barrier(9)
    failures=[]
    def read():
        for phase in range(1000):
            round.wait()
            for _ in range(2 if phase<250 else 1):
                current=store.snapshot()
                if current.resolve('storage').port !=10000+current.version%1000:
                    failures.append(current.version)
            round.wait()
    readers=[threading.Thread(target=read) for _ in range(8)]
    for reader in readers:reader.start()
    for version in range(2,1002):
        candidate=ConfigSnapshot.parse(protocol,document(version))
        round.wait();store.publish(candidate);round.wait()
    for reader in readers:reader.join()
    require(not failures and store.snapshot().version==1001,'torn snapshot')
    cases.append(dict(name='10000_reads_1000_publications',passed=True,reads=10000,publications=1000,inconsistent=0))

    with tempfile.TemporaryDirectory(prefix='arena-config-python-') as directory:
        path=Path(directory)/'runtime.json';path.write_text(document())
        require(ConfigSnapshot.read(protocol,path)==initial,'file snapshot mismatch')
        alias=Path(directory)/'alias';alias.symlink_to(path)
        try:ConfigSnapshot.read(protocol,alias)
        except ConfigError as error:require(error.category=='file','symlink classification')
        else:raise AssertionError('symlink accepted')
        store=ConfigStore(initial)
        reloader=None
        def recursive(_):reloader.request_reload()
        reloader=ConfigReloader(protocol,path,store,recursive)
        path.write_text(document(2))
        for _ in range(1000):reloader.request_reload()
        await until(lambda: reloader.stats['active']==0)
        require(store.snapshot().version==2 and reloader.stats['applied']==1
            and reloader.stats['rejected']==1 and reloader.stats['peak']==1,'reload recursion did not stop')
        path.write_text('{')
        reloader.request_reload();await until(lambda:reloader.stats['active']==0)
        require(store.snapshot().version==2 and reloader.stats['last_error']=='invalid','invalid reload replaced old')
        await reloader.close()
        cases.append(dict(name='bounded_burst_reentrant_and_invalid_reload',passed=True,stats=reloader.stats))

        store=ConfigStore(initial);path.write_text(document())
        reloader=ConfigReloader(protocol,path,store)
        for version in range(2,1002):
            path.write_text(document(version));reloader.request_reload()
            await until(lambda:reloader.stats['active']==0)
        require(store.snapshot().version==1001 and reloader.stats['applied']==1000,'sequential reload lost versions')
        await reloader.close()
        cases.append(dict(name='1000_actual_file_reloads',passed=True,stats=reloader.stats))

        entered,released=threading.Event(),threading.Event()
        original=ConfigSnapshot.read
        def waiting(protocol,path):
            entered.set();require(released.wait(2),'loader not released');return original(protocol,path)
        store=ConfigStore(initial);path.write_text(document(2))
        reloader=ConfigReloader(protocol,path,store)
        with patch.object(ConfigSnapshot,'read',side_effect=waiting):
            reloader.request_reload();await until(entered.is_set)
            closing=asyncio.create_task(reloader.close());await asyncio.sleep(.005)
            closing.cancel();await asyncio.sleep(.005);closing.cancel();await asyncio.sleep(.005)
            require(not closing.done(),'close abandoned actual file worker')
            released.set();await closing
        require(store.snapshot().version==1 and reloader.stats['active']==0,'late callback published after close')
        require(not any(thread.name.startswith('arena-config') for thread in threading.enumerate()),'loader thread leaked')
        cases.append(dict(name='double_cancel_close_joins_file_worker',passed=True))

    state=dict(service='fixture',node_id='node-a',config_version=1,desired_ready=False,draining=False,ready=False)
    health=HealthServer(lambda:state,max_connections=2,timeout=.1)
    port=await health.start()
    async def get(path):
        reader,writer=await asyncio.open_connection('127.0.0.1',port)
        writer.write(('GET '+path+' HTTP/1.1\r\nHost: localhost\r\n\r\n').encode());await writer.drain()
        data=await reader.read();writer.close();await writer.wait_closed()
        head,body=data.split(b'\r\n\r\n',1)
        return int(head.split(b' ')[1]),json.loads(body)
    require((await get('/health/alive'))[0]==200,'liveness depended on readiness')
    require((await get('/health/ready'))[0]==503,'notready not reported')
    state['ready']=True;require((await get('/health/ready'))[0]==200,'readiness did not recover')
    reader,writer=await asyncio.open_connection('127.0.0.1',port)
    writer.write(b'GET /health/alive HTTP/1.1\r\n');await writer.drain()
    closing=asyncio.create_task(health.close());await asyncio.sleep(0);closing.cancel();await closing
    require(await reader.read()==b'' and not health._tasks and not health._writers,'health close leaked streams')
    writer.close();await writer.wait_closed()
    cases.append(dict(name='alive_ready_and_cancelled_http_close',passed=True))

    ready=True;entered=asyncio.Event();release=asyncio.Event()
    async def handler(payload,context):
        entered.set();await release.wait();return b'ok'
    server=Server(protocol,{'work':handler},admission=lambda _:ready)
    port=await server.start();client=Client(protocol,'127.0.0.1',port)
    pending=asyncio.create_task(client.call('work',timeout=1));await entered.wait();ready=False
    try:await client.call('work',timeout=1)
    except RpcError as error:require(error.status==UNAVAILABLE,'wrong admission error')
    else:raise AssertionError('notready admitted work')
    release.set();require(await pending==b'ok','quiesce abandoned an admitted request')
    require(server.method_calls['work']==1 and server.method_completed['work']==1,'method metrics included rejected calls')
    await server.close(grace=0)
    require(client.inflight==0 and server.inflight==0,'RPC quiesce leaked work')
    cases.append(dict(name='new_work_quiesces_accepted_work_drains',passed=True))
    if args.storage_lifecycle:
        cases.extend(await storage_lifecycle(args))
    return dict(passed=True,cases=cases,scope='normal configuration/file-worker/HTTP/RPC acceptance; real PG integration is separate')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--repository-root',type=Path,required=True)
    parser.add_argument('--descriptor',type=Path,required=True);parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--storage-lifecycle',action='store_true',help='include database-free Storage lifecycle regressions; requires Storage dependencies')
    args=parser.parse_args();sys.path.insert(0,str(args.repository_root.resolve()))
    result=asyncio.run(run(args));args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':main()