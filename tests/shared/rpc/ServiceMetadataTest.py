#!/usr/bin/env python3
"""Exercise authenticated versioned RPC paths using actual loopback transports."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
import time


async def check(args):
    sys.path.insert(0, str(args.repository_root))
    from shared.runtime.protocol import Protocol
    from shared.runtime.rpc import (Client, Context, Server, RpcError, _read, _write,
        OK, NOT_FOUND, INVALID_ARGUMENT, UNAUTHENTICATED, DEADLINE_EXCEEDED,
        INCOMPATIBLE_VERSION, CALL_CYCLE, PROTOCOL_VERSION)
    protocol = Protocol(args.descriptor)
    baseline = len(os.listdir('/proc/self/fd'))
    token = 'a'*48
    servers, cases, observed = [], [], []

    async def expect(operation, status):
        try:
            await operation
        except RpcError as error:
            assert error.status == status, (error.status, status)
            assert not error.retryable or status not in (INCOMPATIBLE_VERSION, CALL_CYCLE)
            return error
        raise AssertionError('operation unexpectedly succeeded')

    async def echo(payload, context):
        observed.append(dict(path=context.service_path, deadline=context.deadline,
                             correlation=context.correlation_id))
        return payload

    async def raw(port, **changes):
        request = protocol.Request(request_id=7, correlation_id='raw', method='echo',
            deadline_unix_ms=int(time.time()*1000)+1000, service_token=token,
            protocol_version=PROTOCOL_VERSION, caller_service='lobby', target_service='auth',
            service_path=['gateway', 'lobby'])
        for key, value in changes.items():
            if key == 'service_path':
                request.ClearField(key); request.service_path.extend(value)
            else:
                setattr(request, key, value)
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        try:
            await _write(writer, request)
            return protocol.Response.FromString(await asyncio.wait_for(_read(reader), 1))
        finally:
            writer.close(); await writer.wait_closed()

    try:
        server = Server(protocol, {'echo':echo}, service='auth', caller_tokens={'lobby':token},
                        read_timeout=.2, max_timeout=1)
        servers.append(server); port = await server.start()
        caller = Client(protocol, '127.0.0.1', port, service_token=token,
                        caller_service='lobby', target_service='auth')
        assert await caller.call('echo', b'actual') == b'actual'
        assert observed[-1]['path'] == ('lobby','auth')
        cases.append('role_bound_versioned_roundtrip')

        before = len(observed)
        for fields, status in [({'protocol_version':0},INCOMPATIBLE_VERSION),
            ({'protocol_version':2},INCOMPATIBLE_VERSION),
            ({'caller_service':'gateway'},UNAUTHENTICATED),
            ({'service_token':'b'*48},UNAUTHENTICATED),
            ({'target_service':'lobby'},INVALID_ARGUMENT),
            ({'service_path':[]},INVALID_ARGUMENT),
            ({'service_path':['gateway']},INVALID_ARGUMENT),
            ({'service_path':['gateway','lobby','gateway','lobby']},CALL_CYCLE),
            ({'service_path':['auth','lobby']},CALL_CYCLE),
            ({'service_path':['a','b','c','d','e','f','g','lobby']},CALL_CYCLE)]:
            response = await raw(port, **fields)
            assert response.status == status, (fields, response)
            assert response.protocol_version == PROTOCOL_VERSION and not response.retryable
        assert len(observed) == before
        cases.append('raw_wire_version_role_path_rejections_before_handler')

        parent = Context(8, 'parent-context', time.monotonic()+.2, '', ('gateway','lobby'))
        assert await caller.call('echo', b'child', timeout=1, context=parent) == b'child'
        assert observed[-1]['path'] == ('gateway','lobby','auth')
        assert observed[-1]['deadline'] <= parent.deadline+.005
        assert observed[-1]['correlation'] == parent.correlation_id
        cases.append('child_inherits_entire_path_correlation_and_parent_deadline')

        before = server.accepted
        cycle = Context(8, 'cycle-local', time.monotonic()+1, '', ('gateway','auth','lobby'))
        await expect(caller.call('echo', context=cycle), CALL_CYCLE)
        assert server.accepted == before and caller.inflight == 0
        cases.append('known_cycle_rejected_before_opening_socket')
        await expect(caller.call('missing'), NOT_FOUND)
        cases.append('versioned_unknown_method_typed_error')

        legacy = Server(protocol, {'echo':echo}, service_token=token)
        servers.append(legacy); legacy_port = await legacy.start()
        wrong_profile = Client(protocol, '127.0.0.1', legacy_port, service_token=token,
                               caller_service='lobby', target_service='auth')
        before = len(observed)
        await expect(wrong_profile.call('echo'), INCOMPATIBLE_VERSION)
        assert len(observed) == before and legacy.method_calls['echo'] == 0
        old_profile = Client(protocol, '127.0.0.1', legacy_port, service_token=token)
        assert await old_profile.call('echo', b'legacy') == b'legacy'
        assert observed[-1]['path'] == ()
        cases.append('legacy_profile_cannot_impersonate_versioned_role_authentication')

        trace, next_clients = [], {}
        def forward(name, target):
            async def handle(payload, context):
                trace.append(dict(service=name, path=context.service_path))
                return await next_clients[name].call('loop', payload, context=context)
            return handle
        peers = {'gateway':{'ingress':'i'*48,'lobby':'l'*48},
                 'auth':{'gateway':'g'*48},'lobby':{'auth':'h'*48}}
        nodes = {}
        for name, target in [('gateway','auth'),('auth','lobby'),('lobby','gateway')]:
            node = Server(protocol, {'loop':forward(name,target)}, service=name,
                          caller_tokens=peers[name], max_timeout=1)
            servers.append(node); nodes[name] = await node.start()
        for name,target,credential in [('gateway','auth','g'*48),('auth','lobby','h'*48),('lobby','gateway','l'*48)]:
            next_clients[name] = Client(protocol, '127.0.0.1',nodes[target],service_token=credential,
                                       caller_service=name,target_service=target)
        ingress = Client(protocol,'127.0.0.1',nodes['gateway'],service_token='i'*48,
                         caller_service='ingress',target_service='gateway')
        started = time.monotonic()
        await expect(ingress.call('loop',b'owned',timeout=1),CALL_CYCLE)
        assert time.monotonic()-started < 1
        assert [entry['service'] for entry in trace] == ['gateway','auth','lobby']
        assert all(client.inflight == 0 for client in next_clients.values())
        cases.append('actual_three_server_cycle_is_rejected_before_reentry')

        old_calls = 0
        async def old_peer(reader,writer):
            nonlocal old_calls
            try:
                request = protocol.Request.FromString(await _read(reader)); old_calls += 1
                await _write(writer,protocol.Response(request_id=request.request_id,
                    correlation_id=request.correlation_id,status=OK,payload=b'old'))
            finally:
                writer.close(); await writer.wait_closed()
        old = await asyncio.start_server(old_peer,'127.0.0.1',0)
        try:
            stale = Client(protocol,'127.0.0.1',old.sockets[0].getsockname()[1],service_token=token,
                           caller_service='lobby',target_service='auth')
            await expect(stale.call('echo'),INCOMPATIBLE_VERSION)
            assert old_calls == 1 and stale.inflight == 0
        finally:
            old.close(); await old.wait_closed()
        cases.append('unversioned_reply_is_not_accepted_or_replayed')

        entered, cleaned = asyncio.Event(), asyncio.Event()
        async def slow(payload, context):
            entered.set()
            try:
                await asyncio.sleep(5)
            finally:
                cleaned.set()
        server.handlers['slow'] = slow
        parent = Context(1,'budget-child',time.monotonic()+.04,'',('gateway','lobby'))
        started = time.monotonic()
        await expect(caller.call('slow',context=parent,timeout=1),DEADLINE_EXCEEDED)
        await asyncio.wait_for(cleaned.wait(),1)
        assert entered.is_set() and time.monotonic()-started < .15 and caller.inflight == 0
        cases.append('versioned_deadline_cancels_actual_handler')

        for credentials in ({}, {'lobby':'short'}, {'Bad':'x'*40}, []):
            try:
                Server(protocol, {}, service='auth', caller_tokens=credentials)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid credential profile accepted')
        cases.append('invalid_role_profiles_fail_before_listener')
    finally:
        for server in reversed(servers):
            await server.close(grace=.01)
    await asyncio.sleep(0); await asyncio.sleep(0)
    assert len(os.listdir('/proc/self/fd')) == baseline
    return dict(passed=True,cases=cases,group_count=len(cases),wait_graph=trace,
                parent_fds=[baseline,len(os.listdir('/proc/self/fd'))],scope='real Python loopback; native/whole-process gates separate')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository-root',type=Path,required=True)
    parser.add_argument('--descriptor',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    args = parser.parse_args()
    try:
        report = asyncio.run(check(args))
    except Exception as error:
        report = dict(passed=False,error=f'{type(error).__name__}: {error}')
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())