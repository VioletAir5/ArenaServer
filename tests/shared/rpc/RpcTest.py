#!/usr/bin/env python3
"""Real loopback RPC acceptance: no dependency mocks or external listeners."""
import argparse
import asyncio
from async_timeout import timeout as timeout_scope
import json
import os
from pathlib import Path
import struct
import sys
import time
sys.dont_write_bytecode = True


async def check(args):
    sys.path.insert(0, str(args.repository_root))
    from shared.runtime.protocol import Protocol
    from shared.runtime.rpc import (Client, Server, RpcError, Context, _read, _write,
        INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE, OVERLOADED, DEADLINE_EXCEEDED,
        INTERNAL, OUTCOME_UNKNOWN, UNAUTHENTICATED, MAX_FRAME)
    protocol = Protocol(args.descriptor)
    cases = []
    async def expect(awaitable, status):
        try:
            await awaitable
        except RpcError as error:
            assert error.status == status, (error.status, status)
            return error
        raise AssertionError("missing failure")
    async def idle(server):
        async with timeout_scope(1):
            while server.inflight:
                await asyncio.sleep(.001)
    calls = 0
    active = 0
    entered = asyncio.Event()
    released = asyncio.Event()
    async def echo(data, context):
        nonlocal calls
        calls += 1
        context.check()
        assert context.correlation_id
        return data
    async def wait(data, context):
        nonlocal active
        active += 1
        entered.set()
        try:
            await released.wait()
            return data
        finally:
            active -= 1
    async def failure(data, context):
        raise RuntimeError("secret backend diagnostic")
    async def retry(data, context):
        raise RpcError(UNAVAILABLE, retryable=True)
    server = Server(protocol, {"echo": echo, "wait": wait, "failure": failure,
                              "retry": retry}, max_connections=4, read_timeout=.1)
    port = await server.start()
    client = Client(protocol, "127.0.0.1", port, max_inflight=4)
    downstream = None
    try:
        protected = Server(protocol, {"echo": echo}, service_token="t" * 32)
        protected_port = await protected.start()
        try:
            unauthorized = Client(protocol, "127.0.0.1", protected_port)
            before = calls
            await expect(unauthorized.call("echo"), UNAUTHENTICATED)
            assert calls == before
            authorized = Client(protocol, "127.0.0.1", protected_port, service_token="t" * 32)
            assert await authorized.call("echo", b"authorized") == b"authorized"
            cases.append("internal_token_checked_before_dispatch")
        finally:
            await protected.close()
        payload = b"\x00\xff" * 1000
        assert await client.call("echo", payload, correlation_id="journey-123") == payload
        assert await client.call("echo") == b""
        await expect(client.call("missing"), NOT_FOUND)
        await expect(client.call("Bad Method"), INVALID_ARGUMENT)
        await expect(client.call("echo", b"x" * MAX_FRAME), INVALID_ARGUMENT)
        await expect(client.call("failure"), INTERNAL)
        before = calls
        error = await expect(client.call("retry", idempotency_key="stable-command"), UNAVAILABLE)
        assert error.retryable and calls == before
        cases.append("roundtrip_binary_metadata_typed_errors_no_retry")
        task = asyncio.create_task(client.call("wait", timeout=.04))
        await entered.wait()
        await expect(task, DEADLINE_EXCEEDED)
        await idle(server)
        assert active == 0 and client.inflight == 0
        entered.clear()
        task = asyncio.create_task(client.call("wait", timeout=1))
        await entered.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await idle(server)
        assert active == 0 and client.inflight == 0
        cases.append("deadline_and_disconnect_cancel_handler")
        entered.clear()
        held = [asyncio.create_task(client.call("wait", timeout=1)) for _ in range(4)]
        async with timeout_scope(1):
            while active != 4:
                await asyncio.sleep(.001)
        await expect(client.call("echo"), OVERLOADED)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        assert await reader.read(1) == b""
        writer.close()
        await writer.wait_closed()
        assert server.rejected == 1 and server.inflight == 4
        for task in held:
            task.cancel()
        await asyncio.gather(*held, return_exceptions=True)
        await idle(server)
        assert active == 0 and client.inflight == 0
        cases.append("finite_client_server_admission")
        before = calls
        for offset in range(0, 1000, 4):
            async def expired(index):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                request = protocol.Request(request_id=index + 1, correlation_id="expired",
                    method="echo", deadline_unix_ms=1)
                await _write(writer, request)
                reply = protocol.Response.FromString(await _read(reader))
                assert reply.status == DEADLINE_EXCEEDED
                writer.close()
                await writer.wait_closed()
            await asyncio.gather(*(expired(i) for i in range(offset, offset + 4)))
            await idle(server)
        assert calls == before and server.inflight == 0
        cases.append("1000_expired_requests_never_enter_handler")
        for data in (struct.pack("!I", 0), struct.pack("!I", MAX_FRAME + 1),
                     struct.pack("!I", 1) + b"\xff", b"\x00\x00"):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(data)
            await writer.drain()
            assert await asyncio.wait_for(reader.read(1), .5) == b""
            writer.close()
            await writer.wait_closed()
            await idle(server)
        cases.append("malformed_and_partial_frames_bounded")
        downstream = Server(protocol, {"wait": wait})
        downstream_port = await downstream.start()
        child = Client(protocol, "127.0.0.1", downstream_port)
        async def forward(data, context):
            return await child.call("wait", data, context=context, timeout=5)
        server.handlers["forward"] = forward
        entered.clear()
        start = time.monotonic()
        await expect(client.call("forward", timeout=.05, correlation_id="cross-hop"), DEADLINE_EXCEEDED)
        await idle(server)
        await idle(downstream)
        assert active == 0 and child.inflight == 0
        propagation_ms = (time.monotonic() - start) * 1000
        assert propagation_ms < 300
        cases.append("downstream_remaining_budget_and_cancellation")
        # Commit acknowledgment loss is never disguised as a retryable failure.
        async def lose_ack(reader, writer):
            await _read(reader)
            writer.close()
            await writer.wait_closed()
        loss_server = await asyncio.start_server(lose_ack, "127.0.0.1", 0)
        try:
            loss = Client(protocol, "127.0.0.1", loss_server.sockets[0].getsockname()[1])
            error = await expect(loss.call("mutate", idempotency_key="once"), OUTCOME_UNKNOWN)
            assert not error.retryable and loss.inflight == 0
        finally:
            loss_server.close()
            await loss_server.wait_closed()
        cases.append("lost_response_unknown_without_automatic_retry")
        cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
        cleanup_active = 0
        async def loader():
            nonlocal cleanup_active
            cleanup_active += 1
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cleanup_entered.set()
                await cleanup_release.wait()
            finally:
                cleanup_active -= 1
        async def cleanup(data, context):
            await asyncio.wait_for(loader(), 5)
            return b""
        server.handlers["cleanup"] = cleanup
        task = asyncio.create_task(client.call("cleanup", timeout=.03))
        await asyncio.wait_for(cleanup_entered.wait(), 1)
        await expect(task, DEADLINE_EXCEEDED)
        closing = asyncio.create_task(server.close(grace=.01))
        await asyncio.sleep(.03)
        assert not closing.done() and server.inflight == cleanup_active == 1
        cleanup_release.set()
        await asyncio.wait_for(closing, 1)
        assert server.inflight == active == client.inflight == cleanup_active == 0
        cases.append("drain_during_deadline_cleanup_never_orphans_loader")
        return {"status": "passed", "cases": cases, "expired_requests": 1000,
                "propagation_elapsed_ms": propagation_ms, "remaining_tasks": server.inflight,
                "remaining_handlers": active, "client_inflight": client.inflight,
                "scope": "functional loopback integration; no C++ client or storage worker yet"}
    finally:
        await server.close(grace=.01)
        if downstream:
            await downstream.close(grace=.01)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(check(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
