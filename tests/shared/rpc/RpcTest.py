#!/usr/bin/env python3
"""Real loopback RPC acceptance: no dependency mocks or external listeners."""
import argparse
import asyncio
from async_timeout import timeout as timeout_scope
import json
import gc
import weakref
import os
from pathlib import Path
import struct
import socket
import sys
import time
sys.dont_write_bytecode = True


async def deadline_and_retry_acceptance(protocol):
    from shared.runtime.rpc import (Client, Server, Context, RpcError, _write,
        UNAVAILABLE, DEADLINE_EXCEEDED, MAX_FRAME)
    baseline_fds = len(os.listdir("/proc/self/fd"))
    observed, finished, retry_calls, mutations = {}, {}, {}, {}
    active = 0
    race_active = 0
    race_entered, race_release = asyncio.Event(), asyncio.Event()

    async def wait(data, context):
        nonlocal active
        active += 1
        observed[context.correlation_id] = context.deadline
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1
            finished[context.correlation_id] = time.monotonic()

    async def retry(data, context):
        retry_calls[context.idempotency_key] = retry_calls.get(context.idempotency_key, 0) + 1
        raise RpcError(UNAVAILABLE, retryable=True)

    async def mutate(data, context):
        # This side effect makes even one transparent retry observable.
        mutations[context.idempotency_key] = mutations.get(context.idempotency_key, 0) + 1
        raise RpcError(UNAVAILABLE, retryable=True)

    async def race(data, context):
        nonlocal race_active
        race_active += 1
        if race_active == 4:
            race_entered.set()
        try:
            await race_release.wait()
            return data
        finally:
            race_active -= 1

    async def drain(server):
        async with timeout_scope(.1):
            while server.inflight:
                await asyncio.sleep(.001)

    downstream = Server(protocol, {"wait": wait}, max_connections=4)
    downstream_port = await downstream.start()
    child = Client(protocol, "127.0.0.1", downstream_port, max_inflight=4)
    async def forward(data, context):
        return await child.call("wait", data, context=context, timeout=5,
                                idempotency_key=context.idempotency_key)
    upstream = Server(protocol, {"forward": forward, "retry": retry,
        "mutate": mutate, "race": race}, max_connections=4)
    port = await upstream.start()
    client = Client(protocol, "127.0.0.1", port, max_inflight=4)
    deadline_errors, cancellation_lags, retained_lags = [], [], []
    results = {}
    try:
        for index in range(20):
            correlation = f"deadline-{index}"
            deadline = time.monotonic() + .03
            context = Context(index + 1, correlation, deadline, f"budget-{index}")
            try:
                await client.call("forward", context=context, timeout=2,
                                  idempotency_key=context.idempotency_key)
                raise AssertionError("deadline was not enforced")
            except RpcError as error:
                assert error.status == DEADLINE_EXCEEDED, error.status
            deadline_errors.append(abs(time.monotonic() - deadline) * 1000)
            await drain(upstream)
            await drain(downstream)
            assert correlation in finished and observed[correlation] <= deadline + .002
            cancellation_lags.append(max(0, finished[correlation] - deadline) * 1000)
            retained_lags.append(max(0, time.monotonic() - deadline) * 1000)
            assert active == client.inflight == child.inflight == 0
        assert max(deadline_errors) <= 20, deadline_errors
        assert max(cancellation_lags + retained_lags) <= 100, (cancellation_lags, retained_lags)
        results.update(deadline_samples=20, deadline_error_ms=deadline_errors,
            downstream_cancellation_lag_ms=cancellation_lags,
            actual_resource_release_lag_ms=retained_lags)

        async def failure_once(method, key):
            try:
                await client.call(method, idempotency_key=key)
            except RpcError as error:
                assert error.status == UNAVAILABLE and error.retryable
                return
            raise AssertionError("retryable failure was hidden")
        for offset in range(0, 100, 4):
            await asyncio.gather(*(failure_once("retry", f"storm-{i}")
                                   for i in range(offset, offset + 4)))
            await drain(upstream)
        for offset in range(0, 52, 4):
            await asyncio.gather(*(failure_once("mutate", f"command-{i}")
                                   for i in range(offset, min(offset + 4, 50))))
            await drain(upstream)
        assert len(retry_calls) == 100 and all(n == 1 for n in retry_calls.values())
        assert len(mutations) == 50 and all(n == 1 for n in mutations.values())
        amplification = (sum(retry_calls.values()) + sum(mutations.values())) / 150
        assert amplification <= 1.2
        results.update(retryable_storm_logical_calls=100, non_idempotent_logical_calls=50,
                       actual_handler_calls=sum(retry_calls.values()) + sum(mutations.values()),
                       retry_amplification=amplification, automatic_retries=0)

        successful, cancelled = 0, 0
        for index in range(10):
            race_entered, race_release = asyncio.Event(), asyncio.Event()
            tasks = [asyncio.create_task(client.call("race", b"done")) for _ in range(4)]
            await asyncio.wait_for(race_entered.wait(), .5)
            if index % 2:
                tasks[0].cancel()
                tasks[1].cancel()
            race_release.set()
            if not index % 2:
                await asyncio.sleep(0)
                tasks[0].cancel()
            replies = await asyncio.gather(*tasks, return_exceptions=True)
            for reply in replies:
                if isinstance(reply, asyncio.CancelledError):
                    cancelled += 1
                else:
                    assert reply == b"done", reply
                    successful += 1
            await drain(upstream)
            assert race_active == client.inflight == 0
        assert successful and cancelled
        results.update(completion_race_calls=40, race_successes=successful, race_cancellations=cancelled)

        # Fault injection delays the real protobuf factory, not the transport.
        # Expired preparation must not open a connection or retain admission.
        class DelayedFactory:
            Response = protocol.Response
            @staticmethod
            def Request(**kwargs):
                time.sleep(.05)
                return protocol.Request(**kwargs)
        delayed = Client(DelayedFactory(), "127.0.0.1", port)
        before = upstream.accepted
        try:
            await delayed.call("retry", context=Context(1, "expired-preparation",
                                                       time.monotonic() + .02, ""))
            raise AssertionError("expired preparation was admitted")
        except RpcError as error:
            assert error.status == DEADLINE_EXCEEDED
        assert upstream.accepted == before and delayed.inflight == 0
        results["expired_preparation_connections"] = 0

        class TinyBufferServer(Server):
            def _accept(self, reader, writer):
                writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
                super()._accept(reader, writer)
        async def large(data, context):
            return b"x" * (MAX_FRAME - 512)
        slow = TinyBufferServer(protocol, {"large": large}, read_timeout=.03, max_timeout=.05)
        slow_port = await slow.start()
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        peer.setblocking(False)
        try:
            loop = asyncio.get_running_loop()
            await loop.sock_connect(peer, ("127.0.0.1", slow_port))
            request = protocol.Request(request_id=1, correlation_id="slow-reader", method="large",
                deadline_unix_ms=int(time.time() * 1000 + 50)).SerializeToString()
            started = time.monotonic()
            await loop.sock_sendall(peer, struct.pack("!I", len(request)) + request)
            async with timeout_scope(.15):
                while slow.accepted == 0 or slow.inflight:
                    await asyncio.sleep(.001)
            elapsed = (time.monotonic() - started) * 1000
            assert elapsed <= 150 and not slow._writers
            results["slow_reader_resource_release_ms"] = elapsed
        finally:
            peer.close()
            await slow.close(grace=.01)
        # Queue an actual raw accept, then close before its transport task
        # gets a turn. This deterministically exposes CPython 3.10's otherwise
        # intermittent Server._attach/close descriptor race.
        raw_server = Server(protocol, {})
        raw_port = await raw_server.start()
        loop = asyncio.get_running_loop()
        listener = raw_server._server.sockets[0]
        accept_ready = loop._selector.get_key(listener.fileno()).data[0]
        raw_peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_peer.settimeout(.1)
        before_accept = len(os.listdir("/proc/self/fd"))
        try:
            raw_peer.connect(("127.0.0.1", raw_port))
            accept_ready._run()
            await raw_server.close(grace=.01)
            raw_peer.close()
            assert raw_server.inflight == 0
            assert len(os.listdir("/proc/self/fd")) == before_accept - 2
            results["queued_raw_accept_retained_descriptors"] = 0
        finally:
            raw_peer.close()
            await raw_server.close(grace=.01)
        stopping = Server(protocol, {})
        stop_port = await stopping.start()
        stop_peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stop_peer.settimeout(.1)
        try:
            stop_peer.connect(("127.0.0.1", stop_port))
            closing = asyncio.create_task(stopping.close(grace=.01))
            await asyncio.sleep(0)
            closing.cancel()
            closing.cancel()
            await asyncio.gather(closing, return_exceptions=True)
            assert stopping.inflight == 0 and not stopping._server.sockets
            results["cancelled_shutdown_listener_closed"] = True
        finally:
            stop_peer.close()
            await stopping.close(grace=.01)

        grace_entered = asyncio.Event()
        grace_active = 0
        async def grace_handler(data, context):
            nonlocal grace_active
            grace_active += 1
            grace_entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                grace_active -= 1
        graceful = Server(protocol, {"grace": grace_handler})
        graceful_port = await graceful.start()
        grace_client = Client(protocol, "127.0.0.1", graceful_port)
        pending_call = asyncio.create_task(grace_client.call("grace", timeout=1))
        try:
            await asyncio.wait_for(grace_entered.wait(), .5)
            started = time.monotonic()
            closing = asyncio.create_task(graceful.close(grace=.04))
            await asyncio.sleep(.01)
            assert not graceful._server.sockets and grace_active == 1
            closing.cancel()
            closing.cancel()
            await asyncio.gather(closing, return_exceptions=True)
            assert graceful.inflight == grace_active == 0
            await asyncio.gather(pending_call, return_exceptions=True)
            assert grace_client.inflight == 0 and not graceful._writers
            results["cancelled_grace_shutdown_ms"] = (time.monotonic() - started) * 1000
            assert results["cancelled_grace_shutdown_ms"] <= 100
        finally:
            await graceful.close(grace=.01)
            await asyncio.gather(pending_call, return_exceptions=True)

        class CallbackOwner:
            def __init__(self):
                self.server = Server(protocol, {"cycle": self.handle})
                self.active = 0
            async def handle(self, data, context):
                self.active += 1
                try:
                    await asyncio.Event().wait()
                finally:
                    self.active -= 1
        owner = CallbackOwner()
        owned_port = await owner.server.start()
        owned_client = Client(protocol, "127.0.0.1", owned_port)
        try:
            await owned_client.call("cycle", timeout=.02)
            raise AssertionError("callback deadline was lost")
        except RpcError as error:
            assert error.status == DEADLINE_EXCEEDED
        await drain(owner.server)
        await owner.server.close(grace=.01)
        assert owner.active == owned_client.inflight == 0
        owner_reference, server_reference = weakref.ref(owner), weakref.ref(owner.server)
        del owner
        gc.collect()
        assert owner_reference() is None and server_reference() is None
        results["mutually_owned_callback_objects_collected"] = True
        return results
    finally:
        await upstream.close(grace=.01)
        await downstream.close(grace=.01)
        await asyncio.sleep(0)
        results["remaining_tasks"] = upstream.inflight + downstream.inflight
        results["remaining_handlers"] = active + race_active
        results["remaining_client_calls"] = client.inflight + child.inflight
        results["fd_baseline"] = baseline_fds
        results["fd_final"] = len(os.listdir("/proc/self/fd"))
        assert results["remaining_tasks"] == results["remaining_handlers"] == results["remaining_client_calls"] == 0
        assert results["fd_final"] == baseline_fds, results


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
        day31 = await deadline_and_retry_acceptance(protocol)
        cases.extend(("twenty_samples_20ms_deadline_100ms_downstream_cleanup",
            "retryable_storm_amplification_and_non_idempotent_single_attempt",
            "completion_cancel_races_recover_capacity", "expired_preparation_no_socket",
            "slow_reader_response_flush_releases_server_task", "queued_raw_accept_close_releases_descriptor",
            "cancelled_listener_shutdown_and_callback_reference_cycle_cleanup",
            "cancelled_grace_wait_still_joins_handler_and_connection"))
        async def misleading_unknown(reader, writer):
            request = protocol.Request.FromString(await _read(reader))
            await _write(writer, protocol.Response(request_id=request.request_id,
                correlation_id=request.correlation_id, status=OUTCOME_UNKNOWN, retryable=True))
            writer.close()
            await writer.wait_closed()
        misleading = await asyncio.start_server(misleading_unknown, "127.0.0.1", 0)
        try:
            caller = Client(protocol, "127.0.0.1", misleading.sockets[0].getsockname()[1])
            error = await expect(caller.call("mutate"), OUTCOME_UNKNOWN)
            assert not error.retryable and caller.inflight == 0
        finally:
            misleading.close()
            await misleading.wait_closed()
        cases.append("unknown_commit_outcome_never_advertises_retryable")
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
                "day31": day31,
                "scope": "functional Python loopback; native C++ and PostgreSQL worker have separate acceptance gates"}
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
