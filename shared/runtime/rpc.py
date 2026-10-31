"""Bounded internal RPC with propagated deadlines and cooperative cancellation."""
import asyncio
from async_timeout import timeout as timeout_scope
from dataclasses import dataclass
import math
import hmac
import re
import struct
import time
from google.protobuf.message import DecodeError

OK, INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE, OVERLOADED = range(5)
DEADLINE_EXCEEDED, CANCELLED, CONFLICT, INTERNAL, OUTCOME_UNKNOWN, UNAUTHENTICATED = range(5, 11)
MAX_FRAME = 256 * 1024


class RpcError(Exception):
    """Stable public failure; never expose SQL or backend diagnostics."""
    def __init__(self, status, *, retryable=False):
        if type(status) is not int or not 1 <= status <= 10:
            raise ValueError("invalid RPC status")
        super().__init__("RPC request failed")
        self.status, self.retryable = status, bool(retryable)


@dataclass(frozen=True, slots=True)
class Context:
    request_id: int
    correlation_id: str
    deadline: float
    idempotency_key: str

    def remaining(self):
        return max(0.0, self.deadline - time.monotonic())

    def check(self):
        if self.remaining() <= 0:
            raise RpcError(DEADLINE_EXCEEDED)

    def child_timeout(self, maximum):
        self.check()
        return min(maximum, self.remaining())


def _timeout(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 30:
        raise ValueError("timeout must be finite and in (0, 30]")
    return float(value)


def _metadata(request):
    return (request.request_id != 0
            and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", request.correlation_id)
            and re.fullmatch(r"[a-z][a-z0-9_.]{0,63}", request.method)
            and len(request.idempotency_key.encode()) <= 128)


async def _read(reader):
    size, = struct.unpack("!I", await reader.readexactly(4))
    if not 0 < size <= MAX_FRAME:
        raise RpcError(INVALID_ARGUMENT)
    return await reader.readexactly(size)


async def _write(writer, message):
    data = message.SerializeToString()
    if not 0 < len(data) <= MAX_FRAME:
        raise RpcError(INTERNAL)
    writer.write(struct.pack("!I", len(data)) + data)
    await writer.drain()


class Server:
    """One loop owns streams and handler tasks; excess peers close immediately.

    Each handler must propagate asyncio cancellation to its dependencies.
    Arbitrary blocking functions cannot be force-cancelled by this transport.
    Use a bounded worker adapter for synchronous dependencies.
    """
    def __init__(self, protocol, handlers, *, max_connections=128,
                 read_timeout=1, max_timeout=5, service_token=None):
        if type(max_connections) is not int or not 1 <= max_connections <= 4096:
            raise ValueError("invalid connection limit")
        if service_token is not None and (not isinstance(service_token, str) or not 32 <= len(service_token) <= 256):
            raise ValueError('internal service token must be 32..256 characters')
        self._token = service_token
        self.protocol, self.handlers = protocol, dict(handlers)
        self.max_connections = max_connections
        self.read_timeout, self.max_timeout = _timeout(read_timeout), _timeout(max_timeout)
        self._server = None
        self._tasks, self._writers, self._cancelling = set(), set(), set()
        self._closed = False
        self.accepted = self.rejected = self.completed = self.cancelled = 0

    @property
    def inflight(self):
        return len(self._tasks)

    async def start(self, host="127.0.0.1", port=0):
        if self._server is not None or self._closed or host not in ("127.0.0.1", "::1"):
            raise ValueError("a fresh loopback server is required")
        self._server = await asyncio.start_server(self._accept, host, port, limit=MAX_FRAME + 4)
        return self._server.sockets[0].getsockname()[1]

    def _accept(self, reader, writer):
        if self._closed or len(self._tasks) >= self.max_connections:
            self.rejected += 1
            writer.close()
            return
        self.accepted += 1
        self._writers.add(writer)
        task = asyncio.create_task(self._serve(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _cancel_once(self, task):
        if task is not None and not task.done() and task not in self._cancelling:
            self._cancelling.add(task)
            task.add_done_callback(self._cancelling.discard)
            task.cancel()

    @staticmethod
    async def _join(tasks):
        # Repeated cancellation must not interrupt dependency cleanup.
        joined = asyncio.gather(*tasks, return_exceptions=True)
        while not joined.done():
            try:
                await asyncio.shield(joined)
            except asyncio.CancelledError:
                continue
        return joined.result()

    async def _respond(self, writer, request, status, *, retryable=False, payload=b""):
        async with timeout_scope(self.read_timeout):
            await _write(writer, self.protocol.Response(request_id=request.request_id,
                correlation_id=request.correlation_id[:64], status=status,
                retryable=retryable, payload=payload))

    async def _serve(self, reader, writer):
        operation = peer = request = None
        try:
            async with timeout_scope(self.read_timeout):
                request = self.protocol.Request.FromString(await _read(reader))
            if not _metadata(request):
                raise RpcError(INVALID_ARGUMENT)
            if self._token is not None and not hmac.compare_digest(request.service_token.encode(), self._token.encode()):
                raise RpcError(UNAUTHENTICATED)
            remaining = (request.deadline_unix_ms - time.time() * 1000) / 1000
            if remaining <= 0:
                raise RpcError(DEADLINE_EXCEEDED)
            context = Context(request.request_id, request.correlation_id,
                time.monotonic() + min(remaining, self.max_timeout), request.idempotency_key)
            handler = self.handlers.get(request.method)
            if handler is None:
                raise RpcError(NOT_FOUND)
            context.check()
            operation = asyncio.create_task(handler(request.payload, context))
            peer = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait((operation, peer), timeout=context.remaining(),
                                         return_when=asyncio.FIRST_COMPLETED)
            if peer in done:
                self._cancel_once(operation)
                self.cancelled += 1
                raise RpcError(CANCELLED)
            if operation not in done:
                self._cancel_once(operation)
                self.cancelled += 1
                raise RpcError(DEADLINE_EXCEEDED)
            payload = operation.result()
            context.check()
            if not isinstance(payload, bytes) or len(payload) > MAX_FRAME - 256:
                raise RpcError(INTERNAL)
            await self._respond(writer, request, OK, payload=payload)
            self.completed += 1
        except RpcError as error:
            if request is not None:
                try:
                    await self._respond(writer, request, error.status, retryable=error.retryable)
                except (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
                    pass
        except (DecodeError, asyncio.IncompleteReadError, ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
            pass
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        except Exception:
            if request is not None:
                try:
                    await self._respond(writer, request, INTERNAL)
                except (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
                    pass
        finally:
            for task in (operation, peer):
                self._cancel_once(task)
            await self._join([t for t in (operation, peer) if t is not None])
            self._writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def close(self, grace=1):
        grace = _timeout(grace)
        self._closed = True
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        tasks = tuple(self._tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace)
            for task in pending:
                self._cancel_once(task)
            await self._join(tasks)


class Client:
    """Explicit cancellation and finite admission; never automatically retry."""
    def __init__(self, protocol, host, port, *, max_inflight=128, service_token=""):
        if host not in ("127.0.0.1", "::1") or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("invalid loopback endpoint")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 4096:
            raise ValueError("invalid inflight limit")
        if not isinstance(service_token, str) or len(service_token) > 256:
            raise ValueError('invalid internal service token')
        self._token = service_token
        self.protocol, self.host, self.port = protocol, host, port
        self.max_inflight, self.inflight, self._sequence = max_inflight, 0, 0

    async def call(self, method, payload=b"", *, timeout=1, context=None,
                   correlation_id=None, idempotency_key=""):
        timeout = _timeout(timeout)
        if context is not None:
            timeout = context.child_timeout(timeout)
            correlation_id = context.correlation_id
        self._sequence += 1
        request = self.protocol.Request(request_id=self._sequence,
            correlation_id=correlation_id or f"rpc-{self._sequence}", method=method,
            deadline_unix_ms=int(time.time() * 1000 + timeout * 1000),
            idempotency_key=idempotency_key, payload=payload, service_token=self._token)
        if not _metadata(request) or request.ByteSize() > MAX_FRAME:
            raise RpcError(INVALID_ARGUMENT)
        if self.inflight >= self.max_inflight:
            raise RpcError(OVERLOADED, retryable=True)
        self.inflight += 1
        writer = None
        sent = False
        try:
            async with timeout_scope(timeout):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=MAX_FRAME + 4)
                sent = True
                await _write(writer, request)
                response = self.protocol.Response.FromString(await _read(reader))
                if (response.request_id != request.request_id
                        or response.correlation_id != request.correlation_id
                        or not 0 <= response.status <= 10):
                    raise RpcError(INTERNAL)
                if response.status != OK:
                    raise RpcError(response.status, retryable=response.retryable)
                return response.payload
        except (TimeoutError, asyncio.TimeoutError):
            raise RpcError(DEADLINE_EXCEEDED) from None
        except (DecodeError, asyncio.IncompleteReadError, ConnectionError, OSError):
            raise RpcError(OUTCOME_UNKNOWN if sent else UNAVAILABLE,
                           retryable=not sent) from None
        finally:
            self.inflight -= 1
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
