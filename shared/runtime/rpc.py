"""Bounded internal RPC with propagated deadlines and cooperative cancellation."""
import asyncio
from async_timeout import timeout as timeout_scope, timeout_at as deadline_scope
from dataclasses import dataclass
import math
import hmac
import re
import struct
import time
from google.protobuf.message import DecodeError

OK, INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE, OVERLOADED = range(5)
DEADLINE_EXCEEDED, CANCELLED, CONFLICT, INTERNAL, OUTCOME_UNKNOWN, UNAUTHENTICATED = range(5, 11)
INCOMPATIBLE_VERSION, CALL_CYCLE = 11, 12
PROTOCOL_VERSION, MAX_SERVICE_HOPS = 1, 8
MAX_FRAME = 256 * 1024


class RpcError(Exception):
    """Stable public failure; never expose SQL or backend diagnostics."""
    def __init__(self, status, *, retryable=False):
        if type(status) is not int or not 1 <= status <= CALL_CYCLE:
            raise ValueError("invalid RPC status")
        super().__init__("RPC request failed")
        self.status, self.retryable = status, bool(retryable) and status not in (OUTCOME_UNKNOWN, INCOMPATIBLE_VERSION, CALL_CYCLE)


@dataclass(frozen=True, slots=True)
class Context:
    request_id: int
    correlation_id: str
    deadline: float
    idempotency_key: str
    service_path: tuple[str, ...] = ()

    def remaining(self):
        return max(0.0, self.deadline - time.monotonic())

    def check(self):
        if self.remaining() <= 0:
            raise RpcError(DEADLINE_EXCEEDED)


def _timeout(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 30:
        raise ValueError("timeout must be finite and in (0, 30]")
    return float(value)


def _metadata(request):
    return (request.request_id != 0
            and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", request.correlation_id)
            and re.fullmatch(r"[a-z][a-z0-9_.]{0,63}", request.method)
            and len(request.idempotency_key.encode()) <= 128)


def _service_name(value):
    return isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", value) is not None


def validate_service_credentials(service, caller_tokens):
    """Validate a role-bound profile before an application allocates resources."""
    if (not _service_name(service) or not isinstance(caller_tokens, dict)
            or not 1 <= len(caller_tokens) <= 32):
        raise ValueError("invalid service credential profile")
    result = dict(caller_tokens)
    if any(not _service_name(caller) or not isinstance(token, str)
           or not 32 <= len(token.encode()) <= 256 for caller, token in result.items()):
        raise ValueError("invalid service credential profile")
    return result


def _checked_service_path(caller, target, path):
    if not _service_name(caller) or not _service_name(target):
        raise RpcError(INVALID_ARGUMENT)
    values = tuple(path)
    if not values:
        raise RpcError(INVALID_ARGUMENT)
    if len(values) >= MAX_SERVICE_HOPS:
        raise RpcError(CALL_CYCLE)
    visited = set()
    for value in values:
        if not _service_name(value):
            raise RpcError(INVALID_ARGUMENT)
        if value == target or value in visited:
            raise RpcError(CALL_CYCLE)
        visited.add(value)
    if values[-1] != caller:
        raise RpcError(INVALID_ARGUMENT)
    return values


def _incoming_service_path(request, service, caller_tokens):
    versioned = (service or request.protocol_version or request.caller_service
                 or request.target_service or request.service_path)
    if not service:
        if versioned:
            raise RpcError(INCOMPATIBLE_VERSION)
        return ()
    if request.protocol_version != PROTOCOL_VERSION:
        raise RpcError(INCOMPATIBLE_VERSION)
    if service and request.target_service != service:
        raise RpcError(INVALID_ARGUMENT)
    if caller_tokens is not None:
        expected = caller_tokens.get(request.caller_service)
        if expected is None or not hmac.compare_digest(request.service_token.encode(), expected.encode()):
            raise RpcError(UNAUTHENTICATED)
    path = _checked_service_path(request.caller_service, request.target_service, request.service_path)
    return path + (request.target_service,)


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


async def _close(writer, deadline, *, abort=False):
    """Retire the stream on its loop; a stalled flush cannot retain admission."""
    if abort or time.monotonic() >= deadline:
        writer.transport.abort()
        await asyncio.sleep(0)
        return
    writer.close()
    try:
        async with deadline_scope(deadline):
            await writer.wait_closed()
    except asyncio.CancelledError:
        writer.transport.abort()
        raise
    except (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
        writer.transport.abort()
    finally:
        # abort() schedules descriptor release on this same event loop.
        await asyncio.sleep(0)


async def close_listener(server):
    """Fence queued plain-TCP accepts before retiring a CPython 3.10 listener."""
    if server is None:
        return
    loop = server.get_loop()
    for listener in server.sockets:
        loop.remove_reader(listener.fileno())
    await asyncio.sleep(0)
    server.close()
    await server.wait_closed()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

class Server:
    """One loop owns streams and handler tasks; excess peers close immediately.

    Each handler must propagate asyncio cancellation to its dependencies.
    Arbitrary blocking functions cannot be force-cancelled by this transport.
    Use a bounded worker adapter for synchronous dependencies.
    """
    def __init__(self, protocol, handlers, *, max_connections=128,
                 read_timeout=1, max_timeout=5, service_token=None, admission=None,
                 service="", caller_tokens=None):
        if type(max_connections) is not int or not 1 <= max_connections <= 4096:
            raise ValueError("invalid connection limit")
        if service_token is not None and (not isinstance(service_token, str) or not 32 <= len(service_token) <= 256):
            raise ValueError('internal service token must be 32..256 characters')
        if admission is not None and not callable(admission):
            raise ValueError('admission must be callable')
        if bool(service) != (caller_tokens is not None) or (service and service_token is not None):
            raise ValueError("service profiles require explicit caller credentials")
        self._service = service
        self._caller_tokens = validate_service_credentials(service, caller_tokens) if service else None
        self._admission = admission
        self.admission_rejected = 0
        self._token = service_token
        self.protocol, self.handlers = protocol, dict(handlers)
        self.method_calls = {method:0 for method in self.handlers}
        self.method_completed = {method:0 for method in self.handlers}
        self.max_connections = max_connections
        self.read_timeout, self.max_timeout = _timeout(read_timeout), _timeout(max_timeout)
        self._server = None
        self._tasks, self._writers, self._cancelling = set(), set(), set()
        self._closed = False
        self._close_task = None
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

    async def _respond(self, writer, request, status, *, retryable=False, payload=b"", deadline=None):
        limit = time.monotonic() + self.read_timeout
        if deadline is not None:
            limit = min(limit, deadline)
        # An expired request may enqueue a small error immediately, but cannot
        # wait for a slow peer to make output space beyond its original budget.
        async with deadline_scope(limit):
            await _write(writer, self.protocol.Response(request_id=request.request_id,
                correlation_id=request.correlation_id[:64], status=status,
                retryable=retryable, payload=payload,
                protocol_version=PROTOCOL_VERSION if self._service else 0))

    async def _serve(self, reader, writer):
        operation = peer = request = context = None
        replied = False
        try:
            async with timeout_scope(self.read_timeout):
                request = self.protocol.Request.FromString(await _read(reader))
            if not _metadata(request):
                raise RpcError(INVALID_ARGUMENT)
            if self._token is not None and not hmac.compare_digest(request.service_token.encode(), self._token.encode()):
                raise RpcError(UNAUTHENTICATED)
            path = _incoming_service_path(request, self._service, self._caller_tokens)
            request.ClearField("service_token")
            remaining = (request.deadline_unix_ms - time.time() * 1000) / 1000
            if remaining <= 0:
                raise RpcError(DEADLINE_EXCEEDED)
            context = Context(request.request_id, request.correlation_id,
                time.monotonic() + min(remaining, self.max_timeout), request.idempotency_key, path)
            handler = self.handlers.get(request.method)
            if handler is None:
                raise RpcError(NOT_FOUND)
            context.check()
            if self._admission is not None and not self._admission(request.method):
                self.admission_rejected += 1
                raise RpcError(UNAVAILABLE, retryable=True)
            self.method_calls[request.method] = self.method_calls.get(request.method,0)+1
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
            await self._respond(writer, request, OK, payload=payload, deadline=context.deadline)
            replied = True
            self.completed += 1
            self.method_completed[request.method] = self.method_completed.get(request.method,0)+1
        except RpcError as error:
            if request is not None:
                try:
                    await self._respond(writer, request, error.status, retryable=error.retryable,
                        deadline=context.deadline if context is not None else None)
                    replied = True
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
                    await self._respond(writer, request, INTERNAL,
                        deadline=context.deadline if context is not None else None)
                    replied = True
                except (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
                    pass
        finally:
            for task in (operation, peer):
                self._cancel_once(task)
            await self._join([t for t in (operation, peer) if t is not None])
            try:
                await _close(writer, context.deadline if context is not None else
                             time.monotonic() + self.read_timeout, abort=not replied)
            finally:
                self._writers.discard(writer)

    async def _stop_accepting(self):
        await close_listener(self._server)

    async def _shutdown(self, grace):
        await self._stop_accepting()
        tasks = tuple(self._tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace)
            for task in pending:
                self._cancel_once(task)
            await self._join(tasks)

    async def close(self, grace=1):
        """Finish one shutdown; the first caller establishes the grace period."""
        if type(grace) not in (int,float) or not math.isfinite(grace) or not 0 <= grace <= 30:
            raise ValueError('invalid shutdown grace')
        grace = float(grace)
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._shutdown(grace))
        # Cancellation of any waiter cannot abandon the listener fence,
        # grace wait or dependency cleanup. Repeated callers share this job.
        await self._join([self._close_task])
        self._close_task.result()


class Client:
    """Explicit cancellation and finite admission; never automatically retry."""
    def __init__(self, protocol, host, port, *, max_inflight=128, service_token="",
                 caller_service="", target_service=""):
        if host not in ("127.0.0.1", "::1") or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("invalid loopback endpoint")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 4096:
            raise ValueError("invalid inflight limit")
        if not isinstance(service_token, str) or len(service_token) > 256:
            raise ValueError('invalid internal service token')
        if (bool(caller_service) != bool(target_service) or
                caller_service and (not _service_name(caller_service) or not _service_name(target_service))):
            raise ValueError("invalid RPC service profile")
        self._caller, self._target = caller_service, target_service
        self._token = service_token
        self.protocol, self.host, self.port = protocol, host, port
        self.max_inflight, self.inflight, self._sequence = max_inflight, 0, 0

    async def call(self, method, payload=b"", *, timeout=1, context=None,
                   correlation_id=None, idempotency_key=""):
        timeout = _timeout(timeout)
        deadline = time.monotonic() + timeout
        if context is not None:
            context.check()
            deadline = min(deadline, context.deadline)
            correlation_id = context.correlation_id
        self._sequence += 1
        request = self.protocol.Request(request_id=self._sequence,
            correlation_id=correlation_id or f"rpc-{self._sequence}", method=method,
            deadline_unix_ms=int(time.time() * 1000 +
                                 max(0, deadline - time.monotonic()) * 1000),
            idempotency_key=idempotency_key, payload=payload, service_token=self._token)
        if self._caller:
            path = context.service_path if context is not None and context.service_path else (self._caller,)
            request.service_path.extend(_checked_service_path(self._caller, self._target, path))
            request.caller_service, request.target_service = self._caller, self._target
            request.protocol_version = PROTOCOL_VERSION
        elif context is not None and context.service_path:
            raise RpcError(INVALID_ARGUMENT)
        if not _metadata(request) or request.ByteSize() > MAX_FRAME:
            raise RpcError(INVALID_ARGUMENT)
        if time.monotonic() >= deadline:
            raise RpcError(DEADLINE_EXCEEDED)
        if self.inflight >= self.max_inflight:
            raise RpcError(OVERLOADED, retryable=True)
        self.inflight += 1
        writer = None
        sent = False
        finished = False
        try:
            async with deadline_scope(deadline):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=MAX_FRAME + 4)
                sent = True
                await _write(writer, request)
                response = self.protocol.Response.FromString(await _read(reader))
                if (response.request_id != request.request_id
                        or response.correlation_id != request.correlation_id
                        or not 0 <= response.status <= CALL_CYCLE):
                    raise RpcError(INTERNAL)
                if (response.protocol_version != request.protocol_version and not
                        (response.protocol_version == PROTOCOL_VERSION and response.status == INCOMPATIBLE_VERSION)):
                    raise RpcError(INCOMPATIBLE_VERSION)
                finished = True
                if response.status != OK:
                    raise RpcError(response.status, retryable=response.retryable)
                return response.payload
        except (TimeoutError, asyncio.TimeoutError):
            raise RpcError(DEADLINE_EXCEEDED) from None
        except (DecodeError, asyncio.IncompleteReadError, ConnectionError, OSError):
            raise RpcError(OUTCOME_UNKNOWN if sent else UNAVAILABLE,
                           retryable=not sent) from None
        finally:
            try:
                if writer is not None:
                    await _close(writer, deadline, abort=not finished)
            finally:
                self.inflight -= 1
