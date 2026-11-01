"""Bounded test-only TCP proxy into a harness-owned PostgreSQL Unix socket."""
import asyncio
import struct


class PgFaultProxy:
    """Parse frontend query frames and independently drop replies/cancel packets."""
    def __init__(self, socket_path):
        self.socket_path = str(socket_path)
        self.server = None
        self.port = None
        self.tasks = set()
        self.writers = set()
        self.rule = None
        self.hit = asyncio.Event()
        self.cancel_hit = asyncio.Event()
        self.cancel_blackhole = False
        self.cancel_connections = 0
        self.active_cancels = 0
        self.query_hits = 0
        self.peak = 0
        self.closing = False

    async def start(self):
        self.server = await asyncio.start_server(self._accept, '127.0.0.1', 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    def arm(self, sql, *, cancel_blackhole=False):
        if self.rule is not None:
            raise AssertionError('a fault is already armed')
        self.hit.clear()
        self.cancel_hit.clear()
        self.rule = sql
        self.cancel_blackhole = cancel_blackhole

    @property
    def active(self):
        return len(self.tasks)

    def _accept(self, reader, writer):
        # Register ownership in the synchronous accept callback. An async
        # callback could start after close() took its task/writer snapshot.
        if self.closing or len(self.tasks) >= 16:
            writer.close()
            return
        self.writers.add(writer)
        task = asyncio.create_task(self._serve(reader, writer))
        self.tasks.add(task)
        self.peak = max(self.peak, len(self.tasks))

    async def _serve(self, reader, writer):
        task = asyncio.current_task()
        backend = None
        blocked = False
        is_cancel = False
        pumps = []
        try:
            header = await reader.readexactly(8)
            size, code = struct.unpack('!II', header)
            if not 8 <= size <= 65536:
                raise AssertionError('invalid private PostgreSQL startup length')
            first = header + await reader.readexactly(size-8)
            if code == 80877102:
                is_cancel = True
                self.active_cancels += 1
                self.cancel_connections += 1
                self.cancel_hit.set()
                if self.cancel_blackhole:
                    # Retain the request until the client closes its independent
                    # cancellation descriptor. No PGconn is shared across threads.
                    await reader.read()
                    return
            upstream, backend = await asyncio.open_unix_connection(self.socket_path)
            self.writers.add(backend)
            if self.closing:
                return
            backend.write(first)
            await backend.drain()
            if code == 80877102:
                await upstream.read()
                return

            async def frontend():
                nonlocal blocked
                while True:
                    kind = await reader.readexactly(1)
                    length_bytes = await reader.readexactly(4)
                    length, = struct.unpack('!I', length_bytes)
                    if not 4 <= length <= 1024*1024:
                        raise AssertionError('invalid private PostgreSQL frame length')
                    body = await reader.readexactly(length-4)
                    sql = None
                    if kind == b'Q':
                        sql = body.rstrip(b'\0').decode('utf-8')
                    elif kind == b'P':
                        sql = body.split(b'\0', 2)[1].decode('utf-8')
                    if self.rule is not None and sql == self.rule:
                        self.rule = None
                        blocked = True
                        self.query_hits += 1
                        self.hit.set()
                    backend.write(kind+length_bytes+body)
                    await backend.drain()

            async def replies():
                while data := await upstream.read(65536):
                    if not blocked:
                        writer.write(data)
                        await writer.drain()

            pumps = [asyncio.create_task(frontend()), asyncio.create_task(replies())]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for stream in (writer, backend):
                if stream is not None:
                    self.writers.discard(stream)
                    stream.close()
                    try:
                        await stream.wait_closed()
                    except (ConnectionError, OSError):
                        pass
            if is_cancel:
                self.active_cancels -= 1
            self.tasks.discard(task)

    async def disconnect(self):
        """Release a failed test's blocked peers without closing its listener."""
        for writer in tuple(self.writers):
            writer.close()
        await asyncio.sleep(0)

    async def close(self):
        self.closing = True
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.close()
        # Socket closure wakes readers without a second cancellation tearing
        # through a handler which is already running its finally cleanup.
        await asyncio.wait_for(asyncio.gather(*tuple(self.tasks), return_exceptions=True), 1)
        if self.tasks or self.writers or self.active_cancels:
            raise AssertionError('private PostgreSQL proxy retained a task/socket')