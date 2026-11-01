"""Finite read-only loopback HTTP health independent of business admission."""
import asyncio
import json
import math

from async_timeout import timeout_at
from .rpc import close_listener


class HealthServer:
    """One loop owns a finite set of one-request HTTP connections and their teardown."""
    def __init__(self, provider, *, max_connections=16, timeout=.25):
        if not callable(provider) or type(max_connections) is not int or not 1 <= max_connections <= 128:
            raise ValueError('invalid health server capacity')
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or not 0 < timeout <= 1:
            raise ValueError('invalid health request timeout')
        self.provider, self.maximum, self.timeout = provider, max_connections, timeout
        self._server = self._close_task = None
        self._tasks, self._writers = set(),set()
        self._closed = False
        self.accepted = self.rejected = 0

    async def start(self, port=0):
        if self._closed or self._server is not None or type(port) is not int or not 0 <= port <= 65535:
            raise ValueError('a fresh loopback health server is required')
        self._server = await asyncio.start_server(self._accept,'127.0.0.1',port,limit=4097)
        return self._server.sockets[0].getsockname()[1]

    def _accept(self, reader, writer):
        if self._closed or len(self._tasks) >= self.maximum:
            self.rejected += 1
            writer.close()
            return
        self.accepted += 1
        self._writers.add(writer)
        task = asyncio.create_task(self._serve(reader,writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _serve(self, reader, writer):
        loop = asyncio.get_running_loop()
        deadline = loop.time()+self.timeout
        try:
            async with timeout_at(deadline):
                header = await reader.readuntil(b'\r\n\r\n')
                if len(header) > 4096:
                    return
                lines = header.decode('ascii').split('\r\n')
                parts = lines[0].split(' ')
                status, reason = 400,'Bad Request'
                state = dict(alive=True,ready=False)
                if (len(parts) == 3 and parts[2] in ('HTTP/1.0','HTTP/1.1')
                        and len(lines) <= 64 and all(':' in value for value in lines[1:] if value)):
                    fields = [(line.split(':',1)[0].strip().lower(),line.split(':',1)[1].strip())
                              for line in lines[1:] if line]
                    bodyless = not any(name=='transfer-encoding' or name=='content-length' and value!='0'
                                       for name,value in fields)
                    if parts[0] != 'GET':
                        status,reason = 405,'Method Not Allowed'
                    elif bodyless and parts[1] in ('/health/alive','/health/ready','/health/config'):
                        try:
                            state = dict(self.provider())
                        except Exception:
                            state = dict(alive=True,ready=False,error='provider')
                        state['alive'] = True
                        status,reason = (503,'Service Unavailable') if parts[1]=='/health/ready' and not state.get('ready') else (200,'OK')
                    elif bodyless:
                        status,reason = 404,'Not Found'
                payload = json.dumps(state,separators=(',',':'),allow_nan=False).encode()
                if len(payload) > 16384:
                    payload = b'{"alive":true,"ready":false}'
                    status,reason = 500,'Internal Server Error'
                writer.write((f'HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n'
                    f'Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode()+payload)
                await writer.drain()
        except (asyncio.IncompleteReadError,asyncio.LimitOverrunError,ConnectionError,OSError,
                UnicodeError,ValueError,TimeoutError,asyncio.TimeoutError):
            pass
        finally:
            try:
                writer.transport.abort()
                await asyncio.sleep(0)
            finally:
                self._writers.discard(writer)

    async def _close(self):
        await close_listener(self._server)
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks,return_exceptions=True)
        await asyncio.sleep(0)

    async def close(self):
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                continue
        self._close_task.result()