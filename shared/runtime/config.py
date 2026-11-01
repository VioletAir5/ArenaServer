"""Validated immutable discovery snapshots and finite asynchronous file reloads."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
import re
import stat
import threading

from google.protobuf import json_format

MAX_BYTES = 64 * 1024


class ConfigError(ValueError):
    """Expose a stable category without disclosing file contents or diagnostics."""
    def __init__(self, category):
        super().__init__(category)
        self.category = category


def _canonical(name):
    parts = name.split('_')
    return parts[0] + ''.join(part[:1].upper()+part[1:] for part in parts[1:])


def _invalid_constant(_):
    raise ConfigError('invalid')


def _members(pairs):
    result, seen = {}, set()
    for name, value in pairs:
        normalized = _canonical(name)
        if normalized in seen:
            raise ConfigError('invalid')
        seen.add(normalized)
        result[name] = value
    return result


def _bounded_nesting(text):
    depth, quoted, escaped = 0, False, False
    for character in text:
        if quoted:
            if escaped:
                escaped = False
            elif character == '\\':
                escaped = True
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif character in '{[':
            depth += 1
            if depth > 16:
                return False
        elif character in '}]':
            if depth == 0:
                return False
            depth -= 1
    return not quoted and depth == 0


@dataclass(frozen=True, slots=True)
class Node:
    """One immutable, exactly addressed internal node."""
    service: str
    id: str
    host: str
    port: int
    ready: bool

    def __post_init__(self):
        if (not isinstance(self.service, str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', self.service)
                or not isinstance(self.id, str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', self.id)
                or self.host not in ('127.0.0.1', '::1') or type(self.port) is not int
                or not 1 <= self.port <= 65535 or type(self.ready) is not bool):
            raise ConfigError('invalid')


@dataclass(frozen=True, slots=True)
class ConfigSnapshot:
    """A complete detached version; readers retain it across later publications."""
    version: int
    desired_ready: bool
    drain_grace_ms: int
    nodes: tuple[Node, ...]

    def __post_init__(self):
        if (type(self.version) is not int or not 1 <= self.version < (1 << 64)
                or type(self.desired_ready) is not bool or type(self.drain_grace_ms) is not int
                or not 0 <= self.drain_grace_ms <= 10000 or type(self.nodes) is not tuple
                or not 1 <= len(self.nodes) <= 128 or any(type(node) is not Node for node in self.nodes)
                or len({(node.service, node.id) for node in self.nodes}) != len(self.nodes)
                or self.nodes != tuple(sorted(self.nodes, key=lambda node:(node.service,node.id)))):
            raise ConfigError('invalid')

    def resolve(self, service):
        return next((node for node in self.nodes if node.service == service and node.ready), None)

    @classmethod
    def parse(cls, protocol, data):
        try:
            if isinstance(data, bytes):
                if not 0 < len(data) <= MAX_BYTES:
                    raise ConfigError('invalid')
                data = data.decode('utf-8')
            if not isinstance(data, str) or not 0 < len(data.encode('utf-8')) <= MAX_BYTES or not _bounded_nesting(data):
                raise ConfigError('invalid')
            document = json.loads(data, object_pairs_hook=_members,
                                  parse_constant=_invalid_constant)
            message = protocol.message('arena.runtime.config.RuntimeConfig')()
            json_format.ParseDict(document, message, ignore_unknown_fields=False,
                                  descriptor_pool=protocol.pool, max_recursion_depth=16)
            if not message.HasField('desired_ready') or not message.HasField('drain_grace_ms'):
                raise ConfigError('invalid')
            nodes = []
            for node in message.nodes:
                if not node.HasField('ready'):
                    raise ConfigError('invalid')
                nodes.append(Node(node.service,node.node_id,node.host,node.port,node.ready))
            return cls(message.version,message.desired_ready,message.drain_grace_ms,
                       tuple(sorted(nodes,key=lambda node:(node.service,node.id))))
        except ConfigError:
            raise
        except (ValueError, TypeError, UnicodeError, RecursionError, json_format.ParseError, OverflowError):
            raise ConfigError('invalid') from None

    @classmethod
    def read(cls, protocol, path):
        descriptor = None
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW)
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_BYTES:
                raise ConfigError('file')
            content = bytearray()
            while len(content) < before.st_size:
                block = os.read(descriptor, before.st_size-len(content))
                if not block:
                    raise ConfigError('changed')
                content.extend(block)
            after = os.fstat(descriptor)
            if (before.st_size,before.st_mtime_ns,before.st_ctime_ns) != (after.st_size,after.st_mtime_ns,after.st_ctime_ns):
                raise ConfigError('changed')
            return cls.parse(protocol,bytes(content))
        except OSError:
            raise ConfigError('file') from None
        finally:
            if descriptor is not None:
                os.close(descriptor)


class ConfigStore:
    """Serialize publications while returning only immutable snapshots to readers."""
    def __init__(self, initial):
        if type(initial) is not ConfigSnapshot:
            raise ConfigError('invalid')
        self._current, self._lock, self._notifying = initial, threading.Lock(), False
        self._notification_failures = 0

    def snapshot(self):
        with self._lock:
            return self._current

    @property
    def notification_failures(self):
        with self._lock:
            return self._notification_failures

    def publish(self, candidate, notify=None):
        with self._lock:
            if type(candidate) is not ConfigSnapshot or notify is not None and not callable(notify):
                raise ConfigError('invalid')
            if self._notifying:
                raise ConfigError('busy')
            if candidate.version <= self._current.version:
                raise ConfigError('stale')
            self._current, self._notifying = candidate, True
        try:
            if notify is not None:
                try:
                    notify(candidate)
                except Exception:
                    with self._lock:
                        self._notification_failures += 1
        finally:
            with self._lock:
                self._notifying = False


class ConfigReloader:
    """One loader owns file I/O; the owner loop applies at most one pending reload."""
    def __init__(self, protocol, path, store, notify=None):
        self.protocol, self.path, self.store, self.notify = protocol, path, store, notify
        self._loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(max_workers=1,thread_name_prefix='arena-config')
        self._future = self._close_task = None
        self._again = self._closed = False
        self._counts = dict(applied=0,rejected=0,coalesced=0,active=0,peak=0,last_error=None)

    @property
    def stats(self):
        return dict(self._counts)

    def request_reload(self):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError('config reload belongs to its owner loop')
        if self._closed:
            return False
        if self._future is not None:
            self._again = True
            self._counts['coalesced'] += 1
            return False
        self._counts['active'] = 1
        self._counts['peak'] = 1
        self._future = self._executor.submit(ConfigSnapshot.read,self.protocol,self.path)
        self._future.add_done_callback(lambda future:self._loop.call_soon_threadsafe(self._finished,future))
        return True

    def _finished(self, future):
        try:
            candidate = future.result()
            if not self._closed:
                self.store.publish(candidate,self.notify)
                self._counts['applied'] += 1
                self._counts['last_error'] = None
        except ConfigError as error:
            self._counts['rejected'] += 1
            self._counts['last_error'] = error.category
        except Exception:
            self._counts['rejected'] += 1
            self._counts['last_error'] = 'internal'
        finally:
            self._future = None
            self._counts['active'] = 0
        if self._again and not self._closed:
            self._again = False
            self.request_reload()

    async def _close(self):
        if self._future is not None:
            await asyncio.gather(asyncio.wrap_future(self._future),return_exceptions=True)
        # The actual file operation has returned before its idle thread joins.
        self._executor.shutdown(wait=True)
        await asyncio.sleep(0)

    async def close(self):
        if self._close_task is None:
            self._closed, self._again = True, False
            self._close_task = asyncio.create_task(self._close())
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                continue
        self._close_task.result()