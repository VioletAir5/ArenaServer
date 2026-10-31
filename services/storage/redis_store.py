"""Bounded Redis projections; PostgreSQL remains authoritative."""
from dataclasses import dataclass
import asyncio
import math
from itertools import islice
import re

import redis.asyncio as redis
from redis.asyncio.connection import UnixDomainSocketConnection
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError, ResponseError


class CacheError(Exception):
    """Expose stable categories without leaking credentials or raw replies."""
    def __init__(self, category):
        self.category = category
        super().__init__(category)


def _uint64(value):
    if type(value) is not int or not 0 < value < 1 << 64:
        raise ValueError("identity must be a positive uint64")
    return f"{value:020d}"


def _timeout(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 60:
        raise ValueError("timeout must be finite and in (0, 60]")
    return float(value)


def _ttl(value):
    if type(value) is not int or not 1 <= value <= 3_600_000:
        raise ValueError("TTL must be 1..3600000 milliseconds")
    return value


def _batch(values, limit):
    result = tuple(islice(iter(values), limit+1))
    if not 1 <= len(result) <= limit:
        raise ValueError("batch exceeds limit")
    return result


@dataclass(frozen=True, slots=True)
class SessionProjection:
    player_id: int
    session_id: int
    epoch: int
    room_id: int = 0


_SESSION_PUT = """
if redis.call('STRLEN', KEYS[1]) > 62 then return -1 end
local old = redis.call('GET', KEYS[1])
if old then
  if string.len(old) ~= 62 then return -1 end
  if string.sub(old,1,20) > string.sub(ARGV[1],1,20) then return 0 end
  if string.sub(old,1,20) == string.sub(ARGV[1],1,20)
     and string.sub(old,22,41) ~= string.sub(ARGV[1],22,41) then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); return 1
"""
_SESSION_READ = """
if redis.call('STRLEN', KEYS[1]) > 62 then return {-1} end
local value = redis.call('GET', KEYS[1])
if not value then return {0} end
return {1,value,redis.call('PTTL', KEYS[1])}
"""
_SESSION_TOUCH = """
if redis.call('STRLEN', KEYS[1]) > 62 then return -1 end
local old = redis.call('GET', KEYS[1])
if not old then return 0 end
if string.len(old) ~= 62 then return -1 end
if string.sub(old,1,41) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
"""
_SESSION_DELETE = """
if redis.call('STRLEN', KEYS[1]) > 62 then return -1 end
local old = redis.call('GET', KEYS[1])
if not old then return 0 end
if string.len(old) ~= 62 then return -1 end
if string.sub(old,1,41) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""
_CACHE_READ = """
if redis.call('STRLEN', KEYS[1]) > tonumber(ARGV[1]) then return {-1} end
local value = redis.call('GET', KEYS[1])
if not value then return {0} end
return {1,value}
"""
_LEADERBOARD_PUT = """
if not redis.call('ZSCORE', KEYS[1], ARGV[1])
   and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[4]); return 1
"""
_LEADERBOARD_TOP = """
local rows = redis.call(\'ZREVRANGE\', KEYS[1], ARGV[1], ARGV[2], \'WITHSCORES\')
for i=1,#rows,2 do
  if string.len(rows[i]) ~= 20 then return {-1} end
end
return rows
"""
_LEADERBOARD_READ = """
local rank = redis.call('ZREVRANK', KEYS[1], ARGV[1])
if not rank then return {} end
return {rank,redis.call('ZSCORE', KEYS[1], ARGV[1])}
"""


class RedisStore:
    """Own a fail-fast finite pool on one event loop, with no command retry.

    Projections never authorize players or change inventory. SDK cancellation
    disconnects an incomplete reply before returning its loan. close rejects new
    work and waits for bounded active I/O; callers must join their own tasks.
    """
    def __init__(self, *, namespace="arena", host="127.0.0.1", port=6379,
                 unix_socket_path=None, password=None, username=None,
                 max_connections=8, io_timeout=.1, max_value_bytes=65536,
                 max_batch=128, max_members=4096):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", namespace):
            raise ValueError("invalid Redis namespace")
        if (type(max_connections) is not int or not 1 <= max_connections <= 128
                or type(max_value_bytes) is not int or not 64 <= max_value_bytes <= 262144
                or type(max_batch) is not int or not 1 <= max_batch <= 512
                or type(max_members) is not int or not 1 <= max_members <= 10000):
            raise ValueError("invalid Redis resource limit")
        parameters = dict(max_connections=max_connections, socket_timeout=_timeout(io_timeout),
            retry=Retry(NoBackoff(), 0), decode_responses=False,
            password=password, username=username)
        if unix_socket_path:
            parameters.update(connection_class=UnixDomainSocketConnection, path=str(unix_socket_path))
        else:
            if not isinstance(host, str) or not host or type(port) is not int or not 1 <= port <= 65535:
                raise ValueError("explicit Redis host and port are required")
            parameters.update(host=host, port=port, socket_connect_timeout=_timeout(io_timeout))
        self._pool = redis.ConnectionPool(**parameters)
        self._client = redis.Redis(connection_pool=self._pool)
        self.namespace, self.max_value_bytes = namespace, max_value_bytes
        self.max_batch, self.max_members = max_batch, max_members
        self._limit, self._active = max_connections, 0
        self._closed, self._loop = False, None
        self._idle = None
        self._counts = dict(commands=0, failures=0, rejected=0, peak_operations=0)

    def _bind(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop, self._idle = loop, asyncio.Event()
            self._idle.set()
        elif self._loop is not loop:
            raise CacheError("wrong_event_loop")

    @property
    def stats(self):
        return dict(self._counts, active_operations=self._active, closed=self._closed)

    async def _call(self, operation):
        self._bind()
        if self._closed:
            raise CacheError("closed")
        if self._active >= self._limit:
            self._counts["rejected"] += 1
            raise CacheError("overloaded")
        self._active += 1
        self._idle.clear()
        self._counts["commands"] += 1
        self._counts["peak_operations"] = max(self._active, self._counts["peak_operations"])
        try:
            return await operation()
        except ResponseError:
            self._counts["failures"] += 1
            raise CacheError("invalid_data") from None
        except (RedisError, OSError):
            self._counts["failures"] += 1
            raise CacheError("unavailable") from None
        finally:
            self._active -= 1
            if self._active == 0:
                self._idle.set()

    def _key(self, kind, key):
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key):
            raise ValueError("invalid bounded cache key")
        return f"{self.namespace}:{kind}:{key}"

    def _session_key(self, player_id):
        return self._key("session", _uint64(player_id))

    @staticmethod
    def _session_identity(session_id, epoch):
        return _uint64(epoch) + ":" + _uint64(session_id)

    async def put_session(self, session, *, ttl_ms):
        identity = self._session_identity(session.session_id, session.epoch)
        if type(session.room_id) is not int or not 0 <= session.room_id < 1 << 64:
            raise ValueError("room must be a uint64")
        room = "00000000000000000000" if session.room_id == 0 else _uint64(session.room_id)
        result = await self._call(lambda: self._client.eval(_SESSION_PUT, 1,
            self._session_key(session.player_id), identity + ":" + room, _ttl(ttl_ms)))
        if result == -1:
            raise CacheError("invalid_data")
        return bool(result)

    def _decode_session(self, player_id, result):
        if result == [0]:
            return None
        if (not isinstance(result, list) or len(result) != 3 or result[0] != 1
                or not isinstance(result[1], bytes) or not re.fullmatch(rb"[0-9]{20}:[0-9]{20}:[0-9]{20}", result[1])
                or type(result[2]) is not int or result[2] < 0):
            raise CacheError("invalid_data")
        if result[2] == 0:
            return None
        epoch, session_id, room_id = map(int, result[1].split(b":"))
        if not 0 < epoch < 1 << 64 or not 0 < session_id < 1 << 64 or not 0 <= room_id < 1 << 64:
            raise CacheError("invalid_data")
        return SessionProjection(player_id, session_id, epoch, room_id)

    async def get_session(self, player_id):
        result = await self._call(lambda: self._client.eval(_SESSION_READ, 1, self._session_key(player_id)))
        return self._decode_session(player_id, result)

    async def get_sessions(self, player_ids):
        players = _batch(player_ids, self.max_batch)
        keys = [self._session_key(player) for player in players]
        async def pipeline():
            async with self._client.pipeline(transaction=False) as commands:
                for key in keys:
                    commands.eval(_SESSION_READ, 1, key)
                return await commands.execute()
        values = await self._call(pipeline)
        if len(values) != len(players):
            raise CacheError("invalid_data")
        return tuple(self._decode_session(player, value) for player, value in zip(players, values))

    async def touch_session(self, player_id, session_id, epoch, *, ttl_ms):
        identity = self._session_identity(session_id, epoch)
        result = await self._call(lambda: self._client.eval(_SESSION_TOUCH, 1,
            self._session_key(player_id), identity, _ttl(ttl_ms)))
        if result == -1:
            raise CacheError("invalid_data")
        return bool(result)

    async def delete_session(self, player_id, session_id, epoch):
        identity = self._session_identity(session_id, epoch)
        result = await self._call(lambda: self._client.eval(_SESSION_DELETE, 1,
            self._session_key(player_id), identity))
        if result == -1:
            raise CacheError("invalid_data")
        return bool(result)

    async def put_score(self, board, player_id, score, *, ttl_ms=3_600_000):
        if type(score) is not int or not -(1 << 53) < score < 1 << 53:
            raise ValueError("score must be an exactly representable integer")
        result = await self._call(lambda: self._client.eval(_LEADERBOARD_PUT, 1,
            self._key("leaderboard", board), _uint64(player_id), score, self.max_members, _ttl(ttl_ms)))
        if not result:
            raise CacheError("capacity")

    async def top(self, board, *, offset=0, count=100):
        if type(offset) is not int or not 0 <= offset < self.max_members or type(count) is not int or not 1 <= count <= 100:
            raise ValueError("invalid bounded leaderboard page")
        rows = await self._call(lambda: self._client.eval(_LEADERBOARD_TOP, 1,
            self._key("leaderboard", board), offset, offset + count - 1))
        if not isinstance(rows, list) or len(rows) % 2:
            raise CacheError("invalid_data")
        result = []
        for i in range(0, len(rows), 2):
            player, raw_score = rows[i:i+2]
            try:
                score = float(raw_score)
            except (ValueError, TypeError, OverflowError):
                raise CacheError("invalid_data") from None
            if not isinstance(player, bytes) or not re.fullmatch(rb"[0-9]{20}", player):
                raise CacheError("invalid_data")
            identity = int(player)
            if (not 0 < identity < 1 << 64 or not math.isfinite(score)
                    or not -(1 << 53) < score < 1 << 53 or score != int(score)):
                raise CacheError("invalid_data")
            result.append((identity, int(score)))
        return tuple(result)

    async def ranks(self, board, player_ids):
        players = _batch(player_ids, self.max_batch)
        members = [_uint64(player) for player in players]
        key = self._key("leaderboard", board)
        async def pipeline():
            async with self._client.pipeline(transaction=False) as commands:
                for member in members:
                    commands.eval(_LEADERBOARD_READ, 1, key, member)
                return await commands.execute()
        values = await self._call(pipeline)
        if len(values) != len(players):
            raise CacheError("invalid_data")
        result = []
        for value in values:
            if value == []:
                result.append(None)
            elif isinstance(value, list) and len(value) == 2 and type(value[0]) is int and 0 <= value[0] < self.max_members:
                try:
                    score = int(value[1])
                except (ValueError, TypeError, OverflowError):
                    raise CacheError("invalid_data") from None
                if not -(1 << 53) < score < 1 << 53:
                    raise CacheError("invalid_data")
                result.append((value[0] + 1, score))
            else:
                raise CacheError("invalid_data")
        return tuple(result)

    async def get_cache(self, key):
        value = await self._call(lambda: self._client.eval(_CACHE_READ, 1,
            self._key("cache", key), self.max_value_bytes))
        if value == [0]:
            return None
        if not isinstance(value, list) or len(value) != 2 or value[0] != 1 or not isinstance(value[1], bytes):
            raise CacheError("invalid_data")
        return value[1]

    async def set_cache(self, key, value, *, ttl_ms):
        if not isinstance(value, bytes) or len(value) > self.max_value_bytes:
            raise ValueError("cache value exceeds limit")
        return await self._call(lambda: self._client.set(self._key("cache", key), value, px=_ttl(ttl_ms)))

    async def delete_cache(self, key):
        return await self._call(lambda: self._client.delete(self._key("cache", key)))

    async def close(self):
        self._bind()
        self._closed = True
        await self._idle.wait()
        await self._client.aclose()
        await self._pool.aclose()

    async def __aenter__(self):
        self._bind()
        return self

    async def __aexit__(self, *_):
        await self.close()
