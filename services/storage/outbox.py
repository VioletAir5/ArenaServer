"""Fenced at-least-once outbox delivery and transactional consumer deduplication."""
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import inspect
import json
import math
import random
import re
from uuid import UUID, uuid4

from .repositories import _integer, _timestamp
from .settlement import _json, _uuid
from .transactions import StorageError


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    event_id: UUID
    settlement_id: int
    payload_json: str
    event_type: str = "settlement.committed"
    schema_version: int = 1


@dataclass(frozen=True, slots=True)
class Delivery:
    event: OutboxEvent
    lease_token: UUID
    attempt: int
    leased_until: datetime


class PermanentDeliveryError(Exception):
    """A transport/handler may classify a message as permanently undeliverable."""


def _duration(value, *, zero=False, upper=60):
    if (type(value) not in (int, float) or not math.isfinite(value)
            or not (0 <= value <= upper if zero else 0 < value <= upper)):
        raise StorageError("invalid_request")
    return float(value)


def _event(row):
    return OutboxEvent(row[0], int(row[1]), _json(row[2]), row[3], row[4])


class OutboxRepository:
    """Lease mutation and receipts are owned by the caller's active transaction."""
    def __init__(self, session):
        self._session = session

    @staticmethod
    def _now(cursor, value):
        if value is not None:
            return _timestamp(value)
        cursor.execute("SELECT clock_timestamp()")
        return cursor.fetchone()[0]

    def get(self, event_id):
        connection = self._session.connection()
        _uuid(event_id)
        with connection.cursor() as cursor:
            cursor.execute("SELECT event_id,settlement_id,payload,event_type,schema_version "
                           "FROM arena.outbox WHERE event_id=%s", (event_id,))
            row = cursor.fetchone()
            return None if row is None else _event(row)

    def claim(self, *, limit=32, lease_seconds=5, max_attempts=8, now=None):
        """SKIP LOCKED partitions work; a new UUID fences every reassignment."""
        connection = self._session.connection()
        _integer(limit, 1, 128)
        _integer(max_attempts, 1, 64)
        lease_seconds = _duration(lease_seconds)
        with connection.cursor() as cursor:
            instant = self._now(cursor, now)
            cursor.execute("SELECT o.event_id,o.settlement_id,o.payload,o.event_type,"
                           "o.schema_version,o.attempt_count,coalesce(d.attempt_base,0) FROM arena.outbox o "
                           "LEFT JOIN arena.outbox_delivery d USING(event_id) "
                           "WHERE o.state='pending' AND o.available_at<=%s "
                           "AND d.dead_at IS NULL "
                           "AND (d.leased_until IS NULL OR d.leased_until<=%s) "
                           "ORDER BY o.available_at,o.event_id LIMIT %s FOR UPDATE OF o SKIP LOCKED",
                           (instant, instant, limit))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                cursor.execute("INSERT INTO arena.outbox_delivery(event_id) VALUES (%s) "
                               "ON CONFLICT DO NOTHING", (row[0],))
                if row[5] - row[6] >= max_attempts:
                    cursor.execute("UPDATE arena.outbox_delivery SET lease_token=NULL,"
                                   "leased_until=NULL,dead_at=%s,last_error_code='RETRY_EXHAUSTED' "
                                   "WHERE event_id=%s", (instant, row[0]))
                    continue
                token, until = uuid4(), instant + timedelta(seconds=lease_seconds)
                cursor.execute("UPDATE arena.outbox_delivery SET lease_token=%s,"
                               "leased_until=%s WHERE event_id=%s", (token, until, row[0]))
                cursor.execute("UPDATE arena.outbox SET attempt_count=attempt_count+1,"
                               "last_attempt_at=%s,updated_at=%s WHERE event_id=%s",
                               (instant, instant, row[0]))
                result.append(Delivery(_event(row), token, row[5] - row[6] + 1, until))
            return tuple(result)

    def _owned(self, cursor, delivery, now):
        if not isinstance(delivery, Delivery):
            raise StorageError("invalid_request")
        _uuid(delivery.event.event_id)
        _uuid(delivery.lease_token)
        instant = self._now(cursor, now)
        cursor.execute("SELECT o.state,o.attempt_count-d.attempt_base,d.lease_token,d.leased_until,d.dead_at "
                       "FROM arena.outbox o JOIN arena.outbox_delivery d USING(event_id) "
                       "WHERE o.event_id=%s FOR UPDATE OF o,d", (delivery.event.event_id,))
        row = cursor.fetchone()
        if (row is None or row[0] != "pending" or row[2] != delivery.lease_token
                or row[3] <= instant or row[4] is not None):
            raise StorageError("stale_delivery")
        return instant, row[1]

    def acknowledge(self, delivery, *, now=None):
        connection = self._session.connection()
        with connection.cursor() as cursor:
            instant, _ = self._owned(cursor, delivery, now)
            cursor.execute("UPDATE arena.outbox SET state='published',published_at=%s,"
                           "updated_at=%s WHERE event_id=%s",
                           (instant, instant, delivery.event.event_id))
            cursor.execute("UPDATE arena.outbox_delivery SET lease_token=NULL,leased_until=NULL,"
                           "last_error_code=NULL WHERE event_id=%s", (delivery.event.event_id,))

    def fail(self, delivery, *, delay=0, max_attempts=8, permanent=False,
             error_code="DELIVERY_FAILED", now=None):
        connection = self._session.connection()
        delay = _duration(delay, zero=True)
        _integer(max_attempts, 1, 64)
        if (type(permanent) is not bool or not isinstance(error_code, str)
                or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", error_code) is None):
            raise StorageError("invalid_request")
        with connection.cursor() as cursor:
            instant, attempt = self._owned(cursor, delivery, now)
            dead = permanent or attempt >= max_attempts
            cursor.execute("UPDATE arena.outbox_delivery SET lease_token=NULL,leased_until=NULL,"
                           "dead_at=%s,last_error_code=%s WHERE event_id=%s",
                           (instant if dead else None, error_code, delivery.event.event_id))
            if not dead:
                cursor.execute("UPDATE arena.outbox SET available_at=%s,updated_at=%s "
                               "WHERE event_id=%s",
                               (instant + timedelta(seconds=delay), instant, delivery.event.event_id))
            return dead

    def requeue_dead(self, event_id, *, now=None):
        """Explicit operator action; retains the same immutable event identity."""
        connection = self._session.connection()
        _uuid(event_id)
        with connection.cursor() as cursor:
            instant = self._now(cursor, now)
            cursor.execute("SELECT o.state,d.dead_at FROM arena.outbox o "
                           "JOIN arena.outbox_delivery d USING(event_id) "
                           "WHERE o.event_id=%s FOR UPDATE OF o,d", (event_id,))
            row = cursor.fetchone()
            if row is None or row[0] != "pending" or row[1] is None:
                raise StorageError("invalid_request")
            # Attempt counts are monotonic. An explicit requeue gets a fresh
            # bounded allowance using an offset, never a reset or hidden retry.
            cursor.execute("UPDATE arena.outbox_delivery SET dead_at=NULL,last_error_code=NULL,"
                           "attempt_base=(SELECT attempt_count FROM arena.outbox WHERE event_id=%s) "
                           "WHERE event_id=%s", (event_id, event_id))
            cursor.execute("UPDATE arena.outbox SET available_at=%s,updated_at=%s WHERE event_id=%s",
                           (instant, instant, event_id))

    def consume(self, event, *, consumer="reward_audit"):
        """Dedup plus projection commit together. No consumer mints rewards."""
        connection = self._session.connection()
        if (not isinstance(event, OutboxEvent) or not isinstance(consumer, str)
                or re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", consumer) is None):
            raise StorageError("invalid_request")
        _uuid(event.event_id)
        if (not isinstance(event.payload_json, str)
                or len(event.payload_json.encode("utf-8")) > 65536):
            raise StorageError("invalid_request")
        try:
            incoming = _json(json.loads(event.payload_json))
        except (ValueError, TypeError, RecursionError):
            raise StorageError("invalid_request") from None
        with connection.cursor() as cursor:
            cursor.execute("SELECT event_id,settlement_id,payload,event_type,schema_version "
                           "FROM arena.outbox WHERE event_id=%s", (event.event_id,))
            row = cursor.fetchone()
            if row is None:
                raise StorageError("not_found")
            authoritative = _event(row)
            if (event.settlement_id != authoritative.settlement_id
                    or event.event_type != authoritative.event_type
                    or event.schema_version != authoritative.schema_version
                    or incoming != authoritative.payload_json):
                raise StorageError("idempotency_conflict")
            digest = hashlib.sha256(incoming.encode("ascii")).digest()
            cursor.execute("INSERT INTO arena.consumer_receipt(consumer,event_id,payload_hash) "
                           "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id",
                           (consumer, event.event_id, digest))
            if cursor.fetchone() is None:
                cursor.execute("SELECT payload_hash FROM arena.consumer_receipt "
                               "WHERE consumer=%s AND event_id=%s", (consumer, event.event_id))
                prior = cursor.fetchone()
                if prior is None:
                    # A concurrent invisible winner requires a new SERIALIZABLE snapshot.
                    from psycopg.errors import SerializationFailure
                    raise SerializationFailure("consumer receipt conflict")
                if bytes(prior[0]) != digest:
                    raise StorageError("idempotency_conflict")
                return True
            cursor.execute("INSERT INTO arena.settlement_reward_projection "
                           "(consumer,event_id,player_id,item_id,quantity) "
                           "SELECT %s,%s,player_id,item_id,quantity FROM arena.asset_ledger "
                           "WHERE settlement_id=%s",
                           (consumer, event.event_id, event.settlement_id))
            return False

    def stats(self):
        connection = self._session.connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FILTER (WHERE o.state='pending' AND d.dead_at IS NULL),"
                           "count(*) FILTER (WHERE d.dead_at IS NOT NULL),"
                           "count(*) FILTER (WHERE o.state='published') "
                           "FROM arena.outbox o LEFT JOIN arena.outbox_delivery d USING(event_id)")
            pending, dead, published = cursor.fetchone()
            return dict(pending=pending, dead=dead, published=published)


class Publisher:
    """Deliver outside transactions; acknowledge only after the handler returns.

    Handler side effects must themselves be idempotent by event_id. A crash
    after delivery but before acknowledgement is deliberately redelivered.
    """
    def __init__(self, database, *, lease_seconds=5, max_attempts=8,
                 base_delay=.01, max_delay=5, random_value=None):
        _duration(lease_seconds)
        _integer(max_attempts, 1, 64)
        _duration(base_delay, upper=5)
        _duration(max_delay, upper=5)
        if base_delay > max_delay:
            raise StorageError("invalid_request")
        self.database, self.lease_seconds, self.max_attempts = database, lease_seconds, max_attempts
        self.base_delay, self.max_delay = base_delay, max_delay
        self.random_value = random_value or random.SystemRandom().random

    def run_once(self, deliver, *, limit=32):
        if not callable(deliver):
            raise StorageError("invalid_request")
        claimed = self.database.run(lambda work: work.outbox.claim(
            limit=limit, lease_seconds=self.lease_seconds, max_attempts=self.max_attempts)).value
        published, failed = 0, 0
        for delivery in claimed:
            try:
                result = deliver(delivery.event)
                if inspect.isawaitable(result) or inspect.isgenerator(result):
                    if inspect.iscoroutine(result) or inspect.isgenerator(result):
                        result.close()
                    raise PermanentDeliveryError("synchronous handler required")
            except Exception as error:
                sample = self.random_value()
                if type(sample) not in (int, float) or not math.isfinite(sample) or not 0 <= sample <= 1:
                    raise ValueError("random_value must return a finite value in [0, 1]")
                delay = sample * min(self.max_delay, self.base_delay * 2**(delivery.attempt - 1))
                self.database.run(lambda work: work.outbox.fail(
                    delivery, delay=delay, max_attempts=self.max_attempts,
                    permanent=isinstance(error, PermanentDeliveryError)))
                failed += 1
            else:
                self.database.run(lambda work: work.outbox.acknowledge(delivery))
                published += 1
        return dict(claimed=len(claimed), published=published, failed=failed)


def consume_event(database, event, *, consumer="reward_audit", **options):
    return database.run(lambda work: work.outbox.consume(event, consumer=consumer), **options)
