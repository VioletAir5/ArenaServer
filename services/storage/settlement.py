"""Atomic settlement credits and immutable reward/consumer receipts."""
from dataclasses import dataclass
import hashlib
import json
from uuid import UUID

from psycopg.types.json import Jsonb

from .repositories import _integer, _UINT64_MAX, _INT32_MAX
from .transactions import StorageError

_MAX_BALANCE = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class Reward:
    """One positive credit decided by the authoritative settlement policy."""
    player_id: int
    item_id: int
    quantity: int


@dataclass(frozen=True, slots=True)
class SettlementCommand:
    """Stable identities must be retained across retries and process restarts."""
    settlement_id: int
    match_id: int
    idempotency_key: UUID
    event_id: UUID
    rewards: tuple[Reward, ...] = ()
    result_version: int = 1


@dataclass(frozen=True, slots=True)
class SettlementReceipt:
    settlement_id: int
    match_id: int
    event_id: UUID
    replayed: bool


def _uuid(value):
    if not isinstance(value, UUID) or value.int == 0:
        raise StorageError("invalid_request")
    return value


def _json(value):
    """Use one bounded canonical representation for hashing and wire events."""
    result = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                        allow_nan=False)
    if len(result.encode("ascii")) > 65536:
        raise StorageError("invalid_request")
    return result


def _command(value):
    if not isinstance(value, SettlementCommand):
        raise StorageError("invalid_request")
    _integer(value.settlement_id, 1, _UINT64_MAX)
    _integer(value.match_id, 1, _UINT64_MAX)
    _integer(value.result_version, 1, _INT32_MAX)
    _uuid(value.idempotency_key)
    _uuid(value.event_id)
    if type(value.rewards) is not tuple or len(value.rewards) > 128:
        raise StorageError("invalid_request")
    for reward in value.rewards:
        if not isinstance(reward, Reward):
            raise StorageError("invalid_request")
        _integer(reward.player_id, 1, _UINT64_MAX)
        _integer(reward.item_id, 1, _UINT64_MAX)
        _integer(reward.quantity, 1, _MAX_BALANCE)
    rewards = tuple(sorted(value.rewards, key=lambda r: (r.player_id, r.item_id)))
    if len({(r.player_id, r.item_id) for r in rewards}) != len(rewards):
        raise StorageError("invalid_request")
    return rewards


class SettlementRepository:
    """Credit inventory, journal rewards and enqueue the event in one UoW."""
    def __init__(self, session):
        self._session = session

    def commit(self, command):
        connection = self._session.connection()
        rewards = _command(command)
        with connection.cursor() as cursor:
            # Match ownership serializes all competing settlement identities.
            cursor.execute("SELECT state,result_version,final_tick,final_checksum,"
                           "winner_player_id,finish_reason FROM arena.game_match "
                           "WHERE match_id=%s FOR UPDATE", (command.match_id,))
            match = cursor.fetchone()
            if match is None:
                raise StorageError("not_found")
            if match[0] not in ("completed", "aborted") or match[1] != command.result_version:
                raise StorageError("invalid_request")
            cursor.execute("SELECT player_id FROM arena.match_player WHERE match_id=%s",
                           (command.match_id,))
            members = {int(row[0]) for row in cursor.fetchall()}
            if (match[0] == "aborted" and rewards
                    or any(r.player_id not in members for r in rewards)):
                raise StorageError("invalid_request")
            payload = {
                "settlement_id": str(command.settlement_id),
                "match_id": str(command.match_id),
                "result_version": command.result_version,
                "result": {"state": match[0], "final_tick": str(match[2]),
                           "final_checksum": str(match[3]),
                           "winner_player_id": None if match[4] is None else str(match[4]),
                           "reason": match[5]},
                "rewards": [{"player_id": str(r.player_id), "item_id": str(r.item_id),
                             "quantity": r.quantity} for r in rewards],
            }
            canonical = _json(payload)
            request_hash = hashlib.sha256(
                (str(command.idempotency_key) + ":" + str(command.event_id) + ":" + canonical
                 ).encode("ascii")).digest()
            cursor.execute("SELECT settlement_id,match_id,idempotency_key,request_hash,state "
                           "FROM arena.settlement WHERE settlement_id=%s OR match_id=%s "
                           "OR idempotency_key=%s FOR UPDATE",
                           (command.settlement_id, command.match_id, command.idempotency_key))
            existing = cursor.fetchall()
            if existing:
                if (len(existing) != 1
                        or int(existing[0][0]) != command.settlement_id
                        or int(existing[0][1]) != command.match_id
                        or existing[0][2] != command.idempotency_key
                        or bytes(existing[0][3]) != request_hash
                        or existing[0][4] != "committed"):
                    raise StorageError("idempotency_conflict")
                return SettlementReceipt(command.settlement_id, command.match_id,
                                         command.event_id, True)
            cursor.execute("INSERT INTO arena.settlement "
                           "(settlement_id,match_id,match_state,idempotency_key,request_hash,"
                           "result_version,result_payload) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                           (command.settlement_id, command.match_id, match[0],
                            command.idempotency_key, request_hash, command.result_version,
                            Jsonb(payload)))
            cursor.execute("UPDATE arena.settlement SET state='applying',attempt_count=1,"
                           "last_attempt_at=statement_timestamp(),updated_at=statement_timestamp() "
                           "WHERE settlement_id=%s", (command.settlement_id,))
            # A global player/item ordering also agrees with asset transfers.
            for reward in rewards:
                cursor.execute("INSERT INTO arena.inventory(player_id,item_id) VALUES (%s,%s) "
                               "ON CONFLICT DO NOTHING", (reward.player_id, reward.item_id))
                cursor.execute("SELECT quantity,revision FROM arena.inventory "
                               "WHERE player_id=%s AND item_id=%s FOR UPDATE",
                               (reward.player_id, reward.item_id))
                quantity, revision = cursor.fetchone()
                if quantity > _MAX_BALANCE - reward.quantity:
                    raise StorageError("quantity_overflow")
                if revision == _MAX_BALANCE:
                    raise StorageError("revision_overflow")
                cursor.execute("UPDATE arena.inventory SET quantity=quantity+%s,"
                               "revision=revision+1,updated_at=statement_timestamp() "
                               "WHERE player_id=%s AND item_id=%s",
                               (reward.quantity, reward.player_id, reward.item_id))
                cursor.execute("INSERT INTO arena.asset_ledger "
                               "(settlement_id,player_id,item_id,quantity) VALUES (%s,%s,%s,%s)",
                               (command.settlement_id, reward.player_id, reward.item_id,
                                reward.quantity))
            cursor.execute("UPDATE arena.settlement SET state='committed',"
                           "committed_at=statement_timestamp(),updated_at=statement_timestamp() "
                           "WHERE settlement_id=%s", (command.settlement_id,))
            cursor.execute("INSERT INTO arena.outbox(event_id,settlement_id,payload) "
                           "VALUES (%s,%s,%s)",
                           (command.event_id, command.settlement_id, Jsonb(payload)))
            return SettlementReceipt(command.settlement_id, command.match_id,
                                     command.event_id, False)

    def get(self, settlement_id):
        """Return the stable committed receipt without exposing a live cursor."""
        connection = self._session.connection()
        _integer(settlement_id, 1, _UINT64_MAX)
        with connection.cursor() as cursor:
            cursor.execute("SELECT s.match_id,o.event_id FROM arena.settlement s "
                           "JOIN arena.outbox o USING(settlement_id) "
                           "WHERE s.settlement_id=%s AND s.state='committed'",
                           (settlement_id,))
            row = cursor.fetchone()
            return None if row is None else SettlementReceipt(settlement_id, int(row[0]),
                                                             row[1], True)


def settle(database, command, **options):
    """Business entry: only repositories cross the transaction boundary."""
    return database.run(lambda work: work.settlements.commit(command), **options)
