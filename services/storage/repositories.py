"""Transaction-scoped account and match repositories with immutable results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Literal, cast

from .transactions import StorageError


__all__ = ("Account", "Player", "MatchParticipant", "Match",
           "AccountRepository", "MatchRepository")

AccountState = Literal["active", "disabled"]
PlayerState = Literal["active", "suspended", "retired"]
MatchState = Literal["forming", "running", "completed", "aborted"]
FinishReason = Literal["completed", "member_unavailable", "ready_timeout"]

_UINT64_MAX = (1 << 64) - 1
_INT32_MIN = -(1 << 31)
_INT32_MAX = (1 << 31) - 1
_MMR_MAX = 10000

_ACCOUNT_FIELDS = "account_id, account_name, password_digest, state, created_at, updated_at"
_PLAYER_FIELDS = ("player_id, account_id, display_name, state, mmr, revision, "
                  "created_at, updated_at")
_MATCH_FIELDS = ("match_id, room_id, state, result_version, revision, winner_player_id, "
                 "final_tick, final_checksum, finish_reason, created_at, started_at, finished_at")


@dataclass(frozen=True, slots=True)
class Account:
    """One durable account; credential material is excluded from its repr."""

    account_id: int
    account_name: str
    password_digest: str = field(repr=False)
    state: AccountState
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Player:
    """One durable player and its owning account."""

    player_id: int
    account_id: int
    display_name: str
    state: PlayerState
    mmr: int
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class MatchParticipant:
    """A fixed participant identity with its last recorded integer state."""

    player_id: int
    slot: int
    session_id: int
    membership_epoch: int
    initial_mmr: int
    last_input_seq: int = 0
    final_x: int = 0
    final_y: int = 0


@dataclass(frozen=True, slots=True)
class Match:
    """A coherent parent/roster snapshot, with participants ordered by slot."""

    match_id: int
    room_id: int
    state: MatchState
    result_version: int
    revision: int
    winner_player_id: int | None
    final_tick: int | None
    final_checksum: int | None
    finish_reason: FinishReason | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    participants: tuple[MatchParticipant, ...]



def _integer(value, minimum: int, maximum: int) -> int:
    """Reject coercions, including bool, before binding integer parameters."""
    if type(value) is not int or not minimum <= value <= maximum:
        raise StorageError("invalid_request")
    return value


def _account_name(value: str) -> str:
    """Preserve the schema's case-sensitive, printable ASCII account names."""
    if not isinstance(value, str) or not re.fullmatch(r"[!-~]{1,128}", value):
        raise StorageError("invalid_request")
    return value


def _display_name(value: str) -> str:
    """Bound UTF-8 storage and exclude control characters without normalization."""
    if not isinstance(value, str):
        raise StorageError("invalid_request")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as error:
        raise StorageError("invalid_request") from error
    if (not 1 <= size <= 128 or not value.strip(" ")
            or any(ord(character) < 32 or 127 <= ord(character) <= 159
                   for character in value)):
        raise StorageError("invalid_request")
    return value


def _password_digest(value: str) -> str:
    """Validate the existing versioned digest without exposing it in errors."""
    if not isinstance(value, str):
        raise StorageError("invalid_request")
    match = re.fullmatch(
        r"\$arena-pbkdf2-sha256\$([1-9][0-9]{5,6})\$[0-9a-f]{32}\$[0-9a-f]{64}", value)
    if match is None or not 100000 <= int(match.group(1)) <= 2000000:
        raise StorageError("invalid_request")
    return value


def _timestamp(value: datetime) -> datetime:
    """Require an aware, representable instant and bind it in UTC."""
    if not isinstance(value, datetime):
        raise StorageError("invalid_request")
    try:
        if value.utcoffset() is None:
            raise StorageError("invalid_request")
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError) as error:
        raise StorageError("invalid_request") from error


def _participants(values: tuple[MatchParticipant, ...]) -> tuple[MatchParticipant, ...]:
    """Validate the whole two-player input before any parent or roster insert."""
    if type(values) is not tuple or len(values) != 2:
        raise StorageError("invalid_request")
    for value in values:
        if not isinstance(value, MatchParticipant):
            raise StorageError("invalid_request")
        _integer(value.player_id, 1, _UINT64_MAX)
        _integer(value.slot, 1, 2)
        _integer(value.session_id, 1, _UINT64_MAX)
        _integer(value.membership_epoch, 1, _UINT64_MAX)
        _integer(value.initial_mmr, 0, _MMR_MAX)
        _integer(value.last_input_seq, 0, _UINT64_MAX)
        _integer(value.final_x, _INT32_MIN, _INT32_MAX)
        _integer(value.final_y, _INT32_MIN, _INT32_MAX)
    if (values[0].player_id == values[1].player_id
            or values[0].slot == values[1].slot
            or values[0].session_id == values[1].session_id):
        raise StorageError("invalid_request")
    return tuple(sorted(values, key=lambda value: value.slot))


def _account(row) -> Account:
    """Convert PostgreSQL numeric IDs to Python integers at the persistence seam."""
    return Account(int(row[0]), row[1], row[2], cast(AccountState, row[3]), row[4], row[5])


def _player(row) -> Player:
    """Copy the database row into an immutable player record."""
    return Player(int(row[0]), int(row[1]), row[2], cast(PlayerState, row[3]),
                  int(row[4]), int(row[5]), row[6], row[7])


def _optional_integer(value) -> int | None:
    return None if value is None else int(value)


def _match(row, participants: tuple[MatchParticipant, ...]) -> Match:
    """Retain nullable result fields and the full uint64 checksum range."""
    return Match(int(row[0]), int(row[1]), cast(MatchState, row[2]),
                 int(row[3]), int(row[4]), _optional_integer(row[5]),
                 _optional_integer(row[6]), _optional_integer(row[7]),
                 cast(FinishReason | None, row[8]), row[9], row[10], row[11], participants)


class AccountRepository:
    """Account/player SQL owned by one active transaction session."""

    __slots__ = ("_session",)

    def __init__(self, session):
        self._session = session

    def create(self, account_id: int, account_name: str, password_digest: str) -> Account:
        """Create an active account; unique/FK violations remain database errors."""
        connection = self._session.connection()
        _integer(account_id, 1, _UINT64_MAX)
        _account_name(account_name)
        _password_digest(password_digest)
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO arena.account(account_id, account_name, password_digest) "
                "VALUES (%s, %s, %s) RETURNING " + _ACCOUNT_FIELDS,
                (account_id, account_name, password_digest))
            return _account(cursor.fetchone())

    def get(self, account_id: int) -> Account | None:
        """Read one account or return None without changing transaction ownership."""
        connection = self._session.connection()
        _integer(account_id, 1, _UINT64_MAX)
        with connection.cursor() as cursor:
            cursor.execute("SELECT " + _ACCOUNT_FIELDS + " FROM arena.account "
                           "WHERE account_id = %s", (account_id,))
            row = cursor.fetchone()
            return None if row is None else _account(row)

    def find_by_name(self, account_name: str) -> Account | None:
        """Look up the exact case-sensitive name using a bound value."""
        connection = self._session.connection()
        _account_name(account_name)
        with connection.cursor() as cursor:
            cursor.execute("SELECT " + _ACCOUNT_FIELDS + " FROM arena.account "
                           "WHERE account_name = %s", (account_name,))
            row = cursor.fetchone()
            return None if row is None else _account(row)

    def set_state(self, account_id: int, state: AccountState) -> Account:
        """Set the explicit account state and its database audit timestamp."""
        connection = self._session.connection()
        _integer(account_id, 1, _UINT64_MAX)
        if not isinstance(state, str) or state not in ("active", "disabled"):
            raise StorageError("invalid_request")
        with connection.cursor() as cursor:
            cursor.execute("UPDATE arena.account SET state = %s, "
                           "updated_at = pg_catalog.clock_timestamp() "
                           "WHERE account_id = %s RETURNING " + _ACCOUNT_FIELDS,
                           (state, account_id))
            row = cursor.fetchone()
            if row is None:
                raise StorageError("not_found")
            return _account(row)

    def create_player(self, player_id: int, account_id: int, display_name: str,
                      mmr: int = 1500) -> Player:
        """Create the account's one player in the caller's existing transaction."""
        connection = self._session.connection()
        _integer(player_id, 1, _UINT64_MAX)
        _integer(account_id, 1, _UINT64_MAX)
        _display_name(display_name)
        _integer(mmr, 0, _MMR_MAX)
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO arena.player "
                           "(player_id, account_id, display_name, mmr) VALUES (%s, %s, %s, %s) "
                           "RETURNING " + _PLAYER_FIELDS,
                           (player_id, account_id, display_name, mmr))
            return _player(cursor.fetchone())

    def get_player(self, player_id: int) -> Player | None:
        """Read a player without leaking its database cursor or connection."""
        connection = self._session.connection()
        _integer(player_id, 1, _UINT64_MAX)
        with connection.cursor() as cursor:
            cursor.execute("SELECT " + _PLAYER_FIELDS + " FROM arena.player "
                           "WHERE player_id = %s", (player_id,))
            row = cursor.fetchone()
            return None if row is None else _player(row)


class MatchRepository:
    """Match lifecycle SQL with one parent lock protecting each roster snapshot."""

    __slots__ = ("_session",)

    def __init__(self, session):
        self._session = session

    @staticmethod
    def _get_locked(cursor, match_id: int) -> Match | None:
        """Child writers must lock and version this same parent before mutation."""
        cursor.execute("SELECT " + _MATCH_FIELDS + " FROM arena.game_match "
                       "WHERE match_id = %s FOR UPDATE", (match_id,))
        row = cursor.fetchone()
        if row is None:
            return None
        cursor.execute("SELECT player_id, slot, session_id, membership_epoch, initial_mmr, "
                       "last_input_seq, final_x, final_y FROM arena.match_player "
                       "WHERE match_id = %s ORDER BY slot", (match_id,))
        participants = tuple(MatchParticipant(*(int(value) for value in member))
                             for member in cursor.fetchall())
        return _match(row, participants)

    def create(self, match_id: int, room_id: int,
               participants: tuple[MatchParticipant, ...], result_version: int = 1,
               *, created_at: datetime | None = None) -> Match:
        """Assemble Forming plus two members before exposing a complete result."""
        connection = self._session.connection()
        _integer(match_id, 1, _UINT64_MAX)
        _integer(room_id, 1, _UINT64_MAX)
        _integer(result_version, 1, _INT32_MAX)
        participants = _participants(participants)
        if created_at is not None:
            created_at = _timestamp(created_at)
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO arena.game_match "
                           "(match_id, room_id, result_version, created_at) "
                           "VALUES (%s, %s, %s, coalesce(%s::timestamptz, CURRENT_TIMESTAMP))",
                           (match_id, room_id, result_version, created_at))
            for member in participants:
                cursor.execute("INSERT INTO arena.match_player "
                               "(match_id, player_id, slot, session_id, membership_epoch, "
                               "initial_mmr, last_input_seq, final_x, final_y) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               (match_id, member.player_id, member.slot, member.session_id,
                                member.membership_epoch, member.initial_mmr,
                                member.last_input_seq, member.final_x, member.final_y))
            created = self._get_locked(cursor, match_id)
            if created is None:
                raise StorageError("not_found")
            return created

    def get(self, match_id: int) -> Match | None:
        """Return a coherent locked snapshot; a Forming roster may be partial."""
        connection = self._session.connection()
        _integer(match_id, 1, _UINT64_MAX)
        with connection.cursor() as cursor:
            return self._get_locked(cursor, match_id)

    def start(self, match_id: int, started_at: datetime) -> Match:
        """Start a Forming match and retain the database's roster/time guards."""
        connection = self._session.connection()
        _integer(match_id, 1, _UINT64_MAX)
        started_at = _timestamp(started_at)
        with connection.cursor() as cursor:
            current = self._get_locked(cursor, match_id)
            if current is None:
                raise StorageError("not_found")
            if current.state != "forming":
                raise StorageError("invalid_request")
            cursor.execute("UPDATE arena.game_match SET state = 'running', started_at = %s, "
                           "revision = revision + 1 WHERE match_id = %s RETURNING " + _MATCH_FIELDS,
                           (started_at, match_id))
            return _match(cursor.fetchone(), current.participants)

    def finish(self, match_id: int, finished_at: datetime, final_tick: int,
               final_checksum: int, winner_player_id: int | None = None) -> Match:
        """Complete a running match; None is a draw and checksum zero is valid."""
        connection = self._session.connection()
        _integer(match_id, 1, _UINT64_MAX)
        finished_at = _timestamp(finished_at)
        _integer(final_tick, 0, _UINT64_MAX)
        _integer(final_checksum, 0, _UINT64_MAX)
        if winner_player_id is not None:
            _integer(winner_player_id, 1, _UINT64_MAX)
        with connection.cursor() as cursor:
            current = self._get_locked(cursor, match_id)
            if current is None:
                raise StorageError("not_found")
            if (current.state != "running" or winner_player_id is not None
                    and winner_player_id not in (m.player_id for m in current.participants)):
                raise StorageError("invalid_request")
            cursor.execute("UPDATE arena.game_match SET state = 'completed', finished_at = %s, "
                           "final_tick = %s, final_checksum = %s, winner_player_id = %s, "
                           "finish_reason = 'completed', revision = revision + 1 "
                           "WHERE match_id = %s RETURNING " + _MATCH_FIELDS,
                           (finished_at, final_tick, final_checksum, winner_player_id, match_id))
            return _match(cursor.fetchone(), current.participants)

    def abort(self, match_id: int, finished_at: datetime, final_tick: int,
              final_checksum: int, reason: Literal["member_unavailable", "ready_timeout"]) -> Match:
        """Abort a Forming/running match without inventing a successful winner."""
        connection = self._session.connection()
        _integer(match_id, 1, _UINT64_MAX)
        finished_at = _timestamp(finished_at)
        _integer(final_tick, 0, _UINT64_MAX)
        _integer(final_checksum, 0, _UINT64_MAX)
        if not isinstance(reason, str) or reason not in ("member_unavailable", "ready_timeout"):
            raise StorageError("invalid_request")
        with connection.cursor() as cursor:
            current = self._get_locked(cursor, match_id)
            if current is None:
                raise StorageError("not_found")
            if (current.state not in ("forming", "running")
                    or reason == "ready_timeout" and current.state != "forming"):
                raise StorageError("invalid_request")
            cursor.execute("UPDATE arena.game_match SET state = 'aborted', finished_at = %s, "
                           "final_tick = %s, final_checksum = %s, winner_player_id = NULL, "
                           "finish_reason = %s, revision = revision + 1 "
                           "WHERE match_id = %s RETURNING " + _MATCH_FIELDS,
                           (finished_at, final_tick, final_checksum, reason, match_id))
            return _match(cursor.fetchone(), current.participants)
