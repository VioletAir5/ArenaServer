"""Durable ID block reservation owned by the storage transaction."""
from .repositories import _integer
from .transactions import StorageError


class IdRepository:
    """Reserve disjoint positive signed-63-bit match ID ranges atomically."""
    def __init__(self, session):
        self._session = session

    def reserve(self, count):
        connection = self._session.connection()
        _integer(count, 1, 1024)
        with connection.cursor() as cursor:
            cursor.execute("SELECT next_id FROM arena.runtime_ids WHERE namespace='match' FOR UPDATE")
            row = cursor.fetchone()
            if row is None:
                raise StorageError("id_exhausted")
            cursor.execute("SELECT coalesce(max(match_id),0)+1 FROM arena.game_match "
                           "WHERE match_id<9223372036854775808")
            first = max(int(row[0]), int(cursor.fetchone()[0]))
            if first > (1 << 63)-count:
                raise StorageError("id_exhausted")
            cursor.execute("UPDATE arena.runtime_ids SET next_id=%s WHERE namespace='match'",
                           (first+count,))
            return first, first+count-1
