"""Atomic inventory transfers. SQL stays in this persistence module."""
from dataclasses import dataclass
from typing import Callable
from uuid import UUID
import psycopg

from .transactions import (RetryBudget, TransactionResult, StorageError as TransferError,
                           classify_sqlstate, run_transaction)

MAX_QUANTITY = (1 << 63) - 1
MAX_ENTITY_ID = (1 << 64) - 1


@dataclass(frozen=True)
class TransferRequest:
    """A stable command identity and exact, positive integral item quantity."""

    transfer_id: UUID
    source_player_id: int
    destination_player_id: int
    item_id: int
    quantity: int


@dataclass(frozen=True)
class TransferReceipt:
    """Confirmation of a committed transfer, including deduplicated successes."""

    transfer_id: UUID
    replayed: bool


def _validate(request):
    if not isinstance(request, TransferRequest) or not isinstance(request.transfer_id, UUID) or request.transfer_id.int == 0:
        raise TransferError("invalid_request")
    for value in (request.source_player_id, request.destination_player_id, request.item_id):
        if type(value) is not int or not 1 <= value <= MAX_ENTITY_ID:
            raise TransferError("invalid_request")
    if (type(request.quantity) is not int or not 1 <= request.quantity <= MAX_QUANTITY
            or request.source_player_id == request.destination_player_id):
        raise TransferError("invalid_request")


def _apply(connection, request):
    fields = (request.source_player_id, request.destination_player_id, request.item_id, request.quantity)
    # Reserve the successful command identity first. Any rejection rolls this back.
    try:
        inserted = connection.execute(
            "INSERT INTO arena.asset_transfer "
            "(transfer_id, source_player_id, destination_player_id, item_id, quantity) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (transfer_id) DO NOTHING RETURNING transfer_id",
            (request.transfer_id, *fields)).fetchone()
    except psycopg.errors.ForeignKeyViolation as error:
        if error.diag.constraint_name in ("asset_transfer_source_fk", "asset_transfer_destination_fk",
                                          "asset_transfer_item_fk"):
            raise TransferError("not_found", sqlstate="23503") from None
        raise
    if inserted is None:
        stored = connection.execute(
            "SELECT source_player_id, destination_player_id, item_id, quantity "
            "FROM arena.asset_transfer WHERE transfer_id=%s", (request.transfer_id,)).fetchone()
        if stored is None or tuple(stored) != fields:
            raise TransferError("idempotency_conflict")
        return TransferReceipt(request.transfer_id, True)

    balances = {}
    # Use one order for both missing-row insertion and row locking in either direction.
    for player in sorted((request.source_player_id, request.destination_player_id)):
        connection.execute("INSERT INTO arena.inventory (player_id,item_id) VALUES (%s,%s) "
                           "ON CONFLICT (player_id,item_id) DO NOTHING", (player, request.item_id))
        balances[player] = connection.execute(
            "SELECT quantity, revision FROM arena.inventory WHERE player_id=%s AND item_id=%s FOR UPDATE",
            (player, request.item_id)).fetchone()
    source, destination = balances[request.source_player_id], balances[request.destination_player_id]
    if source[0] < request.quantity:
        raise TransferError("insufficient_assets")
    if destination[0] > MAX_QUANTITY - request.quantity:
        raise TransferError("quantity_overflow")
    if source[1] == MAX_QUANTITY or destination[1] == MAX_QUANTITY:
        raise TransferError("revision_overflow")
    for player, delta in ((request.source_player_id, -request.quantity),
                          (request.destination_player_id, request.quantity)):
        connection.execute("UPDATE arena.inventory SET quantity=quantity+%s, revision=revision+1, "
                           "updated_at=clock_timestamp() WHERE player_id=%s AND item_id=%s",
                           (delta, player, request.item_id))
    return TransferReceipt(request.transfer_id, False)


def transfer(connect: Callable[[], psycopg.Connection], request: TransferRequest,
             budget: RetryBudget = RetryBudget(), **retry_options
             ) -> TransactionResult[TransferReceipt]:
    """Move one item atomically; successful identities are permanently deduplicated.

    Rejections have no durable receipt, so the same request may succeed after
    inventory changes. This primitive does not authorize the source player or
    publish events; the authenticated caller must supply that business boundary.
    """
    _validate(request)
    return run_transaction(connect, lambda connection: _apply(connection, request), budget, **retry_options)
