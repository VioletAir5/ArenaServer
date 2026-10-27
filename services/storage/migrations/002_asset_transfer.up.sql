-- PostgreSQL 14+. The caller owns the transaction and supplies stable UUIDs.
CREATE TABLE arena.asset_transfer (
    transfer_id uuid NOT NULL,
    source_player_id arena.entity_id NOT NULL,
    destination_player_id arena.entity_id NOT NULL,
    item_id arena.entity_id NOT NULL,
    quantity bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT asset_transfer_pk PRIMARY KEY (transfer_id),
    CONSTRAINT asset_transfer_source_fk FOREIGN KEY (source_player_id)
        REFERENCES arena.player (player_id),
    CONSTRAINT asset_transfer_destination_fk FOREIGN KEY (destination_player_id)
        REFERENCES arena.player (player_id),
    CONSTRAINT asset_transfer_item_fk FOREIGN KEY (item_id) REFERENCES arena.item (item_id),
    CONSTRAINT asset_transfer_id_ck CHECK (transfer_id <> '00000000-0000-0000-0000-000000000000'),
    CONSTRAINT asset_transfer_players_ck CHECK (source_player_id <> destination_player_id),
    CONSTRAINT asset_transfer_quantity_ck CHECK (quantity > 0),
    CONSTRAINT asset_transfer_time_ck CHECK (isfinite(created_at))
);
CREATE INDEX asset_transfer_source_idx ON arena.asset_transfer (source_player_id, created_at);
CREATE INDEX asset_transfer_destination_idx ON arena.asset_transfer (destination_player_id, created_at);
CREATE INDEX asset_transfer_item_idx ON arena.asset_transfer (item_id);

CREATE FUNCTION arena.guard_asset_transfer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Never permit deletion/reinsertion to reuse a successful command identity.
    RAISE EXCEPTION USING ERRCODE = '23514', CONSTRAINT = 'asset_transfer_immutable',
        MESSAGE = 'asset transfer receipts are immutable';
END;
$$;
CREATE TRIGGER asset_transfer_immutable BEFORE UPDATE OR DELETE ON arena.asset_transfer
    FOR EACH ROW EXECUTE FUNCTION arena.guard_asset_transfer();
