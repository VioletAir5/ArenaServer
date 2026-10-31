-- Add durable reward receipts, consumer deduplication and publisher leases.
CREATE TABLE arena.asset_ledger (
    settlement_id arena.entity_id NOT NULL,
    settlement_state text NOT NULL DEFAULT 'committed' CHECK (settlement_state = 'committed'),
    player_id arena.entity_id NOT NULL,
    item_id arena.entity_id NOT NULL,
    quantity bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT asset_ledger_pk PRIMARY KEY (settlement_id, player_id, item_id),
    CONSTRAINT asset_ledger_settlement_fk FOREIGN KEY (settlement_id, settlement_state)
        REFERENCES arena.settlement (settlement_id, state) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT asset_ledger_player_fk FOREIGN KEY (player_id) REFERENCES arena.player (player_id),
    CONSTRAINT asset_ledger_item_fk FOREIGN KEY (item_id) REFERENCES arena.item (item_id),
    CONSTRAINT asset_ledger_quantity_ck CHECK (quantity > 0),
    CONSTRAINT asset_ledger_time_ck CHECK (isfinite(created_at))
);
CREATE FUNCTION arena.guard_reward_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
BEGIN
    RAISE EXCEPTION 'reward and consumption receipts are immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'reward_receipt_immutable_ck';
END;
$guard$;
CREATE TRIGGER asset_ledger_guard BEFORE UPDATE OR DELETE ON arena.asset_ledger
    FOR EACH ROW EXECUTE FUNCTION arena.guard_reward_receipt();
CREATE TABLE arena.outbox_delivery (
    event_id uuid NOT NULL PRIMARY KEY REFERENCES arena.outbox (event_id),
    lease_token uuid,
    attempt_base integer NOT NULL DEFAULT 0 CHECK (attempt_base >= 0),
    leased_until timestamptz,
    dead_at timestamptz,
    last_error_code text,
    CONSTRAINT outbox_delivery_lease_ck CHECK (
        (lease_token IS NULL) = (leased_until IS NULL)
        AND (leased_until IS NULL OR isfinite(leased_until))
        AND (dead_at IS NULL OR (isfinite(dead_at) AND lease_token IS NULL))
    ),
    CONSTRAINT outbox_delivery_error_ck CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    )
);
CREATE TABLE arena.consumer_receipt (
    consumer text COLLATE "C" NOT NULL,
    event_id uuid NOT NULL REFERENCES arena.outbox (event_id),
    payload_hash bytea NOT NULL,
    consumed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT consumer_receipt_pk PRIMARY KEY (consumer, event_id),
    CONSTRAINT consumer_receipt_name_ck CHECK (consumer ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    CONSTRAINT consumer_receipt_hash_ck CHECK (octet_length(payload_hash) = 32),
    CONSTRAINT consumer_receipt_time_ck CHECK (isfinite(consumed_at))
);
CREATE TRIGGER consumer_receipt_guard BEFORE UPDATE OR DELETE ON arena.consumer_receipt
    FOR EACH ROW EXECUTE FUNCTION arena.guard_reward_receipt();
-- Inventory is credited by settlement. Consumers only project rewards.
CREATE TABLE arena.settlement_reward_projection (
    consumer text COLLATE "C" NOT NULL,
    event_id uuid NOT NULL,
    player_id arena.entity_id NOT NULL REFERENCES arena.player (player_id),
    item_id arena.entity_id NOT NULL REFERENCES arena.item (item_id),
    quantity bigint NOT NULL CHECK (quantity > 0),
    CONSTRAINT settlement_reward_projection_pk PRIMARY KEY (consumer, event_id, player_id, item_id),
    CONSTRAINT settlement_reward_projection_receipt_fk FOREIGN KEY (consumer, event_id)
        REFERENCES arena.consumer_receipt (consumer, event_id)
);
CREATE TRIGGER settlement_reward_projection_guard BEFORE UPDATE OR DELETE
    ON arena.settlement_reward_projection FOR EACH ROW
    EXECUTE FUNCTION arena.guard_reward_receipt();
COMMENT ON TABLE arena.asset_ledger IS 'Immutable settlement credits; inventory and outbox commit in the same transaction.';
COMMENT ON TABLE arena.outbox_delivery IS 'Fenced publisher leases and explicit dead-letter metadata.';
COMMENT ON TABLE arena.consumer_receipt IS 'Event ID deduplication committed with the downstream projection.';
