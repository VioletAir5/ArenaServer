-- PostgreSQL 14+. Run through migrate.py or psql --single-transaction.
-- Explicit application IDs preserve the existing uint64 identity contract.
CREATE SCHEMA arena;

-- No numeric typmod: numeric(20,0) rounds fractions before CHECK can reject them.
CREATE DOMAIN arena.uint64 AS numeric
    CONSTRAINT uint64_range CHECK (
        VALUE >= 0 AND VALUE <= 18446744073709551615 AND VALUE = trunc(VALUE)
    );
CREATE DOMAIN arena.entity_id AS arena.uint64
    CONSTRAINT entity_id_positive CHECK (VALUE > 0);

CREATE TABLE arena.account (
    account_id arena.entity_id NOT NULL,
    account_name text COLLATE "C" NOT NULL,
    password_digest text NOT NULL,
    state text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT account_pk PRIMARY KEY (account_id),
    CONSTRAINT account_name_uq UNIQUE (account_name),
    -- Preserve FilePasswordAccounts' printable ASCII, case-sensitive names.
    CONSTRAINT account_name_ck CHECK (
        octet_length(account_name) BETWEEN 1 AND 128 AND account_name ~ '^[!-~]+$'
    ),
    CONSTRAINT account_digest_ck CHECK (
        CASE WHEN password_digest ~
            '^\$arena-pbkdf2-sha256\$[1-9][0-9]{5,6}\$[0-9a-f]{32}\$[0-9a-f]{64}$'
        THEN split_part(password_digest, '$', 3)::integer BETWEEN 100000 AND 2000000
        ELSE false END
    ),
    CONSTRAINT account_state_ck CHECK (state IN ('active', 'disabled')),
    CONSTRAINT account_time_ck CHECK (
        isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at
    )
);

CREATE TABLE arena.player (
    player_id arena.entity_id NOT NULL,
    account_id arena.entity_id NOT NULL,
    display_name text NOT NULL,
    state text NOT NULL DEFAULT 'active',
    mmr bigint NOT NULL DEFAULT 1500,
    revision bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT player_pk PRIMARY KEY (player_id),
    CONSTRAINT player_account_fk FOREIGN KEY (account_id) REFERENCES arena.account (account_id),
    CONSTRAINT player_account_uq UNIQUE (account_id),
    CONSTRAINT player_name_ck CHECK (
        octet_length(display_name) BETWEEN 1 AND 128 AND btrim(display_name) <> ''
        AND display_name !~ '[[:cntrl:]]'
    ),
    CONSTRAINT player_state_ck CHECK (state IN ('active', 'suspended', 'retired')),
    CONSTRAINT player_mmr_ck CHECK (mmr BETWEEN 0 AND 10000),
    CONSTRAINT player_revision_ck CHECK (revision >= 0),
    CONSTRAINT player_time_ck CHECK (
        isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at
    )
);

CREATE TABLE arena.item (
    item_id arena.entity_id NOT NULL,
    item_code text COLLATE "C" NOT NULL,
    display_name text NOT NULL,
    CONSTRAINT item_pk PRIMARY KEY (item_id),
    CONSTRAINT item_code_uq UNIQUE (item_code),
    CONSTRAINT item_code_ck CHECK (
        octet_length(item_code) BETWEEN 1 AND 64 AND item_code ~ '^[a-z][a-z0-9_.-]*$'
    ),
    CONSTRAINT item_name_ck CHECK (
        octet_length(display_name) BETWEEN 1 AND 128 AND btrim(display_name) <> ''
        AND display_name !~ '[[:cntrl:]]'
    )
);

CREATE TABLE arena.inventory (
    player_id arena.entity_id NOT NULL,
    item_id arena.entity_id NOT NULL,
    quantity bigint NOT NULL DEFAULT 0,
    revision bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inventory_pk PRIMARY KEY (player_id, item_id),
    CONSTRAINT inventory_player_fk FOREIGN KEY (player_id) REFERENCES arena.player (player_id),
    CONSTRAINT inventory_item_fk FOREIGN KEY (item_id) REFERENCES arena.item (item_id),
    CONSTRAINT inventory_quantity_ck CHECK (quantity >= 0),
    CONSTRAINT inventory_revision_ck CHECK (revision >= 0),
    CONSTRAINT inventory_time_ck CHECK (isfinite(updated_at))
);
CREATE INDEX inventory_item_idx ON arena.inventory (item_id);

-- One authoritative battle record, not a queue ticket or a permanent room.
CREATE TABLE arena.game_match (
    match_id arena.entity_id NOT NULL,
    room_id arena.entity_id NOT NULL,
    state text NOT NULL DEFAULT 'forming',
    result_version integer NOT NULL DEFAULT 1,
    revision bigint NOT NULL DEFAULT 0,
    winner_player_id arena.entity_id,
    final_tick arena.uint64,
    final_checksum arena.uint64,
    finish_reason text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at timestamptz,
    finished_at timestamptz,
    CONSTRAINT game_match_pk PRIMARY KEY (match_id),
    -- This key lets a settlement reference a terminal state declaratively.
    CONSTRAINT game_match_state_uq UNIQUE (match_id, state),
    CONSTRAINT game_match_version_ck CHECK (result_version > 0 AND revision >= 0),
    CONSTRAINT game_match_state_ck CHECK (
        (state = 'forming' AND started_at IS NULL AND finished_at IS NULL
            AND final_tick IS NULL AND final_checksum IS NULL
            AND finish_reason IS NULL AND winner_player_id IS NULL)
        OR (state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL
            AND final_tick IS NULL AND final_checksum IS NULL
            AND finish_reason IS NULL AND winner_player_id IS NULL)
        OR (state = 'completed' AND started_at IS NOT NULL AND finished_at IS NOT NULL
            AND final_tick IS NOT NULL AND final_checksum IS NOT NULL
            AND finish_reason IS NOT NULL AND finish_reason = 'completed')
        OR (state = 'aborted' AND finished_at IS NOT NULL
            AND final_tick IS NOT NULL AND final_checksum IS NOT NULL
            AND finish_reason IS NOT NULL
            AND finish_reason IN ('member_unavailable', 'ready_timeout')
            AND (finish_reason <> 'ready_timeout' OR started_at IS NULL)
            AND winner_player_id IS NULL)
    ),
    CONSTRAINT game_match_time_ck CHECK (
        isfinite(created_at)
        AND (started_at IS NULL OR (isfinite(started_at) AND started_at >= created_at))
        AND (finished_at IS NULL OR (isfinite(finished_at)
            AND finished_at >= coalesce(started_at, created_at)))
    )
);
CREATE INDEX game_match_room_idx ON arena.game_match (room_id, created_at);
CREATE INDEX game_match_active_idx ON arena.game_match (created_at, match_id)
    WHERE state IN ('forming', 'running');

CREATE TABLE arena.match_player (
    match_id arena.entity_id NOT NULL,
    player_id arena.entity_id NOT NULL,
    slot smallint NOT NULL,
    session_id arena.entity_id NOT NULL,
    membership_epoch arena.entity_id NOT NULL,
    initial_mmr bigint NOT NULL,
    last_input_seq arena.uint64 NOT NULL DEFAULT 0,
    final_x integer NOT NULL DEFAULT 0,
    final_y integer NOT NULL DEFAULT 0,
    CONSTRAINT match_player_pk PRIMARY KEY (match_id, player_id),
    CONSTRAINT match_player_slot_uq UNIQUE (match_id, slot),
    CONSTRAINT match_player_match_fk FOREIGN KEY (match_id) REFERENCES arena.game_match (match_id),
    CONSTRAINT match_player_player_fk FOREIGN KEY (player_id) REFERENCES arena.player (player_id),
    CONSTRAINT match_player_slot_ck CHECK (slot IN (1, 2)),
    CONSTRAINT match_player_mmr_ck CHECK (initial_mmr BETWEEN 0 AND 10000)
);
CREATE INDEX match_player_player_idx ON arena.match_player (player_id, match_id);
ALTER TABLE arena.game_match ADD CONSTRAINT game_match_winner_fk
    FOREIGN KEY (match_id, winner_player_id)
    REFERENCES arena.match_player (match_id, player_id);

CREATE TABLE arena.settlement (
    settlement_id arena.entity_id NOT NULL,
    match_id arena.entity_id NOT NULL,
    match_state text NOT NULL,
    idempotency_key uuid NOT NULL,
    request_hash bytea NOT NULL,
    result_version integer NOT NULL DEFAULT 1,
    result_payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempt_at timestamptz,
    next_attempt_at timestamptz,
    committed_at timestamptz,
    failed_at timestamptz,
    CONSTRAINT settlement_pk PRIMARY KEY (settlement_id),
    CONSTRAINT settlement_match_uq UNIQUE (match_id),
    CONSTRAINT settlement_key_uq UNIQUE (idempotency_key),
    CONSTRAINT settlement_state_uq UNIQUE (settlement_id, state),
    CONSTRAINT settlement_match_fk FOREIGN KEY (match_id, match_state)
        REFERENCES arena.game_match (match_id, state),
    CONSTRAINT settlement_terminal_match_ck CHECK (match_state IN ('completed', 'aborted')),
    CONSTRAINT settlement_key_ck CHECK (
        idempotency_key <> '00000000-0000-0000-0000-000000000000'::uuid
    ),
    CONSTRAINT settlement_payload_ck CHECK (
        octet_length(request_hash) = 32 AND result_version > 0
        AND jsonb_typeof(result_payload) = 'object'
        AND octet_length(result_payload::text) <= 65536
    ),
    CONSTRAINT settlement_error_ck CHECK (
        last_error_code IS NULL OR (
            octet_length(last_error_code) BETWEEN 1 AND 64
            AND last_error_code ~ '^[A-Z][A-Z0-9_]*$')
    ),
    CONSTRAINT settlement_state_ck CHECK (
        (state = 'pending' AND attempt_count = 0 AND last_attempt_at IS NULL
            AND next_attempt_at IS NULL AND committed_at IS NULL
            AND failed_at IS NULL AND last_error_code IS NULL)
        OR (state = 'applying' AND attempt_count > 0 AND last_attempt_at IS NOT NULL
            AND next_attempt_at IS NULL AND committed_at IS NULL
            AND failed_at IS NULL AND last_error_code IS NULL)
        OR (state = 'retry_wait' AND attempt_count > 0 AND last_attempt_at IS NOT NULL
            AND next_attempt_at IS NOT NULL AND committed_at IS NULL
            AND failed_at IS NULL AND last_error_code IS NOT NULL)
        OR (state = 'committed' AND attempt_count > 0 AND last_attempt_at IS NOT NULL
            AND next_attempt_at IS NULL AND committed_at IS NOT NULL
            AND failed_at IS NULL AND last_error_code IS NULL)
        OR (state = 'failed' AND attempt_count > 0 AND last_attempt_at IS NOT NULL
            AND next_attempt_at IS NULL AND committed_at IS NULL
            AND failed_at IS NOT NULL AND last_error_code IS NOT NULL)
    ),
    CONSTRAINT settlement_time_ck CHECK (
        isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at
        AND (last_attempt_at IS NULL OR (isfinite(last_attempt_at)
            AND last_attempt_at BETWEEN created_at AND updated_at))
        AND (next_attempt_at IS NULL OR (isfinite(next_attempt_at)
            AND next_attempt_at >= last_attempt_at))
        AND (committed_at IS NULL OR (isfinite(committed_at)
            AND committed_at BETWEEN last_attempt_at AND updated_at))
        AND (failed_at IS NULL OR (isfinite(failed_at)
            AND failed_at BETWEEN last_attempt_at AND updated_at))
    )
);
CREATE INDEX settlement_retry_idx ON arena.settlement (next_attempt_at, settlement_id)
    WHERE state = 'retry_wait';

CREATE TABLE arena.outbox (
    event_id uuid NOT NULL,
    settlement_id arena.entity_id NOT NULL,
    -- A constant FK component forbids events for uncommitted settlements.
    settlement_state text NOT NULL DEFAULT 'committed',
    event_type text NOT NULL DEFAULT 'settlement.committed',
    schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempt_at timestamptz,
    published_at timestamptz,
    CONSTRAINT outbox_pk PRIMARY KEY (event_id),
    CONSTRAINT outbox_source_uq UNIQUE (settlement_id, event_type),
    CONSTRAINT outbox_settlement_fk FOREIGN KEY (settlement_id, settlement_state)
        REFERENCES arena.settlement (settlement_id, state),
    CONSTRAINT outbox_id_ck CHECK (
        event_id <> '00000000-0000-0000-0000-000000000000'::uuid
    ),
    CONSTRAINT outbox_type_ck CHECK (
        settlement_state = 'committed' AND event_type = 'settlement.committed'
        AND schema_version > 0
    ),
    CONSTRAINT outbox_payload_ck CHECK (
        jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 65536
    ),
    CONSTRAINT outbox_state_ck CHECK (
        (state = 'pending' AND attempt_count >= 0 AND published_at IS NULL
            AND ((attempt_count = 0 AND last_attempt_at IS NULL)
                OR (attempt_count > 0 AND last_attempt_at IS NOT NULL)))
        OR (state = 'published' AND attempt_count > 0
            AND last_attempt_at IS NOT NULL AND published_at IS NOT NULL)
    ),
    CONSTRAINT outbox_time_ck CHECK (
        isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at
        AND isfinite(available_at) AND available_at >= created_at
        AND (last_attempt_at IS NULL OR (isfinite(last_attempt_at)
            AND last_attempt_at BETWEEN created_at AND updated_at))
        AND (published_at IS NULL OR (isfinite(published_at)
            AND published_at BETWEEN last_attempt_at AND updated_at AND published_at >= available_at))
    )
);
CREATE INDEX outbox_pending_idx ON arena.outbox (available_at, event_id)
    WHERE state = 'pending';

-- Child changes lock the same parent row that starting/ending a match updates.
-- This serializes roster edits with state changes, rather than trusting COUNT in CHECK.
CREATE FUNCTION arena.guard_match_player() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
DECLARE
    target arena.entity_id;
    parent_state text;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.match_id <> OLD.match_id THEN
        RAISE EXCEPTION 'match identity is immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'match_player_identity_ck';
    END IF;
    IF TG_OP = 'DELETE' THEN target := OLD.match_id; ELSE target := NEW.match_id; END IF;
    SELECT state INTO parent_state FROM arena.game_match WHERE match_id = target FOR UPDATE;
    IF FOUND AND parent_state <> 'forming' THEN
        IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'active match roster is immutable'
                USING ERRCODE = '23514', CONSTRAINT = 'match_player_roster_ck';
        END IF;
        IF ROW(NEW.player_id, NEW.slot, NEW.session_id, NEW.membership_epoch, NEW.initial_mmr)
            IS DISTINCT FROM ROW(OLD.player_id, OLD.slot, OLD.session_id, OLD.membership_epoch, OLD.initial_mmr)
            OR (parent_state IN ('completed', 'aborted') AND NEW IS DISTINCT FROM OLD) THEN
            RAISE EXCEPTION 'active membership or final state is immutable'
                USING ERRCODE = '23514', CONSTRAINT = 'match_player_roster_ck';
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND parent_state = 'running' AND NEW.last_input_seq < OLD.last_input_seq THEN
        RAISE EXCEPTION 'input sequence cannot decrease during a battle'
            USING ERRCODE = '23514', CONSTRAINT = 'match_player_sequence_ck';
    END IF;
    -- A lock alone does not invalidate a REPEATABLE READ snapshot. Publish a
    -- new parent MVCC version so an old roster cannot authorize starting a match.
    IF FOUND AND (TG_OP <> 'UPDATE' OR NEW IS DISTINCT FROM OLD) THEN
        UPDATE arena.game_match SET revision = revision + 1 WHERE match_id = target;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$guard$;
CREATE TRIGGER match_player_guard BEFORE INSERT OR UPDATE OR DELETE
    ON arena.match_player FOR EACH ROW EXECUTE FUNCTION arena.guard_match_player();

CREATE FUNCTION arena.guard_game_match() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.state = 'running' AND NEW.started_at IS DISTINCT FROM OLD.started_at)
            OR (OLD.state = 'forming' AND NEW.state = 'aborted' AND NEW.started_at IS NOT NULL) THEN
            RAISE EXCEPTION 'battle start time is fixed by its start transition'
                USING ERRCODE = '23514', CONSTRAINT = 'game_match_start_ck';
        END IF;
        IF NEW.revision < OLD.revision THEN
            RAISE EXCEPTION 'match revision cannot decrease'
                USING ERRCODE = '23514', CONSTRAINT = 'game_match_revision_ck';
        END IF;
        IF OLD.state <> NEW.state AND NOT (
            (OLD.state = 'forming' AND NEW.state IN ('running', 'aborted')) OR
            (OLD.state = 'running' AND NEW.state IN ('completed', 'aborted'))) THEN
            RAISE EXCEPTION 'invalid match state transition'
                USING ERRCODE = '23514', CONSTRAINT = 'game_match_transition_ck';
        END IF;
        IF ROW(NEW.match_id, NEW.room_id, NEW.created_at, NEW.result_version)
            IS DISTINCT FROM ROW(OLD.match_id, OLD.room_id, OLD.created_at, OLD.result_version) OR
            (OLD.state IN ('completed', 'aborted') AND NEW IS DISTINCT FROM OLD) THEN
            RAISE EXCEPTION 'match identity and final result are immutable'
                USING ERRCODE = '23514', CONSTRAINT = 'game_match_immutable_ck';
        END IF;
    END IF;
    IF NEW.state <> 'forming' AND
        (SELECT count(*) FROM arena.match_player WHERE match_id = NEW.match_id) <> 2 THEN
        RAISE EXCEPTION 'a started or terminal match requires exactly two players'
            USING ERRCODE = '23514', CONSTRAINT = 'game_match_roster_ck';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER game_match_guard BEFORE INSERT OR UPDATE
    ON arena.game_match FOR EACH ROW EXECUTE FUNCTION arena.guard_game_match();

-- Persistent retries retain the same identity and payload, as the domain FSM requires.
CREATE FUNCTION arena.guard_settlement() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state IN ('committed', 'failed') THEN
            RAISE EXCEPTION 'terminal settlements cannot be deleted'
                USING ERRCODE = '23514', CONSTRAINT = 'settlement_immutable_ck';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending' THEN
            RAISE EXCEPTION 'settlements start pending'
                USING ERRCODE = '23514', CONSTRAINT = 'settlement_initial_ck';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.settlement_id, NEW.match_id, NEW.match_state, NEW.idempotency_key,
           NEW.request_hash, NEW.result_version, NEW.result_payload, NEW.created_at)
        IS DISTINCT FROM ROW(OLD.settlement_id, OLD.match_id, OLD.match_state, OLD.idempotency_key,
           OLD.request_hash, OLD.result_version, OLD.result_payload, OLD.created_at)
        OR (OLD.state IN ('committed', 'failed') AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'settlement identity, request and terminal result are immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'settlement_immutable_ck';
    END IF;
    IF OLD.state <> NEW.state AND NOT (
        (OLD.state = 'pending' AND NEW.state = 'applying') OR
        (OLD.state = 'applying' AND NEW.state IN ('retry_wait', 'committed', 'failed')) OR
        (OLD.state = 'retry_wait' AND NEW.state IN ('applying', 'failed'))) THEN
        RAISE EXCEPTION 'invalid settlement state transition'
            USING ERRCODE = '23514', CONSTRAINT = 'settlement_transition_ck';
    END IF;
    IF OLD.state = 'retry_wait' AND NEW.state = 'applying'
        AND NEW.last_attempt_at < OLD.next_attempt_at THEN
        RAISE EXCEPTION 'retry attempt precedes its deadline'
            USING ERRCODE = '23514', CONSTRAINT = 'settlement_retry_due_ck';
    END IF;
    IF NEW.attempt_count::bigint <> OLD.attempt_count::bigint +
        (CASE WHEN NEW.state = 'applying' AND OLD.state <> 'applying' THEN 1 ELSE 0 END)
        OR (NEW.last_attempt_at IS DISTINCT FROM OLD.last_attempt_at AND NOT (
            NEW.state = 'applying' AND OLD.state <> 'applying')) THEN
        RAISE EXCEPTION 'attempt count changes only when an application begins'
            USING ERRCODE = '23514', CONSTRAINT = 'settlement_attempt_ck';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER settlement_guard BEFORE INSERT OR UPDATE OR DELETE
    ON arena.settlement FOR EACH ROW EXECUTE FUNCTION arena.guard_settlement();

CREATE FUNCTION arena.guard_outbox() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'committed event identities cannot be deleted'
            USING ERRCODE = '23514', CONSTRAINT = 'outbox_immutable_ck';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending' OR NEW.attempt_count <> 0 THEN
            RAISE EXCEPTION 'outbox events start pending and unattempted'
                USING ERRCODE = '23514', CONSTRAINT = 'outbox_initial_ck';
        END IF;
        RETURN NEW;
    END IF;
    IF ROW(NEW.event_id, NEW.settlement_id, NEW.settlement_state, NEW.event_type,
           NEW.schema_version, NEW.payload, NEW.created_at)
        IS DISTINCT FROM ROW(OLD.event_id, OLD.settlement_id, OLD.settlement_state, OLD.event_type,
           OLD.schema_version, OLD.payload, OLD.created_at)
        OR (OLD.state = 'published' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'event identity, payload and published result are immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'outbox_immutable_ck';
    END IF;
    IF NEW.attempt_count::bigint NOT BETWEEN OLD.attempt_count::bigint AND OLD.attempt_count::bigint + 1
        OR (NEW.last_attempt_at IS DISTINCT FROM OLD.last_attempt_at AND
            NEW.attempt_count = OLD.attempt_count)
        OR (NEW.attempt_count > OLD.attempt_count AND NEW.last_attempt_at IS NULL) THEN
        RAISE EXCEPTION 'one update records at most one delivery attempt'
            USING ERRCODE = '23514', CONSTRAINT = 'outbox_attempt_ck';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER outbox_guard BEFORE INSERT OR UPDATE OR DELETE
    ON arena.outbox FOR EACH ROW EXECUTE FUNCTION arena.guard_outbox();

COMMENT ON SCHEMA arena IS 'ArenaServer durable model; runtime adapters are not connected yet.';
COMMENT ON TABLE arena.game_match IS 'One authoritative battle; IDs must be unique across process restarts.';
COMMENT ON COLUMN arena.game_match.winner_player_id IS 'NULL is a draw or aborted result; wire winner zero maps to NULL.';
COMMENT ON TABLE arena.settlement IS 'One settlement per terminal match; retries reuse identity and request_hash.';
COMMENT ON TABLE arena.outbox IS 'One committed-settlement event; publisher and consumer deduplication are not implemented here.';
