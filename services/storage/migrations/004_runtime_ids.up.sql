-- Durable match IDs survive storage and gateway process restarts.
CREATE TABLE arena.runtime_ids (
    namespace text COLLATE "C" PRIMARY KEY,
    next_id numeric NOT NULL,
    CONSTRAINT runtime_ids_namespace_ck CHECK (namespace = 'match'),
    CONSTRAINT runtime_ids_next_ck CHECK (
        next_id = trunc(next_id) AND next_id BETWEEN 1 AND 9223372036854775808
    )
);
INSERT INTO arena.runtime_ids(namespace, next_id)
SELECT 'match', coalesce(max(match_id), 0) + 1
FROM arena.game_match WHERE match_id < 9223372036854775808;
CREATE FUNCTION arena.guard_runtime_ids() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, arena AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime identity allocation cannot be reset' USING ERRCODE = '23514';
    END IF;
    IF NEW.namespace <> OLD.namespace OR NEW.next_id <= OLD.next_id THEN
        RAISE EXCEPTION 'runtime identities must advance' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$guard$;
CREATE TRIGGER runtime_ids_guard BEFORE UPDATE OR DELETE ON arena.runtime_ids
    FOR EACH ROW EXECUTE FUNCTION arena.guard_runtime_ids();
COMMENT ON TABLE arena.runtime_ids IS 'Transactional block reservation; gaps are allowed, reuse is not.';
