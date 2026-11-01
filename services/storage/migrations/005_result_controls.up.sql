-- Preserve the sequence replay fence together with the last accepted control.
ALTER TABLE arena.match_player
    ADD COLUMN last_dx smallint NOT NULL DEFAULT 0,
    ADD COLUMN last_dy smallint NOT NULL DEFAULT 0,
    ADD CONSTRAINT match_player_control_ck CHECK (last_dx BETWEEN -1 AND 1 AND last_dy BETWEEN -1 AND 1);
