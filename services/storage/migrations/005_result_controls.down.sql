ALTER TABLE arena.match_player
    DROP CONSTRAINT match_player_control_ck,
    DROP COLUMN last_dy,
    DROP COLUMN last_dx;
