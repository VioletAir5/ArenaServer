-- Destructive rollback: explicit dependency order, no CASCADE.
DROP TABLE arena.outbox;
DROP TABLE arena.settlement;
ALTER TABLE arena.game_match DROP CONSTRAINT game_match_winner_fk;
DROP TABLE arena.match_player;
DROP TABLE arena.game_match;
DROP TABLE arena.inventory;
DROP TABLE arena.item;
DROP TABLE arena.player;
DROP TABLE arena.account;
DROP FUNCTION arena.guard_outbox();
DROP FUNCTION arena.guard_settlement();
DROP FUNCTION arena.guard_game_match();
DROP FUNCTION arena.guard_match_player();
DROP DOMAIN arena.entity_id;
DROP DOMAIN arena.uint64;
DROP SCHEMA arena;
