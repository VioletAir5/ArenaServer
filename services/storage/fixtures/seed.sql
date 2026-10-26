-- Deterministic development/test fixture. Run after migration 001 with
-- psql -X --set=ON_ERROR_STOP=1 --single-transaction --file=seed.sql.
-- Deliberately non-idempotent: replay must fail rather than overwrite data.
-- Digests satisfy the storage format only; they are not real login credentials.
INSERT INTO arena.account (account_id, account_name, password_digest, state, created_at, updated_at) VALUES
(1, 'alice', '$arena-pbkdf2-sha256$100000$00000000000000000000000000000000$1111111111111111111111111111111111111111111111111111111111111111', 'active', '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(2, 'bob', '$arena-pbkdf2-sha256$100000$00000000000000000000000000000000$1111111111111111111111111111111111111111111111111111111111111111', 'active', '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(3, 'Alice', '$arena-pbkdf2-sha256$100000$00000000000000000000000000000000$1111111111111111111111111111111111111111111111111111111111111111', 'active', '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(4, 'disabled', '$arena-pbkdf2-sha256$100000$00000000000000000000000000000000$1111111111111111111111111111111111111111111111111111111111111111', 'disabled', '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(18446744073709551615, 'uint64_max', '$arena-pbkdf2-sha256$100000$00000000000000000000000000000000$1111111111111111111111111111111111111111111111111111111111111111', 'active', '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00');

INSERT INTO arena.player (player_id, account_id, display_name, mmr, created_at, updated_at) VALUES
(42, 1, 'Alice', 1000, '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(43, 2, 'Bob', 1200, '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(44, 3, 'Case-sensitive account', 3000, '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00'),
(18446744073709551615, 18446744073709551615, 'Maximum uint64', 10000, '2026-10-26 00:00:00+00', '2026-10-26 00:00:00+00');
INSERT INTO arena.item (item_id, item_code, display_name) VALUES
(1, 'gold', 'Gold'), (2, 'potion', 'Potion'),
(18446744073709551615, 'maximum_id', 'Maximum item');
INSERT INTO arena.inventory (player_id, item_id, quantity) VALUES
(42, 1, 100), (43, 1, 250), (42, 2, 3), (44, 1, 0),
(18446744073709551615, 18446744073709551615, 0);

-- The roster is assembled while Forming, then finalized through legal edges.
INSERT INTO arena.game_match (match_id, room_id, created_at) VALUES
(1001, 9223372036854776809, '2026-10-26 00:00:00+00'),
(1002, 9223372036854776810, '2026-10-26 00:00:00+00'),
(1003, 9223372036854776811, '2026-10-26 00:00:00+00'),
(1004, 9223372036854776812, '2026-10-26 00:00:00+00'),
(1005, 9223372036854776813, '2026-10-26 00:00:00+00'),
(1006, 9223372036854776814, '2026-10-26 00:00:00+00');
INSERT INTO arena.match_player
(match_id, player_id, slot, session_id, membership_epoch, initial_mmr, last_input_seq, final_x, final_y) VALUES
(1001,42,1,501,1,1000,18446744073709551615,2,-1),
(1001,43,2,502,1,1200,2,-1,2),
(1002,42,1,501,2,1000,0,0,0),
(1002,44,2,503,1,3000,0,0,0),
(1003,42,1,501,3,1000,1,1,0),
(1003,43,2,502,2,1200,1,0,1),
(1004,42,1,501,4,1000,0,0,0),
(1005,42,1,501,5,1000,3,3,0),
(1005,44,2,503,2,3000,3,0,3),
(1006,43,1,502,3,1200,1,1,0),
(1006,44,2,503,3,3000,0,0,0);
UPDATE arena.game_match SET state='running', started_at='2026-10-26 00:00:01+00'
WHERE match_id IN (1001,1003,1005,1006);
UPDATE arena.game_match SET state='completed', finished_at='2026-10-26 00:00:05+00',
    final_tick=100, final_checksum=18446744073709551615, finish_reason='completed'
WHERE match_id=1001;
UPDATE arena.game_match SET state='completed', finished_at='2026-10-26 00:00:05+00',
    final_tick=100, final_checksum=12345, finish_reason='completed', winner_player_id=42
WHERE match_id=1005;
UPDATE arena.game_match SET state='aborted', finished_at='2026-10-26 00:00:05+00',
    final_tick=3, final_checksum=456, finish_reason='ready_timeout'
WHERE match_id=1002;
UPDATE arena.game_match SET state='aborted', finished_at='2026-10-26 00:00:05+00',
    final_tick=4, final_checksum=789, finish_reason='member_unavailable'
WHERE match_id=1006;

INSERT INTO arena.settlement
(settlement_id, match_id, match_state, idempotency_key, request_hash, result_payload, created_at, updated_at) VALUES
(2001,1001,'completed','00000000-0000-0000-0000-000000002001',decode(repeat('ab',32),'hex'),
 '{"winner_player_id":0,"reason":"completed"}','2026-10-26 00:00:10+00','2026-10-26 00:00:10+00'),
(2002,1002,'aborted','00000000-0000-0000-0000-000000002002',decode(repeat('cd',32),'hex'),
 '{"reason":"ready_timeout"}','2026-10-26 00:00:10+00','2026-10-26 00:00:10+00'),
(2005,1005,'completed','00000000-0000-0000-0000-000000002005',decode(repeat('ef',32),'hex'),
 '{"winner_player_id":42,"reason":"completed"}','2026-10-26 00:00:10+00','2026-10-26 00:00:10+00'),
(2006,1006,'aborted','00000000-0000-0000-0000-000000002006',decode(repeat('12',32),'hex'),
 '{"reason":"member_unavailable"}','2026-10-26 00:00:10+00','2026-10-26 00:00:10+00');
UPDATE arena.settlement SET state='applying', attempt_count=1,
    last_attempt_at='2026-10-26 00:00:11+00', updated_at='2026-10-26 00:00:11+00'
WHERE settlement_id IN (2001,2005,2006);
UPDATE arena.settlement SET state='committed', committed_at='2026-10-26 00:00:12+00',
    updated_at='2026-10-26 00:00:12+00'
WHERE settlement_id IN (2001,2005);
UPDATE arena.settlement SET state='retry_wait', next_attempt_at='2026-10-26 00:00:13+00',
    last_error_code='RETRY_LATER', updated_at='2026-10-26 00:00:12+00'
WHERE settlement_id=2006;

INSERT INTO arena.outbox
(event_id, settlement_id, payload, created_at, updated_at, available_at) VALUES
('00000000-0000-0000-0000-000000003001',2001,'{"settlement_id":2001}',
 '2026-10-26 00:00:13+00','2026-10-26 00:00:13+00','2026-10-26 00:00:13+00'),
('00000000-0000-0000-0000-000000003005',2005,'{"settlement_id":2005}',
 '2026-10-26 00:00:13+00','2026-10-26 00:00:13+00','2026-10-26 00:00:13+00');
UPDATE arena.outbox SET state='published', attempt_count=1,
    last_attempt_at='2026-10-26 00:00:14+00', published_at='2026-10-26 00:00:15+00',
    updated_at='2026-10-26 00:00:15+00'
WHERE event_id='00000000-0000-0000-0000-000000003005';
