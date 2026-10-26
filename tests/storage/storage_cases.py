"""Independent direct-SQL constraint cases; every statement runs in a rolled-back real transaction."""
DIGEST = "'$arena-pbkdf2-sha256$100000$" + "00"*16 + "$" + "11"*32 + "'"
T0 = "'2026-10-26 00:00:00+00'"
T10 = "'2026-10-26 00:00:10+00'"
T13 = "'2026-10-26 00:00:13+00'"

def insert(table, values, **changes):
    values = dict(values, **changes)
    return "INSERT INTO arena." + table + "(" + ",".join(values) + ") VALUES(" + ",".join(map(str, values.values())) + ");"

def negative_cases():
    cases = []
    def add(name, sql, state="23514", constraint=None):
        cases.append(dict(name=name, sql=sql, state=state, constraint=constraint))
    def variations(table, base, specs, prefix=""):
        for label, changes, state, constraint in specs:
            add(table+"."+label, prefix+insert(table,base,**changes),state,constraint)

    account = dict(account_id="99",account_name="'fresh'",password_digest=DIGEST,created_at=T0,updated_at=T0)
    variations("account",account,[
        ("primary_key",dict(account_id="1"),"23505","account_pk"),
        ("name_unique",dict(account_name="'alice'"),"23505","account_name_uq"),
        ("null_name",dict(account_name="NULL"),"23502",None),
        ("zero_id",dict(account_id="0"),"23514","entity_id_positive"),
        ("negative_id",dict(account_id="-1"),"23514","uint64_range"),
        ("uint64_overflow",dict(account_id="18446744073709551616"),"23514","uint64_range"),
        ("fraction_id",dict(account_id="1.5"),"23514","uint64_range"),
        ("nan_id",dict(account_id="'NaN'"),"23514","uint64_range"),
        ("empty_name",dict(account_name="''"),"23514","account_name_ck"),
        ("space_name",dict(account_name="'has space'"),"23514","account_name_ck"),
        ("name_length",dict(account_name="repeat('a',129)"),"23514","account_name_ck"),
        ("non_ascii_name",dict(account_name="chr(233)"),"23514","account_name_ck"),
        ("digest_plain",dict(password_digest="'plain'"),"23514","account_digest_ck"),
        ("digest_low_rounds",dict(password_digest=DIGEST.replace("100000","99999")),"23514","account_digest_ck"),
        ("digest_high_rounds",dict(password_digest=DIGEST.replace("100000","2000001")),"23514","account_digest_ck"),
        ("digest_leading_zero",dict(password_digest=DIGEST.replace("100000","0100000")),"23514","account_digest_ck"),
        ("digest_upper_hex",dict(password_digest=DIGEST.replace("1111","ABCD",1)),"23514","account_digest_ck"),
        ("invalid_state",dict(state="'unknown'"),"23514","account_state_ck"),
        ("time_order",dict(updated_at="'2025-01-01 00:00:00+00'"),"23514","account_time_ck"),
        ("infinite_time",dict(created_at="'-infinity'"),"23514","account_time_ck")])
    player=dict(player_id="99",account_id="4",display_name="'New player'",created_at=T0,updated_at=T0)
    variations("player",player,[
        ("primary_key",dict(player_id="42"),"23505","player_pk"),
        ("account_unique",dict(account_id="1"),"23505","player_account_uq"),
        ("account_missing",dict(account_id="99999"),"23503","player_account_fk"),
        ("null_account",dict(account_id="NULL"),"23502",None),
        ("zero_player",dict(player_id="0"),"23514","entity_id_positive"),
        ("blank_name",dict(display_name="'   '"),"23514","player_name_ck"),
        ("control_name",dict(display_name="chr(10)"),"23514","player_name_ck"),
        ("long_name",dict(display_name="repeat('a',129)"),"23514","player_name_ck"),
        ("state",dict(state="'unknown'"),"23514","player_state_ck"),
        ("negative_mmr",dict(mmr="-1"),"23514","player_mmr_ck"),
        ("mmr_limit",dict(mmr="10001"),"23514","player_mmr_ck"),
        ("revision",dict(revision="-1"),"23514","player_revision_ck"),
        ("time",dict(updated_at="'2025-01-01 00:00:00+00'"),"23514","player_time_ck")])
    item=dict(item_id="99",item_code="'new_item'",display_name="'New item'")
    variations("item",item,[
        ("primary_key",dict(item_id="1"),"23505","item_pk"),
        ("code_unique",dict(item_code="'gold'"),"23505","item_code_uq"),
        ("zero_id",dict(item_id="0"),"23514","entity_id_positive"),
        ("null_code",dict(item_code="NULL"),"23502",None),
        ("code_shape",dict(item_code="'Upper Case'"),"23514","item_code_ck"),
        ("code_length",dict(item_code="repeat('a',65)"),"23514","item_code_ck"),
        ("blank_name",dict(display_name="' '"),"23514","item_name_ck")])
    inventory=dict(player_id="44",item_id="2",quantity="1")
    variations("inventory",inventory,[
        ("primary_key",dict(player_id="42"),"23505","inventory_pk"),
        ("player_missing",dict(player_id="99999"),"23503","inventory_player_fk"),
        ("item_missing",dict(item_id="99999"),"23503","inventory_item_fk"),
        ("null_player",dict(player_id="NULL"),"23502",None),
        ("negative_quantity",dict(quantity="-1"),"23514","inventory_quantity_ck"),
        ("quantity_overflow",dict(quantity="9223372036854775808"),"22003",None),
        ("revision",dict(revision="-1"),"23514","inventory_revision_ck"),
        ("time",dict(updated_at="'infinity'"),"23514","inventory_time_ck")])
    game=dict(match_id="9999",room_id="9999",created_at=T0)
    variations("game_match",game,[
        ("primary_key",dict(match_id="1004"),"23505","game_match_pk"),
        ("zero_room",dict(room_id="0"),"23514","entity_id_positive"),
        ("null_room",dict(room_id="NULL"),"23502",None),
        ("version",dict(result_version="0"),"23514","game_match_version_ck"),
        ("revision",dict(revision="-1"),"23514","game_match_version_ck"),
        ("state_shape",dict(started_at="'2026-10-26 00:00:01+00'"),"23514","game_match_state_ck"),
        ("time",dict(created_at="'infinity'"),"23514","game_match_time_ck")])
    for name,sql,constraint in [
        ("missing_second","UPDATE arena.game_match SET state='running',started_at='2026-10-26 00:00:01+00' WHERE match_id=1004;","game_match_roster_ck"),
        ("reverse_state","UPDATE arena.game_match SET state='forming' WHERE match_id=1003;","game_match_transition_ck"),
        ("skip_running","UPDATE arena.game_match SET state='completed',started_at='2026-10-26 00:00:01+00',finished_at='2026-10-26 00:00:02+00',final_tick=1,final_checksum=0,finish_reason='completed' WHERE match_id=1004;","game_match_transition_ck"),
        ("identity","UPDATE arena.game_match SET room_id=88 WHERE match_id=1003;","game_match_immutable_ck"),
        ("revision_decrease","UPDATE arena.game_match SET revision=0 WHERE match_id=1003;","game_match_revision_ck"),
        ("running_started_at","UPDATE arena.game_match SET started_at='2026-10-26 00:00:02+00' WHERE match_id=1003;","game_match_start_ck"),
        ("terminal_mutation","UPDATE arena.game_match SET final_checksum=0 WHERE match_id=1001;","game_match_immutable_ck"),
        ("missing_final","UPDATE arena.game_match SET state='completed',finished_at='2026-10-26 00:00:02+00',final_tick=1,finish_reason='completed' WHERE match_id=1003;","game_match_state_ck")]:
        add("game_match."+name,sql,constraint=constraint)
    add("game_match.winner_not_participant","UPDATE arena.game_match SET state='completed',finished_at='2026-10-26 00:00:02+00',final_tick=1,final_checksum=0,finish_reason='completed',winner_player_id=44 WHERE match_id=1003;","23503","game_match_winner_fk")
    member=dict(match_id="1004",player_id="43",slot="2",session_id="502",membership_epoch="1",initial_mmr="1200")
    variations("match_player",member,[
        ("primary_key",dict(player_id="42"),"23505","match_player_pk"),
        ("slot_unique",dict(slot="1"),"23505","match_player_slot_uq"),
        ("match_missing",dict(match_id="99999"),"23503","match_player_match_fk"),
        ("player_missing",dict(player_id="99999"),"23503","match_player_player_fk"),
        ("slot_range",dict(slot="3"),"23514","match_player_slot_ck"),
        ("epoch_zero",dict(membership_epoch="0"),"23514","entity_id_positive"),
        ("null_session",dict(session_id="NULL"),"23502",None),
        ("mmr_range",dict(initial_mmr="10001"),"23514","match_player_mmr_ck"),
        ("uint64_seq_overflow",dict(last_input_seq="18446744073709551616"),"23514","uint64_range")])
    for name,sql,constraint in [
        ("running_insert",insert("match_player",member,match_id="1003",player_id="44"),"match_player_roster_ck"),
        ("running_delete","DELETE FROM arena.match_player WHERE match_id=1003 AND slot=2;","match_player_roster_ck"),
        ("running_mmr","UPDATE arena.match_player SET initial_mmr=1 WHERE match_id=1003 AND slot=1;","match_player_roster_ck"),
        ("running_sequence","UPDATE arena.match_player SET last_input_seq=0 WHERE match_id=1003 AND slot=1;","match_player_sequence_ck"),
        ("identity","UPDATE arena.match_player SET match_id=1004 WHERE match_id=1003 AND slot=1;","match_player_identity_ck"),
        ("terminal_position","UPDATE arena.match_player SET final_x=99 WHERE match_id=1001 AND slot=1;","match_player_roster_ck")]:
        add("match_player."+name,sql,constraint=constraint)
    settlement=dict(settlement_id="2099",match_id="1006",match_state="'aborted'",
        idempotency_key="'00000000-0000-0000-0000-000000002099'",
        request_hash="decode(repeat('ab',32),'hex')",result_payload="'{}'",created_at=T10,updated_at=T10)
    free="DELETE FROM arena.settlement WHERE settlement_id=2006;"
    variations("settlement",settlement,[
        ("primary_key",dict(settlement_id="2001"),"23505","settlement_pk"),
        ("match_unique",dict(match_id="1002"),"23505","settlement_match_uq"),
        ("key_unique",dict(idempotency_key="'00000000-0000-0000-0000-000000002001'"),"23505","settlement_key_uq"),
        ("match_missing",dict(match_id="99999"),"23503","settlement_match_fk"),
        ("match_state_mismatch",dict(match_state="'completed'"),"23503","settlement_match_fk"),
        ("nonterminal_match",dict(match_id="1003",match_state="'running'"),"23514","settlement_terminal_match_ck"),
        ("zero_key",dict(idempotency_key="'00000000-0000-0000-0000-000000000000'"),"23514","settlement_key_ck"),
        ("hash_length",dict(request_hash="decode('ab','hex')"),"23514","settlement_payload_ck"),
        ("payload_shape",dict(result_payload="'[]'"),"23514","settlement_payload_ck"),
        ("payload_size",dict(result_payload="jsonb_build_object('x',repeat('a',65536))"),"23514","settlement_payload_ck"),
        ("bad_json",dict(result_payload="'{'"),"22P02",None),
        ("null_hash",dict(request_hash="NULL"),"23502",None),
        ("version",dict(result_version="0"),"23514","settlement_payload_ck"),
        ("initial_committed",dict(state="'committed'"),"23514","settlement_initial_ck"),
        ("pending_attempt",dict(attempt_count="1"),"23514","settlement_state_ck"),
        ("error_code",dict(last_error_code="'bad error'"),"23514","settlement_error_ck"),
        ("time",dict(updated_at="'2025-01-01 00:00:00+00'"),"23514","settlement_time_ck")],free)
    for name,sql,constraint in [
        ("immutable_request","UPDATE arena.settlement SET request_hash=decode(repeat('00',32),'hex') WHERE settlement_id=2002;","settlement_immutable_ck"),
        ("immutable_terminal","UPDATE arena.settlement SET updated_at='2026-10-26 00:00:15+00' WHERE settlement_id=2001;","settlement_immutable_ck"),
        ("skip_applying","UPDATE arena.settlement SET state='committed' WHERE settlement_id=2002;","settlement_transition_ck"),
        ("attempt_jump","UPDATE arena.settlement SET state='applying',attempt_count=2,last_attempt_at='2026-10-26 00:00:11+00',updated_at='2026-10-26 00:00:11+00' WHERE settlement_id=2002;","settlement_attempt_ck"),
        ("backoff_bypass","UPDATE arena.settlement SET state='applying',attempt_count=2,last_attempt_at='2026-10-26 00:00:12+00',next_attempt_at=NULL,last_error_code=NULL WHERE settlement_id=2006;","settlement_retry_due_ck")]:
        add("settlement."+name,sql,constraint=constraint)
    outbox=dict(event_id="'00000000-0000-0000-0000-000000003099'",settlement_id="2006",
                payload="'{}'",created_at="'2026-10-26 00:00:15+00'",
                updated_at="'2026-10-26 00:00:15+00'",available_at="'2026-10-26 00:00:15+00'")
    free="""UPDATE arena.settlement SET state='applying',attempt_count=2,
      last_attempt_at='2026-10-26 00:00:13+00',updated_at='2026-10-26 00:00:13+00',
      next_attempt_at=NULL,last_error_code=NULL WHERE settlement_id=2006;
      UPDATE arena.settlement SET state='committed',committed_at='2026-10-26 00:00:14+00',
      updated_at='2026-10-26 00:00:14+00' WHERE settlement_id=2006;"""
    variations("outbox",outbox,[
        ("primary_key",dict(event_id="'00000000-0000-0000-0000-000000003001'"),"23505","outbox_pk"),
        ("source_unique",dict(settlement_id="2001"),"23505","outbox_source_uq"),
        ("source_missing",dict(settlement_id="99999"),"23503","outbox_settlement_fk"),
        ("source_uncommitted",dict(settlement_id="2002"),"23503","outbox_settlement_fk"),
        ("zero_id",dict(event_id="'00000000-0000-0000-0000-000000000000'"),"23514","outbox_id_ck"),
        ("invalid_uuid",dict(event_id="'not-a-uuid'"),"22P02",None),
        ("null_source",dict(settlement_id="NULL"),"23502",None),
        ("event_type",dict(event_type="'other'"),"23514","outbox_type_ck"),
        ("source_state",dict(settlement_state="'pending'"),"23514","outbox_type_ck"),
        ("version",dict(schema_version="0"),"23514","outbox_type_ck"),
        ("payload_shape",dict(payload="'[]'"),"23514","outbox_payload_ck"),
        ("payload_size",dict(payload="jsonb_build_object('x',repeat('a',65536))"),"23514","outbox_payload_ck"),
        ("initial_published",dict(state="'published'"),"23514","outbox_initial_ck"),
        ("initial_attempt",dict(attempt_count="1"),"23514","outbox_initial_ck"),
        ("pending_shape",dict(published_at="'2026-10-26 00:00:15+00'"),"23514","outbox_state_ck"),
        ("time",dict(available_at="'2025-01-01 00:00:00+00'"),"23514","outbox_time_ck")],free)
    for name,sql,constraint in [
        ("immutable_payload","UPDATE arena.outbox SET payload='{\"changed\":true}' WHERE settlement_id=2001;","outbox_immutable_ck"),
        ("immutable_published","UPDATE arena.outbox SET updated_at='2026-10-26 00:00:16+00' WHERE settlement_id=2005;","outbox_immutable_ck"),
        ("attempt_jump","UPDATE arena.outbox SET attempt_count=2,last_attempt_at='2026-10-26 00:00:14+00',updated_at='2026-10-26 00:00:14+00' WHERE settlement_id=2001;","outbox_attempt_ck"),
        ("state","UPDATE arena.outbox SET state='unknown' WHERE settlement_id=2001;","outbox_state_ck"),
        ("publish_before_available","UPDATE arena.outbox SET state='published',attempt_count=1,last_attempt_at='2026-10-26 00:00:14+00',published_at='2026-10-26 00:00:15+00',available_at='2026-10-26 00:00:16+00',updated_at='2026-10-26 00:00:16+00' WHERE settlement_id=2001;","outbox_time_ck")]:
        add("outbox."+name,sql,constraint=constraint)
    for name,sql,constraint in [
        ("account","DELETE FROM arena.account WHERE account_id=1;","player_account_fk"),
        ("player","DELETE FROM arena.player WHERE player_id=42;","inventory_player_fk"),
        ("item","DELETE FROM arena.item WHERE item_id=1;","inventory_item_fk")]:
        add("delete_restrict."+name,sql,"23503",constraint)
    add("settlement.terminal_delete","DELETE FROM arena.settlement WHERE settlement_id=2001;",constraint="settlement_immutable_ck")
    add("outbox.pending_delete","DELETE FROM arena.outbox WHERE settlement_id=2001;",constraint="outbox_immutable_ck")
    add("outbox.published_delete","DELETE FROM arena.outbox WHERE settlement_id=2005;",constraint="outbox_immutable_ck")
    add("game_match.ready_timeout_cannot_run", "UPDATE arena.game_match SET state='aborted',finished_at='2026-10-26 00:00:05+00',final_tick=1,final_checksum=0,finish_reason='ready_timeout' WHERE match_id=1003;",constraint="game_match_state_ck")
    return cases
