"""Two real bots exercise the production gateway, RST recovery and grace expiry."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from RoomIntegrationTest import RoomPeer, decode_fields, snapshot, unsigned_field
from OnlinePresenceIntegrationTest import parse, scalar
from TcpServerIntegrationTest import reserve_port, wait_until


def settlement(raw):
    f = decode_fields(raw)
    return dict(result=scalar(f, 1), match=scalar(f, 2), room=scalar(f, 3),
                tick=scalar(f, 4), checksum=scalar(f, 5), reason=scalar(f, 6),
                winner=scalar(f, 7), players=[decode_fields(p) for p in f.get(8, [])])


def managed(raw):
    f = decode_fields(scalar(parse(raw), 1))
    members = {}
    for raw_member in f.get(4, []):
        m = parse(raw_member)
        members[scalar(m, 1)] = dict(session=scalar(m, 2), epoch=scalar(m, 3),
                                   ready=scalar(m, 4), presence=scalar(m, 5))
    result = dict(snapshot=snapshot(scalar(f, 1)), match=scalar(f, 2), phase=scalar(f, 3),
                  members=members, played=scalar(f, 7), duration=scalar(f, 8))
    if 9 in f:
        result["settlement"] = settlement(scalar(f, 9))
    return result


class Bot(RoomPeer):
    def match_join(self, key):
        return parse(scalar(parse(self.rpc(40, unsigned_field(1, key), 41)), 1))

    def match_status(self, ticket_id):
        return parse(scalar(parse(self.rpc(44, unsigned_field(1, ticket_id), 45)), 1))

    def status(self):
        return managed(self.rpc(56, b"", 57))

    def ready(self, state, player):
        p = state["snapshot"]
        payload = unsigned_field(1, 1) + unsigned_field(2, p["incarnation"])
        payload += unsigned_field(3, p["players"][player]["epoch"])
        return managed(self.rpc(54, payload, 55))

    def result(self, match):
        return settlement(scalar(parse(self.rpc(58, unsigned_field(1, match), 59)), 1))


def scenario(binary, replay_binary, mode, report_dir):
    with tempfile.TemporaryDirectory(prefix="arena-journey-") as temporary:
        root = Path(temporary)
        accounts = root / "accounts.tsv"
        rows = []
        for name, player in (("alice", 42), ("bob", 43)):
            salt = secrets.token_bytes(16)
            key = hashlib.pbkdf2_hmac("sha256", b"test-password", salt, 100000, 32)
            digest = "$arena-pbkdf2-sha256$100000$" + salt.hex() + "$" + key.hex()
            rows.append(f"{name}\t{player}\t{digest}\n")
        accounts.write_text("".join(rows))
        accounts.chmod(0o600)
        tcp, ws = reserve_port(), reserve_port()
        while ws == tcp:
            ws = reserve_port()
        environment = os.environ.copy()
        environment.update(ARENA_JWT_KEY=secrets.token_hex(32), ARENA_ACCOUNTS_FILE=str(accounts),
                           ARENA_TCP_PORT=str(tcp), ARENA_WS_PORT=str(ws),
                           ARENA_BATTLE_TICKS="80", ARENA_READY_TIMEOUT_TICKS="200",
                           ARENA_DISCONNECT_GRACE_MS="300" if mode == "expired" else "3000",
                           ARENA_REPLAY_DIR=str(root / "replays"))
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
        environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
        log_path = root / "gateway.log"
        peers, outcome = [], {}
        with log_path.open("w") as log:
            process = subprocess.Popen([binary, "asio", "handshake"],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment)
            def content():
                return log_path.read_text(errors="replace")
            def connect(destination):
                peer = Bot(destination)
                peers.append(peer)
                return peer
            try:
                wait_until(process, lambda: "GATEWAY_READY" in content(), "production gateway startup")
                alice, bob = connect((tcp, False)), connect((ws, True))
                first, second = alice.login(), bob.login(b"bob")
                assert alice.query()["room"] == bob.query()["room"] == 0
                queued_a, queued_b = alice.match_join(101), bob.match_join(201)
                ticket_a, ticket_b = scalar(queued_a, 2), scalar(queued_b, 2)
                assigned = {}
                def paired():
                    assigned.update(alice.match_status(ticket_a))
                    return scalar(assigned, 4) == 3 and scalar(assigned, 13) != 0
                wait_until(process, paired, "server room allocation")
                other = bob.match_status(ticket_b)
                room, match = scalar(assigned, 13), scalar(assigned, 9)
                assert scalar(other, 9) == match and scalar(other, 13) == room
                assert alice.query()["room"] == bob.query()["room"] == room
                state = alice.status()
                assert state["match"] == match and state["phase"] == 1
                assert set(state["snapshot"]["players"]) == {42, 43}
                alice.ready(state, 42)
                bob.ready(state, 43)
                latest = {}
                def playing():
                    latest.update(alice.status())
                    return latest["phase"] == 3
                wait_until(process, playing, "both ready starts battle")
                with ThreadPoolExecutor(max_workers=2) as workers:
                    a = workers.submit(alice.control, latest["snapshot"], 42, 1, 1, -1)
                    b = workers.submit(bob.control, latest["snapshot"], 43, 1, -1, 1)
                    assert a.result()[1] == b.result()[1] == 1
                before = alice.status()["snapshot"]
                assert (before["players"][42]["x"], before["players"][42]["y"]) == (1, -1)
                assert (before["players"][43]["x"], before["players"][43]["y"]) == (-1, 1)
                alice.close(reset=True)
                detached = {}
                def sees_detached():
                    detached.update(bob.status())
                    return detached["members"][42]["presence"] == 2
                wait_until(process, sees_detached, "RST retained in grace")
                recovery = None
                if mode == "recover":
                    alice = connect((ws, True))
                    recovery = alice.login(token=first["token"])
                    assert recovery["resumed"] and recovery["session"] == first["session"]
                    assert recovery["room"] == room and recovery["epoch"] > first["epoch"]
                    resumed = alice.status()
                    restored = resumed["snapshot"]
                    assert restored["incarnation"] == before["incarnation"]
                    assert restored["players"][42] == before["players"][42]
                    assert resumed["members"][42]["presence"] == 1
                    assert alice.control(restored, 42, 2, 1, 0)[1] == 1
                    assert alice.control(restored, 42, 2, 1, 0)[1] == 2
                    assert bob.control(restored, 43, 2, 0, 1)[1] == 1
                final = {}
                def finished():
                    final.update(bob.status())
                    return final["phase"] == 5
                wait_until(process, finished, "deterministic end and settlement")
                assert final["settlement"]["reason"] == (1 if mode == "recover" else 2)
                if mode == "recover":
                    assert final["played"] == final["duration"] == 80
                    assert final["snapshot"]["players"][42]["seq"] == 2
                    assert final["snapshot"]["players"][43]["seq"] == 2
                    assert (final["snapshot"]["players"][42]["x"], final["snapshot"]["players"][42]["y"]) == (2, -1)
                    assert (final["snapshot"]["players"][43]["x"], final["snapshot"]["players"][43]["y"]) == (-1, 2)
                result = bob.result(match)
                assert result == bob.result(match) == final["settlement"]
                assert result["checksum"] == final["snapshot"]["checksum"]
                assert result["tick"] == final["snapshot"]["tick"]
                assert result["result"] == result["match"] == match and result["room"] == room
                assert not result["winner"] and len(result["players"]) == 2
                assert bob.query()["room"] == 0
                if mode == "recover":
                    assert alice.result(match) == result and alice.query()["room"] == 0
                    alice.close(reset=True)
                    after = connect((tcp, False))
                    again = after.login(token=recovery["token"])
                    assert again["resumed"] and again["session"] == first["session"] and again["room"] == 0
                    assert after.status()["settlement"] == after.result(match) == result
                else:
                    expired = connect((tcp, False))
                    expired.login(token=first["token"], error=1003)
                    fresh = expired.login()
                    assert fresh["session"] != first["session"] and not fresh["resumed"] and fresh["room"] == 0
                for peer in peers:
                    peer.close()
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=12)
                assert process.returncode == 0, content()
                text_log = content()
                assert "ONLINE_COUNTS active=0 retained=0 rooms=0" in text_log
                assert "ROOM_COUNTS actors=0" in text_log
                assert re.search(r"connection_admission active=0 ", text_log)
                assert "event=room_phase" in text_log and f"match_id={match}" in text_log
                phases = re.findall(r"event=room_phase match_id=" + str(match) + r" room_id=\d+ phase=(\w+)", text_log)
                assert phases == ["Forming", "Ready", "Playing", "Settling", "Finished"], phases
                assert len(re.findall(r"event=room_result result_id=" + str(match) + r" ", text_log)) == 1
                assert "event=request_start connection_id=" in text_log and "event=request_end connection_id=" in text_log
                assert "MATCH_COUNTS" in text_log and "REPLAY_COUNTS completed=1 failed=0 rejected=0" in text_log
                assert not any(s in text_log for s in ("test-password", "$arena-pbkdf2", first["token"].decode()))
                assert not re.search(r"AddressSanitizer|runtime error:|ThreadSanitizer: data race", text_log)
                archives = list((root / "replays").rglob("*.areplay"))
                assert len(archives) == 1 and not list((root / "replays").rglob("*.part"))
                replay_report = root / "replay.json"
                replay = subprocess.run([replay_binary, str(archives[0]), "--report", str(replay_report)],
                                        capture_output=True, text=True, timeout=15)
                assert replay.returncode == 0, (replay.stdout, replay.stderr)
                archived = json.loads(replay_report.read_text())
                assert int(archived["final_tick"]) == result["tick"]
                assert int(archived["final_checksum"]) == result["checksum"]
                live_ticks = [(int(t), int(c)) for t, c in re.findall(
                    r"event=room_tick room_id=" + str(room) + r" incarnation=\d+ tick=(\d+) checksum=(\d+)", text_log)]
                saved_ticks = [(int(row["tick"]), int(row["checksum"])) for row in archived["checksums"]]
                assert live_ticks == saved_ticks and live_ticks[-1] == (result["tick"], result["checksum"])
                outcome = dict(mode=mode, transports=["tcp", "websocket"], match=match, room=str(room),
                               session_preserved=bool(recovery), reason=result["reason"],
                               sessions={"alice": str(first["session"]), "bob": str(second["session"])},
                               incarnation=str(before["incarnation"]), observed_phases=phases,
                               final_sequences={str(p): v["seq"] for p, v in final["snapshot"]["players"].items()},
                               played_ticks=final["played"], final_tick=result["tick"],
                               checksum=str(result["checksum"]), replay_verified=True,
                               metrics=[line for line in text_log.splitlines() if "_COUNTS " in line or
                                        line.startswith("connection_admission ")])
                if report_dir:
                    destination = Path(report_dir)
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(log_path, destination / f"{mode}-gateway.log")
                    shutil.copyfile(replay_report, destination / f"{mode}-replay.json")
            finally:
                for peer in peers:
                    peer.close()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=12)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                if not outcome:
                    print(content(), file=sys.stderr)
        return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("replay_binary")
    parser.add_argument("--report-dir")
    args = parser.parse_args()
    results = [scenario(args.binary, args.replay_binary, mode, args.report_dir)
               for mode in ("recover", "expired")]
    text = json.dumps(dict(passed=True, scenarios=results), indent=2)
    print(text)
    if args.report_dir:
        (Path(args.report_dir) / "journey.json").write_text(text + "\n")

if __name__ == "__main__":
    main()
