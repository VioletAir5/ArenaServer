"""Two real bots traverse Gateway/Auth/Lobby/Storage, recovery, and durable replay."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT / "tests/room", ROOT / "tests/auth", ROOT / "tests/lobby"):
    sys.path.insert(0, str(folder))
from OnlinePresenceIntegrationTest import scalar


SANITIZER = re.compile(
    r"AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer|runtime error:|ThreadSanitizer"
)


def log_text(child):
    return "\n".join(path.read_text(errors="replace") for path in child.log_paths)


def stopped_lobby(text):
    rows = []
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and row.get("service") == "lobby" and row.get("state") == "stopped":
            rows.append(row)
    assert len(rows) == 1, "Lobby must publish exactly one final committed metric cut"
    return rows[0]


def scenario(args, mode):
    # The shared fixture owns its private PostgreSQL cluster and all four children.
    # Import lazily: the fixture shares JourneyProtocol without importing this driver.
    from ServiceStackFixture import ServiceStack

    local = argparse.Namespace(**vars(args))
    local.gateway = Path(args.binary)
    local.report = args.report_dir / (mode + "-stack.json")
    stack = ServiceStack(local, battle_ticks=80, ready_ticks=200,
                         grace_ms=300 if mode == "expired" else 3000)
    outcome = dict(mode=mode, passed=False)
    stack.report.update(scope="real four-process complete journey and every-tick replay",
                        current_case=mode, mode=mode)
    try:
        stack.setup()
        stack.sample("journey-started")
        alice, bob = stack.connect(), stack.connect(True)
        first, second = alice.login(), bob.login(b"bob")
        stack.secrets += (first["token"].decode(), second["token"].decode())
        assert alice.query()["room"] == bob.query()["room"] == 0
        queued_a, queued_b = alice.match_join(101), bob.match_join(201)
        ticket_a, ticket_b = scalar(queued_a, 2), scalar(queued_b, 2)
        assigned = {}
        def paired():
            assigned.update(alice.match_status(ticket_a))
            return scalar(assigned, 4) == 3 and scalar(assigned, 13) != 0
        # Seeded canonical ratings need six expansion intervals; local file
        # accounts previously used an equal-rating default and matched at once.
        stack.until(paired, "server room allocation", 10)
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
        stack.until(playing, "both ready starts battle")
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
        stack.until(sees_detached, "RST retained in grace")
        recovery = None
        if mode == "recover":
            alice = stack.connect(True)
            recovery = alice.login(token=first["token"])
            stack.secrets += (recovery["token"].decode(),)
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
        stack.until(finished, "deterministic end and settlement", 12)
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
            after = stack.connect()
            again = after.login(token=recovery["token"])
            stack.secrets += (again["token"].decode(),)
            assert again["resumed"] and again["session"] == first["session"] and again["room"] == 0
            assert after.status()["settlement"] == after.result(match) == result
        else:
            expired = stack.connect()
            expired.login(token=first["token"], error=1003)
            fresh = expired.login()
            stack.secrets += (fresh["token"].decode(),)
            assert fresh["session"] != first["session"] and not fresh["resumed"] and fresh["room"] == 0
        rows = stack.query(
            "SELECT final_tick,final_checksum,state FROM arena.game_match WHERE match_id=%s", (match,))
        assert len(rows) == 1 and int(rows[0][0]) == result["tick"]
        assert int(rows[0][1]) == result["checksum"]
        assert rows[0][2] == ("completed" if mode == "recover" else "aborted")
        for peer in stack.peers:
            peer.close()
        stack.children["gateway"].stop()
        stack.children["lobby"].stop()
        gateway_log = log_text(stack.children["gateway"])
        lobby_log = log_text(stack.children["lobby"])
        assert "LOBBY_COUNTS tracked=0 business_inflight=0 heartbeat_inflight=0 probe_inflight=0 owner_posts=0 stopped=1" in gateway_log
        assert re.search(r"connection_admission active=0 ", gateway_log)
        assert "event=request_start connection_id=" in gateway_log
        assert "event=request_end connection_id=" in gateway_log
        phases = re.findall(r"event=room_phase match_id=" + str(match) + r" room_id=\d+ phase=(\w+)", lobby_log)
        assert phases == ["Forming", "Ready", "Playing", "Settling", "Finished"], phases
        assert len(re.findall(r"event=room_result result_id=" + str(match) + r" ", lobby_log)) == 1
        committed = stopped_lobby(lobby_log)
        assert all(committed[name] == 0 for name in (
            "active", "auth_inflight", "storage_inflight", "journal_pending", "owner_pending"))
        assert committed["journal_closed"] is True
        assert committed["committed_revision"] > 0 and committed["receipts"] > 0
        assert committed["queued"] == 0 and committed["matched_pairs"] == 1
        assert committed["room_indexes"] == 0
        # Finished actors/results and detached sessions remain durable within retention.
        assert committed["room_actors"] == 1
        assert committed["settlements_committed"] == 1 and committed["settlements_blocked"] == 0
        for child in stack.children.values():
            text_log = log_text(child)
            stack.assert_no_secrets(text_log)
            assert not SANITIZER.search(text_log), "sanitizer reported a service failure"

        directory = stack.cluster.root / "exported-replays"
        exported = subprocess.run(
            [str(args.replay_export), "--state-dir", str(stack.cluster.root / "lobby-state"),
             "--output-dir", str(directory)], cwd=args.repository_root,
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=15)
        stack.assert_no_secrets(exported.stdout + exported.stderr)
        assert not SANITIZER.search(exported.stdout + exported.stderr)
        assert exported.returncode == 0, "offline durable replay export failed"
        manifest = json.loads(exported.stdout)
        assert manifest["status"] == "ok"
        assert int(manifest["revision"]) == committed["committed_revision"]
        assert len(manifest["files"]) == 1
        record = manifest["files"][0]
        assert int(record["room_id"]) == room
        assert int(record["incarnation"]) == before["incarnation"]
        assert Path(record["file"]).name == record["file"]
        archives = list(directory.glob("*.arena"))
        assert len(archives) == 1 and archives[0].name == record["file"]
        assert not list(directory.glob("*.part"))
        replay_report = args.report_dir / (mode + "-replay.json")
        replay = subprocess.run(
            [str(args.replay_binary), str(archives[0]), "--report", str(replay_report)],
            cwd=args.repository_root, stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=15)
        stack.assert_no_secrets(replay.stdout + replay.stderr)
        assert not SANITIZER.search(replay.stdout + replay.stderr)
        assert replay.returncode == 0, "original replay tool rejected the durable archive"
        archived = json.loads(replay_report.read_text())
        assert int(archived["final_tick"]) == result["tick"]
        assert int(archived["final_checksum"]) == result["checksum"]
        live_ticks = [(int(t), int(c)) for t, c in re.findall(
            r"event=room_tick room_id=" + str(room) + r" incarnation=\d+ tick=(\d+) checksum=(\d+)",
            lobby_log)]
        saved_ticks = [(int(row["tick"]), int(row["checksum"])) for row in archived["checksums"]]
        assert live_ticks == saved_ticks and live_ticks[-1] == (result["tick"], result["checksum"])
        assert len(live_ticks) == committed["room_ticks"] == int(record["ticks"])
        retained_archive = args.report_dir / (mode + "-archive.arena")
        shutil.copyfile(archives[0], retained_archive)
        (args.report_dir / (mode + "-export.json")).write_text(json.dumps(manifest, indent=2) + "\n")
        outcome.update(
            passed=True, transports=["tcp", "websocket"], match=match, room=str(room),
            clock_fixture=dict(enabled=bool(os.environ.get("ARENA_TEST_CLOCK_REALTIME_NS")),
                               scope="caller-supplied process-tree clock fixture"),
            session_preserved=bool(recovery), reason=result["reason"],
            sessions={"alice": str(first["session"]), "bob": str(second["session"])},
            incarnation=str(before["incarnation"]), observed_phases=phases,
            final_sequences={str(p): v["seq"] for p, v in final["snapshot"]["players"].items()},
            played_ticks=final["played"], final_tick=result["tick"], checksum=str(result["checksum"]),
            replay_verified=True, replay_ticks_verified=len(live_ticks), real_postgres_ack=True,
            archive=str(retained_archive), archive_sha256=hashlib.sha256(archives[0].read_bytes()).hexdigest(),
            metrics=dict(lobby_committed=committed, gateway_physical=[
                line for line in gateway_log.splitlines()
                if "LOBBY_COUNTS " in line or line.startswith("connection_admission ")]))
        stack.report.update(passed=True, cases=[dict(name=mode, passed=True)])
    except (Exception, KeyboardInterrupt) as error:
        # Never serialize a raw protobuf assertion: it may contain a bearer token.
        trace = error.__traceback__
        frames = []
        cursor = trace
        while cursor is not None and len(frames) < 32:
            frames.append(dict(file=Path(cursor.tb_frame.f_code.co_filename).name, line=cursor.tb_lineno))
            cursor = cursor.tb_next
        outcome['failure_trace'] = frames
        while trace is not None and trace.tb_next is not None:
            trace = trace.tb_next
        location = (dict(file=Path(trace.tb_frame.f_code.co_filename).name, line=trace.tb_lineno)
                    if trace is not None else {})
        outcome.update(passed=False, error=type(error).__name__, failure_location=location,
                       interrupted=isinstance(error, KeyboardInterrupt))
        stack.report.update(passed=False, error=type(error).__name__, failure_location=location)
    finally:
        previous = {number: signal.signal(number, signal.SIG_IGN)
                    for number in (signal.SIGTERM, signal.SIGINT)}
        try:
            stack.close()
            logs = Path(stack.report["logs"])
            for path in logs.glob("*.log"):
                text = path.read_text(errors="replace")
                stack.assert_no_secrets(text)
                assert not SANITIZER.search(text), "sanitizer reported a late shutdown failure"
            assert stack.report.get("cluster_cleaned") is True
            assert stack.report["parent_fd_final"] == stack.report["parent_fd_baseline"]
            assert stack.report["forced_kills"] == 0
            assert not stack.report.get("cleanup_errors") and not stack.report.get("service_cleanup_errors")
        except Exception as error:
            outcome.update(passed=False, cleanup_error=type(error).__name__)
            stack.report.update(passed=False, cleanup_error=type(error).__name__)
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
        outcome["passed"] = outcome["passed"] and stack.report["passed"]
        outcome.update(stack_report=str(local.report), logs=stack.report.get("logs"),
                       parent_fd_baseline=stack.report["parent_fd_baseline"],
                       parent_fd_final=stack.report.get("parent_fd_final"),
                       owned_child_pids=stack.report.get("owned_child_pids"))
        stack.report["journey"] = outcome
        stack.report["source_hashes"] = {
            name: hashlib.sha256((args.repository_root / name).read_bytes()).hexdigest()
            for name in ("services/gateway/app/main.cpp", "services/lobby/app/main.cpp",
                         "services/lobby/src/LobbyService.cpp", "tests/lobby/ServiceStackFixture.py",
                         "tests/room/JourneyProtocol.py", "tests/room/FullJourneyIntegrationTest.py")}
        text = json.dumps(stack.report, indent=2)
        stack.assert_no_secrets(text)
        local.report.write_text(text + "\n")
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("replay_binary", type=Path)
    for key in ("auth", "lobby", "descriptor", "pg-bin", "replay-export"):
        parser.add_argument("--" + key, type=Path, required=True)
    parser.add_argument("--pg-share", type=Path)
    parser.add_argument("--pg-lib", type=Path)
    parser.add_argument("--repository-root", type=Path, default=ROOT)
    parser.add_argument("--report-dir", type=Path)
    args = parser.parse_args()
    for name in ("binary", "replay_binary", "auth", "lobby", "descriptor", "pg_bin",
                 "pg_share", "pg_lib", "replay_export", "repository_root"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.resolve())
    args.report_dir = (args.report_dir.resolve() if args.report_dir else
                       Path(tempfile.mkdtemp(prefix="arena-journey-report-")))
    args.report_dir.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(args.repository_root))
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    results = []
    for mode in ("recover", "expired"):
        result = scenario(args, mode)
        results.append(result)
        if result.get("interrupted"):
            break
    report = dict(passed=len(results) == 2 and all(row["passed"] for row in results),
                  scenarios=results, report_dir=str(args.report_dir))
    text = json.dumps(report, indent=2)
    print(text)
    (args.report_dir / "journey.json").write_text(text + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
