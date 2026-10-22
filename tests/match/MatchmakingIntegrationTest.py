"""Authenticated 1v1 assignment across real owners, recovery and passive expiry."""

from concurrent.futures import ThreadPoolExecutor
import os
import re
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "auth"))
from OnlinePresenceIntegrationTest import Peer, parse, scalar
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import run_host, stop_host, varint


def ticket(raw):
    fields = parse(scalar(parse(raw), 1))
    names = ("player", "ticket", "key", "state", "mmr", "window", "waited",
             "remaining", "match", "opponent", "opponent_ticket", "active")
    return {name: scalar(fields, index) for index, name in enumerate(names, 1)}


class MatchPeer(Peer):
    def match_join(self, key, error=None):
        reply = self.rpc(40, b"\x08" + varint(key), 41, error)
        return None if error is not None else ticket(reply)

    def match_cancel(self, identity, error=None):
        reply = self.rpc(42, b"\x08" + varint(identity), 43, error)
        return None if error is not None else ticket(reply)

    def match_status(self, identity, error=None):
        reply = self.rpc(44, b"\x08" + varint(identity), 45, error)
        return None if error is not None else ticket(reply)


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp, ws = run_host(binary, "match")
        destinations = [(tcp, False), (ws, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-match-", suffix=".log", delete=False)
        environment = os.environ.copy()
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
        environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "match"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment)

        def content():
            log.flush()
            with open(log.name, errors="replace") as source:
                return source.read()

        destinations = [(port, False)]
    peers = []
    passed = False

    def connect(destination):
        result = MatchPeer(destination)
        peers.append(result)
        return result

    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "matchmaking host startup")
        baseline = fd_count(process)
        anonymous = connect(destinations[0])
        anonymous.match_join(1, error=1003)
        anonymous.match_status(1, error=1003)
        anonymous.match_cancel(1, error=1003)
        anonymous.close()

        alice = connect(destinations[0])
        first_login = alice.login()
        bob = connect(destinations[-1])
        bob.login(b"bob")
        carl = connect(destinations[-1])
        carl_login = carl.login(b"carl")

        alice.match_join(0, error=1002)
        carl.join(7001)
        carl.match_join(1, error=2001)
        carl.leave()

        original = alice.match_join(100)
        assert original["state"] == 1 and original["mmr"] == 1000
        assert alice.match_join(100)["ticket"] == original["ticket"]
        assert alice.match_join(101)["ticket"] == original["ticket"]
        cancelled = alice.match_cancel(original["ticket"])
        assert cancelled["state"] == 4 and not cancelled["active"]
        assert alice.match_cancel(original["ticket"])["state"] == 4
        assert alice.match_join(100)["state"] == 4
        alice.match_status(0, error=1002)
        bob.match_cancel(original["ticket"], error=2001)

        waiting = alice.match_join(101)
        assert waiting["ticket"] != original["ticket"]
        alice.close(reset=workers == 4 or mode == "ws")
        # A detached player must pause while its original deadline keeps aging.
        other = bob.match_join(200)
        latest = {}

        def expanded_but_not_matched():
            latest.update(bob.match_status(other["ticket"]))
            return latest["window"] >= 250

        wait_until(process, expanded_but_not_matched, "Bob's independent rating window expands")
        assert latest["state"] == 1, "detached Alice was matched before JWT recovery"
        resumed = connect(destinations[-1])
        recovered_login = resumed.login(token=first_login["token"])
        assert recovered_login["session"] == first_login["session"] and recovered_login["resumed"]
        recovered = resumed.match_join(101)
        assert recovered["ticket"] == waiting["ticket"]
        assert recovered["waited"] >= 200 and recovered["remaining"] <= 800

        paired = {}

        def assigned():
            paired.update(resumed.match_status(waiting["ticket"]))
            return paired["state"] == 3

        wait_until(process, assigned, "compatible online pair assigned")
        bob_pair = bob.match_status(other["ticket"])
        assert paired["match"] > 0 and paired["match"] == bob_pair["match"]
        assert paired["opponent"] == 43 and bob_pair["opponent"] == 42
        assert paired["opponent_ticket"] == bob_pair["ticket"]
        assert bob_pair["opponent_ticket"] == paired["ticket"]
        assert paired["window"] >= 200 and bob_pair["window"] >= 200
        resumed.match_cancel(paired["ticket"], error=2001)
        assert resumed.match_join(999)["ticket"] == paired["ticket"], "assigned player entered another queue"
        assert resumed.query()["room"] == 0, "pair assignment unexpectedly allocated a room"

        # Concurrent queries/retries cross the workers but must preserve one pair.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(resumed.match_status, paired["ticket"]),
                       executor.submit(bob.match_join, 200)]
            assert all(f.result()["match"] == paired["match"] for f in futures)

        # A fresh JWT handoff preserves the assigned identity; old close is fenced.
        handoff = connect(destinations[0])
        handoff_login = handoff.login(token=recovered_login["token"])
        resumed.close(reset=True)
        assert handoff_login["session"] == first_login["session"]
        assert handoff.match_status(paired["ticket"])["match"] == paired["match"]

        marker = len(content())
        isolated = carl.match_join(900)
        assert isolated["mmr"] == 3000
        # No status/login query may trigger expiry before this timer evidence.
        wait_until(process, lambda: re.search(r"event=match_tick .*pairs=1 expired=1", content()[marker:]),
                   "traffic-independent queue timeout")
        expired = carl.match_status(isolated["ticket"])
        assert expired["state"] == 5 and not expired["active"] and expired["remaining"] == 0
        assert carl.match_join(900)["ticket"] == isolated["ticket"]
        assert carl.match_join(900)["state"] == 5

        marker = len(content())
        carl.close(reset=True)
        wait_until(process, lambda: re.search(r"event=online_sweep active=2 retained=2 rooms=0", content()[marker:]),
                   "detached logical session physically reclaimed")
        fresh = connect(destinations[-1])
        fresh_login = fresh.login(b"carl")
        assert fresh_login["session"] != carl_login["session"]
        fresh.match_status(isolated["ticket"], error=2001)
        replacement = fresh.match_join(900)
        assert replacement["ticket"] != isolated["ticket"] and replacement["state"] == 1
        fresh.match_cancel(replacement["ticket"])

        for peer in peers:
            peer.close()
        wait_until(process, lambda: fd_count(process) <= baseline, "matchmaking descriptors retired")
        stop_host(process, log)
        with open(log.name, errors="replace") as source:
            output = source.read()
        assert process.returncode == 0, output
        assert "ONLINE_COUNTS active=0 retained=0 rooms=0" in output, output
        counts = re.search(r"MATCH_COUNTS joined=(\d+) replayed=(\d+) cancelled=(\d+) expired=(\d+) matched_pairs=(\d+) rejected=(\d+) released_pairs=(\d+) queued=(\d+) assigned=(\d+) retained=(\d+)", output)
        assert counts, output
        values = list(map(int, counts.groups()))
        assert values[0] >= 5 and values[1] >= 3 and values[2] >= 2 and values[3] == 1, values
        assert values[4] == 1 and values[7:] == [0, 0, 0], values
        metrics = re.search(r"wait_ms_sum=(\d+) mmr_gap_sum=(\d+) matched_wait=([\d,]+) mmr_gap=([\d,]+) abandoned_wait=([\d,]+)", output)
        assert metrics, output
        wait_sum, gap_sum = map(int, metrics.groups()[:2])
        matched_wait, mmr_gap, abandoned_wait = [list(map(int, bins.split(','))) for bins in metrics.groups()[2:]]
        assert len(matched_wait) == len(mmr_gap) == len(abandoned_wait) == 8
        assert sum(matched_wait) == 2 and sum(mmr_gap) == 1 and sum(abandoned_wait) == values[2] + values[3]
        assert wait_sum >= 400 and gap_sum == 200, (wait_sum, gap_sum)
        for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:", "runtime error:",
                           "UndefinedBehaviorSanitizer", "SanitizerTool: CHECK failed"):
            assert diagnostic not in output, output
        passed = True
        print(f"join/cancel, widening pair, JWT queue recovery, passive timeout, owner shutdown passed ({mode}, workers={workers})")
        print(f"MATCH_SAMPLE matched_players=2 pairs=1 wait_ms_sum={wait_sum} mean_wait_ms={wait_sum / 2:.1f} mmr_gap=200 matched_wait={matched_wait} mmr_gap_bins={mmr_gap} abandoned_wait={abandoned_wait}")
    finally:
        for peer in peers:
            peer.close()
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("matchmaking server log:", log.name, file=sys.stderr)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
