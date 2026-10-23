"""Room owner and fixed-tick acceptance on isolated real transports."""

from concurrent.futures import ThreadPoolExecutor
import os
import re
import struct
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "auth"))
from OnlinePresenceIntegrationTest import Peer, parse, scalar
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import run_host, stop_host, varint


def decode_fields(data):
    """Independent protobuf subset, including RoomSnapshot's fixed64 hash."""
    fields = {}
    offset = 0

    def number():
        nonlocal offset
        result, shift = 0, 0
        while offset < len(data) and shift < 70:
            byte = data[offset]
            offset += 1
            result |= (byte & 127) << shift
            if byte < 128:
                return result
            shift += 7
        raise AssertionError("invalid varint")

    while offset < len(data):
        key = number()
        assert key >> 3
        kind = key & 7
        if kind == 0:
            value = number()
        elif kind == 1:
            assert offset + 8 <= len(data)
            value = struct.unpack_from("<Q", data, offset)[0]
            offset += 8
        else:
            assert kind == 2, kind
            size = number()
            assert offset + size <= len(data)
            value = data[offset:offset + size]
            offset += size
        fields.setdefault(key >> 3, []).append(value)
    return fields


def signed(number):
    return (number >> 1) ^ -(number & 1)


def canonical_checksum(snapshot):
    # The test writes canonical integer bytes independently of the server.
    values = [(1, 8), (snapshot["room"], 8), (snapshot["incarnation"], 8),
              (snapshot["tick"], 8), (snapshot["limit"], 4), (len(snapshot["players"]), 8)]
    for p in snapshot["players"].values():
        values.extend(((p["player"], 8), (p["session"], 8), (p["epoch"], 8),
                       (p["x"], 4), (p["y"], 4), (p["seq"], 8), (p["dx"], 4), (p["dy"], 4)))
    result = 14695981039346656037
    for number, size in values:
        for byte in (number & ((1 << (8 * size)) - 1)).to_bytes(size, "little"):
            result = ((result ^ byte) * 1099511628211) & ((1 << 64) - 1)
    return result


def snapshot(raw):
    fields = decode_fields(raw)
    players = {}
    for value in fields.get(5, []):
        f = decode_fields(value)
        player = dict(player=scalar(f, 1), session=scalar(f, 2), epoch=scalar(f, 3),
                      x=signed(scalar(f, 4)), y=signed(scalar(f, 5)), seq=scalar(f, 6),
                      dx=signed(scalar(f, 7)), dy=signed(scalar(f, 8)))
        assert player["player"] not in players
        players[player["player"]] = player
    assert list(players) == sorted(players), "snapshot is not canonical"
    result = dict(room=scalar(fields, 1), incarnation=scalar(fields, 2), tick=scalar(fields, 3),
                  checksum=scalar(fields, 4), version=scalar(fields, 6), limit=signed(scalar(fields, 7)),
                  players=players)
    assert result["version"] == 1 and result["incarnation"] > 0
    assert result["checksum"] == canonical_checksum(result), result
    return result


def unsigned_field(tag, value):
    return varint(tag << 3) + varint(value)


class RoomPeer(Peer):
    def room_snapshot(self, error=None):
        raw = self.rpc(52, b"", 53, error)
        return None if error is not None else snapshot(scalar(parse(raw), 1))

    def control(self, previous, player, seq, dx, dy, error=None, incarnation=None, epoch=None):
        payload = unsigned_field(1, previous["incarnation"] if incarnation is None else incarnation)
        payload += unsigned_field(2, previous["players"][player]["epoch"] if epoch is None else epoch)
        payload += unsigned_field(3, seq)
        payload += unsigned_field(4, (dx << 1) ^ (dx >> 31))
        payload += unsigned_field(5, (dy << 1) ^ (dy >> 31))
        raw = self.rpc(50, payload, 51, error)
        if error is not None:
            return None
        reply = parse(raw)
        return snapshot(scalar(reply, 1)), scalar(reply, 2)


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp, ws = run_host(binary, "room")
        destinations = [(tcp, False), (ws, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-room-", suffix=".log", delete=False)
        environment = os.environ.copy()
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
        environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "room"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment)

        def content():
            log.flush()
            with open(log.name, errors="replace") as source:
                return source.read()

        destinations = [(port, False)]
    peers = []
    passed = False

    def connect(destination):
        peer = RoomPeer(destination)
        peers.append(peer)
        return peer

    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "room host startup")
        baseline = fd_count(process)
        anonymous = connect(destinations[0])
        anonymous.room_snapshot(error=1003)
        anonymous.rpc(50, b"", error=1003)
        anonymous.close()
        alice = connect(destinations[0])
        login = alice.login()
        alice.room_snapshot(error=2001)
        bob = connect(destinations[-1])
        bob_login = bob.login(b"bob")
        alice.join(7001)
        bob.join(7001)
        initial = alice.room_snapshot()
        assert initial["room"] == 7001 and list(initial["players"]) == [42, 43]
        assert initial["players"][42]["session"] == login["session"]
        assert initial["players"][43]["session"] == bob_login["session"]
        assert all(p["seq"] == 0 for p in initial["players"].values())

        # No client traffic during this interval: empty ticks must still advance.
        time.sleep(0.22)
        advancing = alice.room_snapshot()
        assert advancing["tick"] >= initial["tick"] + 3, (initial, advancing)
        assert advancing["players"] == initial["players"]

        first, status = alice.control(initial, 42, 1, 1, -1)
        assert status == 1 and (first["players"][42]["x"], first["players"][42]["y"]) == (1, -1)
        duplicate, status = alice.control(first, 42, 1, 1, -1)
        assert status == 2 and duplicate["players"] == first["players"]
        gap, status = alice.control(first, 42, 3, 1, 0)
        assert status == 3 and gap["players"][42]["seq"] == 1
        _, status = alice.control(first, 42, 1, 0, 1)
        assert status == 5
        second, status = alice.control(first, 42, 2, 0, 1)
        assert status == 1 and second["players"][42]["seq"] == 2
        _, status = alice.control(second, 42, 1, 1, -1)
        assert status == 4
        _, status = alice.control(second, 42, 3, 2, 0)
        assert status == 7
        _, status = alice.control(second, 42, 0, 0, 0)
        assert status == 7
        bob_state, status = bob.control(second, 43, 1, 0, 1)
        assert status == 1

        with ThreadPoolExecutor(max_workers=2) as executor:
            a = executor.submit(alice.control, bob_state, 42, 3, 1, 0)
            b = executor.submit(bob.control, bob_state, 43, 2, -1, 0)
            assert a.result()[1] == b.result()[1] == 1
        both = alice.room_snapshot()
        assert both["players"][42]["seq"] == 3 and both["players"][43]["seq"] == 2

        # Leave/rejoin is a new membership even within the same logical session.
        alice.leave()
        alice.join(7001)
        rejoined = alice.room_snapshot()
        assert rejoined["incarnation"] == initial["incarnation"]
        assert rejoined["players"][42]["epoch"] > initial["players"][42]["epoch"]
        assert rejoined["players"][42]["seq"] == 0
        alice.control(rejoined, 42, 4, 1, 0, error=2001, epoch=initial["players"][42]["epoch"])
        reset, status = alice.control(rejoined, 42, 1, -1, 0)
        assert status == 1

        # When the roster becomes empty, a later actor must have another identity.
        alice.leave()
        bob.leave()
        time.sleep(0.12)
        alice.join(7001)
        replacement = alice.room_snapshot()
        assert replacement["incarnation"] != initial["incarnation"]
        alice.control(replacement, 42, 1, 1, 0, error=2001, incarnation=initial["incarnation"])
        active, status = alice.control(replacement, 42, 1, 1, 0)
        assert status == 1

        # A physical reconnect preserves actor, membership generation and sequence.
        alice.close(reset=mode == "ws" or workers == 4)
        recovered = connect(destinations[-1])
        recovery = recovered.login(token=login["token"])
        assert recovery["resumed"] and recovery["session"] == login["session"] and recovery["room"] == 7001
        restored = recovered.room_snapshot()
        assert restored["incarnation"] == active["incarnation"]
        assert restored["players"][42] == active["players"][42]
        final, status = recovered.control(restored, 42, 2, 0, 1)
        assert status == 1 and final["players"][42]["seq"] == 2

        for peer in peers:
            peer.close()
        wait_until(process, lambda: fd_count(process) <= baseline, "room descriptors retired")
        stop_host(process, log)
        with open(log.name, errors="replace") as source:
            output = source.read()
        assert process.returncode == 0, output
        assert "ONLINE_COUNTS active=0 retained=0 rooms=0" in output, output
        counts = re.search(r"ROOM_COUNTS actors=(\d+) ticks=(\d+) applied=(\d+) rejected=(\d+) duplicates=(\d+) skipped_wall_slots=(\d+)", output)
        assert counts, output
        values = list(map(int, counts.groups()))
        assert values[0] == 0 and values[1] >= 10 and values[2] >= 7 and values[4] >= 1, values
        for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:", "runtime error:",
                           "UndefinedBehaviorSanitizer", "SanitizerTool: CHECK failed"):
            assert diagnostic not in output, output
        passed = True
        print(f"20Hz empty ticks, sequence fences, ABA/JWT recovery, independent state checksum passed ({mode}, workers={workers})")
    finally:
        for peer in peers:
            peer.close()
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("room server log:", log.name, file=sys.stderr)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
