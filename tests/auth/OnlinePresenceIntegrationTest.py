"""Stable session/room recovery and traffic-independent grace reclamation."""

import os
import re
import socket
import struct
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway", "net"))
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import (
    run_host, stop_host, upgrade, send_frame, read_message, packet, read_tcp_packet, varint,
)
from MessageDispatchIntegrationTest import envelope


def parse(data):
    result = {}
    offset = 0

    def number():
        nonlocal offset
        value, shift = 0, 0
        while offset < len(data) and shift < 70:
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7
        raise AssertionError("invalid protobuf varint")

    while offset < len(data):
        key = number()
        assert key >> 3
        if key & 7 == 0:
            value = number()
        else:
            assert key & 7 == 2, key
            size = number()
            assert offset + size <= len(data)
            value = data[offset:offset + size]
            offset += size
        result.setdefault(key >> 3, []).append(value)
    return result


def scalar(fields, key, default=0):
    values = fields.get(key, [default])
    assert len(values) == 1, (key, values)
    return values[0]


def text(tag, value):
    return varint((tag << 3) | 2) + varint(len(value)) + value


def state(data):
    values = parse(data)
    members = {}
    for raw in values.get(5, []):
        member = parse(raw)
        player = scalar(member, 1)
        assert player not in members, "duplicate player in room roster"
        members[player] = (scalar(member, 2), scalar(member, 3))
    return dict(session=scalar(values, 1), player=scalar(values, 2),
                room=scalar(values, 3), presence=scalar(values, 4), members=members)


class Peer:
    def __init__(self, destination):
        port, self.ws = destination
        if self.ws:
            self.socket, response = upgrade(port)
            assert response.startswith(b"HTTP/1.1 101"), response
        else:
            self.socket = socket.create_connection(("127.0.0.1", port), 5)
        self.socket.settimeout(5)
        self.request_id = 0
        self.closed = False

    def rpc(self, msg, payload, response_msg=None, error=None):
        self.request_id += 1
        frame = packet(envelope(msg, self.request_id, msg, payload))
        if self.ws:
            send_frame(self.socket, 2, frame)
            opcode, frame = read_message(self.socket)
            assert opcode == 2, (opcode, frame)
        else:
            self.socket.sendall(frame)
            frame = read_tcp_packet(self.socket)
        magic, length, kind, wire_id, version = struct.unpack("!IIHIB", frame[:15])
        assert (magic, kind, wire_id, version) == (0x4152454E, 2, 7, 1)
        assert length == len(frame) - 8
        reply = parse(frame[15:])
        assert scalar(reply, 1) == self.request_id, reply
        if error is not None:
            assert scalar(reply, 4) == 14, reply
            assert scalar(parse(scalar(reply, 14)), 1) == error, reply
            return
        assert scalar(reply, 4) == response_msg, reply
        return scalar(reply, response_msg)

    def login(self, account=b"alice", token=None, error=None):
        if token is None:
            payload = text(1, account) + text(2, b"test-password")
        else:
            payload = text(2, token) + b"\x20\x01"
        raw = self.rpc(10, payload, 11, error)
        if error is not None:
            return
        reply = parse(raw)
        assert scalar(reply, 1) == 1
        return dict(token=scalar(reply, 2), player=scalar(reply, 4),
                    epoch=scalar(reply, 5), session=scalar(reply, 6),
                    room=scalar(reply, 7), resumed=scalar(reply, 8))

    def query(self):
        return state(self.rpc(20, b"", 21))

    def join(self, room, error=None):
        raw = self.rpc(30, b"\x08" + varint(room), 31, error)
        return None if error is not None else state(scalar(parse(raw), 1))

    def leave(self):
        return state(scalar(parse(self.rpc(32, b"", 33)), 1))

    def close(self, reset=False):
        if self.closed:
            return
        self.closed = True
        if reset:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.socket.close()


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "presence")
        destinations = [(tcp_port, False), (ws_port, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-presence-", suffix=".log", delete=False)
        environment = os.environ.copy()
        environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
        environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "presence"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment)

        def content():
            log.flush()
            with open(log.name, errors="replace") as source:
                return source.read()

        destinations = [(port, False)]
    peers = []
    passed = False

    def connect(destination):
        peer = Peer(destination)
        peers.append(peer)
        return peer

    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "presence host startup")
        baseline = fd_count(process)
        alice = connect(destinations[0])
        original = alice.login()
        assert original["player"] == 42 and original["session"] > 0
        assert original["room"] == 0 and not original["resumed"]
        bob = connect(destinations[-1])
        observer = bob.login(b"bob")
        assert observer["player"] == 43 and observer["session"] != original["session"]

        joined = alice.join(7001)
        assert joined["session"] == original["session"] and joined["room"] == 7001
        alice.join(0, error=1002)
        assert alice.query()["room"] == 7001, "invalid join changed the old binding"
        bob.join(7001)
        assert bob.query()["members"] == {
            42: (original["session"], 1), 43: (observer["session"], 1)}
        stranger = connect(destinations[0])
        stranger.rpc(20, b"", error=1003)
        stranger.join(7001, error=1003)
        stranger.close()

        # EOF/RST detaches without erasing the logical session or room slot.
        alice.close(reset=mode == "ws" or workers == 4)

        def detached():
            members = bob.query()["members"]
            return members.get(42) == (original["session"], 2)

        wait_until(process, detached, "observer sees detached player within grace")
        restored = connect(destinations[-1])
        recovery = restored.login(token=original["token"])
        assert recovery["resumed"] and recovery["session"] == original["session"]
        assert recovery["room"] == 7001 and recovery["epoch"] > original["epoch"]
        assert restored.query()["members"] == {
            42: (original["session"], 1), 43: (observer["session"], 1)}

        # Live token handoff plus old physical close must not detach the winner.
        handoff = connect(destinations[0])
        current = handoff.login(token=recovery["token"])
        assert current["session"] == original["session"] and current["room"] == 7001
        assert current["epoch"] > recovery["epoch"]
        restored.close(reset=True)
        old_token = connect(destinations[-1])
        old_token.login(token=original["token"], error=1003)
        old_token.close()
        assert bob.query()["members"][42] == (original["session"], 1)
        assert handoff.query()["session"] == original["session"]

        # The maintenance log must prove reclamation before any further RPC.
        marker = len(content())
        handoff.close(reset=workers == 0)
        wait_until(process, detached, "second detach keeps the current room slot")
        wait_until(process, lambda: re.search(
            r"event=online_sweep active=1 retained=1 rooms=1", content()[marker:]),
            "traffic-independent grace reclamation")
        after = bob.query()
        assert after["members"] == {43: (observer["session"], 1)}, after
        expired = connect(destinations[-1])
        expired.login(token=current["token"], error=1003)
        expired.close()

        fresh = connect(destinations[0])
        new_session = fresh.login()
        assert new_session["session"] != original["session"] and new_session["room"] == 0
        assert not new_session["resumed"], new_session
        fresh.join(7002)
        left = fresh.leave()
        assert left["session"] == new_session["session"] and left["room"] == 0
        assert left["members"] == {}, left

        marker = len(content())
        bob.close()
        fresh.close(reset=True)
        wait_until(process, lambda: re.search(
            r"event=online_sweep active=0 retained=0 rooms=0", content()[marker:]),
            "all players and empty rooms reclaimed without more traffic")
        wait_until(process, lambda: fd_count(process) <= baseline, "presence descriptors retired")
        stop_host(process, log)
        assert process.returncode == 0, process.returncode
        with open(log.name, errors="replace") as source:
            output = source.read()
        assert "ONLINE_COUNTS active=0 retained=0 rooms=0" in output, output
        for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:", "runtime error:",
                           "UndefinedBehaviorSanitizer", "SanitizerTool: CHECK failed"):
            assert diagnostic not in output, output
        passed = True
        print(f"stable session/room, TCP/WS recovery, proactive grace expiry and zero hanging players passed ({mode}, workers={workers})")
    finally:
        for peer in peers:
            peer.close()
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("presence server log:", log.name, file=sys.stderr)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
