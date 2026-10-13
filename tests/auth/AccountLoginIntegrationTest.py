"""One hundred concurrent logins, stale traffic and JWT resume on real transports."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway", "net"))
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import (
    run_host, stop_host, upgrade, send_frame, read_frame, packet, read_tcp_packet, varint,
)
from MessageDispatchIntegrationTest import fields, envelope

KEY = b"0123456789abcdef0123456789abcdef"  # Same deterministic test-only profile.


class Closed(Exception):
    pass


class Peer:
    def __init__(self, port, ws):
        self.ws = ws
        if ws:
            self.socket, reply = upgrade(port)
            assert reply.startswith(b"HTTP/1.1 101"), reply
        else:
            self.socket = socket.create_connection(("127.0.0.1", port), 5)
        self.socket.settimeout(6)

    def send(self, payload):
        if self.ws:
            send_frame(self.socket, 2, packet(payload))
        else:
            self.socket.sendall(packet(payload))

    def reply(self):
        try:
            if self.ws:
                opcode, data = read_frame(self.socket)
                if opcode == 8:
                    raise Closed()
                assert opcode == 2, opcode
            else:
                data = read_tcp_packet(self.socket)
            assert len(data) >= 15
            return fields(data[15:])
        except AssertionError as error:
            if str(error) == "unexpected EOF":
                raise Closed() from error
            raise
        except (ConnectionResetError, BrokenPipeError) as error:
            raise Closed() from error

    def close(self):
        self.socket.close()


def text_field(tag, value):
    return varint((tag << 3) | 2) + varint(len(value)) + value


def login_request(token=None, password=b"test-password", request_id=1):
    if token is None:
        body = text_field(1, b"alice") + text_field(2, password)
    else:
        body = text_field(2, token) + b"\x20\x01"
    return envelope(10, request_id, 10, body)


def ping_request():
    return envelope(12, 2, 12, b"\x08\x09")


def signed_epoch(epoch):
    def b64(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")
    header = b64(b'{"alg":"HS256","typ":"JWT"}')
    body = b64(json.dumps(dict(iss="arena-auth", aud="arena-gateway", sub="42",
        iat=1700000000, nbf=1700000000, exp=1700000300, epoch=str(epoch)),
        separators=(",", ":")).encode())
    message = header + b"." + body
    return message + b"." + b64(hmac.new(KEY, message, hashlib.sha256).digest())


def check_issued(token, epoch):
    head, body, signature = token.split(b".")
    decode = lambda value: base64.urlsafe_b64decode(value + b"=" * (-len(value) % 4))
    assert hmac.compare_digest(decode(signature), hmac.new(KEY, head + b"." + body, hashlib.sha256).digest())
    assert json.loads(decode(head)) == {"alg": "HS256", "typ": "JWT"}
    claims = json.loads(decode(body))
    assert claims == dict(iss="arena-auth", aud="arena-gateway", sub="42",
        iat=1700000000, nbf=1700000000, exp=1700000300, epoch=str(epoch)), claims


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "auth")
        destinations = [(tcp_port, False), (ws_port, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-auth-", suffix=".log", delete=False)
        env = os.environ.copy()
        env.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
        env.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "auth"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env)
        def content():
            log.flush()
            with open(log.name, errors="replace") as stream:
                return stream.read()

        destinations = [(port, False)]
    peers = []
    passed = False
    try:
        if mode != "ws":
            wait_until(process, lambda: "TEST_HOST_READY" in content(), "auth host ready")
        baseline = fd_count(process)
        for i in range(100):
            peers.append(Peer(*destinations[i % len(destinations)]))
        barrier = threading.Barrier(100)
        def race(peer):
            barrier.wait(timeout=10)
            peer.send(login_request())
            try:
                reply = peer.reply()
            except Closed:
                return None
            # A superseded in-flight login may be suppressed, never accepted as current.
            if reply.get(4) == 14:
                assert fields(reply[14])[1] == 1003, reply
                return None
            assert reply[4] == 11 and reply[1] == 1, reply
            result = fields(reply[11])
            assert result[1] == 1 and result[4] == 42, result
            check_issued(result[2], result[5])
            return result
        with ThreadPoolExecutor(max_workers=100) as pool:
            replies = list(pool.map(race, peers))
        installs = lambda: re.findall(r"event=session_install player_id=42 connection_id=\d+ epoch=(\d+)", content())
        wait_until(process, lambda: len(installs()) == 100, "all 100 logins reach session replacement")
        epochs = sorted(map(int, installs()))
        assert epochs == list(range(epochs[0], epochs[0] + 100)), epochs
        winner_epoch = epochs[-1]
        winners = [i for i, reply in enumerate(replies) if reply and reply[5] == winner_epoch]
        assert len(winners) == 1, winners
        winner = winners[0]
        def stale(index):
            if index == winner:
                return
            peer = peers[index]
            try:
                # Both handshake packets and attempts to re-login on an old socket are invalid.
                peer.send(ping_request())
                peer.send(login_request(request_id=4))
                peer.reply()
            except (Closed, ConnectionResetError, BrokenPipeError):
                return
            raise AssertionError("superseded connection produced a reply")
        with ThreadPoolExecutor(max_workers=100) as pool:
            list(pool.map(stale, range(100)))
        peers[winner].send(ping_request())
        assert fields(peers[winner].reply()[13])[1] == 9
        assert len(installs()) == 100, installs()

        # Cryptographically valid token with an old epoch must not replace the winner.
        invalid = Peer(*destinations[-1])
        peers.append(invalid)
        for token in (signed_epoch(epochs[0]), signed_epoch(winner_epoch)[:-1] + b"!"):
            invalid.send(login_request(token, request_id=5))
            assert fields(invalid.reply()[14])[1] == 1003
        assert len(installs()) == 100
        peers[winner].send(ping_request())
        assert fields(peers[winner].reply()[13])[1] == 9

        resumed = Peer(*destinations[-1])
        peers.append(resumed)
        resumed.send(login_request(replies[winner][2], request_id=6))
        grant = fields(resumed.reply()[11])
        assert grant[1] == 1 and grant[5] == winner_epoch + 1, grant
        check_issued(grant[2], winner_epoch + 1)
        try:
            peers[winner].send(ping_request())
            peers[winner].reply()
        except (Closed, ConnectionResetError, BrokenPipeError):
            pass
        else:
            raise AssertionError("JWT resume did not revoke the old connection")
        resumed.close()
        # A disconnected current token can recover its retained authorization record.
        recovered = Peer(*destinations[0])
        peers.append(recovered)
        recovered.send(login_request(grant[2], request_id=7))
        final = fields(recovered.reply()[11])
        assert final[1] == 1 and final[5] == winner_epoch + 2
        recovered.send(ping_request())
        assert fields(recovered.reply()[13])[1] == 9
        for peer in peers:
            peer.close()
        wait_until(process, lambda: fd_count(process) <= baseline, "all authentication FDs released")
        stop_host(process, log)
        assert process.returncode == 0, process.returncode
        with open(log.name, errors='replace') as stream:
            output = stream.read()
        for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:", "runtime error:",
                           "UndefinedBehaviorSanitizer", "SanitizerTool CHECK failed"):
            assert diagnostic not in output, output
        assert "test-password" not in output and signed_epoch(epochs[0]).decode() not in output
        passed = True
        print(f"100 concurrent logins -> 1 active; 99 stale peers rejected; JWT epoch/resume passed ({mode}, workers={workers})")
    finally:
        for peer in peers:
            peer.close()
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("authentication server log:", log.name, file=sys.stderr)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
