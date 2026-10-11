# tests/gateway/net/SecurityPolicyIntegrationTest.py

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；153行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~python
"""Real owners must reject bursts before decode, guard routing, and retain counts."""
import os
import re
import socket
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import (
    run_host, stop_host, upgrade, send_frame, read_frame, packet,
    read_tcp_packet, close_code, envelope_ping, packet_pong, varint,
)


def login(credential):
    body = b"\x0a\x09test-user" + b"\x12" + varint(len(credential)) + credential
    return b"\x52" + varint(len(body)) + body


def check(port, ws=False):
    def connect():
        if ws:
            peer, response = upgrade(port)
            assert response.startswith(b"HTTP/1.1 101")
            return peer
        peer = socket.create_connection(("127.0.0.1", port), 5)
        peer.settimeout(5)
        return peer

    def send(peer, data):
        if ws:
            send_frame(peer, 2, data)
        else:
            peer.sendall(data)

    def response(peer, expected):
        if ws:
            assert read_frame(peer) == (2, expected)
        else:
            assert read_tcp_packet(peer) == expected

    def closed(peer):
        if ws:
            # No business reply may precede the close frame.
            opcode, payload = read_frame(peer)
            assert opcode == 8 and payload[:2] == b"\x03\xf0", (opcode, payload)
            try:
                send_frame(peer, 8, payload[:2])
            except OSError:
                pass
        else:
            try:
                assert peer.recv(1) == b"", "unauthenticated packet reached router"
            except ConnectionResetError:
                pass

    # Third complete packet is malformed protobuf: rate wins before decode.
    with connect() as peer:
        send(peer, packet(envelope_ping(1)))
        response(peer, packet_pong(1))
        send(peer, packet(envelope_ping(2)))
        response(peer, packet_pong(2))
        send(peer, packet(b"\x62\x80") + packet(envelope_ping(3)))
        closed(peer)

    business = b"\x72\x02\x08\x2a"  # valid Error envelope, test router payload
    with connect() as peer:
        send(peer, packet(business))
        closed(peer)

    with connect() as peer:
        send(peer, packet(login(b"wrong")))
        closed(peer)

    # Only validated test credentials change the session, then routing works.
    with connect() as peer:
        send(peer, packet(login(b"test-only")))
        response(peer, packet(b"\x5a\x02\x08\x01", 2))
        send(peer, packet(business))
        response(peer, packet(business, 2))

    # Fragment count does not consume tokens; Ping leaves session pending.
    with connect() as peer:
        data = packet(envelope_ping(9))
        send(peer, data[:5])
        send(peer, data[5:])
        response(peer, packet_pong(9))
        send(peer, packet(business))
        closed(peer)

    with connect() as peer:
        send(peer, packet(b"\x62\x80"))
        closed(peer)
    with connect() as peer:
        send(peer, b"X" + packet(envelope_ping(1))[1:])
        closed(peer)
    with connect() as peer:
        send(peer, packet(envelope_ping(1), 2))
        closed(peer)


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "security")
        ports = [(tcp_port, False), (ws_port, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-security-", suffix=".log", delete=False)
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "security"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        def content():
            log.flush()
            with open(log.name, errors="replace") as stream:
                return stream.read()
        ports = [(port, False)]

    passed = False
    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "security host startup")
        baseline = fd_count(process)
        for port, ws in ports:
            check(port, ws)
        wait_until(process, lambda: fd_count(process) == baseline, "security FD recovery")
        stop_host(process, log)
        with open(log.name, errors="replace") as stream:
            output = stream.read()
        match = re.search(r"REJECTION_COUNTS (.*)", output)
        assert match, output
        counts = dict((key, int(value)) for key, value in
                      re.findall(r"(\w+)=(\d+)", match.group(1)))
        n = len(ports)
        expected = {
            "packet_rate_exceeded": n, "packet_type_not_allowed": n,
            "unauthenticated": 3 * n, "invalid_frame": n, "invalid_envelope": n,
            "handler_rejected": 0, "callback_failure": 0, "slow_client": 0,
            "routed": n,
        }
        assert counts == expected, (counts, expected, output)
        passed = True
    finally:
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("server log:", log.name, file=sys.stderr)
    print("early burst rejection, authentication gate, exact counters and FD recovery passed")


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
~~~
