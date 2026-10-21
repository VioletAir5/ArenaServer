"""Real TCP and WebSocket pre-login handshake and WebSocket limit checks."""
import base64
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port

MAGIC = 0x4152454E
ORIGIN = "http://localhost:3000"


def exact(peer, count):
    data = bytearray()
    while len(data) < count:
        chunk = peer.recv(count - len(data))
        if not chunk:
            raise AssertionError("unexpected EOF")
        data.extend(chunk)
    return bytes(data)


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def envelope_ping(nonce):
    ping = b"\x08" + varint(nonce)
    return b"\x08\x2a\x62" + varint(len(ping)) + ping


def packet(payload, kind=1):
    return struct.pack("!IIHIB", MAGIC, len(payload) + 7, kind, 7, 1) + payload


def packet_pong(nonce):
    pong = b"\x08" + varint(nonce)
    return packet(b"\x08\x2a\x6a" + varint(len(pong)) + pong, 2)


def read_tcp_packet(peer):
    prefix = exact(peer, 8)
    magic, length = struct.unpack("!II", prefix)
    return prefix + exact(peer, length) if magic == MAGIC else b""


def upgrade(port, origin=ORIGIN):
    peer = socket.create_connection(("127.0.0.1", port), 5)
    peer.settimeout(8)
    key = base64.b64encode(os.urandom(16)).decode()
    request = ("GET /ws HTTP/1.1\r\n"
               f"Host: 127.0.0.1:{port}\r\n"
               "Upgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\n"
               "Sec-WebSocket-Version: 13\r\n"
               f"Origin: {origin}\r\n\r\n")
    peer.sendall(request.encode())
    reply = bytearray()
    while not reply.endswith(b"\r\n\r\n"):
        reply.extend(exact(peer, 1))
        assert len(reply) < 8192
    return peer, bytes(reply)


def send_frame(peer, opcode, payload):
    header = bytearray([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length <= 65535:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    mask = os.urandom(4)
    header.extend(mask)
    peer.sendall(header + bytes(value ^ mask[i % 4]
                                 for i, value in enumerate(payload)))


def read_frame(peer, include_fin=False):
    first, second = exact(peer, 2)
    length = second & 127
    if length == 126:
        length = struct.unpack("!H", exact(peer, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", exact(peer, 8))[0]
    assert not second & 0x80, "server frame must be unmasked"
    assert length <= 1024 * 1024
    result = first & 15, exact(peer, length)
    return (*result, bool(first & 0x80)) if include_fin else result


def read_message(peer):
    opcode, payload, final = read_frame(peer, include_fin=True)
    chunks = [payload]
    total = len(payload)
    while not final:
        continuation, payload, final = read_frame(peer, include_fin=True)
        assert continuation == 0, "fragmented message requires continuation frames"
        chunks.append(payload)
        total += len(payload)
        assert total <= 1024 * 1024
    return opcode, b"".join(chunks)


def close_code(peer):
    while True:
        opcode, payload = read_frame(peer)
        if opcode == 8:
            assert len(payload) >= 2
            code = struct.unpack("!H", payload[:2])[0]
            try:
                send_frame(peer, 8, payload[:2])
            except OSError:
                pass  # The server may close TCP after sending its close code.
            return code


def run_host(binary, mode):
    tcp_port, ws_port = reserve_port(), reserve_port()
    while ws_port == tcp_port:
        ws_port = reserve_port()
    log = tempfile.NamedTemporaryFile(prefix="arena-ws-", suffix=".log",
                                      delete=False)
    process = subprocess.Popen([binary, str(tcp_port), str(ws_port), ORIGIN, mode],
                               stdin=subprocess.DEVNULL, stdout=log,
                               stderr=log)
    def content():
        if not log.closed:
            log.flush()
        with open(log.name, errors="replace") as stream:
            return stream.read()
    deadline = time.monotonic() + 8
    while "WS_TEST_READY" not in content():
        if process.poll() is not None or time.monotonic() > deadline:
            raise AssertionError("WebSocket test host failed: " + content())
        time.sleep(.02)
    return process, log, content, tcp_port, ws_port


def stop_host(process, log):
    if process.poll() is None:
        process.send_signal(signal.SIGUSR1)
        try:
            assert process.wait(timeout=8) == 0
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
    log.close()


def graceful_lifecycle(binary):
    for mode in ("drain", "stop_drain"):
        process, log, content, _, ws_port = run_host(binary, mode)
        try:
            peer, response = upgrade(ws_port)
            with peer:
                assert response.startswith(b"HTTP/1.1 101")
                send_frame(peer, 2, b"go")
                for index in range(6):
                    expected = bytes([ord("a") + index]) * (96 * 1024)
                    assert read_message(peer) == (2, expected), (mode, index, content())
                assert close_code(peer) == 1000, content()
        finally:
            stop_host(process, log)
            assert "WS_TEST_STOPPED connections=0 active=0" in content(), content()
            os.unlink(log.name)

    process, log, content, _, ws_port = run_host(binary, "deadline")
    try:
        peer, response = upgrade(ws_port)
        with peer:
            assert response.startswith(b"HTTP/1.1 101")
            started = time.monotonic()
            process.send_signal(signal.SIGUSR1)
            # Read the server Close, but intentionally never acknowledge it.
            opcode, payload = read_frame(peer)
            assert opcode == 8 and struct.unpack("!H", payload[:2])[0] == 1000
            assert process.wait(timeout=3) == 0, content()
            elapsed = time.monotonic() - started
            assert .15 <= elapsed < 3, (elapsed, content())
        assert "WS_TEST_STOPPED connections=0 active=0" in content(), content()
    finally:
        stop_host(process, log)
        os.unlink(log.name)

    process, log, content, _, ws_port = run_host(binary, "destroy_drain")
    try:
        peer, response = upgrade(ws_port)
        with peer:
            assert response.startswith(b"HTTP/1.1 101")
            started = time.monotonic()
            process.send_signal(signal.SIGUSR1)
            assert process.wait(timeout=2) == 0, content()
            assert time.monotonic() - started < 2
        assert "WS_TEST_STOPPED connections=0 active=0" in content(), content()
    finally:
        stop_host(process, log)
        os.unlink(log.name)


def run(binary):
    process, log, content, tcp_port, ws_port = run_host(binary, "handshake")
    try:
        with socket.create_connection(("127.0.0.1", tcp_port), 5) as peer:
            peer.settimeout(5)
            peer.sendall(packet(envelope_ping(123)))
            assert read_tcp_packet(peer) == packet_pong(123)
        peer, response = upgrade(ws_port)
        with peer:
            assert response.startswith(b"HTTP/1.1 101")
            send_frame(peer, 2, packet(envelope_ping(456)))
            assert read_frame(peer) == (2, packet_pong(456))
            send_frame(peer, 8, struct.pack("!H", 1000))
            assert close_code(peer) == 1000
        for origin in ("https://untrusted.example", ""):
            denied, response = upgrade(ws_port, origin)
            with denied:
                assert response.startswith(b"HTTP/1.1 403")
        peer, response = upgrade(ws_port)
        with peer:
            assert response.startswith(b"HTTP/1.1 101")
            send_frame(peer, 1, b"text not allowed")
            assert close_code(peer) == 1003
        peer, response = upgrade(ws_port)
        with peer:
            send_frame(peer, 2, b"x" * (256 * 1024 + 9))
            assert close_code(peer) == 1009
        peer, response = upgrade(ws_port)
        with peer:
            send_frame(peer, 8, struct.pack("!H", 4001))
            assert close_code(peer) == 4001
        deadline = time.monotonic() + 3
        while "code=4001" not in content() and time.monotonic() < deadline:
            time.sleep(.02)
        output = content()
        assert "code=1003" in output and "code=4001" in output, output
        assert "code=1009" in output, output
    finally:
        stop_host(process, log)
        os.unlink(log.name)

    process, log, content, _, ws_port = run_host(binary, "flood")
    try:
        peer, response = upgrade(ws_port)
        with peer:
            assert response.startswith(b"HTTP/1.1 101")
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            send_frame(peer, 2, b"go")
            time.sleep(.2)
            assert close_code(peer) == 1008
        match = re.search(r"WebSocket output limit id=\d+ pending=(\d+)", content())
        assert match and int(match.group(1)) <= 1024 * 1024, content()
    finally:
        stop_host(process, log)
        os.unlink(log.name)
    graceful_lifecycle(binary)
    print("TCP/WS pre-login, limits, output drain, close deadline and destructor cleanup passed")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: WebSocketIntegrationTest.py HOST")
    run(sys.argv[1])
