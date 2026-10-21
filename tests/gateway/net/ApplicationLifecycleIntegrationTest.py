"""Exercise the actual application entry point, configuration and signal drain."""

import base64
import contextlib
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from WebSocketIntegrationTest import (
    ORIGIN, envelope_ping, exact, packet, packet_pong, read_frame,
    read_tcp_packet, send_frame,
)


def ports():
    with socket.socket() as tcp, socket.socket() as websocket:
        tcp.bind(("127.0.0.1", 0))
        websocket.bind(("127.0.0.1", 0))
        return tcp.getsockname()[1], websocket.getsockname()[1]


def environment(tcp_port, ws_port, **overrides):
    result = os.environ.copy()
    for key in ("ARENA_JWT_KEY", "ARENA_MAX_CONNECTIONS",
                "ARENA_MAX_CONNECTIONS_PER_IP", "ARENA_TCP_PORT", "ARENA_WS_PORT",
                "ARENA_DISCONNECT_GRACE_MS"):
        result.pop(key, None)
    result.update(ARENA_TCP_PORT=str(tcp_port), ARENA_WS_PORT=str(ws_port))
    result.update(overrides)
    return result


def contents(log):
    log.flush()
    log.seek(0)
    return log.read().decode(errors="replace")


@contextlib.contextmanager
def application(binary, backend, protocol="raw", **overrides):
    tcp_port, ws_port = ports()
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(
            [binary, backend, protocol], stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT,
            env=environment(tcp_port, ws_port, **overrides))
        try:
            yield process, log, tcp_port, ws_port
        except BaseException:
            print(contents(log), file=sys.stderr)
            raise
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def connect_ready(process, port):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        assert process.poll() is None, f"application exited {process.returncode} before startup"
        try:
            peer = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            peer.settimeout(5)
            return peer
        except (ConnectionRefusedError, TimeoutError):
            time.sleep(0.01)
    raise AssertionError("application listener did not start")


def stopped(process, log):
    assert process.wait(timeout=8) == 0, contents(log)
    output = contents(log)
    counts = re.search(
        r"connection_admission active=(\d+) peak=(\d+) rejected=(\d+) "
        r"global=(\d+) per_ip=(\d+) local=(\d+)", output)
    assert counts, output
    values = tuple(int(value) for value in counts.groups())
    assert values[0] == 0, output
    assert "AddressSanitizer" not in output and "runtime error:" not in output, output
    return values


def raw_shutdown(binary, backend):
    for shutdown_signal in (signal.SIGTERM, signal.SIGINT):
        with application(binary, backend) as (process, log, tcp_port, _):
            with connect_ready(process, tcp_port) as peer:
                peer.sendall(b"application-lifecycle")
                assert exact(peer, 21) == b"application-lifecycle"
                process.send_signal(shutdown_signal)
                assert peer.recv(1) == b"", "stop did not drain and send FIN"
            values = stopped(process, log)
            assert values[1] == 1 and values[2] == 0, values


def invalid_configuration(binary):
    invalid = {
        "ARENA_MAX_CONNECTIONS": ("0", "-1", "12x", "18446744073709551616"),
        "ARENA_MAX_CONNECTIONS_PER_IP": ("0", "", " 2"),
        "ARENA_TCP_PORT": ("0", "65536", "-1", "abc"),
        "ARENA_WS_PORT": ("0", "65536", "2.5"),
        "ARENA_DISCONNECT_GRACE_MS": ("0", "-1", "1x", "3600001", ""),
    }
    for key, values in invalid.items():
        for value in values:
            with application(binary, "epoll", **{key: value}) as (process, log, _, _):
                assert process.wait(timeout=5) == 2, (key, value, contents(log))
                assert key in contents(log), contents(log)
    with application(binary, "asio", "handshake", ARENA_TCP_PORT="12345",
                     ARENA_WS_PORT="12345") as (process, log, _, _):
        assert process.wait(timeout=5) == 2, contents(log)
        assert "must differ" in contents(log), contents(log)


def websocket_upgrade(peer, port):
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        "GET /ws HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
        f"Origin: {ORIGIN}\r\n\r\n")
    response = bytearray()
    try:
        peer.sendall(request.encode())
        while not response.endswith(b"\r\n\r\n"):
            chunk = peer.recv(1)
            if not chunk:
                break
            response.extend(chunk)
            assert len(response) < 8192
    except (ConnectionResetError, BrokenPipeError):
        pass
    return bytes(response)


def shared_tcp_websocket(binary):
    with application(binary, "asio", "handshake", ARENA_MAX_CONNECTIONS="2",
                     ARENA_MAX_CONNECTIONS_PER_IP="1") as (process, log, tcp_port, ws_port):
        with connect_ready(process, tcp_port) as tcp:
            tcp.sendall(packet(envelope_ping(101)))
            assert read_tcp_packet(tcp) == packet_pong(101)
            with socket.create_connection(("127.0.0.1", ws_port), timeout=5) as denied:
                denied.settimeout(5)
                response = websocket_upgrade(denied, ws_port)
                assert not response.startswith(b"HTTP/1.1 101"), response
            tcp.shutdown(socket.SHUT_WR)
            assert tcp.recv(1) == b""

        # A successful new upgrade proves that releasing TCP returns the same IP lease.
        deadline = time.monotonic() + 5
        websocket = None
        while time.monotonic() < deadline:
            candidate = socket.create_connection(("127.0.0.1", ws_port), timeout=5)
            candidate.settimeout(5)
            if websocket_upgrade(candidate, ws_port).startswith(b"HTTP/1.1 101"):
                websocket = candidate
                break
            candidate.close()
            time.sleep(0.01)
        assert websocket is not None, "TCP disconnect did not release shared IP capacity"
        with websocket:
            send_frame(websocket, 2, packet(envelope_ping(202)))
            assert read_frame(websocket) == (2, packet_pong(202))
            process.send_signal(signal.SIGTERM)
            opcode, payload = read_frame(websocket)
            assert opcode == 8, (opcode, payload)
            send_frame(websocket, 8, payload)
            assert websocket.recv(1) == b""
        values = stopped(process, log)
        assert values[1] == 1 and values[4] >= 1, values


def authenticated_shutdown(binary):
    # An active management timer must stop without keeping io_context alive.
    with application(binary, "asio", "handshake",
                     ARENA_JWT_KEY="0123456789abcdef0123456789abcdef",
                     ARENA_DISCONNECT_GRACE_MS="1000") as (process, log, tcp_port, _):
        with connect_ready(process, tcp_port) as peer:
            peer.sendall(packet(envelope_ping(303)))
            assert read_tcp_packet(peer) == packet_pong(303)
            process.send_signal(signal.SIGTERM)
            assert peer.recv(1) == b""
        stopped(process, log)


if __name__ == "__main__":
    binary, scenario = sys.argv[1:]
    if scenario in ("epoll", "asio"):
        raw_shutdown(binary, scenario)
    elif scenario == "invalid":
        invalid_configuration(binary)
    elif scenario == "shared_ws":
        shared_tcp_websocket(binary)
    elif scenario == "authenticated_shutdown":
        authenticated_shutdown(binary)
    else:
        raise AssertionError(f"unknown scenario: {scenario}")
    print(f"Application lifecycle {scenario} passed")
