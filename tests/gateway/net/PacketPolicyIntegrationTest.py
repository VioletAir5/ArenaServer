"""The same per-connection policy profile on epoll, Asio and WebSocket."""
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import (
    run_host, stop_host, upgrade, send_frame, read_frame, packet,
    read_tcp_packet, close_code,
)


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

    def echo(peer, payload):
        if ws:
            assert read_frame(peer) == (2, packet(payload, 2))
        else:
            assert read_tcp_packet(peer) == packet(payload, 2)

    def closed(peer):
        if ws:
            assert close_code(peer) == 1008
        else:
            try:
                assert peer.recv(1) == b"", "rejected packet reached business"
            except ConnectionResetError:
                pass

    with connect() as first, connect() as independent:
        # PacketCodec fragments can cross TCP reads or WS messages;
        # their count must not consume packet tokens.
        data = packet(b"fragmented")
        send(first, data[:5])
        send(independent, packet(b"independent"))
        echo(independent, b"independent")
        send(first, data[5:])
        echo(first, b"fragmented")
        send(first, packet(b"second"))
        echo(first, b"second")
        send(first, packet(b"excess"))
        closed(first)
        send(independent, packet(b"second independent"))
        echo(independent, b"second independent")

    with connect() as coalesced:
        send(coalesced, packet(b"one") + packet(b"two"))
        echo(coalesced, b"one")
        echo(coalesced, b"two")
        send(coalesced, packet(b"three"))
        closed(coalesced)

    # A valid outer response type must be denied by the packet allowlist.
    with connect() as denied:
        send(denied, packet(b"disallowed", 2))
        closed(denied)


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "features")
        passed = False
        try:
            baseline = fd_count(process)
            check(tcp_port)
            check(ws_port, True)
            wait_until(process, lambda: fd_count(process) == baseline, "policy FD recovery")
            stop_host(process, log)
            with open(log.name, errors="replace") as stream:
                output = stream.read()
            assert_policy_counts(output, 2)
            assert "code=1008" in output, output
            passed = True
        finally:
            stop_host(process, log)
            if passed:
                os.unlink(log.name)
            else:
                print("server log:", log.name, file=sys.stderr)
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-features-", suffix=".log", delete=False)
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "features"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        def content():
            log.flush()
            with open(log.name, errors="replace") as stream:
                return stream.read()
        passed = False
        try:
            wait_until(process, lambda: "TEST_HOST_READY" in content(), "policy host startup")
            baseline = fd_count(process)
            check(port)
            wait_until(process, lambda: fd_count(process) == baseline, "policy FD recovery")
            stop_host(process, log)
            with open(log.name, errors="replace") as stream:
                output = stream.read()
            assert_policy_counts(output, 1)
            passed = True
        finally:
            stop_host(process, log)
            if passed:
                os.unlink(log.name)
            else:
                print("server log:", log.name, file=sys.stderr)
    print("framed policy, per-connection isolation, rejection and FD recovery passed")


def assert_policy_counts(output, transports):
    match = re.search(r"POLICY_COUNTS packet_rate_exceeded=(\d+) packet_type_not_allowed=(\d+)", output)
    assert match, output
    assert tuple(map(int, match.groups())) == (2 * transports, transports), output


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
