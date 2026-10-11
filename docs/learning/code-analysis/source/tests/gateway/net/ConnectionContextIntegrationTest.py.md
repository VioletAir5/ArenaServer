# tests/gateway/net/ConnectionContextIntegrationTest.py

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；97行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~python
"""Verify context metadata, owner affinity and session isolation on real sockets."""
import os
import socket
import sys
import time

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port
from WebSocketIntegrationTest import run_host, stop_host, upgrade, send_frame, read_frame
import signal
import subprocess
import tempfile


def parse(response, peer, worker):
    fields = response.decode().strip().split("|")
    assert len(fields) == 6, fields
    identity, ip, port, age, foreign, state = fields
    assert int(identity) > 0 and ip == peer.getsockname()[0], fields
    assert int(port) == peer.getsockname()[1], fields
    assert int(age) >= 0 and int(foreign) == worker, fields
    return int(identity), int(state)


def command(peer, value, ws, worker):
    if ws:
        send_frame(peer, 2, value)
        opcode, response = read_frame(peer)
        assert opcode == 2
    else:
        peer.sendall(value)
        response = bytearray()
        while not response.endswith(b"\n"):
            chunk = peer.recv(1024)
            assert chunk, "unexpected EOF"
            response.extend(chunk)
    return parse(response, peer, worker)


def check(port, ws, worker):
    def connect():
        if ws:
            peer, response = upgrade(port)
            assert response.startswith(b"HTTP/1.1 101")
            return peer
        peer = socket.create_connection(("127.0.0.1", port), 5)
        peer.settimeout(5)
        return peer
    with connect() as first, connect() as second:
        first_id, state = command(first, b"Q", ws, worker)
        assert state == 0
        second_id, state = command(second, b"Q", ws, worker)
        assert state == 0 and second_id != first_id
        assert command(first, b"A", ws, worker) == (first_id, 1)
        assert command(second, b"Q", ws, worker) == (second_id, 0)
        assert command(second, b"E", ws, worker) == (second_id, 2)
        assert command(second, b"A", ws, worker) == (second_id, 2)
        assert command(first, b"E", ws, worker) == (first_id, 1)


def run(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "context")
        try:
            check(tcp_port, False, 0)
            check(ws_port, True, 0)
        finally:
            stop_host(process, log)
            os.unlink(log.name)
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-context-", suffix=".log", delete=False)
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "context"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        passed = False
        try:
            deadline = time.monotonic() + 8
            while True:
                with open(log.name, errors="replace") as stream:
                    output = stream.read()
                if "TEST_HOST_READY" in output:
                    break
                assert process.poll() is None and time.monotonic() < deadline, output
                time.sleep(.02)
            check(port, False, int(workers != 0))
            passed = True
        finally:
            stop_host(process, log)
            if passed:
                os.unlink(log.name)
    print("connection context peer, owner and session isolation passed")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: ConnectionContextIntegrationTest.py HOST tcp|ws WORKERS")
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
~~~
