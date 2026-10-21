"""Typed dispatch and request correlation through real TCP/WebSocket clients."""
import os
import re
import signal
import socket
import struct
import subprocess
import tempfile
import sys

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port, wait_until, fd_count
from WebSocketIntegrationTest import (
    run_host, stop_host, upgrade, send_frame, read_frame, packet,
    read_tcp_packet, varint,
)


def envelope(msg_id, request_id, tag, body):
    return b"\x08" + varint(request_id) + b"\x20" + varint(msg_id) + \
        varint((tag << 3) | 2) + varint(len(body)) + body


def fields(data):
    offset = 0
    def number():
        nonlocal offset
        value, shift = 0, 0
        while True:
            assert offset < len(data) and shift < 70
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7
    result = {}
    while offset < len(data):
        key = number()
        if key & 7 == 0:
            value = number()
        else:
            assert key & 7 == 2
            size = number()
            assert offset + size <= len(data)
            value = data[offset:offset + size]
            offset += size
        result[key >> 3] = value
    return result


def response(peer, ws):
    if ws:
        opcode, data = read_frame(peer)
        assert opcode == 2
    else:
        data = read_tcp_packet(peer)
    magic, size, kind, request_id, version = struct.unpack("!IIHIB", data[:15])
    assert magic == 0x4152454E and size == len(data) - 8
    assert kind == 2 and request_id == 7 and version == 1
    return fields(data[15:])


def run(binary):
    process, log, content, tcp_port, ws_port = run_host(binary, "handshake")
    passed = False
    try:
        for port, ws in ((tcp_port, False), (ws_port, True)):
            if ws:
                peer, upgrade_reply = upgrade(port)
                assert upgrade_reply.startswith(b"HTTP/1.1 101")
            else:
                peer = socket.create_connection(("127.0.0.1", port), 5)
                peer.settimeout(5)
            with peer:
                request = packet(envelope(999, 424242, 12, b"\x08\x1f"))
                if ws:
                    send_frame(peer, 2, request)
                else:
                    peer.sendall(request)
                reply = response(peer, ws)
                assert reply[1] == 424242 and reply[4] == 14, reply
                error = fields(reply[14])
                assert error[1] == 1001 and error[2] == b"unknown message", error
        stop_host(process, log)
        with open(log.name, errors="replace") as stream:
            output = stream.read()
        starts = re.findall(r"event=request_start connection_id=(\d+) request_id=424242 msg_id=999", output)
        ends = re.findall(r"event=request_end connection_id=(\d+) request_id=424242 msg_id=999 code=1001", output)
        assert len(starts) == 2 and sorted(starts) == sorted(ends), output
        passed = True
    finally:
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("server log:", log.name, file=sys.stderr)
    print("TCP/WS typed unknown-message reply and request trace passed")



def run_profile(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "rpc")
        ports = [(tcp_port, False), (ws_port, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-rpc-", suffix=".log", delete=False)
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "rpc"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        def content():
            log.flush()
            with open(log.name, errors="replace") as stream:
                return stream.read()
        ports = [(port, False)]
    passed = False
    expected = []
    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "rpc host startup")
        baseline = fd_count(process)
        for index, (port, ws) in enumerate(ports):
            if ws:
                peer, upgraded = upgrade(port)
                assert upgraded.startswith(b"HTTP/1.1 101")
            else:
                peer = socket.create_connection(("127.0.0.1", port), 5)
                peer.settimeout(5)
            with peer:
                base = 800000 + index * 100
                def exchange(msg_id, rid, tag, body, error=0):
                    frame = packet(envelope(msg_id, rid, tag, body))
                    if ws:
                        send_frame(peer, 2, frame)
                    else:
                        peer.sendall(frame)
                    reply = response(peer, ws)
                    assert reply[1] == rid, reply
                    if error:
                        assert reply[4] == 14 and fields(reply[14])[1] == error, reply
                    else:
                        assert reply[4] in (11, 13), reply
                    expected.append((rid, msg_id, error))
                    return reply
                exchange(999, base + 1, 12, b"\x08\x01", 1001)
                exchange(12, base + 2, 10, b"\x0a\x01x", 1002)
                exchange(10, base + 3, 10, b"\x0a\x04wait", 1004)
                exchange(10, base + 4, 10, b"\x0a\x08business", 2001)
                exchange(10, base + 5, 10, b"\x0a\x05crash", 9000)
                exchange(100, base + 6, 14, b"", 1003)
                logged_in = exchange(10, base + 7, 10, b"\x0a\x04auth")
                assert fields(logged_in[11])[1] == 1
                routed = exchange(100, base + 8, 14, b"")
                assert fields(routed[13])[1] == 42
                pong = exchange(12, base + 9, 12, b"\x08\x63")
                assert fields(pong[13])[1] == 99
                foreign = exchange(10, base + 10, 10, b"\x0a\x07foreign")
                assert fields(foreign[11])[1] == 1
                # A duplicate foreign completion must not precede this reply.
                pong = exchange(12, base + 11, 12, b"\x08\x64")
                assert fields(pong[13])[1] == 100
        wait_until(process, lambda: fd_count(process) == baseline, "rpc FD recovery")
        stop_host(process, log)
        with open(log.name, errors="replace") as stream:
            logs = stream.read()
        assert "do-not-log-private-business-data" not in logs, logs
        for rid, msg, code in expected:
            starts = re.findall(
                rf"event=request_start connection_id=(\d+) request_id={rid} msg_id={msg} code=0", logs)
            ends = re.findall(
                rf"event=request_end connection_id=(\d+) request_id={rid} msg_id={msg} code={code} outcome=reply", logs)
            assert len(starts) == 1 and starts == ends, (rid, code, logs)
        passed = True
    finally:
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("server log:", log.name, file=sys.stderr)
    print("typed routing, foreign once-only completion, deadlines, traces and FD recovery passed")

def run_drain_profile(binary, mode, workers):
    if mode == "ws":
        process, log, content, tcp_port, ws_port = run_host(binary, "rpc")
        ports = [(tcp_port, False), (ws_port, True)]
    else:
        port = reserve_port()
        log = tempfile.NamedTemporaryFile(prefix="arena-rpc-drain-", suffix=".log", delete=False)
        process = subprocess.Popen([binary, str(port), "60000", str(workers), "rpc"],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        def content():
            log.flush()
            with open(log.name, errors="replace") as stream:
                return stream.read()
        ports = [(port, False)]
    peers = []
    passed = False
    try:
        ready = "WS_TEST_READY" if mode == "ws" else "TEST_HOST_READY"
        wait_until(process, lambda: ready in content(), "request-drain host startup")
        for index, (port, ws) in enumerate(ports):
            if ws:
                peer, upgraded = upgrade(port)
                assert upgraded.startswith(b"HTTP/1.1 101")
            else:
                peer = socket.create_connection(("127.0.0.1", port), 5)
                peer.settimeout(5)
            rid = 990000 + index
            peers.append((peer, ws, rid))
            frame = packet(envelope(10, rid, 10, b"\x0a\x04wait"))
            if ws:
                send_frame(peer, 2, frame)
            else:
                peer.sendall(frame)
        wait_until(process, lambda: all(
            re.search(rf"event=request_handler .*request_id={rid} ", content())
            for _, _, rid in peers), "pending requests before drain")
        process.send_signal(signal.SIGUSR1)
        for peer, ws, rid in peers:
            reply = response(peer, ws)
            assert reply[1] == rid and fields(reply[14])[1] == 1004, reply
            if ws:
                opcode, payload = read_frame(peer)
                assert opcode == 8 and payload[:2] == b"\x03\xe8", (opcode, payload)
                send_frame(peer, 8, payload)
            assert peer.recv(1) == b"", "drain did not close after the final pending reply"
            peer.close()
        assert process.wait(timeout=5) == 0, content()
        output = content()
        assert "AddressSanitizer" not in output and "runtime error:" not in output, output
        passed = True
    finally:
        for peer, _, _ in peers:
            peer.close()
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("server log:", log.name, file=sys.stderr)
    print("pending request deadlines reply before TCP/WS graceful retirement passed")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        run(sys.argv[1])
    elif len(sys.argv) == 5 and sys.argv[4] == "drain":
        run_drain_profile(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    else:
        run_profile(sys.argv[1], sys.argv[2], int(sys.argv[3]))
