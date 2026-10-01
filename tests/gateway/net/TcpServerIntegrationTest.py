"""End-to-end TCP server checks against an isolated server process."""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

CAPACITY = 1000
WAIT_SECONDS = 20


def wait_until(server, condition, description):
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise AssertionError(
                f"server exited {server.returncode} while waiting for {description}"
            )
        if condition():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


def fd_count(server):
    return len(os.listdir(f"/proc/{server.pid}/fd"))


def reserve_port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


def connect(port):
    client = socket.create_connection(("127.0.0.1", port), timeout=5)
    client.settimeout(5)
    return client


def run(binary, scenario):
    port = reserve_port()
    log = tempfile.NamedTemporaryFile(
        prefix="arena-tcp-integration-", suffix=".log", delete=False
    )
    environment = os.environ.copy()
    environment.setdefault(
        "ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1"
    )
    environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1")
    server = subprocess.Popen(
        [binary, str(port)],
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=environment,
    )
    clients = []
    passed = False
    try:
        def ready():
            log.flush()
            if os.path.getsize(log.name) > 1024 * 1024:
                raise AssertionError("server produced excessive startup log")
            with open(log.name, errors="replace") as stream:
                return "TEST_HOST_READY" in stream.read(4096)

        wait_until(server, ready, "server startup")
        baseline = fd_count(server)

        if scenario == "churn":
            for round_number in range(1, 4):
                for _ in range(CAPACITY):
                    clients.append(connect(port))
                wait_until(
                    server,
                    lambda: fd_count(server) == baseline + CAPACITY,
                    f"round {round_number}: accepting 1000 connections",
                )
                for client in clients:
                    client.sendall(b"x")
                    if client.recv(1) != b"x":
                        raise AssertionError("echo mismatch")
                for client in clients:
                    client.close()
                clients.clear()
                wait_until(
                    server,
                    lambda: fd_count(server) == baseline,
                    f"round {round_number}: FD recovery",
                )
                print(
                    f"round {round_number}: FD {baseline} -> "
                    f"{baseline + CAPACITY} -> {fd_count(server)}"
                )

        elif scenario == "limit":
            for _ in range(CAPACITY):
                clients.append(connect(port))
            wait_until(
                server,
                lambda: fd_count(server) == baseline + CAPACITY,
                "accepting 1000 connections",
            )
            with connect(port) as extra:
                try:
                    data = extra.recv(1)
                except ConnectionResetError:
                    data = b""
                if data != b"":
                    raise AssertionError(f"over-limit connection received {data!r}")
            if fd_count(server) != baseline + CAPACITY:
                raise AssertionError("over-limit connection was retained")
            for client in clients:
                client.close()
            clients.clear()
            wait_until(server, lambda: fd_count(server) == baseline, "FD recovery")
            print("1001st connection closed; capacity recovered")

        elif scenario == "slow":
            client = connect(port)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            client.setblocking(False)
            clients.append(client)

            sent = 0
            payload = b"x" * 8192
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and sent < 8 * 1024 * 1024:
                try:
                    sent += client.send(payload)
                except BlockingIOError:
                    time.sleep(0.001)

            def slow_client_closed():
                with open(log.name, errors="replace") as stream:
                    return "slow client timeout" in stream.read()

            wait_until(server, slow_client_closed, "slow-client timeout")
            wait_until(
                server,
                lambda: fd_count(server) == baseline,
                "slow-client FD recovery",
            )
            print(f"slow client timed out after sending {sent} bytes")
        else:
            raise ValueError(f"unknown scenario: {scenario}")

        server.send_signal(signal.SIGTERM)
        server.wait(timeout=10)
        log.flush()
        with open(log.name, errors="replace") as stream:
            output = stream.read()
        if server.returncode != 0:
            raise AssertionError(f"server exited {server.returncode}: {output[-2000:]}")
        if ("ERROR: AddressSanitizer" in output or "LeakSanitizer" in output
                or "AddressSanitizer:DEADLYSIGNAL" in output):
            raise AssertionError(f"sanitizer error: {output[-2000:]}")
        if scenario == "limit" and "connection limit reached" not in output:
            raise AssertionError("missing explicit connection-limit log")
        passed = True
    finally:
        for client in clients:
            client.close()
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)
        log.close()
        if passed:
            os.unlink(log.name)
        else:
            print(f"server log: {log.name}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: TcpServerIntegrationTest.py HOST churn|limit|slow")
    run(sys.argv[1], sys.argv[2])
