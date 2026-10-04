"""End-to-end TCP server checks against an isolated server process."""

import os
import random
import resource
import signal
import struct
import socket
import subprocess
import sys
import tempfile
import threading
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


def run(binary, scenario, worker_count=0):
    port = reserve_port()
    log = tempfile.NamedTemporaryFile(
        prefix="arena-tcp-integration-", suffix=".log", delete=False
    )
    environment = os.environ.copy()
    environment.setdefault(
        "ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1"
    )
    environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1")
    command = [
        binary, str(port), "1000" if scenario == "idle" else "60000",
        str(worker_count),
    ]
    def constrain_fds():
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))

    server = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=environment,
        preexec_fn=constrain_fds if scenario.startswith("fd_limit") else None,
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

        elif scenario in ("fd_limit", "fd_limit_stop"):
            def resource_errors():
                with open(log.name, errors="replace") as stream:
                    return sum("Too many open files" in line for line in stream)

            def cpu_seconds():
                with open(f"/proc/{server.pid}/stat") as stream:
                    # Skip pid/comm: comm can itself contain spaces.
                    fields = stream.read().rsplit(")", 1)[1].split()
                return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")

            for round_number in range(1, 3):
                before_errors = resource_errors()
                for _ in range(80):
                    clients.append(connect(port))
                wait_until(server,
                           lambda: resource_errors() > before_errors,
                           "real EMFILE")
                start_cpu = cpu_seconds()
                start = time.monotonic()
                time.sleep(0.6)
                elapsed = time.monotonic() - start
                error_count = resource_errors() - before_errors
                cpu_used = cpu_seconds() - start_cpu
                if error_count > 15:
                    raise AssertionError(f"accept log busy loop: {error_count} errors")
                if cpu_used > elapsed * 0.5:
                    raise AssertionError(f"accept CPU busy loop: {cpu_used:.3f}s")
                refused = 0
                for client in clients:
                    client.setblocking(False)
                    try:
                        if client.recv(1) == b"":
                            refused += 1
                    except ConnectionResetError:
                        refused += 1
                    except BlockingIOError:
                        pass
                if refused == 0:
                    raise AssertionError("no exhausted connection was refused")
                if scenario == "fd_limit_stop":
                    # Stop just after the next failure, within the
                    # 100ms backoff window with a retry still queued.
                    last_errors = resource_errors()
                    wait_until(server,
                               lambda: resource_errors() > last_errors,
                               "accept retry scheduled before stop")
                    server.send_signal(signal.SIGUSR1)
                    server.wait(timeout=3)
                    with open(log.name, errors="replace") as stream:
                        stopped_log = stream.read()
                    if "TcpServer stopped" not in stopped_log:
                        raise AssertionError("server failed to stop during accept backoff")
                    try:
                        with connect(port):
                            raise AssertionError("listener stayed open after stop")
                    except OSError:
                        pass
                    print("server stopped safely with accept retry pending")
                    break
                for client in clients:
                    client.close()
                clients.clear()
                wait_until(server, lambda: fd_count(server) == baseline,
                           "EMFILE FD recovery")
                with connect(port) as recovered:
                    recovered.sendall(b"recovered")
                    if recovered.recv(9) != b"recovered":
                        raise AssertionError("accept did not recover after EMFILE")
                wait_until(server, lambda: fd_count(server) == baseline,
                           "recovered echo FD cleanup")
                print(f"EMFILE round {round_number}: errors={error_count}, "
                      f"CPU={cpu_used:.3f}s/{elapsed:.3f}s, refused={refused}, "
                      f"FD recovered to {baseline}")
        elif scenario == "stream":
            client = connect(port)
            client.settimeout(10)
            clients.append(client)

            frame_rng = random.Random(20261002)
            # Slices may split a record or combine several records.
            frames = []
            for _ in range(1024):
                size = frame_rng.randrange(0, 8192)
                frames.append(size.to_bytes(2, "big") + frame_rng.randbytes(size))
            payload = b"".join(frames)

            send_errors = []

            def send_random_slices():
                try:
                    send_rng = random.Random(0xA11CE)
                    offset = 0
                    chunk_sizes = (1, 2, 7, 31, 256, 2048, 16384, 32768)
                    while offset < len(payload):
                        size = send_rng.choice(chunk_sizes)
                        end = min(offset + size, len(payload))
                        client.sendall(memoryview(payload)[offset:end])
                        offset = end
                    client.shutdown(socket.SHUT_WR)
                except Exception as error:
                    send_errors.append(error)

            sender = threading.Thread(target=send_random_slices, daemon=True)
            sender.start()

            recv_rng = random.Random(0xBEE)
            received = 0
            recv_sizes = (1, 3, 17, 127, 1024, 8192, 32768)
            while True:
                chunk = client.recv(recv_rng.choice(recv_sizes))
                if not chunk:
                    break
                expected = payload[received : received + len(chunk)]
                if chunk != expected:
                    raise AssertionError(f"byte stream mismatch at offset {received}")
                received += len(chunk)
                if received > len(payload):
                    raise AssertionError("server repeated bytes")

            sender.join(timeout=10)
            if sender.is_alive():
                raise AssertionError("sender did not finish")
            if send_errors:
                raise AssertionError(f"sender failed: {send_errors[0]}")
            if received != len(payload):
                raise AssertionError(
                    f"truncated stream: received {received} of {len(payload)} bytes"
                )
            print(f"byte stream preserved: {received} bytes in 1024 records")
        elif scenario == "idle":
            client = connect(port)
            clients.append(client)
            for _ in range(7):
                client.sendall(b"x")
                if client.recv(1) != b"x":
                    raise AssertionError("idle test echo mismatch")
                time.sleep(0.2)
            client.settimeout(4)
            if client.recv(1) != b"":
                raise AssertionError("idle connection stayed open")
            wait_until(
                server,
                lambda: fd_count(server) == baseline,
                "idle timeout FD recovery",
            )
            print("activity refreshed idle deadline; inactive connection closed")
        elif scenario == "rst":
            client = connect(port)
            client.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
            client.sendall(b"reset me")
            client.close()
            wait_until(
                server,
                lambda: fd_count(server) == baseline,
                "RST connection FD recovery",
            )

            def logged_reset():
                with open(log.name, errors="replace") as stream:
                    return "peer reset" in stream.read()

            wait_until(server, logged_reset, "explicit RST handling")
            with connect(port) as next_client:
                next_client.sendall(b"alive")
                if next_client.recv(5) != b"alive":
                    raise AssertionError("server did not survive peer RST")
            print("RST logged and FD recovered; server still accepts")
        elif scenario == "stop_fast":
            client = connect(port)
            clients.append(client)
            server.send_signal(signal.SIGUSR1)
            client.settimeout(3)
            if client.recv(1) != b"":
                raise AssertionError("server did not send FIN")
            client.shutdown(socket.SHUT_WR)
            server.wait(timeout=3)
            with open(log.name, errors="replace") as stream:
                stop_log = stream.read()
            if "TcpServer stopped" not in stop_log:
                raise AssertionError("cooperative stop did not complete")
            if "stop deadline reached" in stop_log:
                raise AssertionError("cooperative stop waited for deadline")
            print("cooperative stop completed before deadline")
        elif scenario == "stop":
            cooperative = connect(port)
            lingering = connect(port)
            clients.extend((cooperative, lingering))
            start = time.monotonic()
            server.send_signal(signal.SIGUSR1)
            cooperative.settimeout(3)
            lingering.settimeout(3)
            if cooperative.recv(1) != b"" or lingering.recv(1) != b"":
                raise AssertionError("server did not send FIN on stop")
            cooperative.shutdown(socket.SHUT_WR)
            server.wait(timeout=3)
            elapsed = time.monotonic() - start
            with open(log.name, errors="replace") as stream:
                stop_log = stream.read()
            if "stop deadline reached" not in stop_log:
                raise AssertionError("lingering peer did not reach stop deadline")
            if elapsed > 3:
                raise AssertionError(f"stop took too long: {elapsed:.3f}s")
            try:
                with connect(port):
                    raise AssertionError("listener accepted after stop")
            except OSError:
                pass
            print(f"stop sent FIN, waited {elapsed:.3f}s, then reclaimed peers")
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

        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
        server.wait(timeout=10)
        log.flush()
        with open(log.name, errors="replace") as stream:
            output = stream.read()
        if server.returncode != 0:
            raise AssertionError(f"server exited {server.returncode}: {output[-2000:]}")
        if ("ERROR: AddressSanitizer" in output or "LeakSanitizer" in output
                or "AddressSanitizer:DEADLYSIGNAL" in output
                or "WARNING: ThreadSanitizer" in output
                or "runtime error:" in output):
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
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: TcpServerIntegrationTest.py HOST churn|limit|fd_limit|fd_limit_stop|slow|stream|idle|rst|stop|stop_fast [WORKER_COUNT]")
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else 0)
