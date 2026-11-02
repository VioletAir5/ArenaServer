"""Exercise the actual application entry point, configuration and signal drain."""

import argparse
import base64
import contextlib
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
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
    for key in tuple(result):
        if key.startswith("ARENA_") and not key.startswith("ARENA_TEST_CLOCK_"):
            result.pop(key)
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
    return admission_counts(contents(log))


def admission_counts(output):
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
    # Handshake configuration fails closed before any business dependency is contacted.
    for token in (None, "short"):
        settings = {} if token is None else {"ARENA_LOBBY_TOKEN": token}
        with application(binary, "asio", "handshake", **settings) as (process, log, _, _):
            assert process.wait(timeout=5) == 2, contents(log)
            assert "gateway runtime failed" in contents(log), contents(log)


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


@contextlib.contextmanager
def real_stack(args, **gateway_settings):
    """Use independent production services and harness-owned PG, never a test host."""
    sys.path.insert(0, str(args.repository_root / "tests/lobby"))
    from ServiceStackFixture import ServiceStack

    class LifecycleStack(ServiceStack):
        def command(self, role):
            command, configured = super().command(role)
            if role == "gateway":
                configured.update(gateway_settings)
            return command, configured

    args.gateway = args.binary
    args.report.parent.mkdir(parents=True, exist_ok=True)
    stack = LifecycleStack(args, grace_ms=1000)
    failure = None
    try:
        stack.setup()
        yield stack
        stack.report.update(passed=True, scenario=args.scenario)
    except BaseException as error:
        locations = []
        trace = error.__traceback__
        while trace is not None:
            locations.append(dict(file=Path(trace.tb_frame.f_code.co_filename).name, line=trace.tb_lineno))
            trace = trace.tb_next
        failure = type(error).__name__
        stack.report.update(passed=False, error=failure, failure_traceback=locations)
    finally:
        previous = {number: signal.signal(number, signal.SIG_IGN)
                    for number in (signal.SIGTERM, signal.SIGINT)}
        try:
            stack.close()
            for path in Path(stack.report["logs"]).glob("*.log"):
                text = path.read_text(errors="replace")
                stack.assert_no_secrets(text)
                assert not re.search(r"AddressSanitizer|LeakSanitizer|runtime error:|ThreadSanitizer", text)
            assert stack.report.get("cluster_cleaned") is True
            assert stack.report["parent_fd_final"] == stack.report["parent_fd_baseline"]
            assert stack.report["forced_kills"] == 0
            assert not stack.report.get("cleanup_errors") and not stack.report.get("service_cleanup_errors")
        except Exception as error:
            failure = type(error).__name__
            stack.report.update(passed=False, cleanup_error=failure)
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
        output = json.dumps(stack.report, indent=2)
        stack.assert_no_secrets(output)
        args.report.write_text(output + "\n")
    if failure or not stack.report["passed"]:
        raise AssertionError("real application lifecycle failed; inspect " + str(args.report))


def gateway_counts(stack):
    child = stack.children["gateway"]
    output = "\n".join(path.read_text(errors="replace") for path in child.log_paths)
    assert "LOBBY_COUNTS tracked=0 business_inflight=0 heartbeat_inflight=0 probe_inflight=0 owner_posts=0 stopped=1" in output
    return admission_counts(output)


def shared_tcp_websocket(args):
    with real_stack(args, ARENA_MAX_CONNECTIONS="2", ARENA_MAX_CONNECTIONS_PER_IP="1") as stack:
        process = stack.children["gateway"].process
        tcp_port, ws_port = stack.ports["gateway"], stack.ports["gateway-ws"]
        with connect_ready(process, tcp_port) as tcp:
            tcp.sendall(packet(envelope_ping(101)))
            assert read_tcp_packet(tcp) == packet_pong(101)
            with socket.create_connection(("127.0.0.1", ws_port), timeout=5) as denied:
                denied.settimeout(5)
                response = websocket_upgrade(denied, ws_port)
                assert not response.startswith(b"HTTP/1.1 101"), response
            tcp.shutdown(socket.SHUT_WR)
            assert tcp.recv(1) == b""

        # A new real upgrade proves the TCP close returned the shared IP lease.
        deadline = time.monotonic() + 5
        websocket = None
        while time.monotonic() < deadline:
            candidate = socket.create_connection(("127.0.0.1", ws_port), timeout=5)
            candidate.settimeout(5)
            if websocket_upgrade(candidate, ws_port).startswith(b"HTTP/1.1 101"):
                websocket = candidate
                break
            candidate.close()
            time.sleep(.01)
        assert websocket is not None, "TCP disconnect did not release shared IP capacity"
        with websocket, ThreadPoolExecutor(max_workers=1) as workers:
            send_frame(websocket, 2, packet(envelope_ping(202)))
            assert read_frame(websocket) == (2, packet_pong(202))
            stopping = workers.submit(stack.children["gateway"].stop)
            opcode, payload = read_frame(websocket)
            assert opcode == 8, (opcode, payload)
            send_frame(websocket, 8, payload)
            assert websocket.recv(1) == b""
            stopping.result(timeout=12)
        values = gateway_counts(stack)
        assert values[1] == 1 and values[4] >= 1, values
        stack.report["shared_admission"] = dict(peak=values[1], per_ip_rejected=values[4])


def invalid_lobby_grace(stack):
    # Grace moved to Lobby. Validate its real startup parser, with no lock/port collision.
    cases = []
    for index, value in enumerate(("0", "-1", "1x", "3600001", "")):
        command, configured = stack.command("lobby")
        directory = stack.cluster.root / ("invalid-grace-" + str(index))
        configured.update(ARENA_DISCONNECT_GRACE_MS=value, ARENA_LOBBY_PORT="0",
                          ARENA_HEALTH_PORT="0", ARENA_LOBBY_STATE_DIR=str(directory))
        result = subprocess.run(command, cwd=stack.args.repository_root, env=configured,
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=5)
        stack.assert_no_secrets(result.stdout + result.stderr)
        assert result.returncode == 2 and '"state":"listening"' not in result.stdout, \
            "Lobby grace did not fail before listener readiness"
        assert not re.search(r"AddressSanitizer|runtime error:", result.stdout + result.stderr)
        cases.append(dict(value=value, exit_code=result.returncode))
    stack.report["invalid_lobby_grace"] = cases


def authenticated_shutdown(args):
    with real_stack(args) as stack:
        from AuthProcessIntegrationTest import health_http
        invalid_lobby_grace(stack)
        peer = stack.connect()
        peer.socket.sendall(packet(envelope_ping(303)))
        assert read_tcp_packet(peer.socket) == packet_pong(303)
        grant = peer.login()
        stack.secrets += (grant["token"].decode(),)
        peer.join(9001)
        state = peer.room_snapshot()
        lobby = stack.children["lobby"].process
        # Freeze an owned real dependency to make the frontend RPC observably pending.
        os.kill(lobby.pid, signal.SIGSTOP)
        try:
            with ThreadPoolExecutor(max_workers=2) as workers:
                pending = workers.submit(peer.control, state, 42, 1, 1, 0)
                try:
                    stack.until(lambda: health_http(stack.ports["gateway-health"])[1]["inflight"] == 1
                                and not pending.done(), "real Gateway business RPC in flight", .5)
                    stopping = workers.submit(stack.children["gateway"].stop)
                    stack.until(lambda: health_http(stack.ports["gateway-health"])[1]["draining"]
                                and not pending.done(), "Gateway drains an actual pending request", .5)
                    stack.report["pending_at_drain"] = True
                finally:
                    os.kill(lobby.pid, signal.SIGCONT)
                applied, status = pending.result(timeout=5)
                assert status == 1 and applied["players"][42]["seq"] == 1
                assert peer.socket.recv(1) == b"", "stop did not send FIN after the accepted reply"
                peer.close()
                stopping.result(timeout=12)
        finally:
            # Always release SIGSTOP, including assertion/timeout/cancellation paths.
            try:
                os.kill(lobby.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        values = gateway_counts(stack)
        assert values[1] == 1 and values[2] == 0, values
        stack.report["authenticated_shutdown"] = dict(actual_login=True, pending_reply_delivered=True,
                                                      remote_inflight_after_join=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("scenario", choices=("epoll", "asio", "invalid", "shared_ws", "authenticated_shutdown"))
    parser.add_argument("--repository-root", type=Path, default=Path(__file__).resolve().parents[3])
    for name in ("auth", "lobby", "descriptor", "pg-bin", "pg-share", "pg-lib", "report"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    if args.scenario in ("epoll", "asio"):
        raw_shutdown(str(args.binary), args.scenario)
    elif args.scenario == "invalid":
        invalid_configuration(str(args.binary))
    else:
        for name in ("auth", "lobby", "descriptor", "pg_bin", "report"):
            if getattr(args, name) is None:
                parser.error("real handshake lifecycle requires --" + name.replace("_", "-"))
        if args.scenario == "shared_ws":
            shared_tcp_websocket(args)
        else:
            authenticated_shutdown(args)
    print("Application lifecycle " + args.scenario + " passed")


if __name__ == "__main__":
    main()
