#!/usr/bin/env python3
"""Supervisor-only executable-fixture contracts; not real service acceptance."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
ROLES = ("storage", "auth", "lobby", "gateway")
PORT_NAMES = (*ROLES, "gateway-ws", *(role + "-health" for role in ROLES))
FIXTURE = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import socket
import sys
import time

role = os.environ["ARENA_NODE_ID"]
config = json.loads(Path(".supervisor-fixture.json").read_text())
behavior = config.get(role, {})
running = True

def event(name, **values):
    print(json.dumps(dict(fixture=role, event=name, **values)), flush=True)

def stop(*_):
    global running
    running = False

def reload(*_):
    event("reload")

signal.signal(signal.SIGTERM, signal.SIG_IGN if behavior.get("ignore_term") else stop)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGHUP, reload)
event("environment", names=sorted(os.environ))
if behavior.get("fail_start"):
    raise SystemExit(17)

if role == "storage":
    ports = [int(sys.argv[sys.argv.index("--port") + 1])]
elif role == "auth":
    ports = [int(os.environ["ARENA_AUTH_PORT"])]
elif role == "lobby":
    ports = [int(os.environ["ARENA_LOBBY_PORT"])]
else:
    ports = [int(os.environ["ARENA_TCP_PORT"]), int(os.environ["ARENA_WS_PORT"])]
listeners = []
try:
    for port in (*ports, int(os.environ["ARENA_HEALTH_PORT"])):
        listener = socket.socket()
        listeners.append(listener)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(8)
    health = listeners[-1]
    health.settimeout(.05)
    event("listening")
    while running:
        try:
            peer, _ = health.accept()
        except socket.timeout:
            continue
        with peer:
            peer.settimeout(.2)
            try:
                request = b""
                while b"\r\n\r\n" not in request and len(request) < 4096:
                    part = peer.recv(4096 - len(request))
                    if not part:
                        break
                    request += part
                if not request.startswith(b"GET /health/ready HTTP/1.1\r\n"):
                    continue
                ready = not behavior.get("never_ready") and not behavior.get("false_ack")
                body = json.dumps(dict(service=role, alive=True, ready=ready)).encode()
                if behavior.get("large_body"):
                    body = b"x" * 20000
                status = 200 if ready or behavior.get("false_ack") else 503
                peer.sendall((f"HTTP/1.1 {status} Test\r\nContent-Length: {len(body)}\r\n"
                              "Connection: close\r\n\r\n").encode())
                if behavior.get("slow_body"):
                    for byte in body:
                        if not running:
                            break
                        peer.sendall(bytes([byte]))
                        time.sleep(.02)
                else:
                    peer.sendall(body)
            except OSError:
                pass
finally:
    for listener in listeners:
        listener.close()
event("stopped")
'''


class ServiceSupervisorTest(unittest.TestCase):
    def setUp(self):
        self.fd_before = len(os.listdir("/proc/self/fd"))
        self.temporary = tempfile.TemporaryDirectory(prefix="arena-supervisor-")
        self.root = Path(self.temporary.name)
        self.repo, self.build = self.root / "repo", self.root / "build"
        self.repo.mkdir()
        self.build.mkdir()
        self.state, self.logs = self.root / "state", self.root / "logs"
        source = self.repo / "services/storage/server.py"
        source.parent.mkdir(parents=True)
        source.write_text(FIXTURE)
        for relative in ("services/auth/ArenaAuth", "services/lobby/ArenaLobby",
                         "services/gateway/app/ArenaServer"):
            path = self.build / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!" + sys.executable + "\n" + FIXTURE.split("\n", 1)[1])
            path.chmod(0o700)
        descriptor = self.build / "shared/generated/rpc.desc"
        descriptor.parent.mkdir(parents=True)
        descriptor.write_bytes(b"supervisor executable fixture; not a real protobuf descriptor")
        self.behavior = self.repo / ".supervisor-fixture.json"
        self.behavior.write_text("{}")
        self.environment = {name: value for name, value in os.environ.items()
                            if not name.startswith(("ARENA_", "PG"))}
        self.secret_values = [secrets.token_hex(32) for _ in range(7)]
        names = ("ARENA_STORAGE_AUTH_TOKEN", "ARENA_STORAGE_LOBBY_TOKEN",
                 "ARENA_AUTH_TOKEN", "ARENA_LOBBY_TOKEN", "ARENA_JWT_KEY",
                 "PGPASSWORD", "ARENA_INTERNAL_TOKEN")
        self.environment.update(zip(names, self.secret_values))
        self.environment.update(PGHOST="/explicit/unused/fixture/socket", PGPORT="5432",
                                PGDATABASE="existing_fixture_db", PGUSER="fixture_owner",
                                PGSERVICE="must-not-inherit", ARENA_ACCOUNTS_FILE="forbidden")
        self.ports = {}
        sockets = []
        try:
            for name in PORT_NAMES:
                peer = socket.socket()
                peer.bind(("127.0.0.1", 0))
                sockets.append(peer)
                self.ports[name] = peer.getsockname()[1]
        finally:
            for peer in sockets:
                peer.close()
        self.launchers = []
        self.output = self.root / "launcher.stdout"

    def command(self, extra=()):
        command = [sys.executable, str(ROOT / "tools/runtime/RunServices.py"),
                   "--repo-root", str(self.repo), "--build-dir", str(self.build),
                   "--python", sys.executable, "--state-dir", str(self.state),
                   "--log-dir", str(self.logs), "--startup-timeout", "2",
                   "--stop-timeout", ".3", "--kill-timeout", "1",
                   "--probe-timeout", ".1"]
        for name, port in self.ports.items():
            command.extend(("--" + name + "-port", str(port)))
        return [*command, *extra]

    def start(self, *, extra=(), environment=None):
        self.output = self.root / ("launcher-" + str(len(self.launchers)) + ".stdout")
        descriptor = os.open(self.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            process = subprocess.Popen(self.command(extra), env=environment or self.environment,
                                       stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                       close_fds=True, start_new_session=True)
        self.launchers.append((process, self.output))
        return process

    def rows(self, path=None):
        path = path or self.output
        result = []
        if path.exists():
            for line in path.read_text().splitlines():
                try:
                    result.append(json.loads(line))
                except ValueError:
                    pass
        return result

    def until(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.assertTrue(predicate(), "supervisor executable fixture deadline")

    def ready(self, process):
        self.until(lambda: process.poll() is not None or any(
            row.get("event") == "stack_ready" for row in self.rows()))
        self.assertIsNone(process.poll(), self.output.read_text())

    def stop(self, process, number=signal.SIGTERM, expected=0):
        process.send_signal(number)
        self.assertEqual(process.wait(timeout=6), expected, self.output.read_text())
        self.assert_joined()

    def assert_joined(self):
        for _, output in self.launchers:
            for row in self.rows(output):
                if row.get("event") == "started":
                    self.assertFalse(Path("/proc").joinpath(str(row["pid"])).exists(),
                                     "managed child was not joined")

    def fixture_rows(self, role):
        return [row for path in self.logs.glob(role + "-*.log") for row in self.rows(path)]

    def assert_private_and_redacted(self):
        self.assertEqual(stat.S_IMODE(self.logs.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o700)
        paths = [*self.logs.iterdir(), *(path for _, path in self.launchers)]
        for path in paths:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            text = path.read_text()
            for secret in self.secret_values:
                self.assertNotIn(secret, text)

    def tearDown(self):
        try:
            for process, _ in self.launchers:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    try:
                        process.wait(timeout=6)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=2)
            self.assert_joined()
        finally:
            self.temporary.cleanup()
        self.assertEqual(len(os.listdir("/proc/self/fd")), self.fd_before,
                         "test retained a launcher, log, or socket descriptor")

    def test_ordered_start_stop_and_role_environment(self):
        process = self.start()
        self.ready(process)
        rows = self.rows()
        self.assertEqual([row["role"] for row in rows if row["event"] == "ready"], list(ROLES))
        allowed = {
            "storage": {"ARENA_NODE_ID", "ARENA_HEALTH_PORT", "ARENA_STORAGE_CALLER_TOKENS"},
            "auth": {"ARENA_NODE_ID", "ARENA_HEALTH_PORT", "ARENA_AUTH_PORT",
                     "ARENA_STORAGE_PORT", "ARENA_AUTH_TOKEN", "ARENA_STORAGE_AUTH_TOKEN",
                     "ARENA_JWT_KEY"},
            "lobby": {"ARENA_NODE_ID", "ARENA_HEALTH_PORT", "ARENA_LOBBY_PORT",
                      "ARENA_AUTH_PORT", "ARENA_STORAGE_PORT", "ARENA_AUTH_TOKEN",
                      "ARENA_LOBBY_TOKEN", "ARENA_STORAGE_LOBBY_TOKEN", "ARENA_LOBBY_STATE_DIR"},
            "gateway": {"ARENA_NODE_ID", "ARENA_HEALTH_PORT", "ARENA_TCP_PORT",
                        "ARENA_WS_PORT", "ARENA_LOBBY_PORT", "ARENA_LOBBY_TOKEN"},
        }
        for role in ROLES:
            environment = next(row for row in self.fixture_rows(role)
                               if row["event"] == "environment")["names"]
            self.assertEqual({name for name in environment if name.startswith("ARENA_")},
                             allowed[role])
            pg = {name for name in environment if name.startswith("PG")}
            self.assertEqual(pg, {"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"}
                             if role == "storage" else set())
        self.stop(process)
        self.assertEqual([row["role"] for row in self.rows() if row["event"] == "stopping"],
                         list(reversed(ROLES)))
        self.assertFalse(any(row["event"] == "forced_kill" for row in self.rows()))
        self.assert_private_and_redacted()

    def test_startup_failure_joins_preceding_children(self):
        self.behavior.write_text(json.dumps({"lobby": {"fail_start": True}}))
        process = self.start()
        self.assertEqual(process.wait(timeout=6), 1)
        self.assertEqual([row["role"] for row in self.rows() if row["event"] == "started"],
                         ["storage", "auth", "lobby"])
        self.assertEqual([row["role"] for row in self.rows() if row["event"] == "stopping"],
                         ["lobby", "auth", "storage"])
        self.assert_joined()
        self.assert_private_and_redacted()

    def test_health_timeout_malformed_false_ack_and_slow_read_are_bounded(self):
        for behavior in ("never_ready", "false_ack", "large_body", "slow_body"):
            with self.subTest(behavior=behavior):
                self.behavior.write_text(json.dumps({"auth": {behavior: True}}))
                start = time.monotonic()
                process = self.start(extra=("--startup-timeout", ".35"))
                self.assertEqual(process.wait(timeout=5), 1)
                self.assertLess(time.monotonic() - start, 3)
                self.assertFalse(any(row.get("role") == "lobby" for row in self.rows()))
                self.assertTrue(any(row["event"] == "readiness_timeout" for row in self.rows()))
                self.assert_joined()

    def initial_pids(self):
        return {row["role"]: row["pid"] for row in self.rows()
                if row["event"] == "started" and row["attempt"] == 0}

    def current_pid(self, role):
        return [row["pid"] for row in self.rows()
                if row["event"] == "started" and row["role"] == role][-1]

    def assert_bystanders_alive(self, pids, excluded):
        for role, pid in pids.items():
            if role not in excluded:
                self.assertTrue(Path("/proc", str(pid)).exists(), role + " was cascaded")
                self.assertEqual(self.current_pid(role), pid)

    def test_runtime_auth_exit_recovers_without_restarting_other_roles(self):
        process = self.start()
        self.ready(process)
        pids = self.initial_pids()
        descriptor_count = len(os.listdir(f"/proc/{process.pid}/fd"))
        os.kill(pids["auth"], signal.SIGKILL)
        self.until(lambda: any(row["event"] == "recovered" and row["role"] == "auth"
                               for row in self.rows()))
        self.assertIsNone(process.poll())
        self.assertNotEqual(self.current_pid("auth"), pids["auth"])
        self.assert_bystanders_alive(pids, {"auth"})
        self.until(lambda: len(os.listdir(f"/proc/{process.pid}/fd")) == descriptor_count)
        schedules = [row for row in self.rows() if row["event"] == "restart_scheduled"]
        self.assertEqual(len(schedules), 1)
        self.assertLessEqual(schedules[0]["backoff_ms"], schedules[0]["backoff_cap_ms"])
        self.stop(process)
        self.assertEqual(sum(row["event"] == "started" for row in self.rows()), 5)
        self.assertEqual(len(list(self.logs.glob("auth-*.log"))), 2)
        self.assert_private_and_redacted()

    def test_lifetime_budget_does_not_reset_after_healthy_restarts(self):
        process = self.start(extra=("--restart-attempts", "2", "--restart-backoff-base", ".01",
                                    "--restart-backoff-max", ".05"))
        self.ready(process)
        pids = self.initial_pids()
        for attempt in (1, 2):
            os.kill(self.current_pid("auth"), signal.SIGKILL)
            self.until(lambda: any(row["event"] == "recovered" and row["role"] == "auth"
                                   and row["attempt"] == attempt for row in self.rows()))
            self.assert_bystanders_alive(pids, {"auth"})
        os.kill(self.current_pid("auth"), signal.SIGKILL)
        self.until(lambda: any(row["event"] == "degraded" and row["role"] == "auth"
                               for row in self.rows()))
        self.assertIsNone(process.poll())
        self.assert_bystanders_alive(pids, {"auth"})
        time.sleep(.15)
        auth = [row for row in self.rows() if row["event"] == "started" and row["role"] == "auth"]
        self.assertEqual([row["attempt"] for row in auth], [0, 1, 2])
        self.assertFalse(any(row["event"] == "stopping" for row in self.rows()))
        self.stop(process, expected=1)

    def test_zero_budget_degrades_only_failed_role_until_operator_stop(self):
        process = self.start(extra=("--restart-attempts", "0"))
        self.ready(process)
        pids = self.initial_pids()
        os.kill(pids["auth"], signal.SIGKILL)
        self.until(lambda: any(row["event"] == "degraded" for row in self.rows()))
        self.assertIsNone(process.poll())
        self.assert_bystanders_alive(pids, {"auth"})
        self.assertEqual(sum(row["event"] == "started" for row in self.rows()), 4)
        self.stop(process, expected=1)

    def test_slow_recovery_probe_does_not_block_another_roles_recovery(self):
        process = self.start(extra=("--restart-attempts", "1", "--startup-timeout", "1.2",
                                    "--probe-timeout", "1", "--restart-backoff-base", ".01",
                                    "--restart-backoff-max", ".05"))
        self.ready(process)
        pids = self.initial_pids()
        self.behavior.write_text(json.dumps({"auth": {"slow_body": True}}))
        os.kill(pids["auth"], signal.SIGKILL)
        self.until(lambda: sum(row["event"] == "started" and row["role"] == "auth"
                               for row in self.rows()) == 2)
        os.kill(pids["gateway"], signal.SIGKILL)
        self.until(lambda: any(row["event"] == "recovered" and row["role"] == "gateway"
                               for row in self.rows()))
        self.assert_bystanders_alive(pids, {"auth", "gateway"})
        self.until(lambda: any(row["event"] == "degraded" and row["role"] == "auth"
                               for row in self.rows()))
        rows = self.rows()
        recovered = next(index for index, row in enumerate(rows)
                         if row["event"] == "recovered" and row["role"] == "gateway")
        expired = next(index for index, row in enumerate(rows)
                       if row["event"] == "restart_readiness_timeout" and row["role"] == "auth")
        self.assertLess(recovered, expired, "slow Auth health blocked Gateway recovery")
        self.assertIsNone(process.poll())
        self.assertEqual(sum(row["event"] == "started" and row["role"] == "auth"
                             for row in self.rows()), 2)
        self.stop(process, expected=1)

    def test_sigint_forced_kill_is_explicit_and_nonzero(self):
        self.behavior.write_text(json.dumps({"auth": {"ignore_term": True}}))
        process = self.start()
        self.ready(process)
        self.stop(process, signal.SIGINT, expected=1)
        self.assertEqual([row["role"] for row in self.rows() if row["event"] == "forced_kill"],
                         ["auth"])

    def test_hup_only_reloads_configured_existing_children(self):
        configurations = []
        for role in ("lobby", "gateway"):
            path = self.root / (role + ".json")
            path.write_text("{}")
            configurations.extend(("--" + role + "-runtime-config", str(path)))
        process = self.start(extra=configurations)
        self.ready(process)
        before = [(row["role"], row["pid"]) for row in self.rows() if row["event"] == "started"]
        process.send_signal(signal.SIGHUP)
        self.until(lambda: all(any(row["event"] == "reload" for row in self.fixture_rows(role))
                               for role in ("lobby", "gateway")))
        self.assertFalse(any(row["event"] == "reload" for role in ("storage", "auth")
                             for row in self.fixture_rows(role)))
        self.assertEqual(before, [(row["role"], row["pid"]) for row in self.rows()
                                  if row["event"] == "started"])
        self.stop(process)
        self.assert_private_and_redacted()

    def test_invalid_inputs_fail_before_any_child_and_do_not_echo_secret(self):
        cases = [(("--gateway-backend", "epoll"), None),
                 (("--auth-port", str(self.ports["storage"])), None),
                 (("--probe-timeout", "nan"), None),
                 (("--stop-timeout", "0"), None),
                 (("--restart-attempts", "11"), None),
                 (("--restart-backoff-max", "nan"), None)]
        for missing in ("ARENA_JWT_KEY", "ARENA_LOBBY_TOKEN", "PGHOST", "PGUSER"):
            environment = dict(self.environment)
            environment.pop(missing)
            cases.append(((), environment))
        for extra, environment in cases:
            with self.subTest(extra=extra, missing=environment is not None):
                process = self.start(extra=extra, environment=environment)
                self.assertEqual(process.wait(timeout=4), 2)
                self.assertFalse(any(row["event"] == "started" for row in self.rows()))
                for secret in self.secret_values:
                    self.assertNotIn(secret, self.output.read_text())

    def test_explicit_virtualenv_interpreter_and_credential_values_are_preserved(self):
        path = ROOT / "tools/runtime/RunServices.py"
        spec = importlib.util.spec_from_file_location("arena_supervisor_contract", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
            interpreter = self.root / "venv/bin/python"
            interpreter.parent.mkdir(parents=True)
            interpreter.symlink_to(sys.executable)
            argv = self.command(("--python", str(interpreter)))[2:]
            settings = module.configuration(module.arguments(argv), self.environment)
            children = {child.role: child for child in settings.specs}
            self.assertEqual(children["storage"].command[0], str(interpreter))
            self.assertEqual(json.loads(children["storage"].environment[
                "ARENA_STORAGE_CALLER_TOKENS"]), {
                    "auth": self.environment["ARENA_STORAGE_AUTH_TOKEN"],
                    "lobby": self.environment["ARENA_STORAGE_LOBBY_TOKEN"]})
            self.assertEqual(children["gateway"].environment["ARENA_LOBBY_TOKEN"],
                             self.environment["ARENA_LOBBY_TOKEN"])
            self.assertEqual(children["auth"].environment["ARENA_JWT_KEY"],
                             self.environment["ARENA_JWT_KEY"])
            self.assertEqual(children["lobby"].environment["ARENA_AUTH_TOKEN"],
                             self.environment["ARENA_AUTH_TOKEN"])
            signer = children["auth"].environment["ARENA_JWT_KEY"]
            self.environment["ARENA_JWT_KEY"] = secrets.token_hex(32)
            self.assertEqual(children["auth"].environment["ARENA_JWT_KEY"], signer)
        finally:
            sys.modules.pop(spec.name, None)
    def test_public_log_directory_is_rejected_without_chmod_or_children(self):
        self.logs.mkdir()
        self.logs.chmod(0o755)
        process = self.start()
        self.assertEqual(process.wait(timeout=4), 2)
        self.assertEqual(stat.S_IMODE(self.logs.stat().st_mode), 0o755)
        self.assertFalse(any(row["event"] == "started" for row in self.rows()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    options = parser.parse_args()
    ROOT = options.repo_root.resolve()
    sys.dont_write_bytecode = True
    unittest.main(argv=[sys.argv[0]], verbosity=2)
