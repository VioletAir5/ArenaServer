#!/usr/bin/env python3
"""Run the real four-service stack against an explicitly configured existing PG.

No database, migration, account, credential, or runtime manifest is generated.
Only the supervisor lifecycle is written to stdout; child output stays private.
"""
import argparse
from contextlib import ExitStack
from dataclasses import dataclass, field
import errno
import fcntl
import json
import math
import os
import random
from pathlib import Path
import select
import signal
import socket
import stat
import subprocess
import sys
import time


ROLES = ("storage", "auth", "lobby", "gateway")
TOKENS = ("ARENA_STORAGE_AUTH_TOKEN", "ARENA_STORAGE_LOBBY_TOKEN",
          "ARENA_AUTH_TOKEN", "ARENA_LOBBY_TOKEN")
MAX_HEALTH_HEADER = 4096
MAX_HEALTH_BODY = 16384
MAX_PATH = 4096
NUMERIC_ENV = {
    "gateway": {"ARENA_MAX_CONNECTIONS": 65536,
                "ARENA_MAX_CONNECTIONS_PER_IP": 65536,
                "ARENA_LOBBY_MAX_INFLIGHT": 1024,
                "ARENA_LOBBY_TIMEOUT_MS": 1000},
    "auth": {"ARENA_AUTH_WORKERS": 16, "ARENA_AUTH_MAX_QUEUED": 4096,
             "ARENA_AUTH_MAX_INFLIGHT": 1024},
    "lobby": {"ARENA_LOBBY_MAX_PLAYERS": 100000, "ARENA_MAX_ROOMS": 10000,
              "ARENA_DISCONNECT_GRACE_MS": 3600000,
              "ARENA_BATTLE_TICKS": 1000000,
              "ARENA_READY_TIMEOUT_TICKS": 1000000,
              "ARENA_LOBBY_MAX_INFLIGHT": 1024,
              "ARENA_TRANSPORT_LEASE_MS": 30000},
    "storage": {},
}


class ConfigurationError(Exception):
    """A safe diagnostic containing configuration names, never their values."""


class StackFailure(Exception):
    """An initial child exit, readiness deadline, or lifecycle failure."""


class StopRequested(Exception):
    """An operator signal interrupted startup."""


def bounded_int(text, maximum, name, minimum=1):
    if not isinstance(text, str) or not text.isascii() or not text.isdecimal():
        raise ConfigurationError("invalid " + name)
    if len(text) > 10 or not minimum <= int(text) <= maximum:
        raise ConfigurationError("invalid " + name)
    return int(text)


def absolute_path(value, name, *, file=False, executable=False, preserve_link=False):
    """Validate bounded paths before any process is admitted."""
    raw = os.fspath(value)
    if not raw or len(os.fsencode(raw)) > MAX_PATH or any(ord(c) < 32 for c in raw):
        raise ConfigurationError("invalid " + name)
    path = Path(raw)
    if not path.is_absolute():
        raise ConfigurationError(name + " must be absolute")
    path = Path(os.path.abspath(path)) if preserve_link else path.resolve()
    if file and (not path.is_file() or not os.access(path, os.R_OK)):
        raise ConfigurationError("unreadable " + name)
    if executable and not os.access(path, os.X_OK):
        raise ConfigurationError("non-executable " + name)
    return path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def runtime_file(value, role):
    if value is None:
        return None
    path = absolute_path(value, role + " runtime file", file=True)
    if not 0 < path.stat().st_size <= 1024 * 1024:
        raise ConfigurationError("invalid " + role + " runtime file size")
    try:
        with path.open("rb") as stream:
            data = stream.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024 or not isinstance(
                json.loads(data, object_pairs_hook=unique_object), dict):
            raise ValueError()
    except (ValueError, UnicodeError, RecursionError):
        raise ConfigurationError("invalid " + role + " runtime JSON") from None
    return path


def secret(environment, name, maximum=256):
    value = environment.get(name)
    if not isinstance(value, str) or not 32 <= len(value.encode()) <= maximum:
        raise ConfigurationError("missing or invalid " + name)
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ConfigurationError("invalid " + name)
    return value


@dataclass(frozen=True)
class ChildSpec:
    role: str
    command: tuple
    environment: dict = field(repr=False)
    health_port: int
    reloadable: bool


@dataclass(frozen=True)
class Settings:
    repo: Path
    state: Path
    logs: Path
    specs: tuple = field(repr=False)
    ports: tuple
    startup_timeout: float
    stop_timeout: float
    probe_timeout: float
    kill_timeout: float
    restart_attempts: int
    restart_backoff_base: float
    restart_backoff_max: float


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo-root", "build-dir", "python", "state-dir", "log-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--descriptor", type=Path,
                        help="defaults to BUILD/shared/generated/rpc.desc")
    defaults = {"storage": 7105, "auth": 7101, "lobby": 7102,
                "gateway": 8888, "gateway-ws": 8889}
    for name, port in defaults.items():
        parser.add_argument("--" + name + "-port", default=str(port))
    for role, port in zip(ROLES, (17105, 17101, 17102, 18888)):
        parser.add_argument("--" + role + "-health-port", default=str(port))
        parser.add_argument("--" + role + "-runtime-config", type=Path)
    parser.add_argument("--storage-workers", default="4")
    parser.add_argument("--startup-timeout", type=float, default=15.0,
                        help="per-service readiness budget, at most 120 seconds")
    parser.add_argument("--stop-timeout", type=float, default=10.0,
                        help="per-child graceful shutdown budget, at most 30 seconds")
    parser.add_argument("--probe-timeout", type=float, default=.25,
                        help="whole HTTP request deadline, at most one second")
    parser.add_argument("--kill-timeout", type=float, default=2.0,
                        help="per-child post-SIGKILL join budget, at most five seconds")
    parser.add_argument("--restart-attempts", default="3",
                        help="lifetime restart budget per role after stack readiness, 0..10")
    parser.add_argument("--restart-backoff-base", type=float, default=.1)
    parser.add_argument("--restart-backoff-max", type=float, default=2.0,
                        help="full-jitter exponential backoff ceiling, at most 30 seconds")
    return parser.parse_args(argv)


def configuration(args, environment):
    """Build explicit role-scoped environments without ambient secret inheritance."""
    if not sys.platform.startswith("linux"):
        raise ConfigurationError("the supervisor requires Linux process signals")
    for name, maximum in (("startup_timeout", 120), ("stop_timeout", 30),
                          ("probe_timeout", 1), ("kill_timeout", 5)):
        value = getattr(args, name)
        if not math.isfinite(value) or not .05 <= value <= maximum:
            raise ConfigurationError("invalid " + name)
    attempts = bounded_int(args.restart_attempts, 10, "restart attempts", minimum=0)
    if not math.isfinite(args.restart_backoff_base) or \
            not math.isfinite(args.restart_backoff_max) or \
            not .01 <= args.restart_backoff_base <= 5 or \
            not args.restart_backoff_base <= args.restart_backoff_max <= 30:
        raise ConfigurationError("invalid restart backoff")
    repo = absolute_path(args.repo_root, "repo root")
    build = absolute_path(args.build_dir, "build directory")
    if not repo.is_dir() or not build.is_dir():
        raise ConfigurationError("missing repo or build directory")
    # Resolving a virtualenv Python symlink would discard its package environment.
    python = absolute_path(args.python, "Python executable", file=True, executable=True,
                           preserve_link=True)
    for source in (repo / "services/storage/server.py",):
        absolute_path(source, "Storage source", file=True)
    descriptor = absolute_path(args.descriptor or build / "shared/generated/rpc.desc",
                               "RPC descriptor", file=True)
    if not 0 < descriptor.stat().st_size <= 4 * 1024 * 1024:
        raise ConfigurationError("invalid RPC descriptor size")
    binaries = {"auth": build / "services/auth/ArenaAuth",
                "lobby": build / "services/lobby/ArenaLobby",
                "gateway": build / "services/gateway/app/ArenaServer"}
    binaries = {role: absolute_path(path, role + " executable", file=True, executable=True)
                for role, path in binaries.items()}
    state = absolute_path(args.state_dir, "state directory")
    logs = absolute_path(args.log_dir, "log directory")
    if state == logs:
        raise ConfigurationError("state and log directories must differ")
    ports = {role: bounded_int(getattr(args, role.replace("-", "_") + "_port"),
                               65535, role + " port")
             for role in (*ROLES, "gateway-ws")}
    health = {role: bounded_int(getattr(args, role + "_health_port"),
                                65535, role + " health port") for role in ROLES}
    all_ports = (*ports.values(), *health.values())
    if len(set(all_ports)) != len(all_ports):
        raise ConfigurationError("listener ports must not overlap")
    workers = bounded_int(args.storage_workers, 16, "storage workers")
    credentials = {name: secret(environment, name) for name in TOKENS}
    credentials["ARENA_JWT_KEY"] = secret(environment, "ARENA_JWT_KEY", 4096)
    if len(set(credentials.values())) != len(credentials):
        raise ConfigurationError("role credentials and signer key must differ")
    pg = {}
    for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER"):
        value = environment.get(name)
        if not value or len(value.encode()) > 1024 or any(ord(c) < 32 for c in value):
            raise ConfigurationError("missing or invalid " + name)
        pg[name] = value
    bounded_int(pg["PGPORT"], 65535, "PGPORT")
    if "PGPASSWORD" in environment:
        value = environment["PGPASSWORD"]
        if not value or len(value.encode()) > 4096 or "\0" in value:
            raise ConfigurationError("invalid PGPASSWORD")
        pg["PGPASSWORD"] = value
    if "PGPASSFILE" in environment:
        path = absolute_path(environment["PGPASSFILE"], "PGPASSFILE", file=True)
        mode = path.stat()
        if mode.st_uid != os.geteuid() or stat.S_IMODE(mode.st_mode) & 0o077:
            raise ConfigurationError("PGPASSFILE must be private and owned")
        pg["PGPASSFILE"] = str(path)
    if "PGSSLMODE" in environment:
        value = environment["PGSSLMODE"]
        if value not in ("disable", "allow", "prefer", "require", "verify-ca", "verify-full"):
            raise ConfigurationError("invalid PGSSLMODE")
        pg["PGSSLMODE"] = value
    # Deliberately exclude HOME, LD_PRELOAD, PYTHONPATH, PGSERVICE, PGOPTIONS,
    # legacy account credentials, clock injection, and all ambient ARENA_*.
    base = {name: environment[name] for name in ("PATH", "LANG", "LC_ALL", "TZ")
            if name in environment}
    base.update(PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    specs = []
    for role in ROLES:
        runtime = runtime_file(getattr(args, role + "_runtime_config"), role)
        child = dict(base, ARENA_NODE_ID=role, ARENA_HEALTH_PORT=str(health[role]))
        if runtime is not None:
            child["ARENA_RUNTIME_CONFIG"] = str(runtime)
        for name, maximum in NUMERIC_ENV[role].items():
            if name in environment:
                bounded_int(environment[name], maximum, name)
                child[name] = environment[name]
        if role == "storage":
            child.update(pg)
            child["ARENA_STORAGE_CALLER_TOKENS"] = json.dumps({
                "auth": credentials["ARENA_STORAGE_AUTH_TOKEN"],
                "lobby": credentials["ARENA_STORAGE_LOBBY_TOKEN"]}, separators=(",", ":"))
            command = (str(python), "-m", "services.storage.server", "--descriptor",
                       str(descriptor), "--port", str(ports[role]), "--workers", str(workers))
        elif role == "auth":
            child.update(ARENA_AUTH_PORT=str(ports[role]),
                         ARENA_STORAGE_PORT=str(ports["storage"]),
                         ARENA_AUTH_TOKEN=credentials["ARENA_AUTH_TOKEN"],
                         ARENA_STORAGE_AUTH_TOKEN=credentials["ARENA_STORAGE_AUTH_TOKEN"],
                         ARENA_JWT_KEY=credentials["ARENA_JWT_KEY"])
            command = (str(binaries[role]),)
        elif role == "lobby":
            child.update(ARENA_LOBBY_PORT=str(ports[role]),
                         ARENA_AUTH_PORT=str(ports["auth"]),
                         ARENA_STORAGE_PORT=str(ports["storage"]),
                         ARENA_AUTH_TOKEN=credentials["ARENA_AUTH_TOKEN"],
                         ARENA_LOBBY_TOKEN=credentials["ARENA_LOBBY_TOKEN"],
                         ARENA_STORAGE_LOBBY_TOKEN=credentials["ARENA_STORAGE_LOBBY_TOKEN"],
                         ARENA_LOBBY_STATE_DIR=str(state / "lobby"))
            command = (str(binaries[role]),)
        else:
            child.update(ARENA_TCP_PORT=str(ports[role]),
                         ARENA_WS_PORT=str(ports["gateway-ws"]),
                         ARENA_LOBBY_PORT=str(ports["lobby"]),
                         ARENA_LOBBY_TOKEN=credentials["ARENA_LOBBY_TOKEN"])
            command = (str(binaries[role]), "asio", "handshake")
        specs.append(ChildSpec(role, command, child, health[role], runtime is not None))
    return Settings(repo, state, logs, tuple(specs), all_ports, args.startup_timeout,
                    args.stop_timeout, args.probe_timeout, args.kill_timeout,
                    attempts, args.restart_backoff_base, args.restart_backoff_max)


def private_directory(path):
    """Refuse public or foreign-owned output directories instead of chmodding them."""
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or \
            stat.S_IMODE(info.st_mode) & 0o077:
        raise ConfigurationError("output directories must be private and owned")


def private_file(path):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    return os.fdopen(descriptor, "w", encoding="utf-8", buffering=1)


def check_ports(ports):
    """Reject occupied listeners before starting; children still own bind races."""
    with ExitStack() as stack:
        for port in ports:
            peer = stack.enter_context(socket.socket())
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                peer.bind(("127.0.0.1", port))
            except OSError:
                raise ConfigurationError("a configured listener is unavailable") from None


def health_length(header):
    lines = header.decode("ascii").split("\r\n")
    if not 1 <= len(lines) <= 64:
        raise ValueError("invalid health header")
    status = lines[0].split(" ", 2)
    if len(status) != 3 or status[0] not in ("HTTP/1.0", "HTTP/1.1") or status[1] != "200":
        raise ValueError("not ready")
    headers = unique_object((key.strip().lower(), value.strip())
                            for key, value in (line.split(":", 1) for line in lines[1:]))
    length = headers.get("content-length", "")
    if "transfer-encoding" in headers or not length.isascii() or not length.isdecimal():
        raise ValueError("invalid health framing")
    if len(length) > 5 or not 0 < int(length) <= MAX_HEALTH_BODY:
        raise ValueError("invalid health body size")
    return int(length)


def health_body(body, role):
    state = json.loads(body, object_pairs_hook=unique_object)
    return isinstance(state, dict) and state.get("service") == role and \
        state.get("alive") is True and state.get("ready") is True


HEALTH_REQUEST = (b"GET /health/ready HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                  b"Connection: close\r\n\r\n")


def health_ready(port, role, timeout):
    """Read one startup HTTP message under a bounded whole-request deadline."""
    deadline = time.monotonic() + timeout
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout) as peer:
            def receive(maximum):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError()
                peer.settimeout(remaining)
                data = peer.recv(maximum)
                if not data:
                    raise ValueError("incomplete HTTP message")
                return data
            peer.settimeout(max(.001, deadline - time.monotonic()))
            peer.sendall(HEALTH_REQUEST)
            message = b""
            while b"\r\n\r\n" not in message:
                if len(message) >= MAX_HEALTH_HEADER:
                    return False
                message += receive(min(1024, MAX_HEALTH_HEADER - len(message)))
            header, body = message.split(b"\r\n\r\n", 1)
            length = health_length(header)
            if len(body) > length:
                return False
            while len(body) < length:
                body += receive(length - len(body))
            return health_body(body, role)
    except (OSError, ValueError, UnicodeError, RecursionError):
        return False


class HealthProbe:
    """One finite nonblocking recovery probe, polled by the supervisor owner."""
    def __init__(self, spec, deadline):
        self.role, self.deadline = spec.role, deadline
        self.socket = socket.socket()
        self.stage, self.sent, self.message = "connect", 0, b""
        self.length = None
        self.result = None
        try:
            self.socket.setblocking(False)
            error = self.socket.connect_ex(("127.0.0.1", spec.health_port))
            if error == 0:
                self.stage = "write"
            elif error not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                self.finish(False)
        except OSError:
            self.finish(False)

    def finish(self, result):
        self.result = result
        self.socket.close()

    def close(self):
        if self.result is None:
            self.finish(False)

    def advance(self, readable, writable, now):
        if self.result is not None:
            return
        if now >= self.deadline:
            return self.finish(False)
        try:
            if self.stage == "connect" and writable:
                if self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                    return self.finish(False)
                self.stage = "write"
            if self.stage == "write" and writable:
                count = self.socket.send(HEALTH_REQUEST[self.sent:])
                if not count:
                    return self.finish(False)
                self.sent += count
                if self.sent == len(HEALTH_REQUEST):
                    self.stage = "read"
            if self.stage == "read" and readable:
                cap = MAX_HEALTH_HEADER if self.length is None else self.length
                remaining = cap - len(self.message)
                if remaining <= 0:
                    return self.finish(False)
                data = self.socket.recv(min(4096, remaining))
                if not data:
                    return self.finish(False)
                self.message += data
                if self.length is None and b"\r\n\r\n" in self.message:
                    header, self.message = self.message.split(b"\r\n\r\n", 1)
                    self.length = health_length(header)
                if self.length is None and len(self.message) >= MAX_HEALTH_HEADER:
                    return self.finish(False)
                if self.length is not None and len(self.message) >= self.length:
                    self.finish(len(self.message) == self.length and
                                health_body(self.message, self.role))
        except BlockingIOError:
            pass
        except (OSError, ValueError, UnicodeError, RecursionError):
            self.finish(False)


@dataclass
class Child:
    spec: ChildSpec
    process: subprocess.Popen
    attempt: int
    ready: bool = False
    retired: bool = False
    reload_generation: int = 0


@dataclass
class RoleState:
    spec: ChildSpec
    child: Child | None = None
    phase: str = "initial"
    attempts: int = 0
    due: float = 0
    deadline: float = 0
    next_probe: float = 0
    probe: HealthProbe | None = None

    def close_probe(self):
        if self.probe is not None:
            self.probe.close()
            self.probe = None


class Supervisor:
    """One owner supervises independent role budgets, probes, signals, and joins."""
    def __init__(self, settings):
        self.settings = settings
        self.children = []
        self.roles = {spec.role: RoleState(spec) for spec in settings.specs}
        self.stop_signal = None
        self.reload_pending = False
        self.reload_generation = 0
        self.running = False
        self.degraded = False
        self.forced = False
        self.events = None
        self.stdout_open = True
        self.jitter = random.SystemRandom()
        self.run_id = str(os.getpid()) + "-" + str(time.time_ns())

    def emit(self, event, **fields):
        row = json.dumps(dict(component="supervisor", event=event, **fields),
                         separators=(",", ":"), allow_nan=False)
        if self.events is not None:
            self.events.write(row + "\n")
        if self.stdout_open:
            try:
                print(row, flush=True)
            except BrokenPipeError:
                self.stdout_open = False

    def signal(self, number, _frame):
        if number == signal.SIGHUP:
            self.reload_pending = True
        else:
            self.stop_signal = number

    def inspect(self):
        if self.stop_signal is not None:
            raise StopRequested()
        if not self.running:
            for child in self.children:
                code = child.process.poll()
                if code is not None:
                    self.emit("unexpected_exit", role=child.spec.role,
                              pid=child.process.pid, exit_code=code)
                    raise StackFailure()
        if self.reload_pending:
            self.reload_pending = False
            self.reload_generation += 1
        for role in self.roles.values():
            child = role.child
            if child is not None and child.ready and child.spec.reloadable and \
                    child.reload_generation < self.reload_generation:
                child.process.send_signal(signal.SIGHUP)
                child.reload_generation = self.reload_generation
                self.emit("reload", role=child.spec.role, pid=child.process.pid)

    def spawn(self, spec, attempt):
        name = spec.role + "-" + self.run_id + "-" + str(attempt) + ".log"
        with private_file(self.settings.logs / name) as output:
            process = subprocess.Popen(spec.command, cwd=self.settings.repo,
                                       env=spec.environment, stdin=subprocess.DEVNULL,
                                       stdout=output, stderr=output, close_fds=True,
                                       start_new_session=True)
        child = Child(spec, process, attempt)
        self.children.append(child)
        self.roles[spec.role].child = child
        self.emit("started", role=spec.role, pid=process.pid, attempt=attempt)
        return child

    def start(self, spec):
        self.inspect()
        child = self.spawn(spec, 0)
        deadline = time.monotonic() + self.settings.startup_timeout
        while True:
            self.inspect()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.emit("readiness_timeout", role=spec.role, pid=child.process.pid)
                raise StackFailure()
            if health_ready(spec.health_port, spec.role,
                            min(remaining, self.settings.probe_timeout)):
                child.ready = True
                self.roles[spec.role].phase = "healthy"
                self.inspect()
                self.emit("ready", role=spec.role, pid=child.process.pid)
                return
            time.sleep(min(.05, max(0, deadline - time.monotonic())))

    def schedule_restart(self, role, now):
        role.close_probe()
        if role.child is not None:
            role.child.retired = True
            role.child = None
        if role.attempts >= self.settings.restart_attempts:
            role.phase = "degraded"
            self.degraded = True
            self.emit("degraded", role=role.spec.role, attempts=role.attempts)
            return
        cap = min(self.settings.restart_backoff_max,
                  self.settings.restart_backoff_base * 2 ** role.attempts)
        delay = self.jitter.uniform(0, cap)
        role.phase, role.due = "backoff", now + delay
        self.emit("restart_scheduled", role=role.spec.role, attempt=role.attempts + 1,
                  backoff_ms=round(delay * 1000, 3), backoff_cap_ms=round(cap * 1000, 3))

    def begin_recovery_stop(self, role, now):
        role.close_probe()
        role.child.ready = False
        role.phase, role.deadline = "terminating", now + self.settings.stop_timeout
        self.emit("restart_readiness_timeout", role=role.spec.role,
                  pid=role.child.process.pid, attempt=role.attempts)
        try:
            os.killpg(role.child.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    def monitor(self):
        """Advance each role without waiting for another role's health or exit."""
        self.inspect()
        now = time.monotonic()
        for role in self.roles.values():
            if role.child is not None:
                code = role.child.process.poll()
                if code is not None:
                    if role.phase == "degraded":
                        role.child.retired = True
                        role.child = None
                        continue
                    self.emit("unexpected_exit" if role.phase in ("healthy", "starting")
                              else "restart_stopped", role=role.spec.role,
                              pid=role.child.process.pid, exit_code=code)
                    self.schedule_restart(role, now)
            if role.phase == "backoff" and now >= role.due:
                role.attempts += 1
                try:
                    self.spawn(role.spec, role.attempts)
                except OSError:
                    self.emit("restart_spawn_failed", role=role.spec.role, attempt=role.attempts)
                    self.schedule_restart(role, now)
                    continue
                role.phase, role.deadline = "starting", now + self.settings.startup_timeout
                role.next_probe = now
            if role.phase == "starting":
                if now >= role.deadline:
                    self.begin_recovery_stop(role, now)
                elif role.probe is not None and role.probe.result is not None:
                    accepted = role.probe.result
                    role.close_probe()
                    if accepted:
                        role.child.ready = True
                        role.phase = "healthy"
                        self.emit("recovered", role=role.spec.role, pid=role.child.process.pid,
                                  attempt=role.attempts)
                    else:
                        role.next_probe = now + .05
                if role.phase == "starting" and role.probe is None and now >= role.next_probe:
                    try:
                        role.probe = HealthProbe(role.spec, min(role.deadline,
                                                              now + self.settings.probe_timeout))
                    except OSError:
                        # Resource failure consumes this role's readiness budget;
                        # it must not terminate already healthy bystanders.
                        role.next_probe = now + .05
            if role.phase in ("terminating", "killing") and now >= role.deadline:
                if role.phase == "terminating":
                    self.forced = True
                    role.phase, role.deadline = "killing", now + self.settings.kill_timeout
                    self.emit("forced_kill", role=role.spec.role, pid=role.child.process.pid,
                              scope="recovery")
                    try:
                        os.killpg(role.child.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                else:
                    # Never create another child while its predecessor remains unjoined.
                    role.phase = "degraded"
                    self.degraded = True
                    self.emit("join_timeout", role=role.spec.role, pid=role.child.process.pid)
        probes = [role.probe for role in self.roles.values()
                  if role.probe is not None and role.probe.result is None]
        readable = [probe.socket for probe in probes if probe.stage == "read"]
        writable = [probe.socket for probe in probes if probe.stage != "read"]
        ready_read, ready_write, _ = select.select(readable, writable, [], .05)
        now = time.monotonic()
        for probe in probes:
            probe.advance(probe.socket in ready_read, probe.socket in ready_write, now)

    def stop(self):
        clean = True
        for role in self.roles.values():
            role.close_probe()
        ordered = [child for role in reversed(ROLES) for child in reversed(self.children)
                   if child.spec.role == role]
        for child in ordered:
            process, role = child.process, child.spec.role
            try:
                self.emit("stopping", role=role, pid=process.pid)
            except OSError:
                clean = False
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=self.settings.stop_timeout)
                except subprocess.TimeoutExpired:
                    clean = False
                    try:
                        self.emit("forced_kill", role=role, pid=process.pid, scope="shutdown")
                    except OSError:
                        pass
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=self.settings.kill_timeout)
                    except subprocess.TimeoutExpired:
                        try:
                            self.emit("join_timeout", role=role, pid=process.pid)
                        except OSError:
                            pass
            if process.poll() is None or not child.retired and process.returncode != 0:
                clean = False
            try:
                self.emit("stopped", role=role, pid=process.pid, exit_code=process.poll())
            except OSError:
                clean = False
        return clean

    def run(self):
        old_handlers = {}
        code = 0
        os.umask(0o077)
        with ExitStack() as resources:
            private_directory(self.settings.state)
            private_directory(self.settings.logs)
            private_directory(self.settings.state / "lobby")
            lock_fd = os.open(self.settings.state / "supervisor.lock",
                              os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            lock = resources.enter_context(os.fdopen(lock_fd, "r+"))
            info = os.fstat(lock.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or \
                    stat.S_IMODE(info.st_mode) & 0o077 or info.st_nlink != 1:
                raise ConfigurationError("supervisor lock must be private and owned")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ConfigurationError("state directory already supervised") from None
            check_ports(self.settings.ports)
            self.events = resources.enter_context(private_file(
                self.settings.logs / ("supervisor-" + self.run_id + ".jsonl")))
            for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                old_handlers[number] = signal.signal(number, self.signal)
            try:
                for spec in self.settings.specs:
                    self.start(spec)
                self.running = True
                self.emit("stack_ready")
                while True:
                    self.monitor()
            except StopRequested:
                self.emit("stop_requested", signal=self.stop_signal)
            except (StackFailure, OSError, ValueError):
                code = 1
                try:
                    self.emit("failure")
                except OSError:
                    pass
            finally:
                if not self.stop() or self.degraded or self.forced:
                    code = 1
                try:
                    self.emit("finished", exit_code=code, degraded=self.degraded)
                finally:
                    for number, handler in old_handlers.items():
                        signal.signal(number, handler)
                    self.events = None
        return code

def main(argv=None):
    try:
        settings = configuration(arguments(argv), os.environ)
        return Supervisor(settings).run()
    except ConfigurationError as error:
        print(json.dumps(dict(component="supervisor", event="invalid_configuration",
                              reason=str(error)), separators=(",", ":")), flush=True)
        return 2
    except (OSError, ValueError):
        # Exception messages can contain environment credentials or operator paths.
        print('{"component":"supervisor","event":"startup_failure"}', flush=True)
        return 2


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    raise SystemExit(main())
