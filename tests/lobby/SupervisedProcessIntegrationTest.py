#!/usr/bin/env python3
"""Real production supervisor, strict-role services, Auth recovery, and owned PG."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, ROOT / "tests/lobby", ROOT / "tests/auth", ROOT / "tests/storage"):
    sys.path.insert(0, str(folder))
from ServiceStackFixture import ServiceStack
from AuthProcessIntegrationTest import health_http
from StorageMigrationTest import require, run

ROLES = ("storage", "auth", "lobby", "gateway")
SANITIZER = re.compile(
    r"AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer|runtime error:|ThreadSanitizer"
)
MAX_LOG = 4 * 1024 * 1024


def bounded_text(path):
    with path.open("rb") as stream:
        data = stream.read(MAX_LOG + 1)
    require(len(data) <= MAX_LOG, "acceptance log exceeded four MiB")
    return data.decode("utf-8", errors="replace")


def json_rows(path):
    rows = []
    for line in bounded_text(path).splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue  # A live line may still be incomplete.
        if isinstance(row, dict):
            rows.append(row)
    return rows


def trace_locations(error):
    """Keep all call sites while excluding exception text, values, and protobufs."""
    locations = []
    trace = error.__traceback__
    while trace is not None:
        locations.append(dict(file=Path(trace.tb_frame.f_code.co_filename).name, line=trace.tb_lineno))
        trace = trace.tb_next
    return locations


class Acceptance(ServiceStack):
    def __init__(self, args):
        super().__init__(args, grace_ms=3000)
        self.supervisor = None
        self.stdout = None
        self.owned_history = []
        self.retained_logs = []
        self.logs = args.report.parent / (args.report.stem + "-logs")
        self.room = 9001
        self.report.update(scope="real production launcher and single-role Auth recovery",
                           current_case="validate_artifacts", manual_room=self.room,
                           test_clock_injected=False)

    def rows(self):
        return json_rows(self.stdout) if self.stdout is not None and self.stdout.exists() else []

    def lifecycle_starts(self):
        rows = [row for row in self.rows() if row.get("event") == "started"]
        known = {row["pid"] for row in self.owned_history}
        self.owned_history.extend(row for row in rows if row["pid"] not in known)
        return rows

    def current(self):
        return {row["role"]: row["pid"] for row in self.lifecycle_starts()}

    def run_log_paths(self):
        # A report directory retains prior runs; only this supervisor owns this cut.
        paths = set(self.retained_logs)
        if self.stdout is not None and self.stdout.exists():
            paths.add(self.stdout)
        if self.supervisor is not None:
            for prefix in (*ROLES, "supervisor"):
                paths.update(self.logs.glob(prefix + "-" + str(self.supervisor.pid) + "-*"))
        return sorted(paths)

    def until(self, predicate, message, timeout=10):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            require(self.supervisor is None or self.supervisor.poll() is None,
                    "production supervisor exited before " + message)
            try:
                if predicate():
                    return
            except (OSError, ConnectionError, TimeoutError, ValueError):
                pass
            time.sleep(.01)
        raise AssertionError("deadline waiting for " + message)

    def validate_artifacts(self):
        require(self.args.gateway.name == "ArenaServer" and len(self.args.gateway.parents) >= 4,
                "Gateway must be the real build artifact")
        self.build = self.args.gateway.parents[3]
        expected = {"gateway": self.build / "services/gateway/app/ArenaServer",
                    "auth": self.build / "services/auth/ArenaAuth",
                    "lobby": self.build / "services/lobby/ArenaLobby"}
        for role, path in expected.items():
            selected = getattr(self.args, role)
            require(selected == path.resolve() and selected.is_file() and os.access(selected, os.X_OK),
                    role + " artifact does not belong to the selected build")
            with selected.open("rb") as stream:
                require(stream.read(4) == b"\x7fELF", role + " must be a real ELF service executable")
        require((self.args.repository_root / "tools/runtime/RunServices.py").is_file(),
                "production launcher is missing")

    def retain_log(self, name, text):
        require(len(text.encode()) <= MAX_LOG, "retained acceptance log exceeded four MiB")
        self.assert_no_secrets(text)
        self.logs.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = self.logs.stat()
        require(info.st_uid == os.geteuid() and not stat.S_IMODE(info.st_mode) & 0o077,
                "acceptance logs must be private and owned")
        destination = self.logs / (name + "-" + str(os.getpid()) + ".log")
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write(text)
        self.retained_logs.append(destination)

    def prepare_database(self):
        # Do not call ServiceStack.setup(): only the production launcher may spawn roles.
        self.cluster.start()
        self.cluster.create(self.database)
        migrated = run([sys.executable, self.args.repository_root / "services/storage/migrate.py", "up"],
                       self.cluster.environment(self.database), timeout=95)
        self.retain_log("migrations", migrated.stdout + migrated.stderr)
        require(migrated.returncode == 0, "private PostgreSQL migrations failed")
        self.cluster.sql((self.args.repository_root / "services/storage/fixtures/seed.sql").read_text(),
                         self.database, single=True)
        salt = bytes(16)
        digest = "$arena-pbkdf2-sha256$100000$" + salt.hex() + "$" + hashlib.pbkdf2_hmac(
            "sha256", self.password.encode(), salt, 100000).hex()
        self.query("UPDATE arena.account SET password_digest=%s WHERE account_name IN ('alice','bob')",
                   (digest,))

    def start_supervisor(self):
        self.logs.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = self.logs.stat()
        require(info.st_uid == os.geteuid() and not stat.S_IMODE(info.st_mode) & 0o077,
                "supervisor acceptance logs must be private and owned")
        self.stdout = self.logs / ("launcher-" + str(os.getpid()) + "-" + str(time.time_ns()) + ".jsonl")
        environment = self.environment()
        environment.update(ARENA_LOBBY_TOKEN=self.lobby_token, ARENA_AUTH_TOKEN=self.auth_token,
                           ARENA_STORAGE_AUTH_TOKEN=self.storage_token,
                           ARENA_STORAGE_LOBBY_TOKEN=self.other_token, ARENA_JWT_KEY=self.signer_key)
        command = [sys.executable, str(self.args.repository_root / "tools/runtime/RunServices.py"),
                   "--repo-root", str(self.args.repository_root), "--build-dir", str(self.build),
                   "--python", sys.executable, "--descriptor", str(self.args.descriptor),
                   "--state-dir", str(self.cluster.root / "supervised-state"),
                   "--log-dir", str(self.logs), "--startup-timeout", "15",
                   "--stop-timeout", "10", "--probe-timeout", ".25",
                   "--restart-attempts", "2", "--restart-backoff-base", ".05",
                   "--restart-backoff-max", ".2"]
        for role in ROLES:
            command.extend(("--" + role + "-port", str(self.ports[role]),
                            "--" + role + "-health-port", str(self.ports[role + "-health"])))
        command.extend(("--gateway-ws-port", str(self.ports["gateway-ws"])))
        descriptor = os.open(self.stdout, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as output:
            self.supervisor = subprocess.Popen(command, cwd=self.args.repository_root, env=environment,
                                               stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                               close_fds=True, start_new_session=True)
        self.until(lambda: any(row.get("event") == "stack_ready" for row in self.rows()),
                   "real production stack readiness", 65)
        require([row["role"] for row in self.lifecycle_starts()] == list(ROLES), "wrong initial service order")
        require(all(row["attempt"] == 0 for row in self.lifecycle_starts()), "initial startup retried a service")

    def sample(self, label):
        current = self.current()
        require(set(current) == set(ROLES), "missing production service PID")
        sampled = {}
        for role, pid in current.items():
            executable = Path(os.readlink(f"/proc/{pid}/exe")).resolve()
            expected = Path(sys.executable).resolve() if role == "storage" else getattr(self.args, role)
            require(executable == expected, role + " is not the expected real executable")
            if role == "storage":
                command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
                require(b"-m" in command and b"services.storage.server" in command,
                        "Storage is not the production Python module")
            sampled[role] = dict(pid=pid, executable=str(executable),
                                 fd=len(os.listdir(f"/proc/{pid}/fd")))
        self.report["physical_process_samples"].append(dict(label=label, roles=sampled))
        return current

    def grant(self, peer, **options):
        result = peer.login(**options)
        self.secrets += (result["token"].decode(),)
        return result

    def journey_and_auth_recovery(self):
        initial = self.sample("before-auth-crash")
        alice, bob = self.connect(), self.connect(True)
        first, second = self.grant(alice), self.grant(bob, account=b"bob")
        alice.join(self.room)
        bob.join(self.room)
        require(alice.query()["room"] == bob.query()["room"] == self.room, "manual room binding failed")
        state = alice.room_snapshot()
        require(set(state["players"]) == {42, 43}, "manual actor roster differs")
        require(alice.control(state, 42, 1, 1, -1)[1] == 1, "Alice initial input was not acknowledged")
        require(bob.control(state, 43, 1, -1, 1)[1] == 1, "Bob initial input was not acknowledged")
        before = alice.room_snapshot()
        killed_at = time.monotonic()
        os.kill(initial["auth"], signal.SIGKILL)  # PID was obtained from owned started + /proc verification.
        during, status = alice.control(before, 42, 2, 0, 1)
        require(status == 1 and during["incarnation"] == before["incarnation"],
                "Auth crash interrupted an existing room input")
        kept, status = bob.control(during, 43, 2, -1, 0)
        require(status == 1, "Auth crash interrupted Bob input")
        self.until(lambda: any(row.get("event") == "recovered" and row.get("role") == "auth"
                               for row in self.rows()), "bounded automatic Auth recovery", 20)
        after = self.sample("after-auth-recovery")
        require(after["auth"] != initial["auth"], "Auth was not actually restarted")
        require(all(after[role] == initial[role] for role in ("gateway", "lobby", "storage")),
                "Auth recovery cascaded into a bystander process")
        require([row["role"] for row in self.lifecycle_starts()] == [*ROLES, "auth"],
                "supervisor restarted an unrelated role")
        self.until(lambda: all(health_http(self.ports[role + "-health"], "/health/ready")[0] == 200
                               for role in ROLES), "four actual service health endpoints recover", 10)
        current = alice.room_snapshot()
        require(current["incarnation"] == before["incarnation"] and current["players"] == kept["players"],
                "Auth recovery reset acknowledged room state")
        require(alice.query()["session"] == first["session"] and bob.query()["session"] == second["session"],
                "Auth recovery changed a live logical session")
        alice.close(reset=True)
        resumed_peer = self.connect(True)
        resumed = self.grant(resumed_peer, token=first["token"])
        require(resumed["resumed"] and resumed["session"] == first["session"] and
                resumed["room"] == self.room and resumed["epoch"] > first["epoch"],
                "restarted Auth did not resume the same session and room")
        restored = resumed_peer.room_snapshot()
        require(restored["incarnation"] == before["incarnation"] and restored["players"] == kept["players"],
                "JWT resume replaced actor membership or controls")
        require(resumed_peer.control(restored, 42, 3, 1, 0)[1] == 1, "resumed input was not acknowledged")
        require(resumed_peer.control(restored, 42, 3, 1, 0)[1] == 2, "duplicate input was not preserved")
        final, status = bob.control(restored, 43, 3, 0, -1)
        require(status == 1 and (final["players"][42]["x"], final["players"][42]["y"]) == (2, 0) and
                (final["players"][43]["x"], final["players"][43]["y"]) == (-2, 0),
                "recovery changed ordered input application")
        require(resumed_peer.leave()["room"] == bob.leave()["room"] == 0, "room membership was not released")
        self.report["cases"].append(dict(name="real_auth_role_recovery", passed=True,
            recovery_elapsed_ms=(time.monotonic() - killed_at) * 1000, unchanged_bystanders=True,
            same_session=True, same_room=True, same_actor=True, input_ack_and_duplicate=True,
            room_incarnation=str(before["incarnation"]), final_checksum=str(final["checksum"])))

    def stop_supervisor(self):
        for peer in self.peers:
            peer.close()
        require(self.supervisor is not None and self.supervisor.poll() is None,
                "supervisor exited before operator stop")
        self.supervisor.send_signal(signal.SIGTERM)
        require(self.supervisor.wait(timeout=50) == 0, "production supervisor failed finite graceful stop")
        require(not any(row.get("event") in ("forced_kill", "join_timeout", "degraded", "failure")
                        for row in self.rows()), "supervisor exhausted or forced cleanup")
        owned = self.lifecycle_starts()
        require(len({row["pid"] for row in owned}) == 5, "unexpected owned process history")
        require(all(not Path("/proc", str(row["pid"])).exists() for row in owned),
                "supervisor retained an owned historical or current PID")
        self.report["supervisor_owned_history"] = owned
        self.report["supervisor_exit_code"] = self.supervisor.returncode
        self.validate_logs()
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                           "AND state LIKE 'idle in transaction%'")[0][0] == 0,
                "private PostgreSQL retained an idle transaction")
        require(self.query("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                           "AND application_name='arena-storage'")[0][0] == 0,
                "Storage retained a physical PostgreSQL connection")
        self.report.update(passed=True, idle_transactions=0, checked_out=0, active_workers=0)

    def validate_logs(self):
        total = 0
        stops = {}
        for path in self.run_log_paths():
            require(path.is_file() and stat.S_IMODE(path.stat().st_mode) == 0o600,
                    "supervised process log is not private")
            total += path.stat().st_size
            text = bounded_text(path)
            self.assert_no_secrets(text)
            require(not SANITIZER.search(text), "sanitizer reported a production stack failure")
            for row in json_rows(path):
                if row.get("state") == "stopped" and row.get("service") in ROLES:
                    require(row["service"] not in stops, "duplicate final service stop report")
                    stops[row["service"]] = row
            if path.name.startswith("gateway-"):
                require("LOBBY_COUNTS tracked=0 business_inflight=0 heartbeat_inflight=0 probe_inflight=0 owner_posts=0 stopped=1" in text,
                        "Gateway retained remote physical work")
                require(re.search(r"connection_admission active=0 ", text), "Gateway retained connections")
        require(total <= 16 * 1024 * 1024, "aggregate acceptance logs exceeded sixteen MiB")
        require(set(stops) == {"storage", "auth", "lobby"}, "missing real service stop counters")
        require(all(stops["auth"][name] == 0 for name in ("active", "queued", "busy", "workers")),
                "Auth retained workers or operations")
        require(all(stops["lobby"][name] == 0 for name in (
            "active", "auth_inflight", "storage_inflight", "journal_pending", "owner_pending")) and
                stops["lobby"]["journal_closed"], "Lobby retained operations or journal resources")
        require(stops["lobby"]["room_indexes"] == 0, "Lobby retained a dangling room binding")
        require(stops["storage"]["worker"]["active"] == stops["storage"]["pool"]["checked_out"] == 0 and
                stops["storage"]["pool"]["closed"], "Storage retained worker jobs or pool loans")
        self.report["stop_counters"] = stops
        self.report["supervisor_lifecycle"] = self.rows()
        self.report["owned_run_logs"] = [str(path) for path in self.run_log_paths()]

    def run(self):
        self.validate_artifacts()
        self.report["current_case"] = "private_database_preparation"
        self.prepare_database()
        self.report["current_case"] = "production_launcher_startup"
        self.start_supervisor()
        self.report["current_case"] = "auth_crash_and_existing_room_continuity"
        self.journey_and_auth_recovery()
        self.report["current_case"] = "operator_stop_and_physical_cleanup"
        self.stop_supervisor()

    def close(self):
        for peer in self.peers:
            try:
                peer.close()
            except OSError:
                pass
        try:
            self.lifecycle_starts()
        except Exception:
            self.report.update(passed=False, lifecycle_log_failure=True)
        try:
            # Keep the supervisor alive during finite cleanup so it can reap its own groups.
            if self.supervisor is not None and self.supervisor.poll() is None:
                self.supervisor.send_signal(signal.SIGTERM)
                try:
                    self.supervisor.wait(timeout=50)
                except subprocess.TimeoutExpired:
                    self.report.update(passed=False, supervisor_cleanup_timeout=True)
                    for row in reversed(self.owned_history):
                        try:
                            status = Path(f"/proc/{row['pid']}/status").read_text()
                            parent = re.search(r"^PPid:\s+(\d+)$", status, re.MULTILINE)
                            # A dead historical PID may have been reused by another program.
                            if parent and int(parent.group(1)) == self.supervisor.pid:
                                os.killpg(row["pid"], signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        except FileNotFoundError:
                            pass
                    self.supervisor.kill()
                    self.supervisor.wait(timeout=5)
        finally:
            # A malformed/oversized lifecycle log must not prevent private PG teardown.
            try:
                self.lifecycle_starts()
            except Exception:
                self.report.update(passed=False, lifecycle_log_failure=True)
            if any(Path("/proc", str(row["pid"])).exists() for row in self.owned_history):
                self.report.update(passed=False, owned_pid_leak=True)
            try:
                if self.cluster.log.exists():
                    self.retain_log("postgres", bounded_text(self.cluster.log))
            except Exception:
                self.report.update(passed=False, postgres_log_retention_failed=True)
            finally:
                super().close()
        self.report["supervisor_owned_history"] = self.owned_history
        self.report["launcher_stdout"] = str(self.stdout) if self.stdout is not None else None
        paths = self.run_log_paths()
        require(sum(path.stat().st_size for path in paths) <= 16 * 1024 * 1024,
                "aggregate retained acceptance logs exceeded sixteen MiB")
        self.report["owned_run_logs"] = [str(path) for path in paths]
        for path in paths:
            text = bounded_text(path)
            self.assert_no_secrets(text)
            if SANITIZER.search(text):
                self.report.update(passed=False, sanitizer_failure=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=ROOT)
    for name in ("pg-bin", "descriptor", "auth", "lobby", "gateway", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--pg-share", type=Path)
    parser.add_argument("--pg-lib", type=Path)
    args = parser.parse_args()
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    args.report.parent.mkdir(parents=True, exist_ok=True)
    test = None
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        test = Acceptance(args)
        test.run()
    except (Exception, KeyboardInterrupt) as error:
        # Raw RPC assertion payloads may contain bearer tokens; preserve only safe location metadata.
        locations = trace_locations(error)
        failure = dict(passed=False, error=type(error).__name__,
                       failure_location=locations[-1] if locations else {},
                       failure_traceback=locations)
        if test is None:
            report = failure
        else:
            test.report.update(failure)
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if test is not None:
            try:
                test.close()
            except Exception as error:
                test.report.update(passed=False, cleanup_error=type(error).__name__,
                                   cleanup_traceback=trace_locations(error))
            report = test.report
    report["source_hashes"] = {}
    for name in ("tools/runtime/RunServices.py", "services/auth/app/main.cpp",
                 "services/lobby/app/main.cpp", "services/gateway/app/main.cpp",
                 "services/storage/server.py", "tests/lobby/SupervisedProcessIntegrationTest.py"):
        try:
            report["source_hashes"][name] = hashlib.sha256((args.repository_root / name).read_bytes()).hexdigest()
        except OSError:
            report.update(passed=False, source_missing=True)
    output = json.dumps(report, indent=2)
    if test is not None:
        test.assert_no_secrets(output)
    args.report.write_text(output + "\n")
    print(json.dumps(dict(passed=report["passed"], report=str(args.report),
                          current_case=report.get("current_case"), error=report.get("error"))))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
