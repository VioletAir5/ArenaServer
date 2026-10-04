#!/usr/bin/env python3
# Real TCP churn with external /proc telemetry and a loopback-only dashboard.
import argparse
import asyncio
import collections
import csv
import http.server
import json
import os
from pathlib import Path
import random
import signal
import socket
import struct
import subprocess
import threading
import time
from urllib.parse import urlsplit

SANITIZER_ERRORS = (
    "ERROR: AddressSanitizer", "LeakSanitizer", "AddressSanitizer:DEADLYSIGNAL",
    "WARNING: ThreadSanitizer", "runtime error:", "Sanitizer CHECK failed",
)
UNEXPECTED_ERRORS = (
    "connection setup failed", "connection limit reached",
    "output limit reached", "idle timeout", "slow client timeout",
    "Too many open files", "TcpConnection I/O error",
)


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[int((len(ordered) - 1) * fraction)], 3)


def ticks(path):
    fields = path.read_text().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def proc_metrics(pid):
    root = Path(f"/proc/{pid}")
    status = {}
    for line in (root / "status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    epolls = []
    descriptors = list((root / "fd").iterdir())
    for fd in descriptors:
        try:
            if os.readlink(fd) == "anon_inode:[eventpoll]":
                data = (root / "fdinfo" / fd.name).read_text()
                epolls.append({"fd": int(fd.name), "watched": data.count("tfd:")})
        except FileNotFoundError:
            pass  # A connection can close between enumeration and readlink.
    threads = []
    for task in (root / "task").iterdir():
        try:
            threads.append({
                "tid": int(task.name),
                "ticks": ticks(task / "stat"),
                "name": (task / "comm").read_text().strip(),
            })
        except FileNotFoundError:
            pass
    kb = lambda key: int(status.get(key, "0 kB").split()[0])
    return {
        "fd": len(descriptors), "rss_mib": round(kb("VmRSS") / 1024, 2),
        "rss_peak_mib": round(kb("VmHWM") / 1024, 2),
        "ticks": ticks(root / "stat"),
        "threads": sorted(threads, key=lambda item: item["tid"]),
        "epolls": sorted(epolls, key=lambda item: item["fd"]),
    }


class Monitor:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.failure = threading.Event()
        self.started = time.monotonic()
        self.process = None
        self.http = None
        self.samples = []
        self.events = collections.deque(maxlen=80)
        self.rounds = []
        self.connect_times = collections.deque(maxlen=100000)
        self.echo_times = collections.deque(maxlen=100000)
        self.previous = None
        self.csv_file = (self.output / "metrics.csv").open("w", newline="")
        self.csv = csv.writer(self.csv_file)
        self.csv.writerow([
            "elapsed_s", "phase", "fd", "rss_mib", "rss_peak_mib", "cpu_pct",
            "completed", "connections_per_s", "bytes_verified", "client_open",
        ])
        self.state = {
            "status": "starting", "phase": "startup", "duration_s": args.duration,
            "workers": args.workers, "connections_per_round": args.connections,
            "completed": 0, "opened": 0, "round": 0, "client_open": 0,
            "bytes_verified": 0, "fin": 0, "rst": 0, "errors": 0,
            "sanitizer_errors": 0, "peer_resets_logged": 0,
            "baseline_fd": None, "pid": None, "port": None, "last_error": None,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
            "server": str(Path(args.binary).resolve()),
            "output": str(self.output), "http_port": None,
        }

    def event(self, message, error=False):
        with self.lock:
            self.events.append({"t": round(time.monotonic() - self.started, 1),
                                "message": message, "error": error})
            if error:
                self.state["errors"] += 1
                self.state["last_error"] = message
                self.failure.set()

    def snapshot(self):
        with self.lock:
            data = dict(self.state)
            data["elapsed_s"] = self.state.get(
                "finished_elapsed_s", round(time.monotonic() - self.started, 1))
            data["samples"] = list(self.samples)
            data["events"] = list(self.events)
            data["rounds"] = list(self.rounds[-60:])
            data["connect_p50_ms"] = percentile(self.connect_times, .50)
            data["connect_p99_ms"] = percentile(self.connect_times, .99)
            data["echo_p50_ms"] = percentile(self.echo_times, .50)
            data["echo_p99_ms"] = percentile(self.echo_times, .99)
            data["latency_window"] = len(self.echo_times)
            data["latest"] = dict(self.samples[-1]) if self.samples else None
            return data

    def write_snapshot(self, name="live.json"):
        temporary = self.output / (name + ".tmp")
        temporary.write_text(json.dumps(self.snapshot(), ensure_ascii=False))
        temporary.replace(self.output / name)

    def read_logs(self):
        # Bound verbose server output to 3 x 5 MiB; diagnostic counts persist.
        log_path = self.output / "server.log"
        log = log_path.open("w", buffering=1)
        size = 0
        try:
            for line in self.process.stdout:
                log.write(line)
                size += len(line.encode())
                if "TEST_HOST_READY" in line:
                    self.ready.set()
                if "peer reset" in line:
                    with self.lock:
                        self.state["peer_resets_logged"] += line.count("peer reset")
                if any(marker in line for marker in SANITIZER_ERRORS):
                    with self.lock:
                        self.state["sanitizer_errors"] += 1
                    self.event("Sanitizer: " + line.strip()[:500], True)
                elif any(marker in line for marker in UNEXPECTED_ERRORS):
                    self.event("Server: " + line.strip()[:500], True)
                if size >= 5 * 1024 * 1024:
                    log.close()
                    for old, new in [("server.log.1", "server.log.2"),
                                     ("server.log", "server.log.1")]:
                        source = self.output / old
                        if source.exists():
                            source.replace(self.output / new)
                    log = log_path.open("w", buffering=1)
                    size = 0
        finally:
            log.close()

    def sample(self):
        now = time.monotonic()
        try:
            metrics = proc_metrics(self.process.pid)
        except (FileNotFoundError, ProcessLookupError):
            return
        frequency = os.sysconf("SC_CLK_TCK")
        with self.lock:
            completed = self.state["completed"]
            cpu, rate = 0.0, 0.0
            if self.previous:
                old_time, old_ticks, old_completed, old_threads = self.previous
                elapsed = now - old_time
                cpu = (metrics["ticks"] - old_ticks) / frequency / elapsed * 100
                rate = (completed - old_completed) / elapsed
                for thread in metrics["threads"]:
                    before = old_threads.get(thread["tid"], thread["ticks"])
                    thread["cpu_pct"] = round(
                        (thread["ticks"] - before) / frequency / elapsed * 100, 1)
            else:
                for thread in metrics["threads"]:
                    thread["cpu_pct"] = 0.0
            self.previous = (now, metrics.pop("ticks"), completed,
                             {item["tid"]: item["ticks"] for item in metrics["threads"]})
            for thread in metrics["threads"]:
                thread.pop("ticks")
            sample = {
                **metrics, "t": round(now - self.started, 2),
                "phase": self.state["phase"], "completed": completed,
                "cpu_pct": round(cpu, 1), "connections_per_s": round(rate, 1),
                "client_open": self.state["client_open"],
                "bytes_verified": self.state["bytes_verified"],
            }
            self.samples.append(sample)
            # 30 minutes plus shutdown is well under this bound.
            if len(self.samples) > 7200:
                self.samples.pop(0)
            self.csv.writerow([
                sample[key] for key in [
                    "t", "phase", "fd", "rss_mib", "rss_peak_mib", "cpu_pct",
                    "completed", "connections_per_s", "bytes_verified", "client_open",
                ]
            ])
            self.csv_file.flush()
        self.write_snapshot()

    def sample_loop(self):
        while not self.done.wait(1):
            try:
                self.sample()
            except Exception as error:
                self.event(f"Telemetry failed: {error}", True)

    def start_http(self):
        monitor = self
        html = Path(__file__).with_name("TcpServerChurnDashboard.html").read_bytes()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                path = urlsplit(self.path).path
                if path == "/":
                    payload, kind = html, "text/html; charset=utf-8"
                elif path == "/api/state":
                    payload = json.dumps(monitor.snapshot(), ensure_ascii=False).encode()
                    kind = "application/json; charset=utf-8"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", kind)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *unused):
                pass

        self.http = http.server.ThreadingHTTPServer(("127.0.0.1", self.args.http_port), Handler)
        self.http.daemon_threads = True
        self.state["http_port"] = self.http.server_port
        thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        thread.start()

    async def wait_fd(self, expected, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.check_health()
            count = len(list(Path(f"/proc/{self.process.pid}/fd").iterdir()))
            if count == expected:
                return
            await asyncio.sleep(.02)
        raise AssertionError(f"FD recovery/registration timeout: expected {expected}, got {count}")

    def check_health(self):
        if self.failure.is_set():
            raise AssertionError(self.state["last_error"])
        if self.process.poll() is not None:
            raise AssertionError(f"Server exited early: {self.process.returncode}")

    async def churn_round(self, number):
        clients = []
        gate = asyncio.Semaphore(64)
        rng = random.Random(20261005 + number)
        payloads = [rng.randbytes(rng.randrange(1, 2049))
                    for _ in range(self.args.connections)]
        round_start = time.monotonic()
        with self.lock:
            self.state.update(phase="connecting", round=number)

        async def open_peer(index):
            async with gate:
                start = time.monotonic()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", self.state["port"]), 5)
                clients.append((index, reader, writer))
                with self.lock:
                    self.state["opened"] += 1
                    self.state["client_open"] += 1
                    self.connect_times.append((time.monotonic() - start) * 1000)

        async def echo_peer(index, reader, writer):
            start = time.monotonic()
            data = payloads[index]
            # Some writes deliberately split the stream.
            for offset in range(0, len(data), 137):
                writer.write(data[offset:offset + 137])
            await writer.drain()
            echoed = await asyncio.wait_for(reader.readexactly(len(data)), 5)
            if echoed != data:
                raise AssertionError(f"Echo byte mismatch: round {number}, client {index}")
            with self.lock:
                self.state["bytes_verified"] += len(data)
                self.echo_times.append((time.monotonic() - start) * 1000)

        async def close_peer(index, reader, writer):
            try:
                if index % 5 == 0:
                    writer.get_extra_info("socket").setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    writer.transport.abort()
                    with self.lock:
                        self.state["rst"] += 1
                else:
                    writer.write_eof()
                    trailing = await asyncio.wait_for(reader.read(1), 5)
                    if trailing != b"":
                        raise AssertionError(f"Duplicated trailing byte: client {index}")
                    writer.close()
                    await writer.wait_closed()
                    with self.lock:
                        self.state["fin"] += 1
                with self.lock:
                    self.state["completed"] += 1
                    self.state["client_open"] -= 1
            except Exception:
                writer.transport.abort()
                raise

        async def settle(tasks):
            # On one failure, let all siblings finish before destroying clients.
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

        try:
            await settle([open_peer(i) for i in range(self.args.connections)])
            await self.wait_fd(self.state["baseline_fd"] + self.args.connections)
            with self.lock:
                self.state["phase"] = "echo"
            await settle([echo_peer(*peer) for peer in clients])
            await asyncio.sleep(1)  # Include a sustained full-capacity plateau.
            self.check_health()
            with self.lock:
                self.state["phase"] = "closing"
            await settle([close_peer(*peer) for peer in clients])
            with self.lock:
                self.state["phase"] = "fd_recovery"
            await self.wait_fd(self.state["baseline_fd"])
            await asyncio.sleep(.25)
            await self.wait_fd(self.state["baseline_fd"])
            recovered = len(list(Path(f"/proc/{self.process.pid}/fd").iterdir()))
            with self.lock:
                self.rounds.append({
                    "round": number, "t": round(time.monotonic() - self.started, 2),
                    "fd": recovered, "seconds": round(time.monotonic() - round_start, 3),
                })
            self.event(f"Round {number}: {len(clients)} echo OK, FD {recovered} = baseline")
        finally:
            for _, _, writer in clients:
                writer.transport.abort()

    async def workload(self):
        self.ready = threading.Event()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.state["port"] = reservation.getsockname()[1]
        environment = os.environ.copy()
        environment["ASAN_OPTIONS"] = "detect_leaks=1:halt_on_error=1:abort_on_error=1"
        environment["UBSAN_OPTIONS"] = "halt_on_error=1:print_stacktrace=1"
        self.process = subprocess.Popen(
            [self.args.binary, str(self.state["port"]), "60000", str(self.args.workers)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace", bufsize=1, env=environment,
        )
        self.state["pid"] = self.process.pid
        log_thread = threading.Thread(target=self.read_logs, daemon=True)
        log_thread.start()
        sampler = None
        try:
            deadline = time.monotonic() + 10
            while not self.ready.is_set():
                self.check_health()
                if time.monotonic() >= deadline:
                    raise AssertionError("Server readiness timeout")
                await asyncio.sleep(.05)
            with self.lock:
                self.started = time.monotonic()
                self.state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S %z")
                self.state["baseline_fd"] = len(
                    list(Path(f"/proc/{self.process.pid}/fd").iterdir()))
                self.state["status"] = "running"
            self.sample()
            sampler = threading.Thread(target=self.sample_loop, daemon=True)
            sampler.start()
            self.event(f"Start {self.args.duration}s churn, PID {self.process.pid}, "
                       f"baseline FD {self.state['baseline_fd']}")
            deadline = self.started + self.args.duration
            number = 1
            while time.monotonic() < deadline:
                await self.churn_round(number)
                number += 1
            self.check_health()
            with self.lock:
                self.state["phase"] = "shutdown"
            self.process.send_signal(signal.SIGUSR1)
            await asyncio.to_thread(self.process.wait, 10)
            await asyncio.to_thread(log_thread.join, 5)
            if log_thread.is_alive():
                raise AssertionError("Server log reader did not drain")
            if self.process.returncode != 0:
                raise AssertionError(f"Server exit code {self.process.returncode}")
            if self.failure.is_set():
                raise AssertionError(self.state["last_error"])
            with self.lock:
                self.state.update(status="passed", phase="finished",
                                  workload_seconds=round(time.monotonic() - self.started, 2),
                                  server_exit_code=0)
            self.event("PASS: all rounds recovered FD baseline; clean sanitizer/server exit")
        except Exception as error:
            self.event(str(error), True)
            with self.lock:
                self.state.update(status="failed", phase="finished")
        finally:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGTERM)
                try:
                    await asyncio.to_thread(self.process.wait, 10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    await asyncio.to_thread(self.process.wait)
            await asyncio.to_thread(log_thread.join, 5)
            self.done.set()
            if sampler:
                await asyncio.to_thread(sampler.join, 5)
            self.csv_file.close()
            with self.lock:
                self.state["server_exit_code"] = self.process.returncode
                self.state["finished_elapsed_s"] = round(time.monotonic() - self.started, 1)
                self.state["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S %z")
                self.state["all_rounds"] = list(self.rounds)
                if self.failure.is_set():
                    self.state["status"] = "failed"
            self.write_snapshot("summary.json")
            self.write_snapshot()
            self.write_report()

    def write_report(self):
        data = self.snapshot()
        samples = data["samples"]
        rss = [item["rss_mib"] for item in samples]
        lines = [
            "# TcpServer churn report", "",
            f"- Result: {data['status']}",
            f"- Started: {data['started_at']}; finished: {data['finished_at']}",
            f"- Server: {data['server']}; PID: {data['pid']}",
            f"- Requested workload: {self.args.duration}s; elapsed: {data['elapsed_s']}s",
            f"- Workers: {self.args.workers}; connections per round: {self.args.connections}",
            f"- Completed rounds: {len(self.rounds)}; verified connections: {data['completed']}",
            f"- FIN: {data['fin']}; RST: {data['rst']}",
            f"- Verified echo bytes: {data['bytes_verified']}",
            f"- FD baseline: {data['baseline_fd']}; all recovery points: "
            + str(sorted({item['fd'] for item in self.rounds})),
            f"- RSS min/max: {min(rss, default=0):.2f}/{max(rss, default=0):.2f} MiB",
            f"- Connect P50/P99: {data['connect_p50_ms']}/{data['connect_p99_ms']} ms",
            f"- Echo P50/P99: {data['echo_p50_ms']}/{data['echo_p99_ms']} ms",
            f"- Latency percentiles use latest {len(self.echo_times)} observations",
            f"- Errors: {data['errors']}; sanitizer diagnostics: {data['sanitizer_errors']}",
            f"- Server exit code: {data['server_exit_code']}", "",
            "## Method and limits", "",
            "1000 live sockets per round (configurable), 1-2048B deterministic random echo; "
            "split writes, 80% FIN with EOF validation, 20% SO_LINGER(1,0) RST. "
            "Require exact server FD baseline after each round. /proc sampled at 1Hz; "
            "CPU 100% means one core. Round admission is throttled to 64 concurrent opens.",
            "ASan/UBSan and leak detection enabled. /proc RSS includes sanitizer allocator "
            "quarantine and retained pages; RSS not returning to startup is not alone a leak. "
            "Client-measured latency includes the Python generator and loopback scheduling. "
            "This is a correctness/stability soak, not a maximum-throughput benchmark. "
            "No system TCP/FD limits were changed. Function-level tracing is not enabled.", "",
            "## Outputs", "",
            "metrics.csv, live.json, summary.json, server.log (rotates: 3 x 5MiB).",
        ]
        if data["last_error"]:
            lines += ["", "## Failure", "", data["last_error"]]
        (self.output / "report.md").write_text("\n".join(lines) + "\n")

    def run(self):
        self.start_http()
        self.write_snapshot()
        print(json.dumps({
            "url": f"http://localhost:{self.http.server_port}/",
            "output": str(self.output), "monitor_pid": os.getpid(),
        }), flush=True)
        try:
            asyncio.run(self.workload())
            print(json.dumps({key: self.state[key] for key in [
                "status", "completed", "round", "errors", "server_exit_code"]}), flush=True)
            # Keep the final dashboard available without keeping the TCP server alive.
            time.sleep(self.args.keep_dashboard)
        finally:
            self.http.shutdown()
            self.http.server_close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--connections", type=int, default=1000)
    parser.add_argument("--http-port", type=int, default=0)
    parser.add_argument("--keep-dashboard", type=float, default=3600)
    args = parser.parse_args()
    if args.duration <= 0 or not 1 <= args.connections <= 1000 or args.workers < 0 or args.keep_dashboard < 0:
        parser.error("positive duration; 1..1000 connections; nonnegative workers and dashboard lifetime")
    if not Path(args.binary).is_file():
        parser.error("server binary not found")
    monitor = Monitor(args)
    monitor.run()
    raise SystemExit(0 if monitor.state["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
