#!/usr/bin/env python3
"""Seeded mixed TCP/WS chaos with bounded telemetry and clean sanitizer exit."""
import argparse
import collections
import concurrent.futures
import csv
import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback

sys.dont_write_bytecode = True
from TcpServerIntegrationTest import reserve_port
from TcpServerChurnMonitor import proc_metrics
from WebSocketIntegrationTest import (
    ORIGIN, MAGIC, upgrade, packet, packet_pong, envelope_ping,
    read_tcp_packet, send_frame, close_code, varint, exact,
)
from MessageDispatchIntegrationTest import fields

DIAGNOSTICS = ("ERROR: AddressSanitizer", "LeakSanitizer",
               "AddressSanitizer:DEADLYSIGNAL", "runtime error:",
               "SanitizerTool: CHECK failed", "Sanitizer CHECK failed",
               "WARNING: ThreadSanitizer")
POLICIES = {"packet_rate_exceeded", "packet_type_not_allowed", "invalid_frame",
            "invalid_envelope", "handler_rejected", "slow_client",
            "unauthenticated", "callback_failure"}



def read_ws_message(peer):
    """Reassemble Beast continuation frames without assuming frame=message."""
    result = bytearray()
    started = False
    while True:
        first, second = exact(peer, 2)
        assert not (first & 0x70), "unexpected RSV bits"
        assert not (second & 0x80), "masked server frame"
        size = second & 127
        if size == 126:
            size = struct.unpack("!H", exact(peer, 2))[0]
        elif size == 127:
            size = struct.unpack("!Q", exact(peer, 8))[0]
        assert size <= 1024 * 1024
        opcode = first & 15
        payload = exact(peer, size)
        if opcode in (9, 10):
            assert first & 0x80 and size <= 125
            if opcode == 9:
                send_frame(peer, 10, payload)
            continue
        assert opcode == (0 if started else 2), ("unexpected WS opcode", opcode)
        started = True
        result.extend(payload)
        assert len(result) <= 1024 * 1024, "unbounded WS response"
        if first & 0x80:
            return bytes(result)

def timestamp():
    return datetime.datetime.now(datetime.timezone(
        datetime.timedelta(hours=8))).isoformat(timespec="seconds")


class Host:
    def __init__(self, root, backend, profile, output):
        self.name = backend + "-" + profile
        self.ws = backend == "ws"
        self.profile = profile
        self.port = reserve_port()
        binary_name = {"epoll": "arena_tcp_server_test_host",
                       "asio": "arena_asio_tcp_server_test_host",
                       "ws": "arena_websocket_test_host"}[backend]
        self.binary = str(root / binary_name)
        if self.ws:
            unused_port = reserve_port()
            while unused_port == self.port:
                unused_port = reserve_port()
            args = [self.binary, str(unused_port), str(self.port),
                    ORIGIN, profile]
        else:
            args = [self.binary, str(self.port), "60000", "4", profile]
        environment = os.environ.copy()
        environment["ASAN_OPTIONS"] = "detect_leaks=1:halt_on_error=1:abort_on_error=1"
        environment["UBSAN_OPTIONS"] = "halt_on_error=1:print_stacktrace=1"
        self.process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, text=True, errors="replace",
            bufsize=1, env=environment)
        self.ready = threading.Event()
        self.errors = []
        self.rejections = {}
        self.log_path = output / (self.name + ".log")
        self.reader = threading.Thread(target=self.logs, daemon=True)
        self.reader.start()
        self.baseline_fd = None
        self.connections_peak_sampled = 0
        self.fd_peak = 0
        self.last_metrics = None
        self.rss_start_mib = None
        self.rss_last_mib = None
        self.rss_peak_mib = 0
        self.healthy_rtt = collections.Counter()
        self.connect_latency = collections.Counter()
        self.counts = collections.Counter()
        self.lock = threading.Lock()

    def logs(self):
        size = 0
        log = self.log_path.open("w", buffering=1)
        try:
            for line in self.process.stdout:
                log.write(line)
                size += len(line.encode())
                if "TEST_HOST_READY" in line or "WS_TEST_READY" in line:
                    self.ready.set()
                if any(marker in line for marker in DIAGNOSTICS):
                    self.errors.append(line.strip()[:1000])
                match = re.search(r"CHAOS_REJECTIONS reason=(\w+) count=(\d+)", line)
                if match:
                    self.rejections[match[1]] = int(match[2])
                if size >= 5 * 1024 * 1024:
                    log.close()
                    for suffix, target in ((".1", ".2"), ("", ".1")):
                        source = Path(str(self.log_path) + suffix)
                        if source.exists():
                            source.replace(str(self.log_path) + target)
                    log = self.log_path.open("w", buffering=1)
                    size = 0
        finally:
            log.close()
            self.process.stdout.close()

    def health(self):
        assert not self.errors, (self.name, self.errors)
        assert self.process.poll() is None, (
            self.name, "unexpected exit", self.process.returncode)

    def wait_ready(self):
        limit = time.monotonic() + 15
        while not self.ready.wait(.02):
            self.health()
            assert time.monotonic() < limit, (self.name, "startup timeout")
        self.baseline_fd = len(list(Path(f"/proc/{self.process.pid}/fd").iterdir()))

    def count(self, key, value=1):
        with self.lock:
            self.counts[key] += value

    def connect(self):
        started = time.monotonic()
        if self.ws:
            peer, reply = upgrade(self.port)
            assert reply.startswith(b"HTTP/1.1 101"), reply
        else:
            peer = socket.create_connection(("127.0.0.1", self.port), 5)
        peer.settimeout(5)
        with self.lock:
            # Fixed 0.1ms bins preserve the whole run, no moving window.
            self.connect_latency[math.ceil((time.monotonic() - started) * 10000)] += 1
            self.counts["connections_opened"] += 1
        return peer

    def send(self, peer, data):
        if self.ws:
            send_frame(peer, 2, data)
        else:
            peer.sendall(data)

    def receive(self, peer):
        if self.ws:
            return read_ws_message(peer)
        return read_tcp_packet(peer)

    def expect_closed(self, peer):
        if self.ws:
            code = close_code(peer)
            assert code == 1008, (self.name, code)
            self.count("ws_close_1008")
        else:
            size = 0
            try:
                while True:
                    data = peer.recv(16384)
                    if not data:
                        break
                    size += len(data)
                    assert size < 2 * 1024 * 1024, "unbounded rejection output"
            except ConnectionResetError:
                pass
        self.count("rejected_connections_observed")

    def exchange(self, peer, rng):
        nonce = rng.randrange(1, 1000000)
        if self.profile == "chaos":
            data = packet(envelope_ping(nonce))
            expected = packet_pong(nonce)
        else:
            payload = rng.randbytes(rng.randrange(1, 4097))
            data = packet(payload)
            expected = packet(payload, 2)
        start = time.monotonic()
        # Random stream split inside Packet headers/payloads.
        if not self.ws and rng.random() < .4:
            cut = rng.randrange(1, len(data))
            peer.sendall(data[:cut])
            peer.sendall(data[cut:])
        else:
            self.send(peer, data)
        assert self.receive(peer) == expected, (self.name, "response mismatch")
        with self.lock:
            self.healthy_rtt[math.ceil((time.monotonic() - start) * 10000)] += 1
        self.count("healthy_replies")

    def job(self, scenario, seed):
        rng = random.Random(seed)
        self.count("scenario_" + scenario)
        if scenario == "origin":
            peer, reply = upgrade(self.port, "https://untrusted.example")
            with peer:
                assert reply.startswith(b"HTTP/1.1 403"), reply
            self.count("origin_rejected")
            return
        with self.connect() as peer:
            if scenario == "healthy":
                for _ in range(rng.randrange(2, 5)):
                    self.exchange(peer, rng)
            elif scenario == "reconnect":
                self.exchange(peer, rng)
                peer.close()
                for _ in range(2):
                    with self.connect() as again:
                        self.exchange(again, rng)
                self.count("reconnections", 2)
            elif scenario == "reset":
                self.send(peer, packet(b"x" * 1024)[:rng.randrange(1, 15)])
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                struct.pack("ii", 1, 0))
                self.count("rst_disconnects")
            elif scenario == "truncated":
                self.send(peer, packet(b"x" * 1024)[:rng.randrange(15, 1024)])
                peer.shutdown(socket.SHUT_WR)
                self.count("truncated_fin")
            elif scenario in ("magic", "length", "version", "type"):
                data = {"magic": b"BAD!",
                        "length": struct.pack("!II", MAGIC, 256 * 1024 + 1),
                        "version": struct.pack("!IIHIB", MAGIC, 7, 1, 7, 2),
                        "type": struct.pack("!IIHIB", MAGIC, 7, 99, 7, 1)}[scenario]
                self.send(peer, data)
                self.expect_closed(peer)
            elif scenario == "whitelist":
                self.send(peer, packet(envelope_ping(1), 2))
                self.expect_closed(peer)
            elif scenario == "protobuf":
                self.send(peer, packet(b"\xff"))
                self.expect_closed(peer)
            elif scenario == "depth":
                nested = b""
                for _ in range(24):
                    nested = b"\x1a" + varint(len(nested)) + nested
                self.send(peer, packet(b"\x08\x2a\x72" +
                                       varint(len(nested)) + nested))
                self.expect_closed(peer)
            elif scenario == "unknown":
                self.send(peer, packet(b"\x08\x2a\x20" + varint(999)))
                reply = fields(self.receive(peer)[15:])
                assert reply[1] == 42 and fields(reply[14])[1] == 1001
                self.count("typed_unknown_errors")
            elif scenario == "rate":
                payload = envelope_ping(1) if self.profile == "chaos" else b"x"
                self.send(peer, packet(payload) * 512)
                self.expect_closed(peer)
            elif scenario == "text":
                send_frame(peer, 1, b"not binary")
                assert close_code(peer) == 1003
                self.count("ws_close_1003")
            elif scenario == "ws_length":
                send_frame(peer, 2, b"x" * (256 * 1024 + 9))
                assert close_code(peer) == 1009
                self.count("ws_close_1009")
            elif scenario == "slow":
                # Large legal echo payloads fill the peer window and
                # exercise BackpressureFeature's real 5s deadline.
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                peer.settimeout(.25)
                data = packet(b"s" * (64 * 1024))
                sent = 0
                for _ in range(160):
                    try:
                        self.send(peer, data)
                        sent += len(data)
                    except (OSError, socket.timeout):
                        break
                self.count("slow_input_bytes", sent)
                time.sleep(6.2)
                self.count("slow_no_read_cycles")
            else:
                raise AssertionError(scenario)

    def wait_fd(self, expected, timeout=12):
        limit = time.monotonic() + timeout
        while time.monotonic() < limit:
            self.health()
            count = len(list(Path(f"/proc/{self.process.pid}/fd").iterdir()))
            if count == expected:
                return
            time.sleep(.02)
        raise AssertionError((self.name, "FD recovery", expected, count))

    def stop(self):
        self.health()
        self.process.send_signal(signal.SIGUSR1)
        code = self.process.wait(timeout=15)
        self.reader.join(timeout=5)
        assert not self.reader.is_alive(), (self.name, "log not drained")
        assert code == 0 and not self.errors, (self.name, code, self.errors)
        assert set(self.rejections) == POLICIES, (self.name, self.rejections)
        assert self.rejections["callback_failure"] == 0
        for key in ("invalid_frame", "packet_type_not_allowed", "packet_rate_exceeded"):
            assert self.rejections[key] > 0, (self.name, key, self.rejections)
        if self.profile == "chaos":
            assert self.rejections["invalid_envelope"] > 0
            assert self.rejections["handler_rejected"] > 0
        else:
            assert self.rejections["slow_client"] > 0, (
                self.name, "slow-reader path not exercised", self.rejections)

    def summary(self):
        def quantile(histogram, fraction):
            if not histogram:
                return None
            rank = math.ceil(sum(histogram.values()) * fraction)
            count = 0
            for bucket, occurrences in sorted(histogram.items()):
                count += occurrences
                if count >= rank:
                    return bucket / 10
        with self.lock:
            return {"name": self.name, "pid": self.process.pid,
                    "profile": self.profile, "ws": self.ws,
                    "binary": self.binary, "baseline_fd": self.baseline_fd,
                    "fd_peak": self.fd_peak,
                    "connections_peak_sampled": self.connections_peak_sampled,
                    "rss_start_mib": self.rss_start_mib,
                    "rss_last_mib": self.rss_last_mib,
                    "rss_peak_mib": self.rss_peak_mib,
                    "connect_p99_ms": quantile(self.connect_latency, .99),
                    "healthy_rtt_p99_ms": quantile(self.healthy_rtt, .99),
                    "healthy_rtt_p50_ms": quantile(self.healthy_rtt, .5),
                    "counts": dict(self.counts), "rejections": self.rejections,
                    "diagnostics": self.errors,
                    "exit_code": self.process.poll()}


class Run:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.hosts = []
        self.samples = []
        self.failure = []
        self.finished = threading.Event()
        self.start = None
        self.rounds = 0
        self.started_at = None
        self.workload_seconds = 0
        self.aggregate_connections_peak_sampled = 0

    def sample(self, phase):
        aggregate = 0
        for host in self.hosts:
            host.health()
            m = proc_metrics(host.process.pid)
            connections = max(0, m["fd"] - host.baseline_fd)
            aggregate += connections
            with host.lock:
                host.fd_peak = max(host.fd_peak, m["fd"])
                host.connections_peak_sampled = max(host.connections_peak_sampled, connections)
                host.rss_peak_mib = max(host.rss_peak_mib, m["rss_peak_mib"])
                host.rss_last_mib = m["rss_mib"]
                if host.rss_start_mib is None:
                    host.rss_start_mib = m["rss_mib"]
            self.samples.append({"elapsed_s": round(time.monotonic() - self.start, 3),
                                 "phase": phase, "host": host.name,
                                 "fd": m["fd"], "connections": connections,
                                 "rss_mib": m["rss_mib"],
                                 "rss_peak_mib": m["rss_peak_mib"]})
        self.aggregate_connections_peak_sampled = max(
            self.aggregate_connections_peak_sampled, aggregate)

    def snapshot(self, status):
        return {"status": status, "started_at": self.started_at,
                "updated_at": timestamp(), "requested_duration_s": self.args.duration,
                "workload_seconds": self.workload_seconds or (
                    round(time.monotonic() - self.start, 2) if self.start else 0),
                "seed": self.args.seed, "rounds": self.rounds,
                "aggregate_connections_peak_sampled":
                    self.aggregate_connections_peak_sampled,
                "sanitizers": "ASan+UBSan+LeakSanitizer",
                "latency_measurement": "client successful RTT; full-run 0.1ms histogram upper bins",
                "hosts": [host.summary() for host in self.hosts],
                "failures": self.failure}

    def write_live(self, status):
        temporary = self.output / "live.json.tmp"
        temporary.write_text(json.dumps(self.snapshot(status), indent=2))
        temporary.replace(self.output / "live.json")

    def telemetry(self):
        while not self.finished.wait(1):
            try:
                self.sample("mixed")
                self.write_live("running")
            except BaseException as error:
                self.failure.append("telemetry: " + repr(error))
                return

    def surge(self):
        clients = []
        try:
            for host in self.hosts:
                group = [host.connect() for _ in range(self.args.peak_connections)]
                clients.extend(group)
                host.wait_fd(host.baseline_fd + self.args.peak_connections)
            self.sample("connection_peak")
            time.sleep(.5)
        finally:
            for peer in clients:
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                struct.pack("ii", 1, 0))
                peer.close()
        for host in self.hosts:
            host.wait_fd(host.baseline_fd)

    def execute(self):
        sampler = None
        status = "failed"
        try:
            root = Path(self.args.binary_dir).resolve()
            for backend in ("epoll", "asio", "ws"):
                for profile in ("chaos", "chaos_packet"):
                    host = Host(root, backend, profile, self.output)
                    self.hosts.append(host)
                    host.wait_ready()
            self.start = time.monotonic()
            self.started_at = timestamp()
            self.sample("baseline")
            self.surge()
            sampler = threading.Thread(target=self.telemetry, daemon=True)
            sampler.start()
            deadline = self.start + self.args.duration
            rng = random.Random(self.args.seed)
            with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
                while time.monotonic() < deadline:
                    for host in self.hosts:
                        host.health()
                    assert not self.failure, self.failure
                    jobs = []
                    for host in self.hosts:
                        cases = ["healthy"] * 6 + ["reconnect"] * 2 + [
                            "reset", "reset", "truncated", "magic", "length",
                            "version", "type", "whitelist", "rate"]
                        if host.profile == "chaos":
                            cases += ["protobuf", "depth", "unknown"]
                        else:
                            cases += ["slow"]
                        if host.ws:
                            cases += ["origin", "text", "ws_length"]
                        rng.shuffle(cases)
                        jobs.extend(executor.submit(host.job, case, rng.randrange(2**63))
                                    for case in cases)
                    for future in concurrent.futures.as_completed(jobs):
                        future.result()
                    for host in self.hosts:
                        host.wait_fd(host.baseline_fd)
                        host.count("fd_recovered_rounds")
                    self.rounds += 1
                    if self.rounds % 10 == 0:
                        self.surge()
                    self.write_live("running")
                    print(f"round={self.rounds} elapsed={time.monotonic()-self.start:.1f}s "
                          f"healthy={sum(h.counts['healthy_replies'] for h in self.hosts)}",
                          flush=True)
            self.workload_seconds = round(time.monotonic() - self.start, 3)
            assert self.workload_seconds >= self.args.duration
            self.finished.set()
            if sampler:
                sampler.join(timeout=5)
            assert not self.failure, self.failure
            self.sample("drained")
            for host in self.hosts:
                host.stop()
            status = "passed"
        except BaseException as error:
            self.failure.append(repr(error))
            traceback.print_exc()
        finally:
            self.finished.set()
            if sampler:
                sampler.join(timeout=5)
            for host in self.hosts:
                if host.process.poll() is None:
                    host.process.send_signal(signal.SIGTERM)
                    try:
                        host.process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        host.process.kill()
                        host.process.wait()
                        self.failure.append(host.name + ": forced kill; no leak pass")
                host.reader.join(timeout=5)
            if any(host.errors or host.process.returncode != 0 for host in self.hosts):
                status = "failed"
            self.write_live(status)
            (self.output / "summary.json").write_text(
                json.dumps(self.snapshot(status), indent=2) + "\n")
            with (self.output / "metrics.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=[
                    "elapsed_s", "phase", "host", "fd", "connections",
                    "rss_mib", "rss_peak_mib"])
                writer.writeheader()
                writer.writerows(self.samples)
            print(json.dumps(self.snapshot(status), indent=2), flush=True)
        return 0 if status == "passed" else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--seed", type=int, default=20261011)
    parser.add_argument("--peak-connections", type=int, default=64)
    args = parser.parse_args()
    assert args.duration >= 10
    assert 1 <= args.peak_connections <= 80
    return Run(args).execute()


if __name__ == "__main__":
    raise SystemExit(main())
