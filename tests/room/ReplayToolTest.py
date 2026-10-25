"""Validate all saved live tick checksums with the standalone replay process."""

import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile


def execute(arguments, expected, environment):
    process = subprocess.run(arguments, text=True, capture_output=True,
                             timeout=30, env=environment)
    assert process.returncode == expected, (arguments, process.returncode,
                                          process.stdout, process.stderr)
    for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:",
                       "runtime error:", "UndefinedBehaviorSanitizer",
                       "SanitizerTool: CHECK failed"):
        assert diagnostic not in process.stderr, process.stderr
    try:
        report = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError((arguments, process.stdout, process.stderr)) from error
    assert isinstance(report["reason"], str) and report["reason"]
    return report


def manifest(path):
    lines = path.read_text().splitlines()
    marker, checkpoint, checksum = lines[0].split()
    assert marker == "checkpoint"
    rows = [tuple(map(int, line.split())) for line in lines[1:]]
    assert all(tick == int(checkpoint) + index + 1
               for index, (tick, _) in enumerate(rows))
    return int(checkpoint), int(checksum), rows


def frames(data):
    offset = 80 + 48 * struct.unpack_from("<I", data, 76)[0]
    records = {}
    while offset < len(data):
        tag, size = struct.unpack_from("<II", data, offset)
        body = offset + 8
        assert body + size <= len(data)
        if tag == 0x21444E45:
            assert body + size == len(data)
            break
        assert tag == 0x4B434954
        tick = struct.unpack_from("<Q", data, body)[0]
        records[tick] = (offset, body, size)
        offset = body + size
    return records


def run(tests, tool):
    environment = os.environ.copy()
    environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
    environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
    with tempfile.TemporaryDirectory(prefix="arena-replay-cli-") as root:
        directory = Path(root)
        environment["ARENA_REPLAY_ACCEPTANCE_DIR"] = str(directory)
        generated = subprocess.run(
            [tests, "--gtest_filter=ReplayAcceptanceTest.*"],
            text=True, capture_output=True, timeout=120, env=environment)
        assert generated.returncode == 0, (generated.stdout, generated.stderr)
        manifests = sorted(directory.glob("seed-*.checksums"))
        assert len(manifests) == 100, len(manifests)
        compared = 0
        for expected_path in manifests + [directory / "diagnostic.checksums",
                                          directory / "owner.checksums"]:
            assert expected_path.is_file(), expected_path
            checkpoint, _, expected = manifest(expected_path)
            replay_path = expected_path.with_suffix(".arena")
            report = execute([tool, str(replay_path)], 0, environment)
            assert report["status"] == "ok" and report["failed_tick"] is None, report
            actual = [(int(row["tick"]), int(row["checksum"])) for row in report["checksums"]]
            assert actual == expected, (expected_path.name, actual, expected)
            assert int(report["checkpoint_tick"]) == checkpoint
            assert int(report["final_tick"]) == expected[-1][0]
            assert int(report["final_checksum"]) == expected[-1][1]
            if expected_path.name.startswith("seed-"):
                assert len(actual) == 256 and checkpoint == 0
                compared += len(actual)
        assert compared == 25600

        fixture = directory / "diagnostic.arena"
        original = fixture.read_bytes()
        layout = frames(original)
        assert struct.unpack_from("<Q", original, 60)[0] == 3
        _, body, _ = layout[6]
        members, attempts = struct.unpack_from("<II", original, body + 16)
        assert members == attempts == 1
        first_input = body + 24 + members * 24

        # Alter one valid control in the middle; this must identify tick six.
        mutated = bytearray(original)
        struct.pack_into("<i", mutated, first_input + 32, 1)
        changed_input = directory / "changed-input.arena"
        changed_input.write_bytes(mutated)
        failure = execute([tool, str(changed_input)], 1, environment)
        assert failure["status"] == "desync" and failure["failed_tick"] == "6", failure
        assert failure["expected_checksum"] != failure["actual_checksum"]

        # Corrupt only a stored expected value, preserving valid framing.
        mutated = bytearray(original)
        expected_hash = struct.unpack_from("<Q", mutated, body + 8)[0]
        struct.pack_into("<Q", mutated, body + 8, expected_hash ^ 1)
        changed_hash = directory / "changed-hash.arena"
        changed_hash.write_bytes(mutated)
        failure = execute([tool, str(changed_hash)], 1, environment)
        assert failure["failed_tick"] == "6", failure
        assert int(failure["expected_checksum"]) == expected_hash ^ 1
        assert int(failure["actual_checksum"]) == expected_hash

        # A valid-range changed checkpoint must fail at its absolute tick three.
        mutated = bytearray(original)
        assert struct.unpack_from("<I", mutated, 76)[0] == 1
        struct.pack_into("<i", mutated, 104, 4)
        changed_initial = directory / "changed-initial.arena"
        changed_initial.write_bytes(mutated)
        failure = execute([tool, str(changed_initial)], 1, environment)
        assert failure["status"] == "desync" and failure["failed_tick"] == "3", failure

        invalid = [
            ("truncated", original[:-1]),
            ("trailing", original + b"x"),
            ("magic", b"X" + original[1:]),
        ]
        for name, offset, value, code in (
                ("version", 8, 2, "<I"),
                ("checksum-version", 12, 2, "<I"),
                ("configuration", 40, 0, "<i"),
                ("huge-count", 76, 0xFFFFFFFF, "<I"),
                ("huge-frame", layout[4][0] + 4, 0xFFFFFFFF, "<I"),
                ("zero-incarnation", 52, 0, "<Q")):
            mutated = bytearray(original)
            struct.pack_into(code, mutated, offset, value)
            invalid.append((name, mutated))
        for name, data in invalid:
            path = directory / (name + ".arena")
            path.write_bytes(data)
            failure = execute([tool, str(path)], 2, environment)
            assert failure["status"] == "invalid", (name, failure)

        missing = execute([tool, str(directory / "absent.arena")], 2, environment)
        assert missing["status"] == "io", missing
        usage = execute([tool], 2, environment)
        assert usage["status"] == "invalid", usage
        unknown = execute([tool, str(fixture), "--unknown"], 2, environment)
        assert unknown["status"] == "invalid", unknown

        report_path = directory / "report.json"
        good = execute([tool, str(fixture), "--report", str(report_path)], 0, environment)
        assert json.loads(report_path.read_text()) == good
        failure = execute([tool, str(changed_input), "--report", str(report_path)], 1, environment)
        assert json.loads(report_path.read_text()) == failure
        bad_destination = execute([tool, str(fixture), "--report", str(directory)], 2, environment)
        assert bad_destination["status"] == "io", bad_destination
        before = fixture.read_bytes()
        alias = execute([tool, str(fixture), "--report", str(fixture)], 2, environment)
        assert alias["status"] == "invalid" and fixture.read_bytes() == before

        print("100 seeded matches / 25600 live ticks match saved CLI replay; "
              "real owner archive, absolute first failure and exit 0/1/2 boundaries passed")


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
