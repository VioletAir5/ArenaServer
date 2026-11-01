#!/usr/bin/env python3
"""Give an isolated test process tree one civil clock based on monotonic time."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clock-shim', type=Path, required=True)
    parser.add_argument('--sanitizer-binary', type=Path)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or not args.clock_shim.is_file():
        parser.error('an existing clock shim and a command are required')
    environment = dict(os.environ)
    keys = ('ARENA_TEST_CLOCK_REALTIME_NS', 'ARENA_TEST_CLOCK_MONOTONIC_NS')
    if any(key in environment for key in keys):
        if not all(key in environment and re.fullmatch(r'[0-9]{1,19}', environment[key])
               and int(environment[key]) <= (1 << 63)-1 for key in keys):
            parser.error('both clock origins must be unsigned integer nanoseconds')
    else:
        before = time.monotonic_ns()
        civil = time.time_ns()
        after = time.monotonic_ns()
        environment[keys[0]] = str(civil)
        environment[keys[1]] = str((before + after) // 2)
    preloads = []
    if args.sanitizer_binary is not None:
        if not args.sanitizer_binary.is_file():
            parser.error('sanitizer binary must exist')
        inspect_environment = dict(environment)
        inspect_environment.pop('LD_PRELOAD', None)
        result = subprocess.run(['ldd', str(args.sanitizer_binary)], text=True,
            env=inspect_environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=5, check=True)
        for line in result.stdout.splitlines():
            if not re.match(r'\s*(?:lib(?:asan|tsan)\.so|libclang_rt\.(?:asan|tsan)[^\s]*\.so)', line):
                continue
            match = re.search(r'=>\s+(/\S+)', line)
            if match is None:
                parser.error('trusted target has an unresolved sanitizer runtime')
            preloads.append(str(Path(match.group(1)).resolve(strict=True)))
    preloads.append(str(args.clock_shim.resolve()))
    inherited = environment.get('LD_PRELOAD', '')
    if inherited:
        preloads.append(inherited)
    environment['LD_PRELOAD'] = ':'.join(dict.fromkeys(preloads))
    os.execvpe(command[0], command, environment)


if __name__ == '__main__':
    main()