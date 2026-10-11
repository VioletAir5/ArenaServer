"""Real Beast auto-fragmentation regression for the chaos client."""
import os
import sys
sys.dont_write_bytecode = True
from ChaosRun import read_ws_message
from WebSocketIntegrationTest import run_host, stop_host, upgrade, packet, send_frame

def run(binary):
    process, log, _, _, port = run_host(binary, "chaos_packet")
    passed = False
    try:
        peer, reply = upgrade(port)
        with peer:
            assert reply.startswith(b"HTTP/1.1 101")
            # 4111 bytes straddles Beast's default 4096-byte fragment.
            # The larger case traverses many continuation frames.
            for size in (4096, 65536):
                payload = b"x" * size
                send_frame(peer, 2, packet(payload))
                assert read_ws_message(peer) == packet(payload, 2)
        passed = True
    finally:
        stop_host(process, log)
        if passed:
            os.unlink(log.name)
        else:
            print("server log:", log.name, file=sys.stderr)
    print("Beast fragmented 4KiB/64KiB replies reassembled correctly")

if __name__ == "__main__":
    run(sys.argv[1])
