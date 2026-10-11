# tests/gateway/net/PacketCodecIntegrationTest.py

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；172行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~python
"""One framing acceptance suite, used unchanged by epoll and Asio hosts."""
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

sys.dont_write_bytecode = True

from TcpServerIntegrationTest import connect, fd_count, reserve_port, wait_until

BODY_LIMIT = 256 * 1024
MAGIC = 0x4152454E
HEADER_SIZE = 15

def packet(payload, request_id=1, packet_type=1, version=1):
    return struct.pack("!IIHIB", MAGIC, len(payload) + 7, packet_type, request_id, version) + payload

def receive_exact(peer, length):
    result = bytearray()
    while len(result) < length:
        chunk = peer.recv(length - len(result))
        if not chunk:
            raise AssertionError("unexpected EOF inside response")
        result.extend(chunk)
    return bytes(result)

def expect_closed(peer):
    try:
        if peer.recv(1) != b"":
            raise AssertionError("invalid/truncated frame produced a response")
    except ConnectionResetError:
        pass

def run(binary, workers):
    port = reserve_port()
    log = tempfile.NamedTemporaryFile(prefix="arena-packet-", suffix=".log", delete=False)
    environment = os.environ.copy()
    environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
    environment.setdefault("UBSAN_OPTIONS", "halt_on_error=1")
    server = subprocess.Popen([binary,str(port),"60000",str(workers),"packet"],
                              stdin=subprocess.DEVNULL,stdout=log,stderr=log,env=environment)
    passed = False
    try:
        def log_text():
            log.flush()
            with open(log.name,errors="replace") as stream:
                return stream.read()
        wait_until(server,lambda:"TEST_HOST_READY" in log_text(),"packet host startup")
        baseline = fd_count(server)

        # Different connections must retain independent incomplete tails.
        with connect(port) as first, connect(port) as second:
            first.sendall(packet(b"independent",10)[:8])
            response = packet(b"other",20,2)
            second.sendall(packet(b"other",20))
            assert receive_exact(second,len(response)) == response
            first.sendall(packet(b"independent",10)[8:])
            response = packet(b"independent",10,2)
            assert receive_exact(first,len(response)) == response

        # Every header byte is fragmented; no callback on a partial frame.
        with connect(port) as peer:
            wire = packet(b"\x00\xffbinary",0x01020304)
            for byte in wire[:HEADER_SIZE-1]:
                peer.sendall(bytes([byte]))
            peer.settimeout(.05)
            try:
                peer.recv(1)
                raise AssertionError("partial header delivered")
            except socket.timeout:
                pass
            peer.settimeout(5)
            peer.sendall(wire[HEADER_SIZE-1:])
            expected = packet(b"\x00\xffbinary",0x01020304,2)
            assert receive_exact(peer,len(expected)) == expected

        # Pipelined aggregate exceeds a frame/input bound; boundaries vary
        # independently of send() boundaries. Read concurrently for backpressure.
        rng = random.Random(73)
        payloads = [bytes(rng.randrange(256) for _ in range(4096)) for _ in range(300)]
        payloads.insert(0,b"")
        wire = b"".join(packet(payload,i) for i,payload in enumerate(payloads))
        expected = b"".join(packet(payload,i,2) for i,payload in enumerate(payloads))
        with connect(port) as peer:
            peer.settimeout(10)
            failures = []
            def send_fragments():
                try:
                    offset = 0
                    while offset < len(wire):
                        size = rng.randrange(1,12001)
                        peer.sendall(wire[offset:offset+size])
                        offset += size
                    peer.shutdown(socket.SHUT_WR)
                except Exception as error:
                    failures.append(error)
            sender = threading.Thread(target=send_fragments)
            sender.start()
            try:
                assert receive_exact(peer,len(expected)) == expected
                assert peer.recv(1) == b""
            finally:
                sender.join(timeout=12)
            assert not sender.is_alive() and not failures, failures

        # Largest gateway frame accepted by both transports, under shared v1.
        payload = bytes(range(256)) * ((BODY_LIMIT-7)//256) + bytes(range((BODY_LIMIT-7)%256))
        with connect(port) as peer:
            peer.sendall(packet(payload,0xffffffff))
            expected = packet(payload,0xffffffff,2)
            assert receive_exact(peer,len(expected)) == expected
            peer.shutdown(socket.SHUT_WR)
            assert peer.recv(1) == b""

        # Invalid magic is rejected after four bytes; length after eight bytes.
        invalid = [struct.pack("!II",MAGIC,size) for size in (0,6,BODY_LIMIT+1,4*1024*1024+1)]
        invalid += [struct.pack("!I", 0xDEADBEEF)]
        invalid += [packet(b"",1,3),packet(b"",1,1,2),packet(b"",1,2)]
        for wire in invalid:
            with connect(port) as peer:
                peer.sendall(wire)
                expect_closed(peer)
        for prefix in (3,4,7,8,14,17):
            with connect(port) as peer:
                peer.sendall(packet(b"truncated body",3)[:prefix])
                peer.shutdown(socket.SHUT_WR)
                expect_closed(peer)
        with connect(port) as peer:
            peer.sendall(packet(b"still alive",30))
            expected=packet(b"still alive",30,2)
            assert receive_exact(peer,len(expected))==expected
        wait_until(server,lambda:fd_count(server)==baseline,"packet FD recovery")

        # Stop on the common management seam after a framed response.
        with connect(port) as peer:
            peer.sendall(packet(b"before stop",40))
            expected=packet(b"before stop",40,2)
            assert receive_exact(peer,len(expected))==expected
            peer.shutdown(socket.SHUT_WR)
            server.send_signal(signal.SIGUSR1)
            assert peer.recv(1)==b""
            assert server.wait(timeout=5)==0
        diagnostics=log_text()
        assert "TcpServer stopped" in diagnostics
        assert not any(token in diagnostics for token in
                       ("AddressSanitizer","runtime error:","ThreadSanitizer","SanitizerTool:"))
        print(f"packet codec acceptance passed: workers={workers}; fragmented/coalesced/binary/max/invalid/EOF/isolation/stop")
        passed=True
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        log.close()
        if passed:
            os.unlink(log.name)
        else:
            print(f"server log: {log.name}",file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv)!=3:
        raise SystemExit("usage: PacketCodecIntegrationTest.py HOST WORKER_COUNT")
    run(sys.argv[1],int(sys.argv[2]))
~~~
