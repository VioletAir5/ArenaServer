# 26 · 51aaeff 完整diff

提交：51aaeff811f0924192cc44cf88eade0e4b0e6fc4；父提交：8217f36543cac488c95957e138f0d0ff17e34207。

原题：Reassemble WebSocket continuation frames in the chaos client。日期：2026-10-11T16:44:27+08:00。

SHA256（原始diff字节）：94686efebfadeff622a439811bca93fbb2b8aac3d6eb664e8a53bd315a14094a。

[代码分析](../09-commit-code.md#commit-26)。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 51aaeff811f0924192cc44cf88eade0e4b0e6fc4生成。

~~~diff
diff --git a/tests/gateway/net/CMakeLists.txt b/tests/gateway/net/CMakeLists.txt
index 0d01ec1..f561f06 100644
--- a/tests/gateway/net/CMakeLists.txt
+++ b/tests/gateway/net/CMakeLists.txt
@@ -270,3 +270,11 @@ add_test(NAME MessageDispatchTransport_ws
         "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
 set_tests_properties(MessageDispatchTransport_ws PROPERTIES
     TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc;websocket")
+
+# A short framing regression; the 30-minute chaos run stays opt-in.
+add_test(NAME ChaosRun_WebSocketFraming
+    COMMAND "${Python3_EXECUTABLE}"
+        "${CMAKE_CURRENT_SOURCE_DIR}/ChaosRunFramingTest.py"
+        "$<TARGET_FILE:arena_websocket_test_host>")
+set_tests_properties(ChaosRun_WebSocketFraming PROPERTIES
+    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;chaos;websocket")
diff --git a/tests/gateway/net/ChaosRun.py b/tests/gateway/net/ChaosRun.py
index 33e8324..f39d880 100644
--- a/tests/gateway/net/ChaosRun.py
+++ b/tests/gateway/net/ChaosRun.py
@@ -25,7 +25,7 @@ from TcpServerIntegrationTest import reserve_port
 from TcpServerChurnMonitor import proc_metrics
 from WebSocketIntegrationTest import (
     ORIGIN, MAGIC, upgrade, packet, packet_pong, envelope_ping,
-    read_tcp_packet, send_frame, read_frame, close_code, varint,
+    read_tcp_packet, send_frame, close_code, varint, exact,
 )
 from MessageDispatchIntegrationTest import fields
 
@@ -38,6 +38,35 @@ POLICIES = {"packet_rate_exceeded", "packet_type_not_allowed", "invalid_frame",
             "unauthenticated", "callback_failure"}
 
 
+
+def read_ws_message(peer):
+    """Reassemble Beast continuation frames without assuming frame=message."""
+    result = bytearray()
+    started = False
+    while True:
+        first, second = exact(peer, 2)
+        assert not (first & 0x70), "unexpected RSV bits"
+        assert not (second & 0x80), "masked server frame"
+        size = second & 127
+        if size == 126:
+            size = struct.unpack("!H", exact(peer, 2))[0]
+        elif size == 127:
+            size = struct.unpack("!Q", exact(peer, 8))[0]
+        assert size <= 1024 * 1024
+        opcode = first & 15
+        payload = exact(peer, size)
+        if opcode in (9, 10):
+            assert first & 0x80 and size <= 125
+            if opcode == 9:
+                send_frame(peer, 10, payload)
+            continue
+        assert opcode == (0 if started else 2), ("unexpected WS opcode", opcode)
+        started = True
+        result.extend(payload)
+        assert len(result) <= 1024 * 1024, "unbounded WS response"
+        if first & 0x80:
+            return bytes(result)
+
 def timestamp():
     return datetime.datetime.now(datetime.timezone(
         datetime.timedelta(hours=8))).isoformat(timespec="seconds")
@@ -150,9 +179,7 @@ class Host:
 
     def receive(self, peer):
         if self.ws:
-            opcode, data = read_frame(peer)
-            assert opcode == 2, (self.name, opcode)
-            return data
+            return read_ws_message(peer)
         return read_tcp_packet(peer)
 
     def expect_closed(self, peer):
diff --git a/tests/gateway/net/ChaosRunFramingTest.py b/tests/gateway/net/ChaosRunFramingTest.py
new file mode 100644
index 0000000..5b9588d
--- /dev/null
+++ b/tests/gateway/net/ChaosRunFramingTest.py
@@ -0,0 +1,31 @@
+"""Real Beast auto-fragmentation regression for the chaos client."""
+import os
+import sys
+sys.dont_write_bytecode = True
+from ChaosRun import read_ws_message
+from WebSocketIntegrationTest import run_host, stop_host, upgrade, packet, send_frame
+
+def run(binary):
+    process, log, _, _, port = run_host(binary, "chaos_packet")
+    passed = False
+    try:
+        peer, reply = upgrade(port)
+        with peer:
+            assert reply.startswith(b"HTTP/1.1 101")
+            # 4111 bytes straddles Beast's default 4096-byte fragment.
+            # The larger case traverses many continuation frames.
+            for size in (4096, 65536):
+                payload = b"x" * size
+                send_frame(peer, 2, packet(payload))
+                assert read_ws_message(peer) == packet(payload, 2)
+        passed = True
+    finally:
+        stop_host(process, log)
+        if passed:
+            os.unlink(log.name)
+        else:
+            print("server log:", log.name, file=sys.stderr)
+    print("Beast fragmented 4KiB/64KiB replies reassembled correctly")
+
+if __name__ == "__main__":
+    run(sys.argv[1])
~~~
