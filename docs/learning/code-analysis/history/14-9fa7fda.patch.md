# 14 · 9fa7fda 完整diff

提交：9fa7fdaed716b9dfa72895f1b60b020f6dce123f；父提交：b40ed339ee9711c970026aebb3d36a500c3c48e3。

原题：Add connection context to transport handler factories。日期：2026-10-10T00:30:17+08:00。

SHA256（原始diff字节）：976a0ec569bd35095898644b984d131f4ad1f20da470adc597428fcd87005fa8。

[代码分析](../09-commit-code.md#commit-14)。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 9fa7fdaed716b9dfa72895f1b60b020f6dce123f生成。

~~~diff
diff --git a/services/gateway/README.md b/services/gateway/README.md
index 2b52f4c..c03638b 100644
--- a/services/gateway/README.md
+++ b/services/gateway/README.md
@@ -36,8 +36,39 @@ connection callback owns the chain. Links and node state are used on the
 connection owner; callback copies retain the same decoder and chain, so create
 a fresh callback for each connection through ConnectionHandlerFactory.
 
-IP policies need trusted peer metadata supplied at connection admission;
-IConnection currently exposes no peer address. A login deadline also needs an
-owner timer so an inactive client can expire without sending another packet.
-IP rate limits, whitelists, authentication, and login deadlines are future
-policies; this change supplies their handler interface.
+Connection policies receive trusted peer metadata through ConnectionContext,
+passed to ConnectionHandlerFactory on each connection owner. IConnection
+continues to provide transport operations.
+
+## Connection context
+
+ConnectionHandlerFactory now accepts ConnectionContext::Ptr, a shared pointer
+to immutable metadata: the process-wide connection ID, socket peer IPv4 address
+and port, and steady_clock acceptance time. Acceptance time is recorded in the
+successful accept path before worker handoff and, for WS, before HTTP Upgrade.
+The context and a fresh ConnectionSession are created on the target owner.
+
+For example:
+
+~~~cpp
+ConnectionHandlerFactory factory = [](ConnectionContext::Ptr context) {
+    std::cout << context->connectionId << " peer="
+              << context->peer.toString() << '\n';
+    return makePacketEchoHandler();
+};
+~~~
+
+makeConnectionHandler retains the context for the callback lifetime even if
+the factory ignores it. Factories may retain the context or its session in
+Handler nodes; create a fresh callback per connection.
+
+ConnectionSession starts Pending. markAuthenticated() changes Pending to
+Authenticated; expireLogin() changes Pending to Expired. Both return false
+after either terminal transition. Every session read and mutation requires
+the owner thread and throws logic_error otherwise; shared_ptr only extends
+lifetime. The context/session own no connection, socket, or timer.
+
+Authentication, login deadline checks/timers, token buckets, IP connection
+limits, and packet whitelist policies remain subsequent steps. Ping does not
+change login state. The session transition methods alone enforce no deadline
+and do not close the transport.
diff --git a/services/gateway/app/main.cpp b/services/gateway/app/main.cpp
index 30ff21c..6d9d012 100644
--- a/services/gateway/app/main.cpp
+++ b/services/gateway/app/main.cpp
@@ -36,11 +36,12 @@ int main(int argc, char** argv)
     ConnectionHandlerFactory handlerFactory;
     if (protocol == "packet")
     {
-        handlerFactory = [] { return makePacketEchoHandler(); };
+        handlerFactory = [](ConnectionContext::Ptr)
+        { return makePacketEchoHandler(); };
     }
     if (protocol == "handshake")
     {
-        handlerFactory = []
+        handlerFactory = [](ConnectionContext::Ptr)
         { return makePreLoginHandshakeHandler(); };
     }
     const InetAddress address("127.0.0.1", 8888);
diff --git a/services/gateway/include/net/ConnectionContext.h b/services/gateway/include/net/ConnectionContext.h
new file mode 100644
index 0000000..dc0289c
--- /dev/null
+++ b/services/gateway/include/net/ConnectionContext.h
@@ -0,0 +1,64 @@
+#pragma once
+
+#include <chrono>
+#include <cstdint>
+#include <memory>
+#include <thread>
+
+#include "net/InetAddress.h"
+
+namespace service::gateway
+{
+// Authentication state belongs to the connection owner. Shared
+// ownership extends lifetime; it does not permit access from a
+// different thread. This passive object owns no socket, timer, or
+// connection.
+class ConnectionSession final
+{
+public:
+    enum class LoginState
+    {
+        Pending,
+        Authenticated,
+        Expired
+    };
+
+    ConnectionSession();
+    ConnectionSession(const ConnectionSession&) = delete;
+    ConnectionSession& operator=(const ConnectionSession&) = delete;
+
+    LoginState loginState() const;
+    // Return true only when Pending transitions to the requested
+    // state. Call markAuthenticated only after successful
+    // authentication.
+    bool markAuthenticated();
+    bool expireLogin();
+
+private:
+    void requireOwner() const;
+
+    const std::thread::id m_owner;
+    LoginState m_loginState{LoginState::Pending};
+};
+
+// Immutable admission metadata, created on the connection owner using
+// values captured by accept. acceptedAt precedes worker handoff and
+// WS upgrade. IPv4 peer is taken from the socket, never HTTP/client
+// input.
+struct ConnectionContext final
+{
+    using Clock = std::chrono::steady_clock;
+    using Ptr = std::shared_ptr<const ConnectionContext>;
+
+    ConnectionContext(
+        std::uint64_t id,
+        InetAddress peerAddress,
+        Clock::time_point accepted
+    );
+
+    const std::uint64_t connectionId;
+    const InetAddress peer;
+    const Clock::time_point acceptedAt;
+    const std::shared_ptr<ConnectionSession> session;
+};
+} // namespace service::gateway
diff --git a/services/gateway/include/net/ConnectionHandler.h b/services/gateway/include/net/ConnectionHandler.h
index 37ab446..2fe905d 100644
--- a/services/gateway/include/net/ConnectionHandler.h
+++ b/services/gateway/include/net/ConnectionHandler.h
@@ -1,17 +1,28 @@
 #pragma once
+
 #include <functional>
 
+#include "net/ConnectionContext.h"
 #include "net/IConnection.h"
 
 namespace service::gateway
 {
 using ConnectionMessageCallback = std::function<
     void(IConnection&, std::span<const std::byte>)>;
-// Copied once per worker, invoked once per connection on that owner.
-// Return a fresh callback with per-connection state; captures shared
-// across worker copies need their own synchronization. Callback state
-// is released with the connection on its owner. Empty factory selects
-// legacy raw echo.
+
+// Copied once per worker; invoked once per connection on its owner
+// before input delivery (before HTTP Upgrade for WS). Return fresh
+// callback state. The immutable context may be retained; session
+// access stays on its owner. Captures shared across worker copies
+// need their own synchronization.
 using ConnectionHandlerFactory = std::function<
-    ConnectionMessageCallback()>;
+    ConnectionMessageCallback(ConnectionContext::Ptr)>;
+
+// Installs a lifetime wrapper that retains context even if the
+// factory ignores it. An empty factory selects raw echo; an empty
+// result is invalid.
+ConnectionMessageCallback makeConnectionHandler(
+    const ConnectionHandlerFactory& factory,
+    ConnectionContext::Ptr context
+);
 } // namespace service::gateway
diff --git a/services/gateway/src/net/AsioTcpServer.cpp b/services/gateway/src/net/AsioTcpServer.cpp
index f669420..088f512 100644
--- a/services/gateway/src/net/AsioTcpServer.cpp
+++ b/services/gateway/src/net/AsioTcpServer.cpp
@@ -153,7 +153,9 @@ struct AsioWorker : std::enable_shared_from_this<AsioWorker>
 
     void
     add(std::uint64_t ticket,
-        const std::shared_ptr<AcceptedFd>& accepted)
+        const std::shared_ptr<AcceptedFd>& accepted,
+        InetAddress peer,
+        ConnectionContext::Clock::time_point acceptedAt)
     {
         if (cleaning || forceClosing)
         {
@@ -175,17 +177,15 @@ struct AsioWorker : std::enable_shared_from_this<AsioWorker>
                 std::move(socket),
                 idleTimeout
             );
-            auto handler = handlerFactory
-                               ? handlerFactory()
-                               : ConnectionMessageCallback(
-                                     [](IConnection& c, auto bytes)
-                                     { c.send(bytes); }
-                                 );
-            if (!handler)
-            {
-                throw std::invalid_argument("empty connection handler"
-                );
-            }
+            auto contextInfo = std::make_shared<ConnectionContext>(
+                connection->id(),
+                std::move(peer),
+                acceptedAt
+            );
+            auto handler = makeConnectionHandler(
+                handlerFactory,
+                std::move(contextInfo)
+            );
             connection->setMessageCallback(std::move(handler));
             connection->setHighWaterMarkCallback(
                 [](IConnection& c, auto) { c.pauseRead(); }
@@ -458,6 +458,8 @@ struct AsioTcpServer::Impl : std::enable_shared_from_this<Impl>
                     self->acceptError(error);
                     return;
                 }
+                const auto acceptedAt = ConnectionContext::Clock::now(
+                );
                 if (self->admissions.size() >= self->capacity)
                 {
                     std::cerr
@@ -467,6 +469,18 @@ struct AsioTcpServer::Impl : std::enable_shared_from_this<Impl>
                 }
                 else
                 {
+                    const auto endpoint = socket->remote_endpoint(
+                        error
+                    );
+                    if (error)
+                    {
+                        self->acceptError(error);
+                        return;
+                    }
+                    const InetAddress peer(
+                        endpoint.address().to_v4().to_string(),
+                        endpoint.port()
+                    );
                     const int flags = ::fcntl(
                         socket->native_handle(),
                         F_GETFD
@@ -507,8 +521,18 @@ struct AsioTcpServer::Impl : std::enable_shared_from_this<Impl>
                     try
                     {
                         worker->dispatch(
-                            [worker, accepted, ticket]
-                            { worker->add(ticket, accepted); }
+                            [worker,
+                             accepted,
+                             ticket,
+                             peer,
+                             acceptedAt] {
+                                worker->add(
+                                    ticket,
+                                    accepted,
+                                    peer,
+                                    acceptedAt
+                                );
+                            }
                         );
                     }
                     catch (...)
diff --git a/services/gateway/src/net/ConnectionContext.cpp b/services/gateway/src/net/ConnectionContext.cpp
new file mode 100644
index 0000000..2e6c93a
--- /dev/null
+++ b/services/gateway/src/net/ConnectionContext.cpp
@@ -0,0 +1,68 @@
+#include "net/ConnectionContext.h"
+
+#include <stdexcept>
+#include <utility>
+
+namespace service::gateway
+{
+ConnectionSession::ConnectionSession()
+    : m_owner(std::this_thread::get_id())
+{
+}
+
+void ConnectionSession::requireOwner() const
+{
+    if (std::this_thread::get_id() != m_owner)
+    {
+        throw std::logic_error(
+            "ConnectionSession requires connection owner"
+        );
+    }
+}
+
+ConnectionSession::LoginState ConnectionSession::loginState() const
+{
+    requireOwner();
+    return m_loginState;
+}
+
+bool ConnectionSession::markAuthenticated()
+{
+    requireOwner();
+    if (m_loginState != LoginState::Pending)
+    {
+        return false;
+    }
+    m_loginState = LoginState::Authenticated;
+    return true;
+}
+
+bool ConnectionSession::expireLogin()
+{
+    requireOwner();
+    if (m_loginState != LoginState::Pending)
+    {
+        return false;
+    }
+    m_loginState = LoginState::Expired;
+    return true;
+}
+
+ConnectionContext::ConnectionContext(
+    std::uint64_t id,
+    InetAddress peerAddress,
+    Clock::time_point accepted
+)
+    : connectionId(id)
+    , peer(std::move(peerAddress))
+    , acceptedAt(accepted)
+    , session(std::make_shared<ConnectionSession>())
+{
+    if (id == 0)
+    {
+        throw std::invalid_argument(
+            "connection context requires nonzero id"
+        );
+    }
+}
+} // namespace service::gateway
diff --git a/services/gateway/src/net/ConnectionHandler.cpp b/services/gateway/src/net/ConnectionHandler.cpp
new file mode 100644
index 0000000..fe5722a
--- /dev/null
+++ b/services/gateway/src/net/ConnectionHandler.cpp
@@ -0,0 +1,33 @@
+#include "net/ConnectionHandler.h"
+
+#include <stdexcept>
+#include <utility>
+
+namespace service::gateway
+{
+ConnectionMessageCallback makeConnectionHandler(
+    const ConnectionHandlerFactory& factory,
+    ConnectionContext::Ptr context
+)
+{
+    if (!context)
+    {
+        throw std::invalid_argument("missing connection context");
+    }
+    auto handler = factory
+                       ? factory(context)
+                       : ConnectionMessageCallback(
+                             [](IConnection& connection, auto bytes)
+                             { connection.send(bytes); }
+                         );
+    if (!handler)
+    {
+        throw std::invalid_argument("empty connection handler");
+    }
+    return
+        [context = std::move(context), handler = std::move(handler)](
+            IConnection& connection,
+            std::span<const std::byte> bytes
+        ) { handler(connection, bytes); };
+}
+} // namespace service::gateway
diff --git a/services/gateway/src/net/TcpServer.cpp b/services/gateway/src/net/TcpServer.cpp
index 0d80cdd..6e49c1c 100644
--- a/services/gateway/src/net/TcpServer.cpp
+++ b/services/gateway/src/net/TcpServer.cpp
@@ -55,7 +55,12 @@ struct TcpServer::Worker : std::enable_shared_from_this<Worker>
     {
     }
 
-    void addConnection(Id id, Socket socket)
+    void addConnection(
+        Id id,
+        Socket socket,
+        InetAddress peer,
+        ConnectionContext::Clock::time_point acceptedAt
+    )
     {
         std::shared_ptr<TcpConnection> connection;
         try
@@ -79,17 +84,15 @@ struct TcpServer::Worker : std::enable_shared_from_this<Worker>
                 std::move(socket)
             );
             connection->channel().tie(connection);
-            auto handler = handlerFactory
-                               ? handlerFactory()
-                               : ConnectionMessageCallback(
-                                     [](IConnection& conn, auto bytes)
-                                     { conn.send(bytes); }
-                                 );
-            if (!handler)
-            {
-                throw std::invalid_argument("empty connection handler"
-                );
-            }
+            auto context = std::make_shared<ConnectionContext>(
+                connection->id(),
+                std::move(peer),
+                acceptedAt
+            );
+            auto handler = makeConnectionHandler(
+                handlerFactory,
+                std::move(context)
+            );
             connection->setMessageCallback(
                 [handler = std::move(handler
                  )](TcpConnection& conn, std::string_view bytes)
@@ -521,6 +524,7 @@ void TcpServer::handleNewConnection(
     Socket&& socket, const InetAddress& peer
 )
 {
+    const auto acceptedAt = ConnectionContext::Clock::now();
     if (m_state != TcpServerState::Running)
     {
         return;
@@ -556,8 +560,15 @@ void TcpServer::handleNewConnection(
     try
     {
         owner->runInLoop(
-            [selected, id, accepted]()
-            { selected->addConnection(id, std::move(*accepted)); }
+            [selected, id, accepted, peer, acceptedAt]()
+            {
+                selected->addConnection(
+                    id,
+                    std::move(*accepted),
+                    peer,
+                    acceptedAt
+                );
+            }
         );
     }
     catch (...)
diff --git a/services/gateway/src/net/WebSocketServer.cpp b/services/gateway/src/net/WebSocketServer.cpp
index 3d0d41e..9d7ea8c 100644
--- a/services/gateway/src/net/WebSocketServer.cpp
+++ b/services/gateway/src/net/WebSocketServer.cpp
@@ -357,8 +357,8 @@ private:
     void writeNext()
     {
         const auto state = m_state.load();
-        if ((state != State::Open &&
-             state != State::ClosePendingWrite) ||
+        if ((state != State::Open && state != State::ClosePendingWrite
+            ) ||
             m_writing)
             return;
         if (state == State::ClosePendingWrite)
@@ -409,7 +409,8 @@ private:
     void startClose(websocket::close_code code, std::string reason)
     {
         const auto state = m_state.load();
-        if (state == State::Closed || state == State::ClosePendingWrite ||
+        if (state == State::Closed ||
+            state == State::ClosePendingWrite ||
             state == State::CloseHandshake)
             return;
         if (state != State::Open)
@@ -481,8 +482,9 @@ private:
     std::deque<std::shared_ptr<std::string>> m_output;
     std::size_t m_pendingBytes{0};
     bool m_acceptingSends{true};
-    // The connection owner changes the lifecycle; connected() may read it
-    // from another thread. Read/write activity is orthogonal to this state.
+    // The connection owner changes the lifecycle; connected() may
+    // read it from another thread. Read/write activity is orthogonal
+    // to this state.
     std::atomic<State> m_state{State::ReadingHttp};
     bool m_reading{false};
     bool m_readPaused{false};
@@ -545,6 +547,8 @@ struct WebSocketServer::Impl : std::enable_shared_from_this<Impl>
             {
                 if (!error && !self->stopping)
                 {
+                    const auto
+                        acceptedAt = ConnectionContext::Clock::now();
                     if (self->connections.size() >= self->capacity)
                     {
                         Error ignored;
@@ -556,11 +560,23 @@ struct WebSocketServer::Impl : std::enable_shared_from_this<Impl>
                         {
                             const auto id = detail::nextConnectionId(
                             );
-                            auto handler = self->factory();
-                            if (!handler)
-                                throw std::invalid_argument(
-                                    "empty handler"
-                                );
+                            const auto
+                                endpoint = socket.remote_endpoint();
+                            auto contextInfo = std::make_shared<
+                                ConnectionContext>(
+                                id,
+                                InetAddress(
+                                    endpoint.address()
+                                        .to_v4()
+                                        .to_string(),
+                                    endpoint.port()
+                                ),
+                                acceptedAt
+                            );
+                            auto handler = makeConnectionHandler(
+                                self->factory,
+                                std::move(contextInfo)
+                            );
                             auto session = std::make_shared<
                                 WebSocketConnection>(
                                 self->context,
diff --git a/tests/gateway/net/AsioTcpServerTestHost.cpp b/tests/gateway/net/AsioTcpServerTestHost.cpp
index ecbd25e..9019219 100644
--- a/tests/gateway/net/AsioTcpServerTestHost.cpp
+++ b/tests/gateway/net/AsioTcpServerTestHost.cpp
@@ -10,6 +10,8 @@
 #include "net/AsioTcpServer.h"
 #include "net/PacketEcho.h"
 
+#include "ConnectionContextTestHandler.h"
+
 int main(int argc, char** argv)
 {
     if (argc < 2 || argc > 5)
@@ -19,12 +21,18 @@ int main(int argc, char** argv)
     service::gateway::ConnectionHandlerFactory handlerFactory;
     if (argc == 5)
     {
-        if (std::string_view(argv[4]) != "packet")
+        if (std::string_view(argv[4]) == "context")
+        {
+            handlerFactory = makeContextTestFactory();
+        }
+        else if (std::string_view(argv[4]) != "packet")
         {
             return 2;
         }
-        handlerFactory = []
-        { return service::gateway::makePacketEchoHandler(); };
+        if (!handlerFactory)
+            handlerFactory =
+                [](service::gateway::ConnectionContext::Ptr)
+            { return service::gateway::makePacketEchoHandler(); };
     }
 
     using namespace service::gateway;
diff --git a/tests/gateway/net/CMakeLists.txt b/tests/gateway/net/CMakeLists.txt
index 2f6664b..1ccc2d8 100644
--- a/tests/gateway/net/CMakeLists.txt
+++ b/tests/gateway/net/CMakeLists.txt
@@ -172,3 +172,26 @@ set_tests_properties(WebSocketTransport_HandshakeLimits PROPERTIES
     TIMEOUT 30
     RUN_SERIAL TRUE
 )
+
+# Admission metadata crosses the real owner handoff in each backend.
+foreach(backend epoll asio)
+    if(backend STREQUAL "epoll")
+        set(context_host arena_tcp_server_test_host)
+    else()
+        set(context_host arena_asio_tcp_server_test_host)
+    endif()
+    foreach(workers 0 4)
+        add_test(NAME ConnectionContextTransport_${backend}_${workers}
+            COMMAND "${Python3_EXECUTABLE}"
+                "${CMAKE_CURRENT_SOURCE_DIR}/ConnectionContextIntegrationTest.py"
+                "$<TARGET_FILE:${context_host}>" tcp ${workers})
+        set_tests_properties(ConnectionContextTransport_${backend}_${workers}
+            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;context")
+    endforeach()
+endforeach()
+add_test(NAME ConnectionContextTransport_ws
+    COMMAND "${Python3_EXECUTABLE}"
+        "${CMAKE_CURRENT_SOURCE_DIR}/ConnectionContextIntegrationTest.py"
+        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
+set_tests_properties(ConnectionContextTransport_ws PROPERTIES
+    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;context;websocket")
diff --git a/tests/gateway/net/ConnectionContextIntegrationTest.py b/tests/gateway/net/ConnectionContextIntegrationTest.py
new file mode 100644
index 0000000..7e80694
--- /dev/null
+++ b/tests/gateway/net/ConnectionContextIntegrationTest.py
@@ -0,0 +1,97 @@
+"""Verify context metadata, owner affinity and session isolation on real sockets."""
+import os
+import socket
+import sys
+import time
+
+sys.dont_write_bytecode = True
+from TcpServerIntegrationTest import reserve_port
+from WebSocketIntegrationTest import run_host, stop_host, upgrade, send_frame, read_frame
+import signal
+import subprocess
+import tempfile
+
+
+def parse(response, peer, worker):
+    fields = response.decode().strip().split("|")
+    assert len(fields) == 6, fields
+    identity, ip, port, age, foreign, state = fields
+    assert int(identity) > 0 and ip == peer.getsockname()[0], fields
+    assert int(port) == peer.getsockname()[1], fields
+    assert int(age) >= 0 and int(foreign) == worker, fields
+    return int(identity), int(state)
+
+
+def command(peer, value, ws, worker):
+    if ws:
+        send_frame(peer, 2, value)
+        opcode, response = read_frame(peer)
+        assert opcode == 2
+    else:
+        peer.sendall(value)
+        response = bytearray()
+        while not response.endswith(b"\n"):
+            chunk = peer.recv(1024)
+            assert chunk, "unexpected EOF"
+            response.extend(chunk)
+    return parse(response, peer, worker)
+
+
+def check(port, ws, worker):
+    def connect():
+        if ws:
+            peer, response = upgrade(port)
+            assert response.startswith(b"HTTP/1.1 101")
+            return peer
+        peer = socket.create_connection(("127.0.0.1", port), 5)
+        peer.settimeout(5)
+        return peer
+    with connect() as first, connect() as second:
+        first_id, state = command(first, b"Q", ws, worker)
+        assert state == 0
+        second_id, state = command(second, b"Q", ws, worker)
+        assert state == 0 and second_id != first_id
+        assert command(first, b"A", ws, worker) == (first_id, 1)
+        assert command(second, b"Q", ws, worker) == (second_id, 0)
+        assert command(second, b"E", ws, worker) == (second_id, 2)
+        assert command(second, b"A", ws, worker) == (second_id, 2)
+        assert command(first, b"E", ws, worker) == (first_id, 1)
+
+
+def run(binary, mode, workers):
+    if mode == "ws":
+        process, log, content, tcp_port, ws_port = run_host(binary, "context")
+        try:
+            check(tcp_port, False, 0)
+            check(ws_port, True, 0)
+        finally:
+            stop_host(process, log)
+            os.unlink(log.name)
+    else:
+        port = reserve_port()
+        log = tempfile.NamedTemporaryFile(prefix="arena-context-", suffix=".log", delete=False)
+        process = subprocess.Popen([binary, str(port), "60000", str(workers), "context"],
+                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
+        passed = False
+        try:
+            deadline = time.monotonic() + 8
+            while True:
+                with open(log.name, errors="replace") as stream:
+                    output = stream.read()
+                if "TEST_HOST_READY" in output:
+                    break
+                assert process.poll() is None and time.monotonic() < deadline, output
+                time.sleep(.02)
+            check(port, False, int(workers != 0))
+            passed = True
+        finally:
+            stop_host(process, log)
+            if passed:
+                os.unlink(log.name)
+    print("connection context peer, owner and session isolation passed")
+
+
+if __name__ == "__main__":
+    if len(sys.argv) != 4:
+        raise SystemExit("usage: ConnectionContextIntegrationTest.py HOST tcp|ws WORKERS")
+    run(sys.argv[1], sys.argv[2], int(sys.argv[3]))
diff --git a/tests/gateway/net/ConnectionContextTest.cpp b/tests/gateway/net/ConnectionContextTest.cpp
new file mode 100644
index 0000000..54bd33b
--- /dev/null
+++ b/tests/gateway/net/ConnectionContextTest.cpp
@@ -0,0 +1,178 @@
+#include <atomic>
+#include <chrono>
+#include <memory>
+#include <stdexcept>
+#include <thread>
+
+#include <gtest/gtest.h>
+
+#include "net/ConnectionHandler.h"
+
+using namespace service::gateway;
+
+TEST(
+    ConnectionContextTest,
+    PreservesAdmissionMetadataAndIsolatesSessions
+)
+{
+    const auto accepted = ConnectionContext::Clock::now() -
+                          std::chrono::seconds(2);
+    const auto first = std::make_shared<ConnectionContext>(
+        42,
+        InetAddress("127.0.0.2", 3456),
+        accepted
+    );
+    const auto second = std::make_shared<ConnectionContext>(
+        43,
+        InetAddress("127.0.0.2", 3457),
+        accepted
+    );
+    EXPECT_EQ(first->connectionId, 42u);
+    EXPECT_EQ(first->peer.ip(), "127.0.0.2");
+    EXPECT_EQ(first->peer.port(), 3456);
+    EXPECT_EQ(first->acceptedAt, accepted);
+    EXPECT_NE(first->session, second->session);
+    EXPECT_TRUE(first->session->markAuthenticated());
+    EXPECT_EQ(
+        second->session->loginState(),
+        ConnectionSession::LoginState::Pending
+    );
+    EXPECT_THROW(
+        (ConnectionContext(
+            0,
+            InetAddress("127.0.0.1", 1234),
+            accepted
+        )),
+        std::invalid_argument
+    );
+}
+
+TEST(
+    ConnectionContextTest,
+    LoginTransitionsAreTerminalAndMutuallyExclusive
+)
+{
+    ConnectionSession authenticated;
+    EXPECT_EQ(
+        authenticated.loginState(),
+        ConnectionSession::LoginState::Pending
+    );
+    EXPECT_TRUE(authenticated.markAuthenticated());
+    EXPECT_FALSE(authenticated.markAuthenticated());
+    EXPECT_FALSE(authenticated.expireLogin());
+    EXPECT_EQ(
+        authenticated.loginState(),
+        ConnectionSession::LoginState::Authenticated
+    );
+    ConnectionSession expired;
+    EXPECT_TRUE(expired.expireLogin());
+    EXPECT_FALSE(expired.expireLogin());
+    EXPECT_FALSE(expired.markAuthenticated());
+    EXPECT_EQ(
+        expired.loginState(),
+        ConnectionSession::LoginState::Expired
+    );
+}
+
+TEST(ConnectionContextTest, SessionAccessRequiresItsOwner)
+{
+    ConnectionSession session;
+    int rejected = 0;
+    std::thread foreign(
+        [&]
+        {
+            try
+            {
+                (void)session.loginState();
+            }
+            catch (const std::logic_error&)
+            {
+                ++rejected;
+            }
+            try
+            {
+                (void)session.markAuthenticated();
+            }
+            catch (const std::logic_error&)
+            {
+                ++rejected;
+            }
+            try
+            {
+                (void)session.expireLogin();
+            }
+            catch (const std::logic_error&)
+            {
+                ++rejected;
+            }
+        }
+    );
+    foreign.join();
+    EXPECT_EQ(rejected, 3);
+    EXPECT_EQ(
+        session.loginState(),
+        ConnectionSession::LoginState::Pending
+    );
+}
+
+TEST(
+    ConnectionContextTest,
+    CallbackCopiesRetainIgnoredContextUntilFinalRelease
+)
+{
+    auto context = std::make_shared<ConnectionContext>(
+        1,
+        InetAddress("127.0.0.1", 1234),
+        ConnectionContext::Clock::now()
+    );
+    std::weak_ptr<const ConnectionContext> observed = context;
+    bool invoked = false;
+    const auto owner = std::this_thread::get_id();
+    ConnectionHandlerFactory factory =
+        [&](ConnectionContext::Ptr info)
+    {
+        invoked = true;
+        EXPECT_EQ(std::this_thread::get_id(), owner);
+        EXPECT_EQ(info.get(), context.get());
+        return [](IConnection&, std::span<const std::byte>) {};
+    };
+    auto callback = makeConnectionHandler(factory, context);
+    auto copy = callback;
+    EXPECT_TRUE(invoked);
+    context.reset();
+    EXPECT_FALSE(observed.expired());
+    callback = {};
+    EXPECT_FALSE(observed.expired());
+    copy = {};
+    EXPECT_TRUE(observed.expired());
+}
+
+TEST(
+    ConnectionContextTest,
+    RejectsMissingContextAndInvalidFactoryResult
+)
+{
+    EXPECT_THROW(
+        makeConnectionHandler({}, {}),
+        std::invalid_argument
+    );
+    auto context = std::make_shared<ConnectionContext>(
+        1,
+        InetAddress("127.0.0.1", 1234),
+        ConnectionContext::Clock::now()
+    );
+    ConnectionHandlerFactory invalid = [](ConnectionContext::Ptr)
+    { return ConnectionMessageCallback{}; };
+    EXPECT_THROW(
+        makeConnectionHandler(invalid, context),
+        std::invalid_argument
+    );
+    ConnectionHandlerFactory throwing = [](ConnectionContext::Ptr
+                                        ) -> ConnectionMessageCallback
+    { throw std::runtime_error("factory failed"); };
+    EXPECT_THROW(
+        makeConnectionHandler(throwing, context),
+        std::runtime_error
+    );
+    EXPECT_TRUE(makeConnectionHandler({}, context));
+}
diff --git a/tests/gateway/net/ConnectionContextTestHandler.h b/tests/gateway/net/ConnectionContextTestHandler.h
new file mode 100644
index 0000000..0cf988c
--- /dev/null
+++ b/tests/gateway/net/ConnectionContextTestHandler.h
@@ -0,0 +1,61 @@
+#pragma once
+
+#include <chrono>
+#include <sstream>
+#include <stdexcept>
+#include <thread>
+#include <utility>
+
+#include "net/ConnectionHandler.h"
+
+// Test-only raw command handler. Report actual socket metadata and
+// exercise session state on the same owner on which the factory was
+// invoked.
+inline service::gateway::ConnectionHandlerFactory
+makeContextTestFactory()
+{
+    using namespace service::gateway;
+    const auto baseOwner = std::this_thread::get_id();
+    return [baseOwner](ConnectionContext::Ptr context)
+    {
+        const auto owner = std::this_thread::get_id();
+        if (context->session->loginState() !=
+            ConnectionSession::LoginState::Pending)
+            throw std::runtime_error(
+                "new connection session is not pending"
+            );
+        return [context = std::move(context), owner, baseOwner](
+                   IConnection& connection,
+                   std::span<const std::byte> bytes
+               )
+        {
+            if (owner != std::this_thread::get_id() ||
+                connection.id() != context->connectionId)
+                throw std::runtime_error(
+                    "factory/callback owner or identity mismatch"
+                );
+            if (bytes.size() != 1)
+                throw std::runtime_error("expected one test command");
+            const auto command = std::to_integer<char>(bytes.front());
+            if (command == 'A')
+                context->session->markAuthenticated();
+            if (command == 'E')
+                context->session->expireLogin();
+            const auto age = std::chrono::duration_cast<
+                                 std::chrono::milliseconds>(
+                                 ConnectionContext::Clock::now() -
+                                 context->acceptedAt
+            )
+                                 .count();
+            std::ostringstream output;
+            output << context->connectionId << '|'
+                   << context->peer.ip() << '|'
+                   << context->peer.port() << '|' << age << '|'
+                   << (owner != baseOwner) << '|'
+                   << static_cast<int>(context->session->loginState())
+                   << '\n';
+            const auto response = output.str();
+            connection.send(std::as_bytes(std::span(response)));
+        };
+    };
+}
diff --git a/tests/gateway/net/TcpServerTestHost.cpp b/tests/gateway/net/TcpServerTestHost.cpp
index bdb6bf3..2515864 100644
--- a/tests/gateway/net/TcpServerTestHost.cpp
+++ b/tests/gateway/net/TcpServerTestHost.cpp
@@ -17,6 +17,8 @@
 #include "net/TcpServer.h"
 #include "os/FileDescriptor.h"
 
+#include "ConnectionContextTestHandler.h"
+
 // A separate process lets the integration test inspect /proc/<pid>/fd
 // and stop the event loop cleanly, including under LeakSanitizer.
 int main(int argc, char** argv)
@@ -28,12 +30,18 @@ int main(int argc, char** argv)
     service::gateway::ConnectionHandlerFactory handlerFactory;
     if (argc == 5)
     {
-        if (std::string_view(argv[4]) != "packet")
+        if (std::string_view(argv[4]) == "context")
+        {
+            handlerFactory = makeContextTestFactory();
+        }
+        else if (std::string_view(argv[4]) != "packet")
         {
             return 2;
         }
-        handlerFactory = []
-        { return service::gateway::makePacketEchoHandler(); };
+        if (!handlerFactory)
+            handlerFactory =
+                [](service::gateway::ConnectionContext::Ptr)
+            { return service::gateway::makePacketEchoHandler(); };
     }
 
     sigset_t signals;
diff --git a/tests/gateway/net/WebSocketTestHost.cpp b/tests/gateway/net/WebSocketTestHost.cpp
index 278f95d..453d010 100644
--- a/tests/gateway/net/WebSocketTestHost.cpp
+++ b/tests/gateway/net/WebSocketTestHost.cpp
@@ -16,6 +16,8 @@
 #include "net/PreLoginHandshake.h"
 #include "net/WebSocketServer.h"
 
+#include "ConnectionContextTestHandler.h"
+
 using namespace service::gateway;
 
 int main(int argc, char** argv)
@@ -29,8 +31,10 @@ int main(int argc, char** argv)
     const std::string origin = argv[3];
     const std::string mode = argv[4];
     boost::asio::io_context context;
-    ConnectionHandlerFactory handshake = []
+    ConnectionHandlerFactory handshake = [](ConnectionContext::Ptr)
     { return makePreLoginHandshakeHandler(); };
+    if (mode == "context")
+        handshake = makeContextTestFactory();
     AsioTcpServer tcp(
         context,
         InetAddress("127.0.0.1", tcpPort),
@@ -42,7 +46,7 @@ int main(int argc, char** argv)
     ConnectionHandlerFactory wsFactory = handshake;
     if (mode == "flood")
     {
-        wsFactory = []
+        wsFactory = [](ConnectionContext::Ptr)
         {
             return [](IConnection& connection,
                       std::span<const std::byte>)
~~~
